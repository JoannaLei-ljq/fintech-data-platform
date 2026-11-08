"""Step 4: the payment event write path (record_payment_event, roles, commit-time checks)."""

from __future__ import annotations

import os
import queue
import random
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import psycopg
import psycopg.sql
import pytest

from payments.events import (EventIdConflict, IllegalTransition, InvalidEvent, PaymentEvent,
                             SequenceConflict, SequenceGap, UnknownPayment, record_payment_event)
from sync.incremental_sync import conninfo
from tests.v2.conftest import connect
from tests.v2.test_lifecycle import build

UTC = timezone.utc
T0 = datetime(2026, 10, 20, 9, 0, tzinfo=UTC)
APP_ROLE = "v2_payment_app"
WRITER_ROLE = "v2_payment_writer"


@pytest.fixture
def db(make_db) -> str:
    return build(make_db, "write")  # V001 + MVP demo seed + every migration


def app_conn(dbname: str, autocommit: bool = True) -> psycopg.Connection:
    """A connection acting as the application role (no superuser rights after SET ROLE)."""
    conn = connect(dbname)
    conn.execute(f"SET ROLE {APP_ROLE}")
    conn.autocommit = autocommit
    return conn


def created(tid: int, **kw) -> PaymentEvent:
    base = dict(event_id=uuid.uuid4(), transaction_id=tid, event_sequence=1, event_type="payment.created",
                occurred_at=T0, source="api", amount_minor=2500, currency="USD", user_id=1, merchant_id=101)
    return PaymentEvent(**{**base, **kw})


def step(tid: int, seq: int, kind: str, **kw) -> PaymentEvent:
    base = dict(event_id=uuid.uuid4(), transaction_id=tid, event_sequence=seq, event_type=f"payment.{kind}",
                occurred_at=T0 + timedelta(minutes=seq), source="processor")
    if kind == "failed":
        base["failure_code"] = "card_declined"
    else:
        base.update(amount_minor=2500, currency="USD")
    return PaymentEvent(**{**base, **kw})


def snapshot(dbname: str) -> tuple[list, list]:
    with connect(dbname) as conn:
        return (conn.execute("SELECT * FROM transactions ORDER BY transaction_id").fetchall(),
                conn.execute("SELECT * FROM payment_events ORDER BY transaction_id, event_sequence").fetchall())


def state(dbname: str, tid: int) -> tuple:
    with connect(dbname) as conn:
        return conn.execute(
            "SELECT status, last_event_sequence, failure_code, authorized_at, captured_at, failed_at "
            "FROM transactions WHERE transaction_id = %s", (tid,)).fetchone()


def history(dbname: str, tid: int) -> list[tuple]:
    with connect(dbname) as conn:
        return conn.execute("SELECT event_sequence, event_type FROM payment_events WHERE transaction_id = %s "
                            "ORDER BY event_sequence", (tid,)).fetchall()


def submit(dbname: str, event: PaymentEvent):
    """Record one event on its own connection; returns the outcome or the exception class."""
    with app_conn(dbname) as conn:
        try:
            return record_payment_event(conn, event).outcome
        except Exception as exc:  # noqa: BLE001 - the class is the result under test
            return type(exc)


def race(dbname: str, events: list[PaymentEvent]) -> list:
    """Submit all events at once from separate connections, released by a barrier."""
    barrier = threading.Barrier(len(events))
    conns = [app_conn(dbname) for _ in events]

    def go(i: int):
        barrier.wait()
        try:
            return record_payment_event(conns[i], events[i]).outcome
        except Exception as exc:  # noqa: BLE001
            return type(exc)

    try:
        with ThreadPoolExecutor(len(events)) as pool:
            return list(pool.map(go, range(len(events))))
    finally:
        for c in conns:
            c.close()


# --- valid lifecycles ---------------------------------------------------------------

def test_valid_lifecycle_paths(db):
    paths = {6001: ["authorized", "captured"], 6002: ["failed"], 6003: ["authorized", "failed"]}
    with app_conn(db) as conn:
        for tid, steps in paths.items():
            assert record_payment_event(conn, created(tid)).outcome == "recorded"
            for seq, kind in enumerate(steps, start=2):
                result = record_payment_event(conn, step(tid, seq, kind))
                assert (result.outcome, result.transaction_status, result.last_event_sequence) == \
                       ("recorded", kind, seq)
    t = lambda m: T0 + timedelta(minutes=m)  # noqa: E731
    assert state(db, 6001) == ("captured", 3, None, t(2), t(3), None)
    assert state(db, 6002) == ("failed", 2, "card_declined", None, None, t(2))
    assert state(db, 6003) == ("failed", 3, "card_declined", t(2), None, t(3))
    assert [k for _, k in history(db, 6003)] == ["payment.created", "payment.authorized", "payment.failed"]
    with connect(db) as conn:  # the new payment's own fields come from its created event
        assert conn.execute("SELECT user_id, merchant_id, amount_minor, currency, created_at FROM transactions "
                            "WHERE transaction_id = 6001").fetchone() == (1, 101, 2500, "USD", T0)


def test_backfilled_payment_continues_from_its_history(db):
    # 3004 was MVP "pending", migrated to created (sequence 1); its next event is sequence 2.
    with app_conn(db) as conn:
        auth = step(3004, 2, "authorized", amount_minor=2000, occurred_at=datetime(2026, 10, 15, 15, 5, tzinfo=UTC))
        cap = replace(auth, event_id=uuid.uuid4(), event_sequence=3, event_type="payment.captured",
                      occurred_at=auth.occurred_at + timedelta(seconds=30))
        record_payment_event(conn, auth)
        record_payment_event(conn, cap)
    assert state(db, 3004)[:2] == ("captured", 3)
    with connect(db) as conn:  # the legacy view now reports what the MVP called succeeded
        assert conn.execute("SELECT status FROM legacy_transactions WHERE transaction_id = 3004").fetchone() == \
               ("succeeded",)


# --- idempotency and event ID conflicts -----------------------------------------------

def test_identical_replay_is_a_no_op(db):
    events = [created(6001), step(6001, 2, "authorized"), step(6001, 3, "captured")]
    with app_conn(db) as conn:
        first = [record_payment_event(conn, e) for e in events]
        before = snapshot(db)
        for e, original in zip(events * 2, first * 2):  # replay all of them, twice, out of order too
            again = record_payment_event(conn, e)
            assert again.outcome == "duplicate"
            assert again.received_at == original.received_at       # the stored event, not a new one
            assert (again.transaction_status, again.last_event_sequence) == ("captured", 3)
    assert snapshot(db) == before


@pytest.mark.parametrize("change", [
    {"amount_minor": 2501}, {"currency": "EUR"}, {"source": "api"},
    {"occurred_at": T0 + timedelta(minutes=9)}, {"event_sequence": 3},
    {"event_type": "payment.captured"}, {"transaction_id": 3004},
    {"event_type": "payment.failed", "amount_minor": None, "currency": None, "failure_code": "x"},
])
def test_conflicting_payload_for_an_existing_event_id_is_rejected(db, change):
    original = step(6001, 2, "authorized")
    with app_conn(db) as conn:
        record_payment_event(conn, created(6001))
        record_payment_event(conn, original)
        before = snapshot(db)
        with pytest.raises(EventIdConflict):
            record_payment_event(conn, replace(original, **change))
    assert snapshot(db) == before


def test_conflicting_created_replay_is_rejected(db):
    first = created(6001)
    with app_conn(db) as conn:
        record_payment_event(conn, first)
        before = snapshot(db)
        for change in ({"user_id": 2}, {"merchant_id": 102}, {"amount_minor": 1}):
            with pytest.raises(EventIdConflict):
                record_payment_event(conn, replace(first, **change))
    assert snapshot(db) == before


# --- sequences and transitions --------------------------------------------------------

def test_sequence_gap_is_rejected_and_retryable(db):
    before = snapshot(db)
    with app_conn(db) as conn:
        with pytest.raises(SequenceGap) as gap:
            record_payment_event(conn, step(3004, 3, "captured", amount_minor=2000))
        assert gap.value.retryable
        assert snapshot(db) == before
        late = step(3004, 3, "captured", amount_minor=2000)
        record_payment_event(conn, step(3004, 2, "authorized", amount_minor=2000))
        assert record_payment_event(conn, late).outcome == "recorded"  # the retry now fits
    assert [s for s, _ in history(db, 3004)] == [1, 2, 3]


def test_taken_sequence_and_unknown_payment_are_rejected(db):
    with app_conn(db) as conn:
        record_payment_event(conn, step(3004, 2, "authorized", amount_minor=2000))
        before = snapshot(db)
        with pytest.raises(SequenceConflict):  # a different event for sequence 2
            record_payment_event(conn, step(3004, 2, "failed"))
        with pytest.raises(SequenceConflict):  # a second created event for an existing payment
            record_payment_event(conn, created(3004, amount_minor=2000))
        with pytest.raises(UnknownPayment):
            record_payment_event(conn, step(9999, 2, "authorized"))
    assert snapshot(db) == before


@pytest.mark.parametrize("tid,seq,kind", [
    (3004, 2, "captured"),     # created -> captured (skips authorization)
    (3001, 4, "authorized"),   # captured is terminal
    (3001, 4, "captured"),
    (3001, 4, "failed"),
    (3003, 3, "authorized"),   # failed is terminal
    (3003, 3, "failed"),
    (6001, 3, "authorized"),   # authorized -> authorized
])
def test_illegal_transitions_are_rejected(db, tid, seq, kind):
    amounts = {3001: 10000, 3003: 3000, 3004: 2000, 6001: 2500}
    with app_conn(db) as conn:
        record_payment_event(conn, created(6001))
        record_payment_event(conn, step(6001, 2, "authorized"))
        before = snapshot(db)
        event = step(tid, seq, kind)
        if kind != "failed":
            event = replace(event, amount_minor=amounts[tid])
        with pytest.raises(IllegalTransition):
            record_payment_event(conn, event)
    assert snapshot(db) == before


@pytest.mark.parametrize("event", [
    step(3004, 2, "authorized", amount_minor=1999),                       # not the original amount
    step(3004, 2, "authorized", amount_minor=2000, currency="EUR"),       # not the original currency
    step(3004, 2, "authorized", amount_minor=2000, occurred_at=datetime(2026, 10, 1, tzinfo=UTC)),  # before created
    step(3004, 2, "authorized", amount_minor=2000, source="migration_backfill"),  # reserved for migrations
    step(3004, 2, "failed", failure_code=" "),
    step(3004, 2, "failed", amount_minor=2000, currency="USD"),
    step(3004, 2, "refunded"),
    created(6001, user_id=None),
    created(6001, currency="XYZ"),
    created(6001, amount_minor=0),
    created(6001, event_sequence=2),
], ids=lambda e: f"{e.transaction_id}-{e.event_type}-{e.event_sequence}")
def test_invalid_payloads_are_rejected(db, event):
    before = snapshot(db)
    with app_conn(db) as conn, pytest.raises(InvalidEvent):
        record_payment_event(conn, event)
    assert snapshot(db) == before


# --- no partial writes ------------------------------------------------------------------

def test_rejected_create_rolls_back_the_inserted_payment_row(db):
    with connect(db) as conn:
        taken = conn.execute("SELECT event_id FROM payment_events WHERE transaction_id = 3001 "
                             "AND event_sequence = 1").fetchone()[0]
    before = snapshot(db)
    with app_conn(db) as conn:
        # The function inserts transaction 6001 first, then finds the event_id belongs to 3001.
        with pytest.raises(EventIdConflict):
            record_payment_event(conn, created(6001, event_id=taken))
        # The row insert fails on its foreign key; nothing of the payment may remain.
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            record_payment_event(conn, created(6002, user_id=999))
    assert snapshot(db) == before


def test_caller_transaction_is_all_or_nothing(db):
    before = snapshot(db)
    with app_conn(db, autocommit=False) as conn:
        record_payment_event(conn, created(6001))
        record_payment_event(conn, step(6001, 2, "authorized"))
        with pytest.raises(SequenceGap):  # rejected inside the open transaction (savepoint)
            record_payment_event(conn, step(6001, 4, "captured"))
        conn.rollback()                   # the caller abandons the whole batch
    assert snapshot(db) == before
    with app_conn(db, autocommit=False) as conn:
        record_payment_event(conn, created(6001))
        record_payment_event(conn, step(6001, 2, "authorized"))
        conn.commit()
    assert state(db, 6001)[:2] == ("authorized", 2)


# --- concurrency ------------------------------------------------------------------------

def test_concurrent_identical_submissions_record_once(db):
    event = step(3004, 2, "authorized", amount_minor=2000)
    results = race(db, [event] * 8)
    assert Counter(results) == {"recorded": 1, "duplicate": 7}
    assert history(db, 3004) == [(1, "payment.created"), (2, "payment.authorized")]


def test_concurrent_competing_events_for_one_sequence(db):
    events = [step(3004, 2, "authorized", amount_minor=2000) if i % 2 else step(3004, 2, "failed")
              for i in range(8)]
    results = race(db, events)
    assert Counter(results) == {"recorded": 1, SequenceConflict: 7}
    winner = events[results.index("recorded")]
    assert history(db, 3004)[-1] == (2, winner.event_type)
    assert state(db, 3004)[:2] == (winner.event_type.removeprefix("payment."), 2)


def test_concurrent_creation_of_the_same_payment(db):
    event = created(6001)
    assert Counter(race(db, [event] * 6)) == {"recorded": 1, "duplicate": 5}
    other_ids = [created(6002) for _ in range(6)]  # same payment, different event IDs
    assert Counter(race(db, other_ids)) == {"recorded": 1, SequenceConflict: 5}
    assert history(db, 6001) == history(db, 6002) == [(1, "payment.created")]


def test_concurrent_reuse_of_an_event_id_across_payments(db):
    shared = uuid.uuid4()
    results = race(db, [created(6001, event_id=shared), created(6002, event_id=shared)])
    assert sorted(results, key=str) == sorted(["recorded", EventIdConflict], key=str)
    with connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM transactions WHERE transaction_id IN (6001, 6002)"
                            ).fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM payment_events WHERE event_id = %s", (shared,)).fetchone()[0] == 1


def wait_until_blocked(dbname: str, pid: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    with connect(dbname) as conn:
        while time.monotonic() < deadline:
            if conn.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s",
                            (pid,)).fetchone() == ("Lock",):
                return
            time.sleep(0.02)
    raise AssertionError("the second writer never waited on the payment row lock")


@pytest.mark.parametrize("first_commits,expected", [(True, "duplicate"), (False, "recorded")])
def test_second_writer_waits_for_the_row_lock(db, first_commits, expected):
    event = step(3004, 2, "authorized", amount_minor=2000)
    first, second = app_conn(db, autocommit=False), app_conn(db)
    try:
        record_payment_event(first, event)                    # holds the lock on payment 3004
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(record_payment_event, second, event)
            wait_until_blocked(db, second.info.backend_pid)
            first.commit() if first_commits else first.rollback()
            assert pending.result(timeout=10).outcome == expected
    finally:
        first.close()
        second.close()
    assert history(db, 3004) == [(1, "payment.created"), (2, "payment.authorized")]


def test_concurrent_out_of_order_producers_converge(db):
    """Many payments, events submitted in random order by parallel workers that retry gaps."""
    payments = range(7001, 7021)
    events = []
    for tid in payments:
        events += [created(tid), step(tid, 2, "authorized"),
                   step(tid, 3, "captured") if tid % 3 else step(tid, 3, "failed")]
    random.Random(42).shuffle(events)

    work: queue.Queue[PaymentEvent] = queue.Queue()
    for event in events:
        work.put(event)
    retries = Counter()

    def worker() -> None:
        with app_conn(db) as conn:
            while True:
                try:
                    event = work.get_nowait()
                except queue.Empty:
                    return
                try:
                    record_payment_event(conn, event)
                except (SequenceGap, UnknownPayment) as exc:  # earlier events not recorded yet
                    retries[type(exc).__name__] += 1
                    assert retries.total() < 20_000, "producers stopped making progress"
                    work.put(event)                            # retry later, after the others

    with ThreadPoolExecutor(8) as pool:
        for future in [pool.submit(worker) for _ in range(8)]:
            future.result()
    assert retries["SequenceGap"] > 0  # the shuffle really produced out-of-order arrivals
    for tid in payments:
        final = "captured" if tid % 3 else "failed"
        assert state(db, tid)[:2] == (final, 3)
        assert [s for s, _ in history(db, tid)] == [1, 2, 3]


# --- roles and privileges ---------------------------------------------------------------

@pytest.mark.parametrize("statement", [
    "UPDATE transactions SET status = 'authorized', last_event_sequence = 2, authorized_at = now() "
    "WHERE transaction_id = 3004",
    "UPDATE transactions SET updated_at = now() WHERE transaction_id = 3004",
    "INSERT INTO transactions (transaction_id, user_id, merchant_id, amount_minor, currency, status, "
    "last_event_sequence) VALUES (6001, 1, 101, 100, 'USD', 'created', 1)",
    "DELETE FROM transactions WHERE transaction_id = 3004",
    "TRUNCATE transactions CASCADE",
    "INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type, amount_minor, currency, "
    "source, occurred_at) VALUES (gen_random_uuid(), 3004, 2, 'payment.authorized', 2000, 'USD', 'api', now())",
    "UPDATE payment_events SET occurred_at = now()",
    "DELETE FROM payment_events",
    "TRUNCATE payment_events",
    "INSERT INTO refunds (refund_id, transaction_id, amount_minor, reason, status) "
    "VALUES (1, 3001, 1, 'x', 'pending')",
    "UPDATE refunds SET status = 'succeeded'",
    "UPDATE currencies SET minor_units = 3",
    "ALTER TABLE transactions DISABLE TRIGGER transactions_history_consistent",
    "ALTER TABLE payment_events DISABLE TRIGGER payment_events_append_only",
    "SET session_replication_role = replica",
    "CREATE TABLE public.shadow (x int)",
    "CREATE FUNCTION public.currencies() RETURNS int LANGUAGE sql RETURN 1",
])
def test_application_role_cannot_write_directly(db, statement):
    before = snapshot(db)
    with app_conn(db) as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(statement)
        assert conn.execute("SELECT count(*) FROM transactions").fetchone()[0] == 4  # reading is allowed
    assert snapshot(db) == before


def test_privilege_catalog(db):
    with connect(db) as conn:
        roles = conn.execute(
            "SELECT rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb, rolbypassrls FROM pg_roles "
            "WHERE rolname IN (%s, %s) ORDER BY 1", (APP_ROLE, WRITER_ROLE)).fetchall()
        assert roles == [(APP_ROLE, False, False, False, False, False),
                         (WRITER_ROLE, False, False, False, False, False)]
        assert not conn.execute("SELECT pg_has_role(%s, %s, 'MEMBER')", (APP_ROLE, WRITER_ROLE)).fetchone()[0]

        tables = [t for (t,) in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")]
        for table in tables:
            for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"):
                assert not conn.execute("SELECT has_table_privilege(%s, %s, %s)",
                                        (APP_ROLE, table, privilege)).fetchone()[0], (table, privilege)
                if privilege in ("DELETE", "TRUNCATE"):
                    assert not conn.execute("SELECT has_table_privilege(%s, %s, %s)",
                                            (WRITER_ROLE, table, privilege)).fetchone()[0], (table, privilege)
        # The writer may update only lifecycle columns, never identity or amount.
        for column in ("amount_minor", "currency", "user_id", "merchant_id", "created_at", "transaction_id"):
            assert not conn.execute("SELECT has_column_privilege(%s, 'transactions', %s, 'UPDATE')",
                                    (WRITER_ROLE, column)).fetchone()[0], column

        definer, owner, config, public_exec, app_exec = conn.execute("""
            SELECT p.prosecdef, pg_get_userbyid(p.proowner), p.proconfig,
                   EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0),
                   has_function_privilege(%s, p.oid, 'EXECUTE')
            FROM pg_proc p WHERE p.proname = 'record_payment_event'""", (APP_ROLE,)).fetchone()
        assert (definer, owner, config, public_exec, app_exec) == \
               (True, WRITER_ROLE, ["search_path=public, pg_temp"], False, True)


# --- commit-time history checks (direct writes by the table owner) ---------------------------

@pytest.mark.parametrize("statements,message", [
    (["UPDATE transactions SET status = 'authorized', last_event_sequence = 2, "
      "authorized_at = '2026-10-15 15:05+00' WHERE transaction_id = 3004"],
     "does not match its last event"),
    (["INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type, amount_minor, "
      "currency, source, occurred_at) VALUES (gen_random_uuid(), 3004, 3, 'payment.authorized', 2000, 'USD', "
      "'api', now())"],
     "gaps"),
    (["INSERT INTO transactions (transaction_id, user_id, merchant_id, amount_minor, currency, status, "
      "last_event_sequence) VALUES (6001, 1, 101, 100, 'USD', 'created', 1)"],
     "has no events"),
    # State and history agree and satisfy every CHECK, but the path itself is illegal.
    (["INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type, amount_minor, "
      "currency, source, occurred_at) VALUES (gen_random_uuid(), 3004, 2, 'payment.captured', 2000, 'USD', "
      "'api', now()), (gen_random_uuid(), 3004, 3, 'payment.captured', 2000, 'USD', 'api', now())",
      "UPDATE transactions SET status = 'captured', last_event_sequence = 3, authorized_at = now(), "
      "captured_at = now() WHERE transaction_id = 3004"],
     "not a legal lifecycle"),
])
def test_owner_direct_writes_are_checked_at_commit(db, statements, message):
    before = snapshot(db)
    with connect(db) as conn:  # the migration owner, not the application role
        with pytest.raises(psycopg.errors.IntegrityConstraintViolation, match=message):
            with conn.transaction():
                for statement in statements:
                    conn.execute(statement)
    assert snapshot(db) == before


def test_history_and_identity_protections_still_hold(db):
    with connect(db) as conn:
        with pytest.raises(psycopg.errors.RestrictViolation, match="append-only"):
            conn.execute("UPDATE payment_events SET occurred_at = now() WHERE transaction_id = 3001")
        with pytest.raises(psycopg.errors.RestrictViolation, match="immutable"):
            conn.execute("UPDATE transactions SET amount_minor = 1 WHERE transaction_id = 3001")


# --- a real login role (no superuser session underneath) -----------------------------------

@pytest.fixture
def login_role(db):
    """A temporary LOGIN role that is a member of v2_payment_app, like a deployed service.
    The password is random per test run and never stored; the role is dropped afterwards."""
    name, password = f"v2_test_login_{uuid.uuid4().hex[:8]}", uuid.uuid4().hex
    with connect("postgres") as admin:
        admin.execute(psycopg.sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} IN ROLE {}").format(
            psycopg.sql.Identifier(name), psycopg.sql.Literal(password), psycopg.sql.Identifier(APP_ROLE)))
    params = psycopg.conninfo.conninfo_to_dict(conninfo(db))
    params.update(user=name, password=password)
    yield psycopg.conninfo.make_conninfo(**params)
    with connect("postgres") as admin:
        assert name.startswith("v2_test_login_")
        admin.execute(psycopg.sql.SQL("DROP ROLE {}").format(psycopg.sql.Identifier(name)))


def test_login_role_can_only_use_the_write_path(db, login_role):
    before = snapshot(db)
    with psycopg.connect(login_role, autocommit=True) as conn:
        for statement in ("UPDATE transactions SET status = 'failed' WHERE transaction_id = 3004",
                          "INSERT INTO payment_events SELECT * FROM payment_events LIMIT 0",
                          f"SET ROLE {WRITER_ROLE}",
                          f"SET ROLE {os.getenv('POSTGRES_USER', 'fintech')}"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(statement)
        assert snapshot(db) == before
        result = record_payment_event(conn, step(3004, 2, "authorized", amount_minor=2000))
        assert (result.outcome, result.transaction_status) == ("recorded", "authorized")
