"""Step 5: refund processing (request_refund / complete_refund) and financial invariants."""

from __future__ import annotations

import threading
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from payments.events import record_payment_event
from payments.refunds import (CurrencyMismatch, IdempotencyKeyConflict, IllegalRefundTransition, InvalidRefund,
                              PaymentNotRefundable, RefundExceedsRemaining, UnknownPaymentOrRefund,
                              complete_refund, request_refund)
from tests.v2.conftest import MVP_DEMO_SEED, OLTP_MIGRATIONS, connect, quiet, run_sql_file
from tests.v2.test_write_path import (WRITER_ROLE, app_conn, created, login_role, step,  # noqa: F401
                                      wait_until_blocked)
from tests.v2.test_lifecycle import build
from tools.migrate import migrate

UTC = timezone.utc
# Demo seed after migration: 3001 captured 10000 USD with two succeeded refunds (1000 + 2000),
# 3002 captured 5000 USD without refunds, 3003 failed, 3004 created.


@pytest.fixture
def db(make_db) -> str:
    return build(make_db, "refund")


def key() -> str:
    return f"rk-{uuid.uuid4().hex}"


def req(conn, tid: int = 3002, amount: int = 1000, currency: str = "USD", reason: str = "customer_request",
        idempotency_key: str | None = None):
    return request_refund(conn, idempotency_key=idempotency_key or key(), transaction_id=tid,
                          amount_minor=amount, currency=currency, reason=reason)


def snapshot(dbname: str) -> tuple[list, list, list]:
    with connect(dbname) as conn:
        return tuple(conn.execute(f"SELECT * FROM {t} ORDER BY 1, 2").fetchall()
                     for t in ("transactions", "payment_events", "refunds"))


def payment_row(dbname: str, tid: int) -> tuple:
    with connect(dbname) as conn:
        return conn.execute("SELECT * FROM transactions WHERE transaction_id = %s", (tid,)).fetchone()


def refunds_of(dbname: str, tid: int) -> list[tuple]:
    with connect(dbname) as conn:
        return conn.execute("SELECT amount_minor, status FROM refunds WHERE transaction_id = %s ORDER BY refund_id",
                            (tid,)).fetchall()


def race(dbname: str, calls) -> list:
    """Run each call(conn) at the same moment on its own application-role connection."""
    barrier = threading.Barrier(len(calls))
    conns = [app_conn(dbname) for _ in calls]

    def go(i: int):
        barrier.wait()
        try:
            return calls[i](conns[i]).outcome
        except Exception as exc:  # noqa: BLE001 - the class is the result under test
            return type(exc)

    try:
        with ThreadPoolExecutor(len(calls)) as pool:
            return list(pool.map(go, range(len(calls))))
    finally:
        for c in conns:
            c.close()


# --- migration of existing refunds -------------------------------------------------------

def test_existing_refunds_are_migrated(db):
    with connect(db) as conn:
        rows = conn.execute("SELECT refund_id, status, currency, idempotency_key, requested_at, succeeded_at, "
                            "failed_at, failure_code FROM refunds ORDER BY 1").fetchall()
    t = lambda s: datetime.fromisoformat(s).replace(tzinfo=UTC)  # noqa: E731
    assert rows == [
        (7001, "succeeded", "USD", "legacy-refund:7001", t("2026-10-15 18:00:00"), t("2026-10-15 18:00:10"), None, None),
        (7002, "succeeded", "USD", "legacy-refund:7002", t("2026-10-15 20:00:00"), t("2026-10-15 20:00:12"), None, None),
    ]


def test_legacy_refunds_view_still_matches_the_mvp(make_db, db):
    reference = make_db("refund_ref")
    migrate(reference, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(reference, MVP_DEMO_SEED)
    query = "SELECT refund_id, transaction_id, amount, reason, status, created_at, updated_at FROM {} ORDER BY 1"
    with connect(reference) as ref, connect(db) as v2:
        assert v2.execute(query.format("legacy_refunds")).fetchall() == ref.execute(query.format("refunds")).fetchall()


def test_migration_maps_pending_and_failed_refunds(make_db):
    db = make_db("refund_map")
    migrate(db, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(db, MVP_DEMO_SEED)
    with connect(db) as conn:
        conn.execute("INSERT INTO refunds (refund_id, transaction_id, amount, reason, status, created_at, updated_at) "
                     "VALUES (7003, 3002, 5.00, 'a', 'pending', '2026-10-16 09:00+00', '2026-10-16 09:00+00'), "
                     "(7004, 3002, 7.00, 'b', 'failed', '2026-10-16 10:00+00', '2026-10-16 10:00:09+00')")
    migrate(db, OLTP_MIGRATIONS, log=quiet)
    with connect(db) as conn:
        assert conn.execute("SELECT refund_id, status, failure_code, failed_at IS NOT NULL FROM refunds "
                            "WHERE refund_id > 7002 ORDER BY 1").fetchall() == [
            (7003, "requested", None, False), (7004, "failed", "legacy_unknown", True)]
        assert conn.execute("SELECT status FROM legacy_refunds WHERE refund_id = 7003").fetchone() == ("pending",)
    with app_conn(db) as conn:  # new refund IDs continue after the existing ones
        assert req(conn, amount=100).refund_id == 7005


def test_migration_aborts_on_refunds_that_break_the_invariants(make_db):
    db = make_db("refund_bad")
    migrate(db, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(db, MVP_DEMO_SEED)
    with connect(db) as conn:  # MVP data with a refund on the failed payment 3003
        conn.execute("INSERT INTO refunds (refund_id, transaction_id, amount, reason, status) "
                     "VALUES (7003, 3003, 1.00, 'x', 'succeeded')")
    with pytest.raises(psycopg.errors.RaiseException, match="has refunds but is failed"):
        migrate(db, OLTP_MIGRATIONS, log=quiet)
    with connect(db) as conn:
        assert [v for (v,) in conn.execute("SELECT version FROM schema_migrations ORDER BY 1")] == [1, 2, 3, 4, 5]


# --- partial and full refunds --------------------------------------------------------------

def test_multiple_partial_refunds_leave_the_payment_unchanged(db):
    payment_before = payment_row(db, 3002)
    events_before = snapshot(db)[1]
    with app_conn(db) as conn:
        first, second = req(conn, amount=1000), req(conn, amount=1500)
        assert (first.refundable_minor, second.refundable_minor) == (4000, 2500)
        assert complete_refund(conn, first.refund_id, "succeeded").refundable_minor == 2500
        assert complete_refund(conn, second.refund_id, "succeeded").refund_status == "succeeded"
        third = req(conn, amount=500)
        assert third.refundable_minor == 2000
    assert refunds_of(db, 3002) == [(1000, "succeeded"), (1500, "succeeded"), (500, "requested")]
    assert payment_row(db, 3002) == payment_before   # amount, status, timestamps, updated_at: all unchanged
    assert snapshot(db)[1] == events_before          # no payment events are written by refunds
    with connect(db) as conn:
        assert conn.execute("SELECT status, amount FROM legacy_transactions WHERE transaction_id = 3002"
                            ).fetchone()[0] == "succeeded"


def test_full_refund_then_nothing_more(db):
    with app_conn(db) as conn:
        full = req(conn, amount=5000)
        assert full.refundable_minor == 0
        complete_refund(conn, full.refund_id, "succeeded")
        with pytest.raises(RefundExceedsRemaining):
            req(conn, amount=1)
    assert refunds_of(db, 3002) == [(5000, "succeeded")]


def test_over_refund_is_rejected(db):
    before = snapshot(db)
    with app_conn(db) as conn:
        with pytest.raises(RefundExceedsRemaining):  # 3001: 10000 - (1000 + 2000) = 7000 left
            req(conn, tid=3001, amount=7001)
        assert snapshot(db) == before
        assert req(conn, tid=3001, amount=7000).refundable_minor == 0


def test_refunds_in_zero_decimal_currency(db):
    with app_conn(db) as conn:
        record_payment_event(conn, created(6001, amount_minor=1500, currency="JPY"))
        record_payment_event(conn, step(6001, 2, "authorized", amount_minor=1500, currency="JPY"))
        record_payment_event(conn, step(6001, 3, "captured", amount_minor=1500, currency="JPY"))
        assert req(conn, tid=6001, amount=1499, currency="JPY").refundable_minor == 1
        with pytest.raises(CurrencyMismatch):
            req(conn, tid=6001, amount=1, currency="USD")


# --- reservations ------------------------------------------------------------------------

def test_pending_requests_reserve_and_failures_release(db):
    with app_conn(db) as conn:
        pending = req(conn, amount=4000)
        with pytest.raises(RefundExceedsRemaining):   # 4000 reserved, 1000 left
            req(conn, amount=1001)
        released = complete_refund(conn, pending.refund_id, "failed", failure_code="processor_declined")
        assert (released.refund_status, released.refundable_minor) == ("failed", 5000)
        assert req(conn, amount=5000).refundable_minor == 0
    assert refunds_of(db, 3002) == [(4000, "failed"), (5000, "requested")]
    with connect(db) as conn:
        assert conn.execute("SELECT failure_code, failed_at IS NOT NULL FROM refunds WHERE refund_id = %s",
                            (pending.refund_id,)).fetchone() == ("processor_declined", True)


# --- idempotency ---------------------------------------------------------------------------

def test_identical_retry_returns_the_same_refund(db):
    k = key()
    with app_conn(db) as conn:
        first = req(conn, amount=1200, idempotency_key=k)
        before = snapshot(db)
        again = req(conn, amount=1200, idempotency_key=k)
        assert (again.outcome, again.refund_id, again.refund_status) == ("duplicate", first.refund_id, "requested")
        assert snapshot(db) == before
        complete_refund(conn, first.refund_id, "succeeded")
        after_completion = req(conn, amount=1200, idempotency_key=k)  # a late retry reports the outcome
        assert (after_completion.outcome, after_completion.refund_status) == ("duplicate", "succeeded")
    assert refunds_of(db, 3002) == [(1200, "succeeded")]


@pytest.mark.parametrize("change", [
    {"amount": 1201}, {"tid": 3001}, {"reason": "other"}, {"currency": "EUR"},
])
def test_conflicting_retry_with_the_same_key_is_rejected(db, change):
    k = key()
    with app_conn(db) as conn:
        req(conn, amount=1200, idempotency_key=k)
        before = snapshot(db)
        with pytest.raises(IdempotencyKeyConflict):
            req(conn, **{"amount": 1200, "idempotency_key": k, **change})
    assert snapshot(db) == before


@pytest.mark.parametrize("bad_key", ["", "has space", "x" * 201, None])
def test_idempotency_key_format(db, bad_key):
    with app_conn(db) as conn, pytest.raises(InvalidRefund):
        request_refund(conn, idempotency_key=bad_key, transaction_id=3002, amount_minor=1, currency="USD",
                       reason="r")


# --- which payments can be refunded ------------------------------------------------------------

def test_refunds_only_on_captured_payments(db):
    with app_conn(db) as conn:
        record_payment_event(conn, created(6001))
        record_payment_event(conn, step(6001, 2, "authorized"))
        before = snapshot(db)
        for tid in (3004, 3003, 6001):  # created, failed, authorized
            with pytest.raises(PaymentNotRefundable):
                req(conn, tid=tid, amount=100)
        with pytest.raises(UnknownPaymentOrRefund):
            req(conn, tid=9999, amount=100)
    assert snapshot(db) == before


def test_currency_must_match_the_payment(db):
    before = snapshot(db)
    with app_conn(db) as conn, pytest.raises(CurrencyMismatch):
        req(conn, currency="EUR")
    assert snapshot(db) == before


@pytest.mark.parametrize("amount,reason", [(0, "r"), (-5, "r"), (100, "  ")])
def test_invalid_request_payloads(db, amount, reason):
    with app_conn(db) as conn, pytest.raises(InvalidRefund):
        req(conn, amount=amount, reason=reason)


# --- status transitions ------------------------------------------------------------------------

def test_refund_status_transitions(db):
    with app_conn(db) as conn:
        ok, bad = req(conn, amount=1000), req(conn, amount=1000)
        done = complete_refund(conn, ok.refund_id, "succeeded")
        assert complete_refund(conn, ok.refund_id, "succeeded").outcome == "duplicate"
        stored = (datetime.now(UTC) + timedelta(days=1))  # a different completion time is a conflict
        with pytest.raises(IllegalRefundTransition):
            complete_refund(conn, ok.refund_id, "succeeded", completed_at=stored)
        with pytest.raises(IllegalRefundTransition):
            complete_refund(conn, ok.refund_id, "failed", failure_code="late_decline")
        complete_refund(conn, bad.refund_id, "failed", failure_code="processor_declined")
        assert complete_refund(conn, bad.refund_id, "failed", failure_code="processor_declined").outcome == "duplicate"
        with pytest.raises(IllegalRefundTransition):
            complete_refund(conn, bad.refund_id, "succeeded")
        with pytest.raises(IllegalRefundTransition):
            complete_refund(conn, bad.refund_id, "failed", failure_code="other_code")
        assert done.refundable_minor == 3000  # both 1000 refunds were reserved at that point
    assert refunds_of(db, 3002) == [(1000, "succeeded"), (1000, "failed")]


def test_invalid_completions(db):
    with app_conn(db) as conn:
        pending = req(conn, amount=1000)
        before = snapshot(db)
        for kwargs, error in [
            (dict(status="requested"), InvalidRefund),
            (dict(status="refunded"), InvalidRefund),
            (dict(status="failed"), InvalidRefund),                                   # failure_code missing
            (dict(status="succeeded", failure_code="x"), InvalidRefund),              # not allowed on success
            (dict(status="succeeded", completed_at=datetime(2020, 1, 1, tzinfo=UTC)), InvalidRefund),  # before request
        ]:
            with pytest.raises(error):
                complete_refund(conn, pending.refund_id, **kwargs)
        with pytest.raises(UnknownPaymentOrRefund):
            complete_refund(conn, 999999, "succeeded")
    assert snapshot(db) == before


# --- rollback ----------------------------------------------------------------------------------

def test_caller_transaction_rollback_and_savepoints(db):
    before = snapshot(db)
    with app_conn(db, autocommit=False) as conn:
        first = req(conn, amount=2000)
        complete_refund(conn, first.refund_id, "succeeded")
        with pytest.raises(RefundExceedsRemaining):   # rejected inside the open transaction
            req(conn, amount=3001)
        assert req(conn, amount=3000).refundable_minor == 0  # the transaction is still usable
        conn.rollback()
    assert snapshot(db) == before
    with app_conn(db, autocommit=False) as conn:
        req(conn, amount=2000)
        conn.commit()
    assert refunds_of(db, 3002) == [(2000, "requested")]


# --- concurrency -------------------------------------------------------------------------------

def test_concurrent_requests_cannot_exceed_the_payment(db):
    results = race(db, [lambda c: req(c, amount=1000) for _ in range(8)])  # 5000 available
    assert Counter(results) == {"recorded": 5, RefundExceedsRemaining: 3}
    assert refunds_of(db, 3002) == [(1000, "requested")] * 5


def test_concurrent_mixed_amounts_never_over_refund(db):
    amounts = [700, 1300, 2100, 900, 1600, 400, 2500, 1100, 300, 1800]
    results = race(db, [lambda c, a=a: req(c, tid=3001, amount=a) for a in amounts])  # 7000 available
    recorded = sum(a for a, r in zip(amounts, results) if r == "recorded")
    assert recorded <= 7000
    assert all(r in ("recorded", RefundExceedsRemaining) for r in results)
    # Every rejected request must have been too large for what was left once all winners were in.
    assert all(a > 7000 - recorded for a, r in zip(amounts, results) if r is RefundExceedsRemaining)
    with connect(db) as conn:
        assert conn.execute("SELECT sum(amount_minor) FROM refunds WHERE transaction_id = 3001 "
                            "AND status IN ('requested', 'succeeded')").fetchone()[0] == 3000 + recorded


def test_concurrent_identical_requests_create_one_refund(db):
    k = key()
    results = race(db, [lambda c: req(c, amount=1000, idempotency_key=k) for _ in range(8)])
    assert Counter(results) == {"recorded": 1, "duplicate": 7}
    assert refunds_of(db, 3002) == [(1000, "requested")]


def test_concurrent_reuse_of_a_key_across_payments(db):
    k = key()
    results = race(db, [lambda c: req(c, tid=3001, amount=100, idempotency_key=k),
                        lambda c: req(c, tid=3002, amount=100, idempotency_key=k)])
    assert sorted(results, key=str) == sorted(["recorded", IdempotencyKeyConflict], key=str)
    with connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM refunds WHERE idempotency_key = %s", (k,)).fetchone()[0] == 1


def test_concurrent_conflicting_completions(db):
    with app_conn(db) as conn:
        refund_id = req(conn, amount=1000).refund_id
    calls = [(lambda c: complete_refund(c, refund_id, "succeeded")) if i % 2 else
             (lambda c: complete_refund(c, refund_id, "failed", failure_code="processor_declined"))
             for i in range(6)]
    results = race(db, calls)
    # One wins; same-outcome losers replay as duplicates, opposite-outcome losers hit the terminal status.
    assert Counter(results) == {"recorded": 1, "duplicate": 2, IllegalRefundTransition: 3}
    assert refunds_of(db, 3002) in ([(1000, "succeeded")], [(1000, "failed")])


def test_refund_request_waits_for_the_payment_lock(db):
    first, second = app_conn(db, autocommit=False), app_conn(db)
    try:
        req(first, amount=4000)                              # holds the lock on payment 3002
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(req, second, amount=2000)
            wait_until_blocked(db, second.info.backend_pid)
            first.commit()                                   # 4000 now reserved
            with pytest.raises(RefundExceedsRemaining):
                pending.result(timeout=10)
    finally:
        first.close()
        second.close()
    assert refunds_of(db, 3002) == [(4000, "requested")]


# --- unauthorized direct writes --------------------------------------------------------------

@pytest.mark.parametrize("statement", [
    "INSERT INTO refunds (transaction_id, amount_minor, currency, reason, status, idempotency_key) "
    "VALUES (3002, 100, 'USD', 'x', 'requested', 'direct')",
    "UPDATE refunds SET status = 'failed', failed_at = now(), failure_code = 'x' WHERE refund_id = 7001",
    "UPDATE refunds SET amount_minor = 1 WHERE refund_id = 7001",
    "DELETE FROM refunds",
    "TRUNCATE refunds",
    "SELECT nextval('refunds_refund_id_seq')",
    "ALTER TABLE refunds DISABLE TRIGGER refunds_invariants",
    "UPDATE transactions SET status = 'failed' WHERE transaction_id = 3002",
])
def test_application_role_cannot_write_refunds_directly(db, statement):
    before = snapshot(db)
    with app_conn(db) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute(statement)
    assert snapshot(db) == before


def test_login_role_can_only_use_the_refund_functions(db, login_role):  # noqa: F811
    with psycopg.connect(login_role, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute("UPDATE refunds SET status = 'failed' WHERE refund_id = 7001")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(f"SET ROLE {WRITER_ROLE}")
        refund = req(conn, amount=1000)
        assert complete_refund(conn, refund.refund_id, "succeeded").refund_status == "succeeded"


def test_refund_privilege_catalog(db):
    with connect(db) as conn:
        for fn in ("request_refund", "complete_refund"):
            assert conn.execute("""
                SELECT p.prosecdef, pg_get_userbyid(p.proowner), p.proconfig,
                       EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0),
                       has_function_privilege('v2_payment_app', p.oid, 'EXECUTE')
                FROM pg_proc p WHERE p.proname = %s""", (fn,)).fetchone() == \
                (True, WRITER_ROLE, ["search_path=public, pg_temp"], False, True)
        for column in ("amount_minor", "currency", "transaction_id", "idempotency_key", "reason", "requested_at",
                       "refund_id", "created_at"):
            assert not conn.execute("SELECT has_column_privilege(%s, 'refunds', %s, 'UPDATE')",
                                    (WRITER_ROLE, column)).fetchone()[0], column
        for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
            assert not conn.execute("SELECT has_table_privilege('v2_payment_app', 'refunds', %s)",
                                    (privilege,)).fetchone()[0]


# --- protections against the table owner -------------------------------------------------------

@pytest.mark.parametrize("statement,error,message", [
    ("UPDATE refunds SET amount_minor = 1 WHERE refund_id = 7001", psycopg.errors.RestrictViolation, "immutable"),
    ("UPDATE refunds SET idempotency_key = 'x' WHERE refund_id = 7001", psycopg.errors.RestrictViolation,
     "immutable"),
    ("UPDATE refunds SET status = 'failed', succeeded_at = NULL, failed_at = now(), failure_code = 'x' "
     "WHERE refund_id = 7001", psycopg.errors.RestrictViolation, "terminal"),
    ("DELETE FROM refunds WHERE refund_id = 7001", psycopg.errors.RestrictViolation, "financial records"),
    ("TRUNCATE refunds", psycopg.errors.RestrictViolation, "financial records"),
    ("INSERT INTO refunds (transaction_id, amount_minor, currency, reason, status, idempotency_key) "
     "VALUES (3002, 100, 'EUR', 'x', 'requested', 'k')", psycopg.errors.IntegrityConstraintViolation,
     "currency differs"),
    ("INSERT INTO refunds (transaction_id, amount_minor, currency, reason, status, idempotency_key) "
     "VALUES (3002, 100, 'USD', 'x', 'succeeded', 'k')", psycopg.errors.CheckViolation, "lifecycle"),
    ("INSERT INTO refunds (transaction_id, amount_minor, currency, reason, status, idempotency_key) "
     "VALUES (3001, 7001, 'USD', 'x', 'requested', 'k')", psycopg.errors.IntegrityConstraintViolation, "exceed"),
    ("INSERT INTO refunds (transaction_id, amount_minor, currency, reason, status, idempotency_key) "
     "VALUES (3004, 100, 'USD', 'x', 'requested', 'k')", psycopg.errors.IntegrityConstraintViolation,
     "has refunds but is created"),
])
def test_owner_writes_are_still_constrained(db, statement, error, message):
    before = snapshot(db)
    with connect(db) as conn:  # the migration owner, not the application role
        with pytest.raises(error, match=message):
            with conn.transaction():
                conn.execute(statement)
    assert snapshot(db) == before


def test_payment_amount_and_status_are_untouched_by_refund_activity(db):
    before = payment_row(db, 3001)
    with app_conn(db) as conn:
        for amount, outcome in [(3000, "succeeded"), (2000, "failed"), (4000, "succeeded")]:
            r = req(conn, tid=3001, amount=amount)
            complete_refund(conn, r.refund_id, outcome, failure_code="declined" if outcome == "failed" else None)
    assert payment_row(db, 3001) == before
    with connect(db) as conn:
        assert conn.execute("SELECT sum(amount_minor) FROM refunds WHERE transaction_id = 3001 AND status = "
                            "'succeeded'").fetchone()[0] == 10000  # fully refunded, payment still captured
        assert conn.execute("SELECT status, amount_minor FROM transactions WHERE transaction_id = 3001"
                            ).fetchone() == ("captured", 10000)
