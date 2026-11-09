"""Step 6: V2 incremental sync (transaction-ID watermark snapshot polling)."""

from __future__ import annotations

import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from payments.events import PaymentEvent, record_payment_event
from payments.refunds import complete_refund, request_refund
from sync.v2_sync import (TABLES, EventConflict, InjectedFailure, SourceIdentityChanged, SyncAlreadyRunning,
                          SyncBlocked, run_sync)
from tests.v2.conftest import (ANALYTICS_MIGRATIONS, MVP_DEMO_SEED, OLTP_MIGRATIONS, _admin, connect, quiet,
                               run_sql_file)
from tests.v2.test_lifecycle import build
from tools.db_guard import UnsafeDatabaseTarget
from tools.migrate import migrate

UTC = timezone.utc
T0 = datetime(2026, 10, 20, 9, 0, tzinfo=UTC)
SEED_COUNTS = {"currencies": 7, "users": 3, "merchants": 2, "transactions": 4, "refunds": 2, "payment_events": 9}


@pytest.fixture
def dbs(make_db) -> tuple[str, str]:
    source = build(make_db, "sync_src")                 # V001 + MVP seed + V002..V007
    target = make_db("sync_tgt")
    migrate(target, ANALYTICS_MIGRATIONS, log=quiet)    # V001 + V002
    return source, target


def app(db: str) -> psycopg.Connection:
    """Application-role connection; NOT autocommit (callers that want autocommit set it)."""
    c = connect(db)
    c.execute("SET ROLE v2_payment_app")
    c.autocommit = False
    return c


def txn(db: str) -> psycopg.Connection:
    """Owner connection whose statements stay in one open transaction until commit/rollback."""
    c = connect(db)
    c.autocommit = False
    return c


_tids = iter(range(20_000, 30_000))


def new_payment(c, steps=("authorized", "captured"), occurred=T0, amount=2500) -> int:
    tid = next(_tids)
    record_payment_event(c, PaymentEvent(uuid.uuid4(), tid, 1, "payment.created", occurred, "api", amount, "USD",
                                         user_id=1, merchant_id=101))
    for seq, kind in enumerate(steps, start=2):
        extra = dict(failure_code="card_declined") if kind == "failed" else dict(amount_minor=amount, currency="USD")
        record_payment_event(c, PaymentEvent(uuid.uuid4(), tid, seq, f"payment.{kind}",
                                             occurred + timedelta(minutes=seq), "processor", **extra))
    return tid


def source_rows(src: str, table) -> list[tuple]:
    with connect(src) as c:
        return c.execute(f"SELECT {', '.join(table.columns)} FROM {table.name} ORDER BY {table.pk}").fetchall()


def raw_rows(tgt: str, table) -> list[tuple]:
    with connect(tgt) as c:
        return c.execute(f"SELECT {', '.join(table.columns)} FROM raw.{table.name} ORDER BY {table.pk}").fetchall()


def assert_raw_matches_source(src: str, tgt: str) -> None:
    for table in TABLES:
        assert raw_rows(tgt, table) == source_rows(src, table), table.name


def raw_snapshot(tgt: str) -> dict:
    with connect(tgt) as c:
        return {t.name: c.execute(f"SELECT * FROM raw.{t.name} ORDER BY {t.pk}").fetchall() for t in TABLES}


def orphans(tgt: str) -> int:
    with connect(tgt) as c:
        return c.execute("""
            SELECT (SELECT count(*) FROM raw.refunds r LEFT JOIN raw.transactions t USING (transaction_id)
                    WHERE t.transaction_id IS NULL)
                 + (SELECT count(*) FROM raw.payment_events e LEFT JOIN raw.transactions t USING (transaction_id)
                    WHERE t.transaction_id IS NULL)
                 + (SELECT count(*) FROM raw.transactions t LEFT JOIN raw.users u USING (user_id)
                    WHERE u.user_id IS NULL)
                 + (SELECT count(*) FROM raw.transactions t LEFT JOIN raw.merchants m USING (merchant_id)
                    WHERE m.merchant_id IS NULL)
                 + (SELECT count(*) FROM raw.transactions t LEFT JOIN raw.currencies c
                    ON c.currency_code = t.currency WHERE c.currency_code IS NULL)
                 + (SELECT count(*) FROM raw.transactions t
                    WHERE t.last_event_sequence <> (SELECT count(*) FROM raw.payment_events e
                                                    WHERE e.transaction_id = t.transaction_id))""").fetchone()[0]


def state(tgt: str) -> tuple:
    with connect(tgt) as c:
        return c.execute("SELECT watermark::text, last_snapshot::text, status, blocked_reason, source_generation::text "
                         "FROM meta.sync_state").fetchone()


def run_row(tgt: str, run_id: int | None = None) -> dict:
    with connect(tgt) as c:
        cur = c.execute("SELECT * FROM meta.sync_runs WHERE run_id = coalesce(%s, (SELECT max(run_id) "
                        "FROM meta.sync_runs))", (run_id,))
        return dict(zip([d.name for d in cur.description], cur.fetchone()))


# --- full and incremental synchronization --------------------------------------------------

def test_initial_full_sync(dbs):
    src, tgt = dbs
    r = run_sync(src, tgt)
    assert (r.mode, r.status, r.watermark_from) == ("initial", "succeeded", "0")
    assert r.rows_read == r.rows_inserted == SEED_COUNTS
    assert_raw_matches_source(src, tgt)
    assert orphans(tgt) == 0
    row = run_row(tgt, r.run_id)
    assert (row["status"], row["rows_read"], row["rows_reread"], row["error_message"]) == \
           ("succeeded", SEED_COUNTS, 0, None)
    with connect(src) as s, connect(tgt) as t:
        generation = s.execute("SELECT generation_id::text FROM sync_source_identity").fetchone()[0]
        assert t.execute("SELECT count(*) FROM raw.users WHERE _run_id = %s", (r.run_id,)).fetchone()[0] == 3
    assert state(tgt)[2:] == ("active", None, generation)


def test_incremental_sync_after_payments_events_and_refunds(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with app(src) as c:
        c.autocommit = True
        tid = new_payment(c)                                        # 1 transaction, 3 events
        refund = request_refund(c, idempotency_key="k1", transaction_id=3002, amount_minor=700,
                                currency="USD", reason="r")
    r = run_sync(src, tgt)
    assert r.rows_read == {"currencies": 0, "users": 0, "merchants": 0, "transactions": 1, "refunds": 1,
                           "payment_events": 3}
    assert (r.rows_inserted["transactions"], r.rows_inserted["payment_events"], r.rows_inserted["refunds"]) == (1, 3, 1)
    with app(src) as c:
        c.autocommit = True
        complete_refund(c, refund.refund_id, "succeeded")
    r = run_sync(src, tgt)
    assert r.rows_read["refunds"] == 1 and r.rows_updated["refunds"] == 1 and r.rows_read["payment_events"] == 0
    assert_raw_matches_source(src, tgt)
    with connect(tgt) as c:
        assert c.execute("SELECT status FROM raw.refunds WHERE refund_id = %s", (refund.refund_id,)).fetchone() == \
               ("succeeded",)
        assert c.execute("SELECT status FROM raw.transactions WHERE transaction_id = %s", (tid,)).fetchone() == \
               ("captured",)


def test_rerun_is_idempotent_and_full_reprocessing_changes_nothing(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    before = raw_snapshot(tgt)
    again = run_sync(src, tgt)
    assert sum(again.rows_read.values()) == 0
    assert raw_snapshot(tgt) == before                         # nothing touched at all
    reprocess = run_sync(src, tgt, lookback_runs=10)           # re-read everything from watermark 0
    assert reprocess.rows_read == SEED_COUNTS
    assert sum(reprocess.rows_inserted.values()) == 0
    assert reprocess.duplicate_events == 9 and reprocess.rows_reread == sum(SEED_COUNTS.values())
    assert_raw_matches_source(src, tgt)


# --- events: duplicates, conflicts, ordering ----------------------------------------------------

def test_duplicate_event_ids_are_counted_not_reinserted(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with app(src) as c:
        c.autocommit = True
        new_payment(c)
    run_sync(src, tgt)
    r = run_sync(src, tgt, lookback_runs=1)                    # re-read the last run's window
    assert (r.rows_read["payment_events"], r.rows_inserted["payment_events"], r.duplicate_events) == (3, 0, 3)
    with connect(tgt) as c:
        assert c.execute("SELECT count(*), count(DISTINCT event_id) FROM raw.payment_events").fetchone() == (12, 12)


def test_conflicting_event_payload_fails_the_run_and_is_recorded(dbs):
    src, tgt = dbs
    first = run_sync(src, tgt)
    with connect(tgt) as c:                                    # corrupt the stored copy of one event
        c.execute("ALTER TABLE raw.payment_events DISABLE TRIGGER payment_events_insert_only")
        c.execute("UPDATE raw.payment_events SET occurred_at = occurred_at + interval '1 hour' "
                  "WHERE transaction_id = 3004")
        c.execute("ALTER TABLE raw.payment_events ENABLE TRIGGER payment_events_insert_only")
    before, state_before = raw_snapshot(tgt), state(tgt)
    with pytest.raises(EventConflict):
        run_sync(src, tgt, lookback_runs=1)
    row = run_row(tgt)
    assert (row["status"], row["error_class"]) == ("failed", "EventConflict")
    assert raw_snapshot(tgt) == before and state(tgt) == state_before
    with connect(tgt) as c:
        conflicts = c.execute("SELECT run_id, table_name, stored->>'occurred_at', incoming->>'occurred_at' "
                              "FROM meta.sync_conflicts").fetchall()
    assert len(conflicts) == 1 and conflicts[0][:2] == (row["run_id"], "payment_events")
    assert conflicts[0][2] != conflicts[0][3]
    assert first.run_id < row["run_id"]


def test_different_event_for_a_stored_payment_sequence_is_a_conflict(dbs):
    src, tgt = dbs
    with connect(tgt) as c:  # a foreign event already claims (3004, 1) under another event_id
        c.execute("INSERT INTO raw.payment_events (event_id, transaction_id, event_sequence, event_type, amount_minor, "
                  "currency, source, occurred_at, received_at, _sync_xid, _run_id, _synced_at) VALUES "
                  "(gen_random_uuid(), 3004, 1, 'payment.created', 2000, 'USD', 'api', now(), now(), '1', 0, now())")
    with pytest.raises(SyncBlocked, match="raw tables hold data"):  # and without state, nothing is assumed
        run_sync(src, tgt)
    with pytest.raises(EventConflict):
        _initial_ignoring_guard(src, tgt)
    with connect(tgt) as c:
        assert c.execute("SELECT count(*) FROM meta.sync_conflicts WHERE row_key IS NOT NULL").fetchone()[0] >= 1


def _initial_ignoring_guard(src, tgt):
    """Write a state row so the run is incremental from watermark 0 over the pre-filled raw table."""
    with connect(src) as s, connect(tgt) as t:
        ident = s.execute("SELECT (SELECT generation_id FROM sync_source_identity), "
                          "(SELECT system_identifier FROM pg_control_system()), "
                          "(SELECT oid FROM pg_database WHERE datname = current_database()), current_database(), "
                          "pg_current_snapshot()::text").fetchone()
        t.execute("INSERT INTO meta.sync_state VALUES ('v2_oltp', %s, %s, %s, %s, '0', %s::pg_snapshot, 0, 'active', "
                  "NULL, now())", ident)
    return run_sync(src, tgt)


def test_out_of_order_business_events_in_raw_ingestion(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    slow = app(src)
    early = new_payment(slow, steps=(), occurred=T0 - timedelta(days=1))   # earliest business time, not committed
    with app(src) as fast:
        fast.autocommit = True
        later = new_payment(fast, steps=("authorized",), occurred=T0)
    first = run_sync(src, tgt)
    slow.commit()
    with app(src) as fast:
        fast.autocommit = True
        record_payment_event(fast, PaymentEvent(uuid.uuid4(), later, 3, "payment.captured", T0 + timedelta(minutes=9),
                                                "processor", 2500, "USD"))
    second = run_sync(src, tgt)
    slow.close()
    with connect(tgt) as c:
        ingested = c.execute("SELECT transaction_id, event_sequence, _run_id FROM raw.payment_events "
                             "WHERE transaction_id IN (%s, %s) ORDER BY occurred_at", (early, later)).fetchall()
    # By business time the early payment comes first, but it was ingested by the later run, and
    # the later payment's events were split across runs. Sequences are intact either way.
    assert ingested[0] == (early, 1, second.run_id)
    assert [(s, r) for t, s, r in ingested if t == later] == [(1, first.run_id), (2, first.run_id), (3, second.run_id)]
    assert_raw_matches_source(src, tgt)
    assert orphans(tgt) == 0


# --- timestamps, late arrivals and long transactions ---------------------------------------------

def test_rows_with_identical_timestamps(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with connect(src) as c:
        with c.transaction():  # one transaction: identical created_at/updated_at (now()) for all 40 rows
            c.execute("INSERT INTO users (user_id, email, full_name, country) "
                      "SELECT g, 'u' || g || '@x', 'U', 'US' FROM generate_series(100, 139) g")
        c.execute("UPDATE users SET updated_at = '2026-10-01 00:00:00+00' WHERE user_id BETWEEN 140 AND 149")
        c.execute("INSERT INTO users (user_id, email, full_name, country, created_at, updated_at) "
                  "SELECT g, 'u' || g || '@x', 'U', 'US', '2026-10-01+00', '2026-10-01+00' "
                  "FROM generate_series(140, 159) g")
    r = run_sync(src, tgt)
    assert r.rows_read["users"] == 60
    assert_raw_matches_source(src, tgt)


def test_late_arriving_record_with_an_old_timestamp(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    late = txn(src)
    late.execute("SELECT now()")                     # starts now; its updated_at will be this old time
    with connect(src) as c:
        c.execute("UPDATE users SET email = 'carol@new' WHERE user_id = 3")
    run_sync(src, tgt)
    late.execute("UPDATE users SET email = 'alice@late' WHERE user_id = 1")
    late.commit()
    late.close()
    r = run_sync(src, tgt)
    assert r.rows_read["users"] == 1
    assert_raw_matches_source(src, tgt)


def test_lower_xid_committing_after_a_sync_is_not_missed(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    a, b = app(src), app(src)
    a.execute("SELECT 1")                            # A starts first
    tid_b = new_payment(b, steps=())                 # B writes first: lower transaction id
    tid_a = new_payment(a, steps=())
    a.commit()                                       # the higher id commits first
    first = run_sync(src, tgt)
    b.commit()
    second = run_sync(src, tgt)
    a.close(); b.close()
    # A arrived in the first run. B (lower xid) was still open, so the watermark stayed at B's xid:
    # the second run delivers B and re-reads A (counted as a re-read, harmless).
    assert (first.rows_inserted["transactions"], second.rows_inserted["transactions"]) == (1, 1)
    assert second.rows_reread >= 1 + 1  # A's transaction row and its created event
    with connect(tgt) as c:
        assert c.execute("SELECT _run_id FROM raw.payment_events WHERE transaction_id = %s", (tid_a,)).fetchone() == \
               (first.run_id,)          # events are insert-only: first delivery wins, re-read is a duplicate
    assert_raw_matches_source(src, tgt)


def test_long_running_transaction_holds_the_watermark(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    holder = txn(src)
    holder.execute("UPDATE merchants SET category = 'held' WHERE merchant_id = 102")
    holder_xid = holder.execute("SELECT pg_current_xact_id()::text").fetchone()[0]
    rereads, results = [], []
    for i in range(3):
        time.sleep(0.05)
        with connect(src) as c:
            c.execute("INSERT INTO users (user_id, email, full_name, country) VALUES (%s, %s, 'U', 'US')",
                      (200 + i, f"u{200 + i}@x"))
        r = run_sync(src, tgt)
        results.append(r)
        rereads.append(r.rows_reread)
        with connect(tgt) as c:  # the stored watermark is exactly the open writer's xid (xid8 equality)
            assert c.execute("SELECT watermark = %s::xid8 FROM meta.sync_state", (holder_xid,)).fetchone()[0]
    assert [r.rows_read["users"] for r in results] == [1, 2, 3]   # the window grows while the writer is open
    assert rereads == [0, 1, 2]                                     # ...and those re-reads are measured
    assert all(r.watermark_lag_xids > 0 and r.oldest_open_writer_secs > 0 for r in results)
    with connect(tgt) as c:
        assert c.execute("SELECT category FROM raw.merchants WHERE merchant_id = 102").fetchone() != ("held",)
    holder.commit()
    holder.close()
    r = run_sync(src, tgt)
    assert r.rows_read["merchants"] == 1 and r.oldest_open_writer_secs is None
    assert_raw_matches_source(src, tgt)


def test_newer_version_with_older_timestamp_and_lower_xid_wins(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    t2 = txn(src)
    t2.execute("UPDATE merchants SET category = category WHERE merchant_id = 101")   # T2 gets the lower xid
    with connect(src) as t1:
        time.sleep(0.05)
        t1.execute("UPDATE users SET email = 'bob@v1' WHERE user_id = 2")
        v1_xid, v1_updated = t1.execute("SELECT sync_xid::text, updated_at FROM users WHERE user_id = 2").fetchone()
    run_sync(src, tgt)
    t2.execute("UPDATE users SET email = 'bob@v2' WHERE user_id = 2")               # the final version
    t2.commit()
    t2.close()
    with connect(src) as c:
        lower, older = c.execute("SELECT sync_xid < %s::xid8, updated_at < %s FROM users WHERE user_id = 2",
                                 (v1_xid, v1_updated)).fetchone()
    assert lower and older        # comparing either value would keep the stale bob@v1
    run_sync(src, tgt)
    with connect(tgt) as c:
        assert c.execute("SELECT email FROM raw.users WHERE user_id = 2").fetchone() == ("bob@v2",)


def test_several_changes_between_runs_keep_only_the_latest_state(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with connect(src) as c:
        for email in ("carol@a", "carol@b", "carol@c"):
            c.execute("UPDATE users SET email = %s WHERE user_id = 3", (email,))
    with app(src) as c:
        c.autocommit = True
        record_payment_event(c, PaymentEvent(uuid.uuid4(), 3004, 2, "payment.authorized",
                                             datetime(2026, 10, 15, 15, 5, tzinfo=UTC), "api", 2000, "USD"))
        record_payment_event(c, PaymentEvent(uuid.uuid4(), 3004, 3, "payment.captured",
                                             datetime(2026, 10, 15, 15, 6, tzinfo=UTC), "api", 2000, "USD"))
    r = run_sync(src, tgt)
    assert (r.rows_read["users"], r.rows_read["transactions"], r.rows_read["payment_events"]) == (1, 1, 2)
    with connect(tgt) as c:
        assert c.execute("SELECT email FROM raw.users WHERE user_id = 3").fetchone() == ("carol@c",)
        assert c.execute("SELECT event_type FROM raw.payment_events WHERE transaction_id = 3004 "
                         "ORDER BY event_sequence").fetchall() == \
               [("payment.created",), ("payment.authorized",), ("payment.captured",)]


# --- failures and recovery ---------------------------------------------------------------------

def test_injected_failure_rolls_back_data_and_checkpoint_then_recovers(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    before, state_before = raw_snapshot(tgt), state(tgt)
    with app(src) as c:
        c.autocommit = True
        tid = new_payment(c)
    with pytest.raises(InjectedFailure):
        run_sync(src, tgt, fail_before_commit=True)
    failed = run_row(tgt)
    assert (failed["status"], failed["error_class"], failed["watermark_to"]) == ("failed", "InjectedFailure", None)
    assert raw_snapshot(tgt) == before and state(tgt) == state_before   # data and checkpoint rolled back together
    recovered = run_sync(src, tgt)
    assert recovered.watermark_from == state_before[0]                  # resumes from the last committed watermark
    assert recovered.rows_inserted["payment_events"] == 3
    assert_raw_matches_source(src, tgt)
    with connect(tgt) as c:
        assert c.execute("SELECT count(*) FROM raw.transactions WHERE transaction_id = %s", (tid,)).fetchone()[0] == 1


def test_abandoned_run_is_marked(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with connect(tgt) as c:  # a process died mid-run: its row stayed 'running'
        dead = c.execute("INSERT INTO meta.sync_runs (source_name, mode, status) VALUES ('v2_oltp', 'incremental', "
                         "'running') RETURNING run_id").fetchone()[0]
    run_sync(src, tgt)
    assert run_row(tgt, dead)["status"] == "abandoned"


# --- concurrency ------------------------------------------------------------------------------------

def test_concurrent_source_writes_keep_raw_consistent(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    stop = threading.Event()
    errors: list[BaseException] = []

    def writer(seed: int):
        r = random.Random(seed)
        try:
            with app(src) as c, connect(src) as owner:
                c.autocommit = True
                owner.autocommit = True
                while not stop.is_set():
                    roll = r.random()
                    if roll < 0.4:
                        new_payment(c, steps=r.choice([(), ("authorized",), ("authorized", "captured"), ("failed",)]))
                    elif roll < 0.6:
                        tid = new_payment(c)
                        refund = request_refund(c, idempotency_key=uuid.uuid4().hex, transaction_id=tid,
                                                amount_minor=r.randint(1, 2500), currency="USD", reason="r")
                        if r.random() < 0.7:
                            outcome = r.choice(["succeeded", "failed"])
                            complete_refund(c, refund.refund_id, outcome,
                                            failure_code="declined" if outcome == "failed" else None)
                    elif roll < 0.8:
                        owner.execute("UPDATE users SET full_name = %s WHERE user_id = %s",
                                      (uuid.uuid4().hex[:8], r.randint(1, 3)))
                    else:  # a slower multi-statement transaction
                        with c.transaction():
                            new_payment(c, steps=("authorized",))
                            time.sleep(r.uniform(0.01, 0.1))
                            new_payment(c, steps=())
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    try:
        for _ in range(12):
            run_sync(src, tgt)
            assert orphans(tgt) == 0          # every run's snapshot is internally consistent
            time.sleep(0.03)
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert errors == []
    run_sync(src, tgt)
    assert_raw_matches_source(src, tgt)
    assert orphans(tgt) == 0


def test_concurrent_sync_attempt_is_rejected(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    holding, release = threading.Event(), threading.Event()

    def hold():
        holding.set()
        assert release.wait(10)

    first: dict = {}
    t = threading.Thread(target=lambda: first.setdefault("r", run_sync(src, tgt, before_target_commit=hold)))
    t.start()
    assert holding.wait(10)
    with pytest.raises(SyncAlreadyRunning):
        run_sync(src, tgt)
    rejected = run_row(tgt)
    assert (rejected["status"], rejected["error_class"]) == ("rejected", "SyncAlreadyRunning")
    release.set()
    t.join()
    assert first["r"].status == "succeeded"
    assert run_sync(src, tgt).status == "succeeded"          # the lock is released afterwards


# --- source identity -----------------------------------------------------------------------------

def rebuild_in_place(name: str) -> None:
    """What scripts/v2/reset_v2.sh does to the source: same name, new database."""
    with _admin() as admin:
        assert name.startswith("v2_test_")
        admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        admin.execute(f'CREATE DATABASE "{name}"')
    migrate(name, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(name, MVP_DEMO_SEED)
    migrate(name, OLTP_MIGRATIONS, log=quiet)


def test_source_reset_blocks_until_explicit_reinitialization(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with app(src) as c:
        c.autocommit = True
        new_payment(c)
    run_sync(src, tgt)
    before = raw_snapshot(tgt)
    rebuild_in_place(src)          # rebuilt rows carry sync_xid 0, below the stored watermark
    with pytest.raises(SourceIdentityChanged, match="generation"):
        run_sync(src, tgt)
    assert run_row(tgt)["status"] == "blocked"
    assert state(tgt)[2] == "blocked" and raw_snapshot(tgt) == before
    with pytest.raises(SyncBlocked, match="blocked"):          # stays blocked, never silently resumes
        run_sync(src, tgt)
    r = run_sync(src, tgt, reinitialize=True)
    assert (r.mode, r.watermark_from, r.rows_read) == ("reinitialize", "0", SEED_COUNTS)
    assert_raw_matches_source(src, tgt)                         # the old source's extra payment is gone
    assert state(tgt)[2:4] == ("active", None)
    assert run_sync(src, tgt).mode == "incremental"


def test_rotated_generation_after_a_restore_is_detected(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with connect(src) as c:  # the documented step after restoring a backup
        c.execute("SELECT rotate_sync_source_generation('restored from backup')")
    with pytest.raises(SourceIdentityChanged, match="generation"):
        run_sync(src, tgt)


def test_stale_checkpoint_ahead_of_the_source_is_detected(dbs):
    """Generation, system identifier, OID and name all match, but the stored watermark is ahead
    of the source's transaction ids: e.g. a restore that kept the generation row into a cluster
    whose transaction counter is behind. Detected by xid8 comparison, not by identity fields."""
    src, tgt = dbs
    run_sync(src, tgt)
    with connect(src) as s:
        future = s.execute("SELECT pg_snapshot_xmax(pg_current_snapshot())::text").fetchone()[0]
    with connect(tgt) as c:
        c.execute("UPDATE meta.sync_state SET watermark = (%s::text::numeric + 1000000)::text::xid8", (future,))
    with pytest.raises(SourceIdentityChanged, match="ahead of the source"):
        run_sync(src, tgt)
    assert state(tgt)[2] == "blocked"


def test_missing_state_with_populated_raw_is_refused(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with connect(tgt) as c:
        c.execute("DELETE FROM meta.sync_state")
    with pytest.raises(SyncBlocked, match="no sync state"):
        run_sync(src, tgt)
    assert run_sync(src, tgt, reinitialize=True).status == "succeeded"


# --- lookback boundaries ------------------------------------------------------------------------

def test_lookback_rereads_exactly_the_chosen_windows(dbs):
    src, tgt = dbs
    run_sync(src, tgt)                                          # run 1: everything
    with connect(src) as c:
        c.execute("UPDATE users SET full_name = 'A' WHERE user_id = 1")
    r2 = run_sync(src, tgt)                                     # window 2: user 1
    with connect(src) as c:
        c.execute("UPDATE users SET full_name = 'B' WHERE user_id = 2")
        c.execute("UPDATE users SET full_name = 'C' WHERE user_id = 3")
    r3 = run_sync(src, tgt)                                     # window 3: users 2 and 3
    assert (r2.rows_read["users"], r3.rows_read["users"]) == (1, 2)
    one = run_sync(src, tgt, lookback_runs=1)                   # re-reads window 3 only
    assert (one.watermark_from, one.rows_read["users"], one.rows_reread) == (r3.watermark_from, 2, 2)
    two = run_sync(src, tgt, lookback_runs=3)                   # runs 4, 3, 2: back to window 2's start
    assert (two.watermark_from, two.rows_read["users"], two.rows_reread) == (r2.watermark_from, 3, 3)
    assert run_sync(src, tgt).rows_read["users"] == 0            # normal runs are unaffected afterwards
    assert_raw_matches_source(src, tgt)


# --- xid8 / snapshot semantics the design relies on -------------------------------------------------

def test_xid8_and_snapshot_boundary_semantics(dbs):
    """The facts the watermark relies on, checked with xid8 operators and pg_snapshot functions.

    Note: a snapshot's xmax is one past the highest *completed* xid, not the next xid to be
    assigned. An xid still in progress at or above xmax is therefore NOT listed in xip. That is
    fine for the watermark: W = xmin, and every unfinished xid is >= xmin.
    """
    src, _ = dbs
    with connect(src) as done:
        done.execute("UPDATE users SET full_name = 'done' WHERE user_id = 1")
        finished = done.execute("SELECT sync_xid::text FROM users WHERE user_id = 1").fetchone()[0]
    open1 = txn(src)                                     # in progress, below a later completed xid
    open1.execute("UPDATE users SET full_name = 'open1' WHERE user_id = 2")
    running = open1.execute("SELECT pg_current_xact_id()::text").fetchone()[0]
    with connect(src) as bump:                           # completes after open1 got its xid
        bump.execute("UPDATE merchants SET category = category WHERE merchant_id = 101")
    open2 = txn(src)                                     # in progress, above every completed xid
    open2.execute("UPDATE merchants SET category = 'open2' WHERE merchant_id = 102")
    running_high = open2.execute("SELECT pg_current_xact_id()::text").fetchone()[0]
    reader = txn(src)
    reader.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    snap = reader.execute("SELECT pg_current_snapshot()::text").fetchone()[0]
    with connect(src) as after:
        after.execute("UPDATE users SET full_name = 'after' WHERE user_id = 3")
        later = after.execute("SELECT sync_xid::text FROM users WHERE user_id = 3").fetchone()[0]
    with connect(src) as c:
        checks = c.execute("""
            WITH s AS (SELECT %(s)s::pg_snapshot AS s)
            SELECT
              -- finished before S: visible, below xmin
              pg_visible_in_snapshot(%(f)s::xid8, s), %(f)s::xid8 < pg_snapshot_xmin(s), pg_xact_status(%(f)s::xid8),
              -- in progress at S, below xmax: invisible, listed in xip, xmin <= xid < xmax
              NOT pg_visible_in_snapshot(%(r)s::xid8, s), %(r)s::xid8 IN (SELECT pg_snapshot_xip(s)),
              %(r)s::xid8 >= pg_snapshot_xmin(s) AND %(r)s::xid8 < pg_snapshot_xmax(s), pg_xact_status(%(r)s::xid8),
              -- in progress at S, at/above xmax: invisible, NOT in xip, still >= xmin
              NOT pg_visible_in_snapshot(%(h)s::xid8, s), %(h)s::xid8 NOT IN (SELECT pg_snapshot_xip(s)),
              %(h)s::xid8 >= pg_snapshot_xmax(s) AND %(h)s::xid8 >= pg_snapshot_xmin(s), pg_xact_status(%(h)s::xid8),
              -- assigned after S: invisible, >= xmax
              NOT pg_visible_in_snapshot(%(l)s::xid8, s), %(l)s::xid8 >= pg_snapshot_xmax(s),
              -- nothing listed as in progress lies below xmin, and xmin is the open1 xid
              pg_snapshot_xmin(s) <= ALL (SELECT pg_snapshot_xip(s)), pg_snapshot_xmin(s) = %(r)s::xid8
            FROM s""", {"f": finished, "r": running, "h": running_high, "l": later, "s": snap}).fetchone()
    assert checks == (True, True, "committed",
                      True, True, True, "in progress",
                      True, True, True, "in progress",
                      True, True,
                      True, True), (snap, running, running_high, checks)
    # The reader's later reads still use S: the change committed after S stays invisible.
    assert reader.execute("SELECT full_name FROM users WHERE user_id = 3").fetchone() != ("after",)
    assert reader.execute("SELECT pg_current_snapshot()::text").fetchone()[0] == snap
    reader.close()
    for c in (open1, open2):
        c.rollback()
        c.close()


def test_sync_xid_is_stamped_by_trigger_with_the_top_level_xid(dbs):
    src, _ = dbs
    with connect(src) as c:
        assert c.execute("SELECT count(*) FROM payment_events WHERE sync_xid = '0'").fetchone()[0] == 9  # pre-V007
    with app(src) as c:
        tid = new_payment(c, steps=("authorized",))
        top = c.execute("SELECT pg_current_xact_id()::text").fetchone()[0]
        c.execute("RESET ROLE")
        stamps = c.execute("SELECT sync_xid = %s::xid8, xmin::text::bigint <> %s::bigint "
                           "FROM payment_events WHERE transaction_id = %s", (top, top, tid)).fetchall()
        c.execute("INSERT INTO users (user_id, email, full_name, country, sync_xid) VALUES (77, 'f@x', 'F', 'US', '5')")
        forged = c.execute("SELECT sync_xid = %s::xid8 FROM users WHERE user_id = 77", (top,)).fetchone()[0]
        c.commit()
    # Written inside the function's exception sub-block: the tuple xmin is a subtransaction id,
    # sync_xid is the top-level id. An explicit value cannot override the trigger.
    assert stamps == [(True, True), (True, True)]
    assert forged


# --- schema, guards, compatibility -------------------------------------------------------------------

def test_watermark_columns_use_xid8_and_pg_snapshot(dbs):
    _, tgt = dbs
    with connect(tgt) as c:
        types = dict(c.execute("SELECT table_name || '.' || column_name, data_type FROM information_schema.columns "
                               "WHERE table_schema = 'meta' AND column_name IN ('watermark', 'last_snapshot', "
                               "'watermark_from', 'watermark_to', 'snapshot')").fetchall())
    assert types == {"sync_state.watermark": "xid8", "sync_state.last_snapshot": "pg_snapshot",
                     "sync_runs.watermark_from": "xid8", "sync_runs.watermark_to": "xid8",
                     "sync_runs.snapshot": "pg_snapshot"}


def test_raw_payment_events_are_insert_only(dbs):
    src, tgt = dbs
    run_sync(src, tgt)
    with connect(tgt) as c:
        for statement in ("UPDATE raw.payment_events SET source = 'api'", "DELETE FROM raw.payment_events"):
            with pytest.raises(psycopg.errors.RestrictViolation, match="insert-only"):
                c.execute(statement)


@pytest.mark.parametrize("source,target", [("fintech_oltp", "v2_analytics"), ("v2_oltp", "fintech_analytics"),
                                           ("demo_oltp", "v2_analytics"), ("v2_oltp", "demo_analytics")])
def test_sync_refuses_non_v2_databases_before_connecting(source, target):
    with pytest.raises(UnsafeDatabaseTarget):
        run_sync(source, target)


def test_analytics_v002_refuses_to_replace_populated_mvp_tables(make_db):
    db = make_db("sync_mvp_raw")
    migrate(db, ANALYTICS_MIGRATIONS, target_version=1, log=quiet)
    with connect(db) as c:
        c.execute("INSERT INTO raw.users VALUES (1, 'a@x', 'A', 'US', now(), now(), now())")
    with pytest.raises(psycopg.errors.RaiseException, match="refusing to replace"):
        migrate(db, ANALYTICS_MIGRATIONS, log=quiet)
    with connect(db) as c:
        assert c.execute("SELECT count(*) FROM raw.users").fetchone()[0] == 1
