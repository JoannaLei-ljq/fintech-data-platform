"""Step 3: payment lifecycle state, payment_events, backfill and protections."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from tests.v2.conftest import MVP_DEMO_SEED, OLTP_MIGRATIONS, connect, quiet, run_sql_file
from tools.migrate import migrate

UTC = timezone.utc


def ts(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def build(make_db, label: str, upto: int | None = None) -> str:
    """V001 baseline + MVP demo seed + later migrations (what reset_v2.sh does)."""
    db = make_db(label)
    migrate(db, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(db, MVP_DEMO_SEED)
    migrate(db, OLTP_MIGRATIONS, target_version=upto, log=quiet)
    return db


@pytest.fixture
def seeded(make_db) -> str:
    return build(make_db, "life")


# --- mapping and backfill ---------------------------------------------------------

def test_status_mapping_and_lifecycle_fields(seeded):
    with connect(seeded) as conn:
        rows = {r[0]: r[1:] for r in conn.execute(
            "SELECT transaction_id, status, last_event_sequence, failure_code, authorized_at, captured_at, failed_at "
            "FROM transactions")}
    assert rows == {
        3001: ("captured", 3, None, ts("2026-10-15 09:00:05"), ts("2026-10-15 09:00:05"), None),
        3002: ("captured", 3, None, ts("2026-10-15 10:30:04"), ts("2026-10-15 10:30:04"), None),
        3003: ("failed", 2, "legacy_unknown", None, None, ts("2026-10-15 12:00:03")),
        3004: ("created", 1, None, None, None, None),  # pending -> created (approved mapping)
    }


def test_migrations_preserve_updated_at_and_amounts(make_db, seeded):
    reference = make_db("life_ref")  # the MVP data exactly as seeded, never migrated past V001
    migrate(reference, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(reference, MVP_DEMO_SEED)
    query_tx = "SELECT transaction_id, amount, created_at, updated_at FROM {} ORDER BY 1"
    query_rf = "SELECT refund_id, transaction_id, amount, reason, status, created_at, updated_at FROM {} ORDER BY 1"
    with connect(reference) as ref, connect(seeded) as v2:
        assert v2.execute(query_tx.format("legacy_transactions")).fetchall() == \
               ref.execute(query_tx.format("transactions")).fetchall()
        assert v2.execute(query_rf.format("legacy_refunds")).fetchall() == \
               ref.execute(query_rf.format("refunds")).fetchall()


def test_legacy_view_reproduces_mvp_statuses(make_db, seeded):
    reference = make_db("life_ref_status")
    migrate(reference, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(reference, MVP_DEMO_SEED)
    query = "SELECT transaction_id, user_id, merchant_id, currency, status FROM {} ORDER BY 1"
    with connect(reference) as ref, connect(seeded) as v2:
        assert v2.execute(query.format("legacy_transactions")).fetchall() == \
               ref.execute(query.format("transactions")).fetchall()


def test_backfilled_event_history(seeded):
    with connect(seeded) as conn:
        history = {}
        for tid, seq, etype, amount, currency, failure, source, occurred in conn.execute(
                "SELECT transaction_id, event_sequence, event_type, amount_minor, currency, failure_code, "
                "source, occurred_at FROM payment_events ORDER BY transaction_id, event_sequence"):
            history.setdefault(tid, []).append((seq, etype, amount, currency, failure, occurred))
            assert source == "migration_backfill"
    assert history == {
        3001: [(1, "payment.created", 10000, "USD", None, ts("2026-10-15 09:00:00")),
               (2, "payment.authorized", 10000, "USD", None, ts("2026-10-15 09:00:05")),
               (3, "payment.captured", 10000, "USD", None, ts("2026-10-15 09:00:05"))],
        3002: [(1, "payment.created", 5000, "USD", None, ts("2026-10-15 10:30:00")),
               (2, "payment.authorized", 5000, "USD", None, ts("2026-10-15 10:30:04")),
               (3, "payment.captured", 5000, "USD", None, ts("2026-10-15 10:30:04"))],
        3003: [(1, "payment.created", 3000, "USD", None, ts("2026-10-15 12:00:00")),
               (2, "payment.failed", None, None, "legacy_unknown", ts("2026-10-15 12:00:03"))],
        3004: [(1, "payment.created", 2000, "USD", None, ts("2026-10-15 15:00:00"))],
    }


def test_event_state_reconciles_with_transactions(seeded):
    with connect(seeded) as conn:
        mismatches = conn.execute("""
            SELECT t.transaction_id FROM transactions t
            JOIN LATERAL (SELECT count(*) AS n, max(event_sequence) AS last_seq,
                                 (array_agg(event_type ORDER BY event_sequence DESC))[1] AS last_type
                          FROM payment_events e WHERE e.transaction_id = t.transaction_id) h ON true
            WHERE h.n <> t.last_event_sequence OR h.last_seq <> t.last_event_sequence
               OR h.last_type <> 'payment.' || t.status""").fetchall()
    assert mismatches == []


def test_backfill_event_ids_are_stable_uuid_v5(make_db, seeded):
    rebuilt = build(make_db, "life_rebuild")
    query = "SELECT transaction_id, event_sequence, event_id FROM payment_events ORDER BY 1, 2"
    with connect(seeded) as a, connect(rebuilt) as b:
        first, second = a.execute(query).fetchall(), b.execute(query).fetchall()
    assert first == second and len(first) == 9
    assert all(isinstance(e, uuid.UUID) and e.version == 5 for *_, e in first)
    assert len({e for *_, e in first}) == 9


# --- protections ------------------------------------------------------------------

@pytest.mark.parametrize("statement", [
    "UPDATE payment_events SET occurred_at = now() WHERE transaction_id = 3001",
    "DELETE FROM payment_events WHERE transaction_id = 3001",
    "TRUNCATE payment_events",
])
def test_payment_events_are_append_only(seeded, statement):
    with connect(seeded) as conn:
        with pytest.raises(psycopg.errors.RestrictViolation, match="append-only"):
            conn.execute(statement)
        assert conn.execute("SELECT count(*) FROM payment_events").fetchone()[0] == 9


@pytest.mark.parametrize("assignment", [
    "amount_minor = 1", "currency = 'EUR'", "user_id = 2", "merchant_id = 101",  # 3001 is user 1 / merchant 102
    "created_at = now()", "transaction_id = 9999",
])
def test_transaction_identity_and_original_amount_are_immutable(seeded, assignment):
    with connect(seeded) as conn:
        with pytest.raises(psycopg.errors.RestrictViolation, match="immutable"):
            conn.execute(f"UPDATE transactions SET {assignment} WHERE transaction_id = 3001")


def test_lifecycle_checks_reject_inconsistent_state(seeded):
    with connect(seeded) as conn:
        for bad in ("status = 'captured'",                                   # no timestamps, wrong sequence
                    "status = 'authorized', last_event_sequence = 2",        # authorized_at missing
                    "status = 'failed', last_event_sequence = 2, failed_at = now()",  # failure_code missing
                    "status = 'refunded'"):
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute(f"UPDATE transactions SET {bad} WHERE transaction_id = 3004")
        # A consistent state change is still possible (the Step 4 write path will own these).
        conn.execute("UPDATE transactions SET status = 'authorized', last_event_sequence = 2, "
                     "authorized_at = '2026-10-15 15:05:00+00' WHERE transaction_id = 3004")


VALID_EVENT = dict(transaction_id=3004, event_sequence=2, event_type="payment.authorized",
                   amount_minor=2000, currency="USD", failure_code=None, source="api",
                   occurred_at="2026-10-15 15:05:00+00")


def insert_event(conn, **overrides):
    row = {"event_id": uuid.uuid4(), **VALID_EVENT, **overrides}
    conn.execute(
        "INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type, amount_minor, "
        "currency, failure_code, source, occurred_at) VALUES (%(event_id)s, %(transaction_id)s, "
        "%(event_sequence)s, %(event_type)s, %(amount_minor)s, %(currency)s, %(failure_code)s, %(source)s, "
        "%(occurred_at)s)", row)
    return row["event_id"]


@pytest.mark.parametrize("overrides,error", [
    # An otherwise valid second "created" event collides on (transaction_id, event_sequence).
    ({"event_sequence": 1, "event_type": "payment.created"}, psycopg.errors.UniqueViolation),
    ({"event_sequence": 1, "event_type": "payment.created", "transaction_id": 3003},
     psycopg.errors.UniqueViolation),
    ({"event_type": "payment.created"}, psycopg.errors.CheckViolation),   # created only at sequence 1
    ({"event_sequence": 0}, psycopg.errors.CheckViolation),
    ({"event_type": "payment.refunded"}, psycopg.errors.CheckViolation),
    ({"event_type": "payment.failed"}, psycopg.errors.CheckViolation),    # failed carries no amount
    ({"event_type": "payment.failed", "amount_minor": None, "currency": None},
     psycopg.errors.CheckViolation),                                      # ...but needs a failure_code
    ({"amount_minor": None}, psycopg.errors.CheckViolation),              # amount and currency together
    ({"currency": "XXX"}, psycopg.errors.ForeignKeyViolation),
    ({"transaction_id": 9999}, psycopg.errors.ForeignKeyViolation),
    ({"source": "spreadsheet"}, psycopg.errors.CheckViolation),
])
def test_payment_event_structural_constraints(seeded, overrides, error):
    with connect(seeded) as conn:
        with pytest.raises(error):
            insert_event(conn, **overrides)


def test_valid_event_insert_and_duplicate_event_id(seeded):
    with connect(seeded) as conn:
        event_id = insert_event(conn)
        received = conn.execute("SELECT received_at FROM payment_events WHERE event_id = %s",
                                (event_id,)).fetchone()[0]
        assert received is not None  # ingestion time is set by the database
        with pytest.raises(psycopg.errors.UniqueViolation):
            insert_event(conn, event_id=event_id, event_sequence=3, event_type="payment.captured")


# --- relationships ----------------------------------------------------------------

def test_relationships_are_preserved(seeded):
    with connect(seeded) as conn:
        fks = {r[0] for r in conn.execute(
            "SELECT conrelid::regclass || ': ' || pg_get_constraintdef(oid) FROM pg_constraint WHERE contype = 'f'")}
        assert fks == {
            "transactions: FOREIGN KEY (user_id) REFERENCES users(user_id)",
            "transactions: FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)",
            "transactions: FOREIGN KEY (currency) REFERENCES currencies(currency_code)",
            "refunds: FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)",
            "payment_events: FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id) ON DELETE RESTRICT",
            "payment_events: FOREIGN KEY (currency) REFERENCES currencies(currency_code)",
        }
        assert conn.execute("SELECT count(*) FROM users").fetchone()[0] == 3
        assert conn.execute("SELECT count(*) FROM merchants").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM refunds r JOIN transactions t USING (transaction_id)"
                            ).fetchone()[0] == 2
        indexes = {r[0] for r in conn.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")}
        assert {"transactions_user_id_idx", "transactions_merchant_id_idx", "refunds_transaction_id_idx",
                "payment_events_received_at_idx", "payment_events_transaction_sequence_key"} <= indexes
        with pytest.raises((psycopg.errors.ForeignKeyViolation, psycopg.errors.RestrictViolation)):
            conn.execute("DELETE FROM transactions WHERE transaction_id = 3004")
