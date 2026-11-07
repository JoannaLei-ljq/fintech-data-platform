"""Step 2: currencies, minor units and exact conversion of existing amounts."""

from __future__ import annotations

from decimal import Decimal

import psycopg
import pytest

from tests.v2.conftest import MVP_DEMO_SEED, OLTP_MIGRATIONS, connect, quiet, run_sql_file
from tools.migrate import migrate

PEOPLE = """
INSERT INTO users (user_id, email, full_name, country) VALUES (1, 'a@example.com', 'A', 'US');
INSERT INTO merchants (merchant_id, merchant_name, category, country) VALUES (101, 'M', 'retail', 'US');
"""

# (transaction_id, amount, currency, expected amount_minor)
MULTI_CURRENCY = [
    (1, Decimal("12.34"), "USD", 1234),
    (2, Decimal("480.00"), "GBP", 48000),
    (3, Decimal("1500.00"), "JPY", 1500),      # zero-decimal currency
    (4, Decimal("50000.00"), "KRW", 50000),    # zero-decimal currency
    (5, Decimal("12.34"), "KWD", 12340),       # three-decimal currency
]


def baseline_with(db: str, transactions: list[tuple], refunds: list[tuple] = ()) -> None:
    migrate(db, OLTP_MIGRATIONS, target_version=1, log=quiet)
    with connect(db) as conn:
        conn.execute(PEOPLE)
        for tid, amount, currency, *_ in transactions:
            conn.execute(
                "INSERT INTO transactions (transaction_id, user_id, merchant_id, amount, currency, status, "
                "created_at, updated_at) VALUES (%s, 1, 101, %s, %s, 'succeeded', "
                "'2026-10-01 00:00:00+00', '2026-10-01 00:00:05+00')", (tid, amount, currency))
        for rid, tid, amount in refunds:
            conn.execute(
                "INSERT INTO refunds (refund_id, transaction_id, amount, reason, status, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'test', 'succeeded', '2026-10-02 00:00:00+00', '2026-10-02 00:00:09+00')",
                (rid, tid, amount))


def versions(db: str) -> list[int]:
    with connect(db) as conn:
        return [v for (v,) in conn.execute("SELECT version FROM schema_migrations ORDER BY 1")]


def test_currency_reference_data(make_db):
    db = make_db("cur")
    migrate(db, OLTP_MIGRATIONS, target_version=2, log=quiet)
    with connect(db) as conn:
        units = dict(conn.execute("SELECT currency_code, minor_units FROM currencies"))
    assert units == {"USD": 2, "EUR": 2, "GBP": 2, "JPY": 0, "KRW": 0, "KWD": 3, "BHD": 3}


def test_multi_currency_amounts_convert_exactly(make_db):
    db = make_db("cur_conv")
    baseline_with(db, MULTI_CURRENCY, refunds=[(10, 1, Decimal("2.34")), (11, 3, Decimal("500.00")),
                                               (12, 5, Decimal("0.01"))])
    with connect(db) as conn:
        updated_before = dict(conn.execute("SELECT transaction_id, updated_at FROM transactions"))
    migrate(db, OLTP_MIGRATIONS, target_version=2, log=quiet)

    with connect(db) as conn:
        minor = dict(conn.execute("SELECT transaction_id, amount_minor FROM transactions"))
        legacy = dict(conn.execute("SELECT transaction_id, amount FROM legacy_transactions"))
        refund_minor = dict(conn.execute("SELECT refund_id, amount_minor FROM refunds"))
        refund_legacy = dict(conn.execute("SELECT refund_id, amount FROM legacy_refunds"))
        updated_after = dict(conn.execute("SELECT transaction_id, updated_at FROM transactions"))
        columns = {c for (c,) in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'transactions'")}

    assert minor == {tid: expected for tid, _, _, expected in MULTI_CURRENCY}
    assert legacy == {tid: amount for tid, amount, *_ in MULTI_CURRENCY}  # original values preserved
    assert legacy[3] == Decimal("1500") and legacy[5] == Decimal("12.340")  # currency's own scale
    assert refund_minor == {10: 234, 11: 500, 12: 10}                      # KWD 0.01 -> 10 fils
    assert refund_legacy == {10: Decimal("2.34"), 11: Decimal("500"), 12: Decimal("0.010")}
    assert updated_after == updated_before                                 # backfill is not a business change
    assert "amount" not in columns and "amount_minor" in columns


@pytest.mark.parametrize("bad_row,message", [
    ((1, Decimal("1500.50"), "JPY"), "not exactly representable"),
    ((1, Decimal("10.00"), "XYZ"), "unknown currency code"),
])
def test_unconvertible_data_aborts_whole_migration(make_db, bad_row, message):
    db = make_db("cur_abort")
    baseline_with(db, [bad_row])
    with pytest.raises(psycopg.errors.RaiseException, match=message):
        migrate(db, OLTP_MIGRATIONS, target_version=2, log=quiet)
    assert versions(db) == [1]
    with connect(db) as conn:  # nothing changed: still the baseline shape and value
        assert conn.execute("SELECT amount, currency FROM transactions").fetchone() == bad_row[1:]
        assert conn.execute("SELECT to_regclass('currencies')").fetchone()[0] is None


def test_new_rows_are_validated(make_db):
    db = make_db("cur_new")
    migrate(db, OLTP_MIGRATIONS, target_version=2, log=quiet)
    with connect(db) as conn:
        conn.execute(PEOPLE)
        insert = ("INSERT INTO transactions (transaction_id, user_id, merchant_id, amount_minor, currency, status) "
                  "VALUES (%s, 1, 101, %s, %s, 'succeeded')")
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            conn.execute(insert, (1, 100, "XXX"))
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(insert, (2, 0, "USD"))
        # Three-decimal amounts were impossible in numeric(18,2); minor units store them exactly.
        conn.execute(insert, (3, 12345, "KWD"))
        conn.execute(insert, (4, 7, "JPY"))
        assert dict(conn.execute("SELECT transaction_id, amount FROM legacy_transactions")) == {
            3: Decimal("12.345"), 4: Decimal("7")}


def test_mvp_demo_seed_converts(make_db):
    db = make_db("cur_seed")
    migrate(db, OLTP_MIGRATIONS, target_version=1, log=quiet)
    run_sql_file(db, MVP_DEMO_SEED)
    migrate(db, OLTP_MIGRATIONS, target_version=2, log=quiet)
    with connect(db) as conn:
        assert dict(conn.execute("SELECT transaction_id, amount_minor FROM transactions")) == {
            3001: 10000, 3002: 5000, 3003: 3000, 3004: 2000}
        assert dict(conn.execute("SELECT refund_id, amount_minor FROM refunds")) == {7001: 1000, 7002: 2000}
        assert dict(conn.execute("SELECT transaction_id, amount FROM legacy_transactions")) == {
            3001: Decimal("100.00"), 3002: Decimal("50.00"), 3003: Decimal("30.00"), 3004: Decimal("20.00")}
