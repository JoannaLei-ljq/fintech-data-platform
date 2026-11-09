"""Step 7: the V2 dbt project against real data (disposable v2_test_* databases).

One source/target pair per module: V001 + MVP seed + all OLTP migrations, analytics migrations,
the V2 sync, then dbt (dbt/v2, profile fintech_v2, DBT_V2_DBNAME = the disposable target).
The tests run in file order: parity first, then date attribution, then negative tests that
corrupt raw data and prove the right dbt test fails (data is detected, never corrected).
"""

from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from payments.events import PaymentEvent, record_payment_event
from payments.refunds import complete_refund, request_refund
from sync.v2_sync import SyncCredentials, run_sync
from tests.v2.conftest import ANALYTICS_MIGRATIONS, PROJECT_ROOT, connect, quiet
from tests.v2.test_lifecycle import build
from tools.migrate import migrate

DBT = PROJECT_ROOT / ".venv-dbt" / "bin" / "dbt"
DBT_PROJECT = PROJECT_ROOT / "dbt" / "v2"
UTC = timezone.utc
pytestmark = pytest.mark.skipif(not DBT.exists(), reason="dbt virtualenv (.venv-dbt) not installed")


def ts(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    from tests.v2.conftest import _admin
    suffix = uuid.uuid4().hex[:6]
    source, target = f"v2_test_dbt_src_{suffix}", f"v2_test_dbt_tgt_{suffix}"
    with _admin() as admin:
        for name in (source, target):
            admin.execute(f'CREATE DATABASE "{name}"')
    try:
        made = iter([source])
        build(lambda _label: next(made), "dbt")          # V001 + MVP seed + V002..V008
        migrate(target, ANALYTICS_MIGRATIONS, log=quiet)
        artifacts = tmp_path_factory.mktemp("dbt_v2")
        yield {"source": source, "target": target, "artifacts": artifacts}
    finally:
        with _admin() as admin:
            for name in (source, target):
                assert name.startswith("v2_test_")
                admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def sync(env) -> None:
    run_sync(env["source"], env["target"], credentials=SyncCredentials.admin(), allow_superuser=True)


def dbt(env, *args: str, dbname: str | None = None) -> subprocess.CompletedProcess:
    run_env = {**os.environ, "DBT_V2_DBNAME": dbname or env["target"]}
    paths = ["--log-path", str(env["artifacts"] / "logs")]
    if args[0] != "debug":
        paths += ["--target-path", str(env["artifacts"] / "target")]
    return subprocess.run(
        [str(DBT), "--no-use-colors", *args, "--profiles-dir", "..", *paths],
        cwd=DBT_PROJECT, env=run_env, capture_output=True, text=True, timeout=300)


def build_ok(env) -> str:
    result = dbt(env, "build")
    assert result.returncode == 0, result.stdout[-4000:]
    return result.stdout


def daily(env, date: str, currency: str = "USD") -> dict:
    with connect(env["target"]) as c:
        cur = c.execute("SELECT * FROM marts_candidate.fct_daily_payments WHERE report_date_utc = %s AND currency = %s",
                        (date, currency))
        row = cur.fetchone()
        return dict(zip([d.name for d in cur.description], row)) if row else {}


def app(db: str):
    c = connect(db)
    c.execute("SET ROLE v2_payment_app")
    return c


# --- parity with the MVP demo -----------------------------------------------------------------

def test_baseline_builds_and_matches_mvp_parity(env):
    sync(env)
    out = build_ok(env)
    assert "ERROR=0" in out and "WARN=0" in out
    d = daily(env, "2026-10-15")
    # $150 / 66.67% / $30 on both date bases (every MVP event happened on 2026-10-15)
    assert (d["cohort_captured_amount_minor"], d["cohort_success_rate_pct"]) == (15000, Decimal("66.67"))
    assert (d["captured_amount_minor"], d["outcome_success_rate_pct"]) == (15000, Decimal("66.67"))
    assert d["succeeded_refund_amount_minor"] == 3000
    assert (d["created_payment_count"], d["cohort_in_flight_count"], d["estimated_capture_time_count"]) == (4, 1, 2)


def test_parity_after_the_pending_payment_completes(env):
    with app(env["source"]) as c:
        record_payment_event(c, PaymentEvent(uuid.uuid4(), 3004, 2, "payment.authorized",
                                             ts("2026-10-15 15:05:00"), "processor", 2000, "USD"))
        record_payment_event(c, PaymentEvent(uuid.uuid4(), 3004, 3, "payment.captured",
                                             ts("2026-10-15 15:06:00"), "processor", 2000, "USD"))
    sync(env)
    build_ok(env)
    d = daily(env, "2026-10-15")
    assert (d["cohort_captured_amount_minor"], d["cohort_success_rate_pct"]) == (17000, Decimal("75.00"))   # $170 / 75%
    assert (d["captured_amount_minor"], d["outcome_success_rate_pct"]) == (17000, Decimal("75.00"))
    assert d["succeeded_refund_amount_minor"] == 3000                                           # $30
    assert d["estimated_capture_time_count"] == 2      # 3004's capture time was observed, not synthetic


# --- date attribution ------------------------------------------------------------------------

def test_created_monday_captured_tuesday(env):
    with app(env["source"]) as c:
        record_payment_event(c, PaymentEvent(uuid.uuid4(), 6001, 1, "payment.created", ts("2026-11-02 22:00:00"),
                                             "api", 4200, "USD", user_id=2, merchant_id=102))
        record_payment_event(c, PaymentEvent(uuid.uuid4(), 6001, 2, "payment.authorized", ts("2026-11-02 22:01:00"),
                                             "processor", 4200, "USD"))
        record_payment_event(c, PaymentEvent(uuid.uuid4(), 6001, 3, "payment.captured", ts("2026-11-03 08:00:00"),
                                             "processor", 4200, "USD"))
    sync(env)
    build_ok(env)
    monday, tuesday = daily(env, "2026-11-02"), daily(env, "2026-11-03")
    assert (monday["created_payment_count"], monday["cohort_captured_amount_minor"], monday["captured_amount_minor"]) == \
           (1, 4200, 0)
    assert (tuesday["created_payment_count"], tuesday["captured_count"], tuesday["captured_amount_minor"]) == (0, 1, 4200)
    assert monday["cohort_success_rate_pct"] == 100 and monday["outcome_success_rate_pct"] is None   # 0/0 -> NULL


def test_refunds_count_on_their_succeeded_date_and_only_when_succeeded(env):
    with app(env["source"]) as c:
        ok = request_refund(c, idempotency_key=f"t-{uuid.uuid4().hex}", transaction_id=6001, amount_minor=1000,
                            currency="USD", reason="partial")
        bad = request_refund(c, idempotency_key=f"t-{uuid.uuid4().hex}", transaction_id=6001, amount_minor=500,
                             currency="USD", reason="partial")
        request_refund(c, idempotency_key=f"t-{uuid.uuid4().hex}", transaction_id=6001, amount_minor=700,
                       currency="USD", reason="still open")
        complete_refund(c, ok.refund_id, "succeeded")
        complete_refund(c, bad.refund_id, "failed", failure_code="processor_declined")
    sync(env)
    build_ok(env)
    today = datetime.now(UTC).date().isoformat()
    assert daily(env, today)["succeeded_refund_amount_minor"] == 1000          # failed 500 and open 700 excluded
    assert daily(env, "2026-11-03")["succeeded_refund_amount_minor"] == 0       # not on the capture date
    with connect(env["target"]) as c:
        row = c.execute("SELECT amount_minor, succeeded_refund_amount_minor, pending_refund_amount_minor, "
                        "failed_refund_amount_minor, remaining_refundable_amount_minor, net_captured_amount_minor "
                        "FROM marts_candidate.fct_payments WHERE transaction_id = 6001").fetchone()
        assert row == (4200, 1000, 700, 500, 2500, 3200)
        assert c.execute("SELECT count(*) FROM marts_candidate.fct_payments").fetchone()[0] == 5   # no fan-out


# --- negative tests: corrupted raw data is detected, not corrected -----------------------------

CORRUPTIONS = [
    pytest.param("UPDATE raw.transactions SET status = 'authorized' WHERE transaction_id = 3002",
                 "UPDATE raw.transactions SET status = 'captured' WHERE transaction_id = 3002",
                 "assert_no_payment_event_issues", id="state_mismatch"),
    pytest.param("INSERT INTO raw.payment_events VALUES ('00000000-0000-4000-8000-00000000dead', 3001, 5, "
                 "'payment.captured', 10000, 'USD', NULL, 'api', now(), now(), '1', 0, now())",
                 "DELETE FROM raw.payment_events WHERE event_id = '00000000-0000-4000-8000-00000000dead'",
                 "assert_no_payment_event_issues", id="sequence_gap_after_terminal"),
    pytest.param("INSERT INTO raw.refunds VALUES (9901, 3002, 6000, 'USD', 'x', 'succeeded', 'neg-1', NULL, now(), "
                 "now(), now(), now(), NULL, '1', 0, now())",
                 "DELETE FROM raw.refunds WHERE refund_id = 9901",
                 "assert_refunds_within_payment_ceiling", id="over_refund"),
    pytest.param("INSERT INTO raw.refunds VALUES (9902, 3003, 100, 'USD', 'x', 'succeeded', 'neg-2', NULL, now(), "
                 "now(), now(), now(), NULL, '1', 0, now())",
                 "DELETE FROM raw.refunds WHERE refund_id = 9902",
                 "assert_refunds_within_payment_ceiling", id="refund_on_failed_payment"),
    pytest.param("INSERT INTO raw.refunds VALUES (9903, 3002, 100, 'EUR', 'x', 'requested', 'neg-3', NULL, now(), "
                 "now(), now(), NULL, NULL, '1', 0, now())",
                 "DELETE FROM raw.refunds WHERE refund_id = 9903",
                 "assert_refund_currency_matches_payment", id="refund_currency_mismatch"),
    pytest.param("INSERT INTO raw.refunds VALUES (9904, 3002, 100, 'USD', 'x', 'failed', 'neg-4', 'declined', now(), "
                 "now(), now(), now(), now(), '1', 0, now())",
                 "DELETE FROM raw.refunds WHERE refund_id = 9904",
                 "assert_refund_lifecycle_timestamps", id="failed_refund_with_success_time"),
    pytest.param("INSERT INTO raw.transactions VALUES (9905, 1, 101, 500, 'USD', 'created', 1, NULL, now(), now(), "
                 "NULL, NULL, NULL, '1', 0, now())",
                 "DELETE FROM raw.transactions WHERE transaction_id = 9905",
                 "assert_no_payment_event_issues", id="payment_without_events"),
]


@pytest.mark.parametrize("corrupt,repair,failing_test", CORRUPTIONS)
def test_corrupted_raw_data_fails_the_matching_test(env, corrupt, repair, failing_test):
    with connect(env["target"]) as c:
        c.execute("ALTER TABLE raw.payment_events DISABLE TRIGGER payment_events_insert_only")
        c.execute(corrupt)
    try:
        result = dbt(env, "test", "--select", failing_test)
        assert result.returncode != 0, result.stdout[-3000:]
        assert f"FAIL" in result.stdout and failing_test in result.stdout
    finally:
        with connect(env["target"]) as c:
            c.execute(repair)
            c.execute("ALTER TABLE raw.payment_events ENABLE TRIGGER payment_events_insert_only")
    assert dbt(env, "test", "--select", failing_test).returncode == 0     # clean again after the repair


def test_injected_failure_switch(env):
    assert dbt(env, "test", "--select", "assert_injected_failure").returncode == 0
    failed = dbt(env, "test", "--select", "assert_injected_failure", "--vars", "{v2_inject_test_failure: true}")
    assert failed.returncode != 0 and "assert_injected_failure" in failed.stdout


@pytest.mark.parametrize("dbname", ["postgres", "demo_analytics", "fintech_analytics"])
def test_profile_refuses_non_v2_databases(env, dbname):
    """The profile maps any non-v2_* name to a database that does not exist, so dbt cannot even
    connect there (nothing is created or read in the named database)."""
    result = dbt(env, "debug", dbname=dbname)
    assert result.returncode != 0
    assert "refused_non_v2_database" in result.stdout
