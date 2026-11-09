"""Step 8: sync/v2_reconcile.py detects divergence between OLTP, raw and marts (disposable databases)."""

from __future__ import annotations

import os
import uuid

import pytest

from sync.v2_reconcile import reconcile
from tests.v2.conftest import connect
from tests.v2.test_dbt_v2 import app, build_ok, env, sync  # noqa: F401  (module-scoped fixture)
from payments.events import PaymentEvent, record_payment_event
from tests.v2.test_dbt_v2 import ts
from tools.db_guard import UnsafeDatabaseTarget


def statuses(report) -> dict[str, str]:
    return {check: status for status, check, _ in report.results}


def test_clean_pipeline_reconciles(env):
    sync(env)
    build_ok(env)
    report = reconcile(env["source"], env["target"])
    assert report.failed == 0 and "source_vs_marts" in statuses(report)
    assert statuses(report)["source_vs_marts"] == "PASS"


def test_source_changes_after_the_sync_are_pending_not_errors(env):
    with app(env["source"]) as c:
        record_payment_event(c, PaymentEvent(uuid.uuid4(), 3004, 2, "payment.authorized", ts("2026-10-15 15:05:00"),
                                             "processor", 2000, "USD"))
    report = reconcile(env["source"], env["target"])
    assert report.failed == 0
    assert statuses(report)["source_vs_marts"] == "WARN"
    detail = next(d for s, c, d in report.results if c == "source_vs_raw.transactions")
    assert "1 pending next sync" in detail
    sync(env)
    build_ok(env)
    assert statuses(reconcile(env["source"], env["target"]))["source_vs_marts"] == "PASS"


@pytest.mark.parametrize("corrupt,repair,failing", [
    ("UPDATE raw.users SET email = 'tampered@x' WHERE user_id = 1",
     "UPDATE raw.users u SET email = 'alice@example.com' WHERE user_id = 1", "source_vs_raw.users"),
    ("UPDATE marts_candidate.fct_payments SET amount_minor = amount_minor + 1 WHERE transaction_id = 3001",
     "UPDATE marts_candidate.fct_payments SET amount_minor = amount_minor - 1 WHERE transaction_id = 3001",
     "marts.payments.fct_payments"),
    ("UPDATE marts_candidate.fct_daily_payments SET succeeded_refund_amount_minor = succeeded_refund_amount_minor + 100",
     "UPDATE marts_candidate.fct_daily_payments SET succeeded_refund_amount_minor = succeeded_refund_amount_minor - 100",
     "marts.refunds.fct_daily_payments"),
    ("INSERT INTO raw.merchants VALUES (999, 'ghost', 'x', 'US', now(), now(), '1', 0, now())",
     "DELETE FROM raw.merchants WHERE merchant_id = 999", "source_vs_raw.merchants"),
])
def test_divergence_is_reported(env, corrupt, repair, failing):
    with connect(env["target"]) as c:
        c.execute(corrupt)
    try:
        report = reconcile(env["source"], env["target"])
        assert statuses(report)[failing] == "FAIL" and report.failed >= 1
    finally:
        with connect(env["target"]) as c:
            c.execute(repair)
    assert reconcile(env["source"], env["target"]).failed == 0


@pytest.mark.parametrize("oltp,analytics", [("fintech_oltp", "v2_analytics"), ("v2_oltp", "fintech_analytics"),
                                            ("demo_oltp", "v2_analytics")])
def test_reconcile_refuses_non_v2_databases(oltp, analytics):
    with pytest.raises(UnsafeDatabaseTarget):
        reconcile(oltp, analytics)


# --- P2: attribution errors that leave every grand total intact ---------------------------------

TOTAL_CHECKS = ("marts.payments.fct_payments", "marts.payments.fct_daily_payments(cohort)",
                "marts.payments.fct_daily_payments(outcome)", "marts.payments.fct_merchant_performance",
                "marts.refunds.fct_payments", "marts.refunds.fct_daily_payments", "marts.refunds.fct_merchant_performance")


def test_multi_day_data_reconciles_independently(env):
    """Adds a payment created Monday / captured Tuesday and a refund that succeeds today."""
    from payments.refunds import complete_refund, request_refund
    with app(env["source"]) as c:
        for seq, kind, at in ((1, "created", "2026-11-02 22:00:00"), (2, "authorized", "2026-11-02 22:01:00"),
                              (3, "captured", "2026-11-03 08:00:00")):
            extra = dict(user_id=2, merchant_id=102) if kind == "created" else {}
            record_payment_event(c, PaymentEvent(uuid.uuid4(), 6101, seq, f"payment.{kind}", ts(at), "api", 4200, "USD",
                                                 **extra))
        r = request_refund(c, idempotency_key=f"rec-{uuid.uuid4().hex}", transaction_id=6101, amount_minor=1200,
                           currency="USD", reason="partial")
        complete_refund(c, r.refund_id, "succeeded")
    sync(env)
    build_ok(env)
    report = reconcile(env["source"], env["target"])
    assert report.failed == 0
    assert {c for s, c, _ in report.results if s == "PASS"} >= {"kpi.payments", "kpi.daily", "kpi.merchant",
                                                                 "dates.events_vs_transactions"}


@pytest.mark.parametrize("table,tamper,failing", [
    ("fct_daily_payments",   # captured volume attributed to the wrong dates (swapped)
     """UPDATE marts_candidate.fct_daily_payments d SET captured_amount_minor = CASE report_date_utc
            WHEN '2026-10-15' THEN (SELECT captured_amount_minor FROM marts_candidate.fct_daily_payments WHERE report_date_utc = '2026-11-03')
            ELSE (SELECT captured_amount_minor FROM marts_candidate.fct_daily_payments WHERE report_date_utc = '2026-10-15') END
        WHERE report_date_utc IN ('2026-10-15', '2026-11-03')""", "kpi.daily"),
    ("fct_daily_payments",   # a failure moved to another date
     """UPDATE marts_candidate.fct_daily_payments SET failed_count = failed_count + CASE report_date_utc
            WHEN '2026-10-15' THEN -1 ELSE 1 END WHERE report_date_utc IN ('2026-10-15', '2026-11-03')""", "kpi.daily"),
    ("fct_daily_payments",   # only a rate is wrong
     "UPDATE marts_candidate.fct_daily_payments SET outcome_success_rate_pct = 50.00 WHERE report_date_utc = '2026-10-15'",
     "kpi.daily"),
    ("fct_merchant_performance",   # refund volume attributed to the wrong merchant
     """UPDATE marts_candidate.fct_merchant_performance SET succeeded_refund_amount_minor = succeeded_refund_amount_minor
            + CASE merchant_id WHEN 102 THEN -1000 ELSE 1000 END WHERE report_date_utc = '2026-10-15'""", "kpi.merchant"),
    ("fct_merchant_performance",   # only the refund rate is wrong
     "UPDATE marts_candidate.fct_merchant_performance SET refund_rate_pct = refund_rate_pct + 1 "
     "WHERE merchant_id = 102 AND report_date_utc = '2026-10-15'", "kpi.merchant"),
    ("fct_payments",   # a payment's capture date is wrong (its amount and status are right)
     "UPDATE marts_candidate.fct_payments SET captured_date_utc = captured_date_utc + 1 WHERE transaction_id = 3002",
     "kpi.payments"),
])
def test_attribution_errors_with_correct_totals_are_caught(env, table, tamper, failing):
    with connect(env["target"]) as c:
        c.execute(f"CREATE TEMP TABLE keep AS SELECT * FROM marts_candidate.{table}")
        c.execute(tamper)
        try:
            report = reconcile(env["source"], env["target"])
            s = statuses(report)
            assert s[failing] == "FAIL"
            assert all(s[t] == "PASS" for t in TOTAL_CHECKS), {t: s[t] for t in TOTAL_CHECKS}   # totals still agree
        finally:
            c.execute(f"DELETE FROM marts_candidate.{table}")
            c.execute(f"INSERT INTO marts_candidate.{table} SELECT * FROM keep")
    assert reconcile(env["source"], env["target"]).failed == 0


def test_a_buggy_model_that_dates_captures_by_creation_is_caught(env, tmp_path):
    """A plausible bug in dbt itself: captured volume dated by the creation day. Two independent
    layers catch it: the dbt unit test ut_daily_dates_follow_each_metric_basis, and, if a bug
    slips past the unit tests (simulated by excluding them), the reconciliation's independent
    recomputation, while every dbt data test and every grand total still passes."""
    import shutil
    import subprocess
    from tests.v2.test_dbt_v2 import DBT, DBT_PROJECT
    buggy = tmp_path / "v2_buggy"
    shutil.copytree(DBT_PROJECT, buggy, ignore=shutil.ignore_patterns("target", "logs"))
    model = buggy / "models" / "marts" / "fct_daily_payments.sql"
    source = model.read_text()
    bug = source.replace("""captures as (
    select
        captured_date_utc                                       as report_date_utc,""", """captures as (
    select
        created_date_utc                                        as report_date_utc,""")
    assert bug != source
    model.write_text(bug)
    def dbt_buggy(*args):
        return subprocess.run([str(DBT), "--no-use-colors", *args, "--profiles-dir", str(DBT_PROJECT.parent),
                               "--target-path", str(tmp_path / "target"), "--log-path", str(tmp_path / "logs")],
                              cwd=buggy, env={**os.environ, "DBT_V2_DBNAME": env["target"]}, capture_output=True,
                              text=True, timeout=300)
    unit = dbt_buggy("test", "--select", "test_type:unit")
    assert unit.returncode != 0 and "ut_daily_dates_follow_each_metric_basis" in unit.stdout   # layer 1
    run = dbt_buggy("build", "--exclude", "test_type:unit")
    try:
        assert run.returncode == 0, run.stdout[-2000:]          # every dbt data test still passes
        s = statuses(reconcile(env["source"], env["target"]))
        assert s["kpi.daily"] == "FAIL"
        assert all(s[t] == "PASS" for t in TOTAL_CHECKS)
    finally:
        build_ok(env)                                            # restore the real candidate
    assert reconcile(env["source"], env["target"]).failed == 0
