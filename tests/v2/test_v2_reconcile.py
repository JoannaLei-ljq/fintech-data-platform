"""Step 8: sync/v2_reconcile.py detects divergence between OLTP, raw and marts (disposable databases)."""

from __future__ import annotations

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
    ("UPDATE marts.fct_payments SET amount_minor = amount_minor + 1 WHERE transaction_id = 3001",
     "UPDATE marts.fct_payments SET amount_minor = amount_minor - 1 WHERE transaction_id = 3001",
     "marts.payments.fct_payments"),
    ("UPDATE marts.fct_daily_payments SET succeeded_refund_amount_minor = succeeded_refund_amount_minor + 100",
     "UPDATE marts.fct_daily_payments SET succeeded_refund_amount_minor = succeeded_refund_amount_minor - 100",
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
