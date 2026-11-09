"""P2: the independent KPI reference calculator, on hand-built rows (no database)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from sync.v2_kpi_reference import Event, Payment, Refund, compare, compute, pct, DAILY_FIELDS

UTC = timezone.utc


def t(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def lifecycle(tid, merchant, currency, amount, created, *steps):
    """A payment plus its events; steps are (event_type, time) after payment.created."""
    status = steps[-1][0].removeprefix("payment.") if steps else "created"
    times = {k: v for k, v in steps}
    p = Payment(tid, merchant, currency, amount, status, t(created),
                t(times["payment.captured"]) if "payment.captured" in times else None,
                t(times["payment.failed"]) if "payment.failed" in times else None)
    events = [Event(tid, "payment.created", t(created))] + [Event(tid, k, t(v)) for k, v in steps]
    return p, events


def build():
    rows = [
        lifecycle(1, 101, "USD", 4200, "2026-11-02 22:00:00", ("payment.authorized", "2026-11-02 22:01:00"),
                  ("payment.captured", "2026-11-03 08:00:00")),                      # created Mon, captured Tue
        lifecycle(2, 101, "USD", 1000, "2026-11-02 10:00:00", ("payment.failed", "2026-11-02 10:05:00")),
        lifecycle(3, 102, "USD", 3000, "2026-11-02 11:00:00", ("payment.authorized", "2026-11-02 11:01:00"),
                  ("payment.captured", "2026-11-02 11:02:00")),
        lifecycle(4, 102, "JPY", 1500, "2026-11-02 12:00:00", ("payment.authorized", "2026-11-02 12:01:00"),
                  ("payment.captured", "2026-11-02 12:02:00")),
        lifecycle(5, 101, "USD", 900, "2026-11-05 09:00:00"),                                # in flight
    ]
    payments = [p for p, _ in rows]
    events = [e for _, es in rows for e in es]
    refunds = [
        Refund(9, 1, "USD", 700, "succeeded", t("2026-11-06 12:00:00")),   # requested Wed (not modelled), succeeded Fri
        Refund(10, 1, "USD", 500, "failed"),
        Refund(11, 1, "USD", 300, "requested"),
        Refund(12, 3, "USD", 3000, "succeeded", t("2026-11-02 18:00:00")),  # full refund, same day
    ]
    return compute(payments, events, refunds)


def test_dates_follow_each_metric_basis():
    d = build().daily
    mon, tue, fri = d[(date(2026, 11, 2), "USD")], d[(date(2026, 11, 3), "USD")], d[(date(2026, 11, 6), "USD")]
    assert (mon["created_payment_count"], mon["cohort_captured_amount_minor"], mon["captured_amount_minor"]) == (3, 7200, 3000)
    assert (tue["created_payment_count"], tue["captured_count"], tue["captured_amount_minor"]) == (0, 1, 4200)
    assert (fri["succeeded_refund_amount_minor"], fri["net_captured_amount_minor"]) == (700, -700)
    assert mon["succeeded_refund_amount_minor"] == 3000 and mon["net_captured_amount_minor"] == 0


def test_rates_and_zero_denominators():
    d = build().daily
    mon = d[(date(2026, 11, 2), "USD")]
    assert mon["cohort_success_rate_pct"] == Decimal("66.67")      # 2 captured / (2 + 1 failed), created Mon
    assert mon["outcome_success_rate_pct"] == Decimal("50.00")     # Mon outcomes: 1 capture, 1 failure
    in_flight_only = d[(date(2026, 11, 5), "USD")]
    assert in_flight_only["cohort_success_rate_pct"] is None and in_flight_only["outcome_success_rate_pct"] is None
    assert pct(1, 3) == Decimal("33.33") and pct(2, 3) == Decimal("66.67") and pct(1, 8) == Decimal("12.50")


def test_currencies_never_mix():
    d = build().daily
    assert d[(date(2026, 11, 2), "JPY")]["captured_amount_minor"] == 1500
    assert d[(date(2026, 11, 2), "USD")]["captured_amount_minor"] == 3000


def test_merchant_split_and_refund_rate_by_capture_cohort():
    m = build().merchant
    m101_tue, m102_mon = m[(101, date(2026, 11, 3), "USD")], m[(102, date(2026, 11, 2), "USD")]
    assert (m101_tue["captured_amount_minor"], m101_tue["refunded_of_captured_amount_minor"], m101_tue["refund_rate_pct"]) == \
           (4200, 700, Decimal("16.67"))         # refund succeeded Fri, counted against the Tue capture cohort
    assert m[(101, date(2026, 11, 6), "USD")]["succeeded_refund_amount_minor"] == 700   # and as a Fri refund flow
    assert (m102_mon["refund_rate_pct"], m102_mon["net_captured_amount_minor"]) == (Decimal("100.00"), 0)
    assert m[(101, date(2026, 11, 2), "USD")]["refund_rate_pct"] is None              # nothing captured


def test_payment_level_refund_measures():
    p = build().payments[1]
    assert (p["succeeded_refund_amount_minor"], p["failed_refund_amount_minor"], p["pending_refund_amount_minor"],
            p["remaining_refundable_amount_minor"], p["net_captured_amount_minor"], p["refund_request_count"]) == \
           (700, 500, 300, 3200, 3500, 3)
    assert build().payments[3]["is_fully_refunded"] is True
    assert build().payments[2]["remaining_refundable_amount_minor"] == 0               # failed payment


def test_disagreeing_date_sources_are_reported():
    p, events = lifecycle(1, 101, "USD", 100, "2026-11-02 09:00:00", ("payment.authorized", "2026-11-02 09:01:00"),
                          ("payment.captured", "2026-11-02 09:02:00"))
    wrong = Payment(p.transaction_id, p.merchant_id, p.currency, p.amount_minor, p.status, p.created_at,
                    t("2026-11-03 09:02:00"), None)
    assert compute([wrong], events, []).date_issues


def test_compare_reports_missing_unexpected_and_differing_rows():
    expected = {("a",): {f: 0 for f in DAILY_FIELDS}, ("b",): {f: 0 for f in DAILY_FIELDS}}
    actual = {("b",): {**{f: 0 for f in DAILY_FIELDS}, "captured_count": 1}, ("c",): {f: 0 for f in DAILY_FIELDS}}
    problems = compare(expected, actual, DAILY_FIELDS)
    assert any("missing row" in p for p in problems) and any("unexpected row" in p for p in problems)
    assert any("captured_count: expected 0, mart has 1" in p for p in problems)
