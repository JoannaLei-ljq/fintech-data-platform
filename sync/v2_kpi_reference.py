"""Independent reference implementation of the V2 mart KPIs, in plain Python.

It recomputes, from raw rows only, every metric that fct_payments, fct_daily_payments and
fct_merchant_performance expose, so reconciliation can compare the marts against values that
did NOT come from dbt's transformations:
  * capture / failure dates come from the payment EVENTS (payment.captured / payment.failed
    occurred_at), whereas dbt dates them by transactions.captured_at / failed_at; the two are
    also cross-checked against each other;
  * refunds are dated by refunds.succeeded_at and attributed to the payment's merchant;
  * rates use Decimal with ROUND_HALF_UP to 2 places (PostgreSQL's numeric round() is half away
    from zero, identical for these non-negative values) and are None on a zero denominator.

Definitions (docs/v2/PHASE1_DESIGN.md, Step 7):
  daily (date, currency)
    cohort_*                 payments created that day, by current status
    captured_* / failed      on the capture / failure date
    succeeded_refund_*       on the succeeded_at date (requested and failed refunds excluded)
    net_captured             captured_amount - succeeded_refund_amount (same day)
  merchant (merchant, date, currency): created count, captured / failed (outcome dates),
    outcome rate, succeeded refunds (succeeded_at), net, and refund rate = refunds so far of the
    payments captured that day / their captured amount.
Small-data implementation: rows are held in memory (fine for this project; documented limit).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

DAILY_FIELDS = ("created_payment_count", "cohort_captured_count", "cohort_failed_count", "cohort_in_flight_count",
                "cohort_captured_amount_minor", "cohort_success_rate_pct", "captured_count", "captured_amount_minor",
                "failed_count", "outcome_success_rate_pct", "succeeded_refund_count", "succeeded_refund_amount_minor",
                "net_captured_amount_minor")
MERCHANT_FIELDS = ("created_payment_count", "captured_count", "captured_amount_minor", "failed_count",
                   "outcome_success_rate_pct", "succeeded_refund_count", "succeeded_refund_amount_minor",
                   "net_captured_amount_minor", "refunded_of_captured_amount_minor", "refund_rate_pct")
PAYMENT_FIELDS = ("merchant_id", "currency", "amount_minor", "status", "created_date_utc", "captured_date_utc",
                  "failed_date_utc", "refund_request_count", "pending_refund_count", "succeeded_refund_count",
                  "failed_refund_count", "requested_refund_amount_minor", "pending_refund_amount_minor",
                  "succeeded_refund_amount_minor", "failed_refund_amount_minor", "remaining_refundable_amount_minor",
                  "net_captured_amount_minor", "is_fully_refunded")


@dataclass(frozen=True)
class Payment:
    transaction_id: int
    merchant_id: int
    currency: str
    amount_minor: int
    status: str
    created_at: datetime
    captured_at: datetime | None = None
    failed_at: datetime | None = None


@dataclass(frozen=True)
class Event:
    transaction_id: int
    event_type: str
    occurred_at: datetime


@dataclass(frozen=True)
class Refund:
    refund_id: int
    transaction_id: int
    currency: str
    amount_minor: int
    status: str
    succeeded_at: datetime | None = None


@dataclass
class Reference:
    payments: dict[int, dict] = field(default_factory=dict)
    daily: dict[tuple[date, str], dict] = field(default_factory=dict)
    merchant: dict[tuple[int, date, str], dict] = field(default_factory=dict)
    date_issues: list[str] = field(default_factory=list)


def utc_day(ts: datetime | None) -> date | None:
    return None if ts is None else ts.astimezone(timezone.utc).date()


def pct(numerator: int, denominator: int) -> Decimal | None:
    if denominator == 0:
        return None
    return (Decimal(100) * numerator / denominator).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def compute(payments: list[Payment], events: list[Event], refunds: list[Refund]) -> Reference:
    ref = Reference()
    by_type: dict[tuple[int, str], datetime] = {}
    for e in events:
        by_type[(e.transaction_id, e.event_type)] = e.occurred_at
    refunds_of: dict[int, list[Refund]] = defaultdict(list)
    for r in refunds:
        refunds_of[r.transaction_id].append(r)
    merchant_of = {p.transaction_id: p.merchant_id for p in payments}

    daily: dict = defaultdict(lambda: defaultdict(int))
    merchant: dict = defaultdict(lambda: defaultdict(int))

    for p in payments:
        captured_event = by_type.get((p.transaction_id, "payment.captured"))
        failed_event = by_type.get((p.transaction_id, "payment.failed"))
        created_event = by_type.get((p.transaction_id, "payment.created"))
        # Cross-check the two independent sources of each lifecycle date.
        for label, from_event, from_row in (("created", created_event, p.created_at),
                                            ("captured", captured_event, p.captured_at),
                                            ("failed", failed_event, p.failed_at)):
            if from_event != from_row:
                ref.date_issues.append(f"{p.transaction_id} {label}: event {from_event} vs transactions {from_row}")
        if p.status == "captured" and captured_event is None:
            ref.date_issues.append(f"{p.transaction_id}: captured without a payment.captured event")
        if p.status == "failed" and failed_event is None:
            ref.date_issues.append(f"{p.transaction_id}: failed without a payment.failed event")

        is_captured, is_failed = p.status == "captured", p.status == "failed"
        rs = refunds_of.get(p.transaction_id, [])
        succeeded = sum(r.amount_minor for r in rs if r.status == "succeeded")
        pending = sum(r.amount_minor for r in rs if r.status == "requested")
        failed_refunds = sum(r.amount_minor for r in rs if r.status == "failed")
        created_day = utc_day(p.created_at)
        capture_day = utc_day(captured_event) if is_captured else None
        failure_day = utc_day(failed_event) if is_failed else None
        ref.payments[p.transaction_id] = {
            "merchant_id": p.merchant_id, "currency": p.currency, "amount_minor": p.amount_minor, "status": p.status,
            "created_date_utc": created_day, "captured_date_utc": capture_day, "failed_date_utc": failure_day,
            "refund_request_count": len(rs),
            "pending_refund_count": sum(r.status == "requested" for r in rs),
            "succeeded_refund_count": sum(r.status == "succeeded" for r in rs),
            "failed_refund_count": sum(r.status == "failed" for r in rs),
            "requested_refund_amount_minor": sum(r.amount_minor for r in rs),
            "pending_refund_amount_minor": pending,
            "succeeded_refund_amount_minor": succeeded,
            "failed_refund_amount_minor": failed_refunds,
            "remaining_refundable_amount_minor": p.amount_minor - pending - succeeded if is_captured else 0,
            "net_captured_amount_minor": p.amount_minor - succeeded if is_captured else 0,
            "is_fully_refunded": is_captured and succeeded == p.amount_minor,
        }

        cohort = daily[(created_day, p.currency)]
        cohort["created_payment_count"] += 1
        cohort["cohort_captured_count"] += is_captured
        cohort["cohort_failed_count"] += is_failed
        cohort["cohort_in_flight_count"] += p.status in ("created", "authorized")
        cohort["cohort_captured_amount_minor"] += p.amount_minor if is_captured else 0
        merchant[(p.merchant_id, created_day, p.currency)]["created_payment_count"] += 1
        if is_captured:
            for bucket in (daily[(capture_day, p.currency)], merchant[(p.merchant_id, capture_day, p.currency)]):
                bucket["captured_count"] += 1
                bucket["captured_amount_minor"] += p.amount_minor
            merchant[(p.merchant_id, capture_day, p.currency)]["refunded_of_captured_amount_minor"] += succeeded
        if is_failed:
            daily[(failure_day, p.currency)]["failed_count"] += 1
            merchant[(p.merchant_id, failure_day, p.currency)]["failed_count"] += 1

    for r in refunds:
        if r.status != "succeeded":
            continue
        day = utc_day(r.succeeded_at)
        for bucket in (daily[(day, r.currency)], merchant[(merchant_of[r.transaction_id], day, r.currency)]):
            bucket["succeeded_refund_count"] += 1
            bucket["succeeded_refund_amount_minor"] += r.amount_minor

    for key, m in daily.items():
        row = {f: m.get(f, 0) for f in DAILY_FIELDS}
        row["cohort_success_rate_pct"] = pct(row["cohort_captured_count"],
                                             row["cohort_captured_count"] + row["cohort_failed_count"])
        row["outcome_success_rate_pct"] = pct(row["captured_count"], row["captured_count"] + row["failed_count"])
        row["net_captured_amount_minor"] = row["captured_amount_minor"] - row["succeeded_refund_amount_minor"]
        ref.daily[key] = row
    for key, m in merchant.items():
        row = {f: m.get(f, 0) for f in MERCHANT_FIELDS}
        row["outcome_success_rate_pct"] = pct(row["captured_count"], row["captured_count"] + row["failed_count"])
        row["net_captured_amount_minor"] = row["captured_amount_minor"] - row["succeeded_refund_amount_minor"]
        row["refund_rate_pct"] = pct(row["refunded_of_captured_amount_minor"], row["captured_amount_minor"])
        ref.merchant[key] = row
    return ref


def load_raw(conn) -> Reference:
    """Read raw.* (session TimeZone UTC) and compute the reference."""
    payments = [Payment(*r) for r in conn.execute(
        "SELECT transaction_id, merchant_id, currency, amount_minor, status, created_at, captured_at, failed_at "
        "FROM raw.transactions")]
    events = [Event(*r) for r in conn.execute("SELECT transaction_id, event_type, occurred_at FROM raw.payment_events")]
    refunds = [Refund(*r) for r in conn.execute(
        "SELECT refund_id, transaction_id, currency, amount_minor, status, succeeded_at FROM raw.refunds")]
    return compute(payments, events, refunds)


def _normalize(value):
    if isinstance(value, Decimal) and value == value.to_integral_value() and value.as_tuple().exponent == 0:
        return int(value)
    return value


def compare(expected: dict, actual: dict, fields: tuple[str, ...]) -> list[str]:
    """Differences between expected and actual rows keyed alike; [] when identical."""
    problems = [f"missing row {k}" for k in sorted(set(expected) - set(actual), key=str)]
    problems += [f"unexpected row {k}" for k in sorted(set(actual) - set(expected), key=str)]
    for key in sorted(set(expected) & set(actual), key=str):
        for f in fields:
            want, got = expected[key][f], _normalize(actual[key][f])
            if want != got:
                problems.append(f"{key} {f}: expected {want}, mart has {got}")
    return problems


def mart_rows(conn, schema: str) -> tuple[dict, dict, dict]:
    def fetch(query: str, key_len: int) -> dict:
        cur = conn.execute(query)
        names = [d.name for d in cur.description]
        out = {}
        for row in cur:
            record = dict(zip(names, row))
            out[row[0] if key_len == 1 else tuple(row[:key_len])] = record
        return out
    payments = fetch(f"SELECT transaction_id, {', '.join(PAYMENT_FIELDS)} FROM {schema}.fct_payments", 1)
    daily = fetch(f"SELECT report_date_utc, currency, {', '.join(DAILY_FIELDS)} FROM {schema}.fct_daily_payments", 2)
    merchant = fetch(f"SELECT merchant_id, report_date_utc, currency, {', '.join(MERCHANT_FIELDS)} "
                     f"FROM {schema}.fct_merchant_performance", 3)
    return payments, daily, merchant
