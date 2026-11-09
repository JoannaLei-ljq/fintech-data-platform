"""V2 pipeline run summary: last sync run, dbt results, daily KPIs and pipeline duration.

Read-only; prints to stdout (the Airflow task log). Amounts are shown in major units by exact
Decimal scaling with each currency's minor_units (never floats). Nothing sensitive is printed.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

from sync.v2_reconcile import _connect


def major(amount_minor, minor_units) -> str:
    return f"{Decimal(int(amount_minor)).scaleb(-int(minor_units)):.{int(minor_units)}f}"


def sync_summary(conn) -> None:
    row = conn.execute("""
        SELECT run_id, mode, status, started_at, finished_at, watermark_from::text, watermark_to::text,
               rows_read, rows_inserted, rows_updated, rows_reread, duplicate_events, watermark_lag_xids,
               oldest_open_writer_secs
        FROM meta.sync_runs WHERE status = 'succeeded' ORDER BY run_id DESC LIMIT 1""").fetchone()
    if row is None:
        print("sync: no successful run yet")
        return
    (run_id, mode, status, started, finished, w_from, w_to, read, inserted, updated, reread, dups, lag,
     oldest) = row
    print(f"sync run {run_id} ({mode}, {status}) {started:%Y-%m-%dT%H:%M:%SZ} -> {finished:%H:%M:%SZ}")
    print(f"  watermark {w_from} -> {w_to}, lag {lag} xids, oldest open writer {oldest if oldest is not None else '-'}s")
    print(f"  rows read {sum(read.values())} {json.dumps(read)}")
    print(f"  inserted {json.dumps(inserted)}")
    print(f"  updated  {json.dumps(updated)}")
    print(f"  re-read {reread}, duplicate events {dups}")


def dbt_summary(path: Path) -> None:
    if not path.exists():
        print(f"dbt: no run_results at {path}")
        return
    results = json.loads(path.read_text())
    counts = Counter((r["unique_id"].split(".")[0], r["status"]) for r in results["results"])
    print(f"dbt build: {results['elapsed_time']:.1f}s, " +
          ", ".join(f"{kind} {status}={n}" for (kind, status), n in sorted(counts.items())))
    bad = [r["unique_id"] for r in results["results"] if r["status"] not in ("success", "pass")]
    print(f"  not passing: {bad or 'none'}")


def kpi_summary(conn) -> None:
    rows = conn.execute("""
        SELECT report_date_utc, currency, minor_units, created_payment_count, cohort_captured_amount_minor,
               cohort_success_rate_pct, captured_count, captured_amount_minor, failed_count, outcome_success_rate_pct,
               succeeded_refund_count, succeeded_refund_amount_minor, net_captured_amount_minor
        FROM marts.fct_daily_payments ORDER BY report_date_utc, currency""").fetchall()
    print("daily KPIs (fct_daily_payments; cohort = created date, outcome = capture/failure date, "
          "refunds = succeeded_at date):")
    print(f"  {'date':10s} {'cur':3s} {'created':>7s} {'cohort_cap':>11s} {'cohort%':>7s} {'captured':>11s} "
          f"{'outcome%':>8s} {'refunds':>10s} {'net':>11s}")
    for (d, cur, mu, created, c_amt, c_rate, _cap, cap_amt, _failed, o_rate, _rc, ref_amt, net) in rows:
        print(f"  {d} {cur:3s} {created:7d} {major(c_amt, mu):>11s} {str(c_rate or '-'):>7s} {major(cap_amt, mu):>11s} "
              f"{str(o_rate or '-'):>8s} {major(ref_amt, mu):>10s} {major(net, mu):>11s}")
    totals = conn.execute("""
        SELECT p.currency, c.minor_units, count(*), count(*) FILTER (WHERE is_captured),
               coalesce(sum(amount_minor) FILTER (WHERE is_captured), 0), sum(succeeded_refund_amount_minor)
        FROM marts.fct_payments p JOIN staging.stg_currencies c ON c.currency_code = p.currency
        GROUP BY 1, 2 ORDER BY 1""").fetchall()
    for cur, mu, payments, captured, cap_amt, ref_amt in totals:
        print(f"  totals {cur}: {payments} payments, {captured} captured = {major(cap_amt, mu)}, "
              f"succeeded refunds {major(ref_amt, mu)}")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Summarise the latest V2 pipeline run (read-only).")
    parser.add_argument("--analytics", default=os.getenv("V2_ANALYTICS_DB", "v2_analytics"))
    parser.add_argument("--dbt-run-results", type=Path, default=Path("/tmp/dbt-v2/target/run_results.json"))
    parser.add_argument("--started-at", help="pipeline start time (ISO 8601), for the duration")
    args = parser.parse_args()
    with _connect(args.analytics) as conn:
        sync_summary(conn)
        dbt_summary(args.dbt_run_results)
        kpi_summary(conn)
    if args.started_at:
        started = datetime.fromisoformat(args.started_at.replace("Z", "+00:00"))
        seconds = (datetime.now(timezone.utc) - started).total_seconds()
        print(f"pipeline duration so far: {seconds:.1f}s")


if __name__ == "__main__":
    main()
