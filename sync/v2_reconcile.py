"""V2 reconciliation: v2_oltp -> v2_analytics.raw -> marts, checked independently of dbt.

Read-only. Every session is opened READ ONLY with TimeZone UTC and DateStyle ISO, so row
fingerprints compare byte-for-byte across the two databases.

Checks (any FAIL exits non-zero; WARN does not):
  sync_state         the sync state is active and the latest attempted run succeeded
  source_vs_raw.*    per table, every OLTP row whose current version was visible to the last
                     sync snapshot (pg_visible_in_snapshot) is identical in raw; raw holds no row
                     the source lacks. Rows changed after that snapshot are reported as pending
                     (they belong to the next run), not as mismatches.
  marts.*            per currency: payment counts by status, captured amounts and succeeded
                     refunds agree between raw, fct_payments, both fct_daily_payments bases and
                     fct_merchant_performance; one row per payment; no event issues.
  source_vs_marts    the same financial totals straight from OLTP (skipped with WARN when rows
                     are pending for the next sync).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field

import psycopg
from dotenv import load_dotenv

from sync.v2_sync import TABLES, SyncCredentials
from tools.db_guard import UnsafeDatabaseTarget


@dataclass
class Report:
    results: list[tuple[str, str, str]] = field(default_factory=list)

    def add(self, status: str, check: str, detail: str) -> None:
        self.results.append((status, check, detail))
        print(f"{status:4s}  {check}: {detail}", flush=True)

    def check(self, ok: bool, check: str, detail: str) -> None:
        self.add("PASS" if ok else "FAIL", check, detail)

    @property
    def failed(self) -> int:
        return sum(1 for s, *_ in self.results if s == "FAIL")


def _connect(dbname: str) -> psycopg.Connection:
    if not dbname.startswith("v2_"):
        raise UnsafeDatabaseTarget(f"reconciliation reads v2_* databases only, got {dbname!r}")
    conn = psycopg.connect(SyncCredentials.admin().conninfo(dbname), autocommit=True)
    conn.execute("SET default_transaction_read_only = on")
    conn.execute("SET TimeZone = 'UTC'")
    conn.execute("SET DateStyle = 'ISO, YMD'")
    return conn


def _fingerprints(conn, relation: str, table, where: str = "", params=()) -> dict[str, str]:
    cols = ", ".join(table.columns)
    rows = conn.execute(f"SELECT {table.pk}::text, md5(row({cols})::text) FROM {relation} {where}", params)
    return dict(rows.fetchall())


def reconcile(oltp_db: str = "v2_oltp", analytics_db: str = "v2_analytics") -> Report:
    report = Report()
    with _connect(oltp_db) as oltp, _connect(analytics_db) as wh:
        state = wh.execute("SELECT status, watermark::text, last_snapshot::text, last_run_id FROM meta.sync_state "
                           "WHERE source_name = 'v2_oltp'").fetchone()
        last = wh.execute("SELECT run_id, status FROM meta.sync_runs WHERE status <> 'rejected' "
                          "ORDER BY run_id DESC LIMIT 1").fetchone()
        report.check(state is not None and state[0] == "active" and last is not None and last[1] == "succeeded",
                     "sync_state", f"state={state[0] if state else None} watermark={state[1] if state else None} "
                                   f"last_run={last}")
        if state is None:
            return report
        snapshot = state[2]

        pending_total = 0
        for table in TABLES:
            visible = _fingerprints(
                oltp, table.name, table,
                "WHERE sync_xid = '0' OR pg_visible_in_snapshot(sync_xid, %s::pg_snapshot)", (snapshot,))
            all_keys = {k for (k,) in oltp.execute(f"SELECT {table.pk}::text FROM {table.name}")}
            raw = _fingerprints(wh, f"raw.{table.name}", table)
            pending = len(all_keys) - len(visible)
            pending_total += pending
            differing = [k for k, h in visible.items() if raw.get(k) != h]
            extra = sorted(set(raw) - all_keys)
            report.check(not differing and not extra, f"source_vs_raw.{table.name}",
                         f"{len(visible)} rows compared, {len(differing)} differ, {len(extra)} only in raw, "
                         f"{pending} pending next sync" + (f"; e.g. {(differing + extra)[:3]}" if differing or extra else ""))

        raw_totals = dict((r[0], r[1:]) for r in wh.execute("""
            SELECT currency, count(*), count(*) FILTER (WHERE status = 'captured'), count(*) FILTER (WHERE status = 'failed'),
                   coalesce(sum(amount_minor) FILTER (WHERE status = 'captured'), 0)
            FROM raw.transactions GROUP BY currency"""))
        layers = {
            "fct_payments": """SELECT currency, count(*), count(*) FILTER (WHERE is_captured), count(*) FILTER (WHERE is_failed),
                                      coalesce(sum(amount_minor) FILTER (WHERE is_captured), 0)
                               FROM marts.fct_payments GROUP BY currency""",
            "fct_daily_payments(cohort)": """SELECT currency, sum(created_payment_count), sum(cohort_captured_count),
                                                    sum(cohort_failed_count), sum(cohort_captured_amount_minor)
                                             FROM marts.fct_daily_payments GROUP BY currency""",
            "fct_daily_payments(outcome)": """SELECT currency, NULL::bigint, sum(captured_count), sum(failed_count),
                                                     sum(captured_amount_minor)
                                              FROM marts.fct_daily_payments GROUP BY currency""",
            "fct_merchant_performance": """SELECT currency, sum(created_payment_count), sum(captured_count), sum(failed_count),
                                                  sum(captured_amount_minor)
                                           FROM marts.fct_merchant_performance GROUP BY currency""",
        }
        for name, query in layers.items():
            got = dict((r[0], r[1:]) for r in wh.execute(query))
            same = set(got) == set(raw_totals) and all(
                (g[0] is None or g[0] == raw_totals[c][0]) and tuple(g[1:]) == tuple(raw_totals[c][1:])
                for c, g in got.items())
            report.check(same, f"marts.payments.{name}",
                         "per currency (payments, captured, failed, captured_amount_minor) "
                         f"raw={_fmt(raw_totals)} mart={_fmt(got)}")

        raw_refunds = dict((r[0], r[1:]) for r in wh.execute("""
            SELECT currency, count(*) FILTER (WHERE status = 'succeeded'),
                   coalesce(sum(amount_minor) FILTER (WHERE status = 'succeeded'), 0)
            FROM raw.refunds GROUP BY currency HAVING count(*) FILTER (WHERE status = 'succeeded') > 0"""))
        for name, query in {
            "fct_payments": "SELECT currency, sum(succeeded_refund_count), sum(succeeded_refund_amount_minor) "
                            "FROM marts.fct_payments GROUP BY currency HAVING sum(succeeded_refund_count) > 0",
            "fct_daily_payments": "SELECT currency, sum(succeeded_refund_count), sum(succeeded_refund_amount_minor) "
                                  "FROM marts.fct_daily_payments GROUP BY currency HAVING sum(succeeded_refund_count) > 0",
            "fct_merchant_performance": "SELECT currency, sum(succeeded_refund_count), sum(succeeded_refund_amount_minor) "
                                        "FROM marts.fct_merchant_performance GROUP BY currency "
                                        "HAVING sum(succeeded_refund_count) > 0",
        }.items():
            got = dict((r[0], r[1:]) for r in wh.execute(query))
            report.check(_norm(got) == _norm(raw_refunds), f"marts.refunds.{name}",
                         f"succeeded (count, amount_minor) raw={_fmt(raw_refunds)} mart={_fmt(got)}")

        rows, distinct, raw_rows = wh.execute(
            "SELECT count(*), count(DISTINCT transaction_id), (SELECT count(*) FROM raw.transactions) "
            "FROM marts.fct_payments").fetchone()
        report.check(rows == distinct == raw_rows, "marts.one_row_per_payment",
                     f"fct_payments {rows} rows, {distinct} distinct, raw {raw_rows}")
        issues, mismatched = wh.execute(
            "SELECT (SELECT count(*) FROM intermediate.int_payment_event_issues), "
            "(SELECT count(*) FROM marts.fct_payments WHERE NOT status_matches_events)").fetchone()
        report.check(issues == 0 and mismatched == 0, "marts.event_order",
                     f"{issues} event issues, {mismatched} payments whose event-derived status differs")

        if pending_total:
            report.add("WARN", "source_vs_marts", f"skipped: {pending_total} source rows changed after the last sync")
        else:
            src = dict((r[0], r[1:]) for r in oltp.execute("""
                SELECT currency, count(*), count(*) FILTER (WHERE status = 'captured'), count(*) FILTER (WHERE status = 'failed'),
                       coalesce(sum(amount_minor) FILTER (WHERE status = 'captured'), 0)
                FROM transactions GROUP BY currency"""))
            src_refunds = dict((r[0], r[1:]) for r in oltp.execute("""
                SELECT currency, count(*) FILTER (WHERE status = 'succeeded'),
                       coalesce(sum(amount_minor) FILTER (WHERE status = 'succeeded'), 0)
                FROM refunds GROUP BY currency HAVING count(*) FILTER (WHERE status = 'succeeded') > 0"""))
            mart = dict((r[0], r[1:]) for r in wh.execute(layers["fct_payments"]))
            mart_refunds = dict((r[0], r[1:]) for r in wh.execute(
                "SELECT currency, sum(succeeded_refund_count), sum(succeeded_refund_amount_minor) FROM marts.fct_payments "
                "GROUP BY currency HAVING sum(succeeded_refund_count) > 0"))
            report.check(_norm(src) == _norm(mart) and _norm(src_refunds) == _norm(mart_refunds), "source_vs_marts",
                         f"payments oltp={_fmt(src)} mart={_fmt(mart)}; succeeded refunds oltp={_fmt(src_refunds)} "
                         f"mart={_fmt(mart_refunds)}")
    return report


def _norm(d: dict) -> dict:
    return {k: tuple(int(x) for x in v) for k, v in d.items()}


def _fmt(d: dict) -> str:
    return json.dumps({k: [None if x is None else int(x) for x in v] for k, v in sorted(d.items())})


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Reconcile v2_oltp, v2_analytics.raw and the V2 marts (read-only).")
    parser.add_argument("--oltp", default=os.getenv("V2_OLTP_DB", "v2_oltp"))
    parser.add_argument("--analytics", default=os.getenv("V2_ANALYTICS_DB", "v2_analytics"))
    args = parser.parse_args()
    report = reconcile(args.oltp, args.analytics)
    summary = {"checks": len(report.results), "failed": report.failed,
               "warnings": sum(1 for s, *_ in report.results if s == "WARN")}
    print("RECONCILE " + json.dumps(summary), flush=True)
    sys.exit(1 if report.failed else 0)


if __name__ == "__main__":
    main()
