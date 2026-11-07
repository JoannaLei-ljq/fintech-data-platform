"""Print row counts, checkpoints and the daily payment summary of an analytics database.

Used as the last DAG task so every run leaves its results in the Airflow task log.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from sync.incremental_sync import conninfo


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    analytics_db = os.getenv("ANALYTICS_DB", "fintech_analytics")
    with psycopg.connect(conninfo(analytics_db)) as conn:
        print(f"== {analytics_db}: row counts (rows / distinct pk)")
        for table, pk in (("transactions", "transaction_id"), ("refunds", "refund_id")):
            rows, distinct = conn.execute(
                f"SELECT count(*), count(DISTINCT {pk}) FROM raw.{table}"
            ).fetchone()
            print(f"   {table:<13} {rows} / {distinct}")

        print("== checkpoints")
        for name, ts, pk, n in conn.execute(
            "SELECT table_name, last_updated_at, last_pk, rows_last_run "
            "FROM meta.sync_checkpoints ORDER BY table_name"
        ):
            print(f"   {name:<13} last_updated_at={ts} last_pk={pk} rows_last_run={n}")

        print("== marts.fct_daily_payments")
        for row in conn.execute(
            "SELECT summary_date, currency, success_amount, success_rate_pct, refund_amount, "
            "succeeded_count, failed_count, pending_count "
            "FROM marts.fct_daily_payments ORDER BY summary_date, currency"
        ):
            d, cur, amt, rate, refund, s, f, p = row
            print(f"   {d} {cur}: success_amount={amt} success_rate={rate}% "
                  f"refund_amount={refund} (succeeded={s} failed={f} pending={p})")


if __name__ == "__main__":
    main()
