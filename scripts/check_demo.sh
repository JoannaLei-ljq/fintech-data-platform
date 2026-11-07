#!/usr/bin/env bash
# Prints the demo analytics state: row counts, the $20 payment, checkpoints and the
# daily summary. The last line is machine-readable (used by run_acceptance.sh).
set -euo pipefail
cd "$(dirname "$0")/.."

q() { docker compose exec -T postgres sh -c 'psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d demo_analytics "$@"' _ "$@"; }

q <<'SQL'
\echo '== raw row counts (rows / distinct primary keys)'
SELECT 'transactions' AS table_name, count(*) AS rows, count(DISTINCT transaction_id) AS distinct_pk FROM raw.transactions
UNION ALL SELECT 'refunds', count(*), count(DISTINCT refund_id) FROM raw.refunds;
\echo '== the $20 payment (3004) in raw'
SELECT transaction_id, amount, status, created_at, updated_at FROM raw.transactions WHERE transaction_id = 3004;
\echo '== sync checkpoints'
SELECT table_name, last_updated_at, last_pk, rows_last_run FROM meta.sync_checkpoints ORDER BY table_name;
\echo '== marts.fct_daily_payments'
SELECT summary_date, currency, payment_count, succeeded_count, failed_count, pending_count,
       success_amount, success_rate_pct, refund_count, refund_amount, net_amount
FROM marts.fct_daily_payments ORDER BY summary_date, currency;
SQL

q -At <<'SQL'
SELECT 'METRICS'
  || ' success_amount=' || coalesce((SELECT sum(success_amount) FROM marts.fct_daily_payments)::text, 'null')
  || ' success_rate_pct=' || coalesce((SELECT max(success_rate_pct) FROM marts.fct_daily_payments)::text, 'null')
  || ' refund_amount=' || coalesce((SELECT sum(refund_amount) FROM marts.fct_daily_payments)::text, 'null')
  || ' summary_rows=' || (SELECT count(*) FROM marts.fct_daily_payments)
  || ' summary_dates=' || coalesce((SELECT string_agg(DISTINCT summary_date::text, ',') FROM marts.fct_daily_payments), '')
  || ' tx_rows=' || (SELECT count(*) FROM raw.transactions)
  || ' tx_distinct=' || (SELECT count(DISTINCT transaction_id) FROM raw.transactions)
  || ' refund_rows=' || (SELECT count(*) FROM raw.refunds)
  || ' refund_distinct=' || (SELECT count(DISTINCT refund_id) FROM raw.refunds)
  || ' tx3004_status=' || coalesce((SELECT status FROM raw.transactions WHERE transaction_id = 3004), 'missing')
  || ' ckpt_transactions=' || coalesce((SELECT to_char(last_updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') || '/' || last_pk
                                        FROM meta.sync_checkpoints WHERE table_name = 'transactions'), 'none');
SQL
