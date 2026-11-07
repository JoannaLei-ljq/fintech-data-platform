-- Run against fintech_analytics after a sync.
\echo '== row counts vs distinct primary keys =='
SELECT 'users' AS table_name, count(*) AS row_count, count(DISTINCT user_id) AS distinct_pk FROM raw.users
UNION ALL SELECT 'merchants',    count(*), count(DISTINCT merchant_id)    FROM raw.merchants
UNION ALL SELECT 'transactions', count(*), count(DISTINCT transaction_id) FROM raw.transactions
UNION ALL SELECT 'refunds',      count(*), count(DISTINCT refund_id)      FROM raw.refunds;

\echo '== transactions =='
SELECT transaction_id, user_id, merchant_id, amount, currency, status, updated_at FROM raw.transactions ORDER BY transaction_id;

\echo '== refunds =='
SELECT refund_id, transaction_id, amount, reason, status, updated_at FROM raw.refunds ORDER BY refund_id;

\echo '== checkpoints =='
SELECT table_name, last_updated_at, last_pk, rows_last_run, checkpointed_at FROM meta.sync_checkpoints ORDER BY table_name;
