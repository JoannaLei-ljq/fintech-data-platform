#!/usr/bin/env bash
# End-to-end acceptance run against the demo databases, through Airflow:
#   1. reset demo data           2. DAG run        3. DAG re-run (idempotent)
#   4. $20 payment -> succeeded  5. DAG run with an injected failure before commit (rollback)
#   6. DAG run after recovery
# Everything is logged to docs/evidence/acceptance_<UTC timestamp>.log and the script
# exits non-zero if any check fails.
set -uo pipefail
cd "$(dirname "$0")/.."

mkdir -p docs/evidence
LOG="docs/evidence/acceptance_$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee "$LOG") 2>&1

PASSED=0; FAILED=0; RESULTS=()
check() {  # check <label> <actual> <expected>
  if [[ "$2" == "$3" ]]; then PASSED=$((PASSED + 1)); RESULTS+=("PASS  $1  ($2)")
  else FAILED=$((FAILED + 1)); RESULTS+=("FAIL  $1  (got '$2', expected '$3')"); fi
  echo "  [$([[ "$2" == "$3" ]] && echo PASS || echo FAIL)] $1: $2"
}
metric() { sed -n "s/.* $1=\([^ ]*\).*/\1/p" <<<"$METRICS"; }
snapshot() { OUT=$(./scripts/check_demo.sh); echo "$OUT" | grep -v '^METRICS'; METRICS=$(grep '^METRICS' <<<"$OUT"); echo "$METRICS"; }
task_log() {  # task_log <run_id> <task_id>: plain-text lines of an Airflow 3 JSON task log
  local f; f=$(find "airflow/logs/dag_id=fintech_demo_pipeline/run_id=$1/task_id=$2" -name '*.log' 2>/dev/null | head -1)
  [[ -n "$f" ]] && jq -r 'select(.event != null) | .event' "$f" 2>/dev/null
}
last_run_id() { sed -n 's/.*run_id=\([^ ]*\).*/\1/p' <<<"$1" | head -1; }
main_db_counts() {  # fingerprint of the main (non-demo) analytics database
  docker compose exec -T postgres sh -c 'psql -X -At -U "$POSTGRES_USER" -d fintech_analytics' <<'SQL'
SELECT (SELECT count(*) FROM raw.transactions) || ' transactions, '
    || (SELECT count(*) FROM raw.refunds) || ' refunds, checkpoint '
    || (SELECT last_updated_at || '/' || last_pk FROM meta.sync_checkpoints WHERE table_name = 'transactions');
SQL
}

echo "### Acceptance run $(date -u +%FT%TZ)"
echo "### Versions: Airflow $(docker compose exec -T airflow-scheduler airflow version 2>/dev/null | tail -1)," \
     "dbt-core $(docker compose exec -T airflow-scheduler /home/airflow/dbt-venv/bin/dbt --version 2>/dev/null | sed -n 's/.*installed: *//p' | head -1)," \
     "PostgreSQL $(docker compose exec -T postgres sh -c 'psql -X -At -U "$POSTGRES_USER" -d postgres -c "SHOW server_version"')"
MAIN_BEFORE=$(main_db_counts)
echo "### Main database fintech_analytics before (transactions/refunds, checkpoint pk): $MAIN_BEFORE"

echo; echo "### Step 0: reset demo databases"
./scripts/reset_demo.sh

echo; echo "### Step 1: first DAG run"
RUN1=$(./scripts/trigger_dag.sh); echo "$RUN1"
check "run 1 state" "$(grep -o 'Run state: .*' <<<"$RUN1")" "Run state: success"
snapshot
check "run 1 success_amount" "$(metric success_amount)" "150.00"
check "run 1 success_rate_pct" "$(metric success_rate_pct)" "66.67"
check "run 1 refund_amount" "$(metric refund_amount)" "30.00"
check "run 1 transactions rows/distinct" "$(metric tx_rows)/$(metric tx_distinct)" "4/4"
check "run 1 refunds rows/distinct" "$(metric refund_rows)/$(metric refund_distinct)" "2/2"
check "run 1 summary on one UTC day" "$(metric summary_rows) $(metric summary_dates)" "1 2026-10-15"
DBT1=$(task_log "$(last_run_id "$RUN1")" dbt_build)
echo "  dbt tests:"
grep -oE '(PASS|FAIL|ERROR) (unique|not_null|relationships|accepted_values|assert)[A-Za-z0-9_]*' <<<"$DBT1" \
  | sort -u -k2 | sed 's/^/    /'
DBT_LINE=$(grep -o 'Done\. PASS=.*' <<<"$DBT1" | tail -1)
echo "  dbt: $DBT_LINE"
check "run 1 dbt build (models + 22 tests)" "$DBT_LINE" "Done. PASS=27 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=27"
check "run 1 refunds not double counted (dbt test)" "$(grep -c 'PASS assert_payments_not_double_counted' <<<"$DBT1")" "1"
check "run 1 refund amount checks (dbt test)" "$(grep -c 'PASS assert_refund_amounts_valid' <<<"$DBT1")" "1"
BASELINE="$(metric success_amount) $(metric success_rate_pct) $(metric refund_amount) $(metric tx_rows) $(metric refund_rows) $(metric ckpt_transactions)"

echo; echo "### Step 2: re-run without source changes"
RUN2=$(./scripts/trigger_dag.sh); echo "$RUN2"
check "run 2 state" "$(grep -o 'Run state: .*' <<<"$RUN2")" "Run state: success"
snapshot
check "run 2 metrics, counts and checkpoint unchanged" \
  "$(metric success_amount) $(metric success_rate_pct) $(metric refund_amount) $(metric tx_rows) $(metric refund_rows) $(metric ckpt_transactions)" \
  "$BASELINE"
echo "  sync log:"; task_log "$(last_run_id "$RUN2")" incremental_sync | grep 'synced' | sed 's/^/    /'

echo; echo "### Step 3: source change, \$20 payment 3004 pending -> succeeded"
docker compose exec -T postgres sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d demo_oltp' <<'SQL'
UPDATE transactions SET status = 'succeeded' WHERE transaction_id = 3004;
SELECT transaction_id, amount, status, created_at, updated_at FROM transactions WHERE transaction_id = 3004;
SQL
CKPT_BEFORE=$(metric ckpt_transactions)

echo; echo "### Step 4: DAG run with failure injected before the transactions batch commits"
RUN3=$(./scripts/trigger_dag.sh transactions); echo "$RUN3"
check "run 3 state (expected to fail)" "$(grep -o 'Run state: .*' <<<"$RUN3")" "Run state: failed"
check "run 3 dbt_build did not run" "$(grep -o 'task dbt_build: .*' <<<"$RUN3")" "task dbt_build: upstream_failed"
echo "  sync log:"; task_log "$(last_run_id "$RUN3")" incremental_sync | grep -E 'synced|^InjectedFailure|Task failed' | sed 's/^/    /' | head -8
snapshot
check "run 3 raw 3004 still pending (data rolled back)" "$(metric tx3004_status)" "pending"
check "run 3 transactions checkpoint unchanged (checkpoint rolled back)" "$(metric ckpt_transactions)" "$CKPT_BEFORE"
check "run 3 summary still the pre-change value" "$(metric success_amount) $(metric success_rate_pct)" "150.00 66.67"

echo; echo "### Step 5: recovery run"
RUN4=$(./scripts/trigger_dag.sh); echo "$RUN4"
check "run 4 state" "$(grep -o 'Run state: .*' <<<"$RUN4")" "Run state: success"
echo "  sync log:"; task_log "$(last_run_id "$RUN4")" incremental_sync | grep 'synced' | sed 's/^/    /'
DBT4=$(task_log "$(last_run_id "$RUN4")" dbt_build)
check "run 4 dbt build (models + 22 tests)" "$(grep -o 'Done\. PASS=.*' <<<"$DBT4" | tail -1)" "Done. PASS=27 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=27"
snapshot
check "run 4 transactions rows/distinct" "$(metric tx_rows)/$(metric tx_distinct)" "4/4"
check "run 4 raw 3004 now succeeded" "$(metric tx3004_status)" "succeeded"
check "run 4 checkpoint advanced" "$([[ "$(metric ckpt_transactions)" != "$CKPT_BEFORE" ]] && echo advanced || echo unchanged)" "advanced"
check "run 4 metrics still on original UTC day" "$(metric summary_rows) $(metric summary_dates)" "1 2026-10-15"
check "run 4 success_amount" "$(metric success_amount)" "170.00"
check "run 4 success_rate_pct" "$(metric success_rate_pct)" "75.00"
check "run 4 refund_amount" "$(metric refund_amount)" "30.00"

echo; echo "### Contrast: naive payment-to-refund join (the bug the model avoids)"
docker compose exec -T postgres sh -c 'psql -X -U "$POSTGRES_USER" -d demo_analytics' <<'SQL'
SELECT sum(t.amount) FILTER (WHERE t.status = 'succeeded') AS naive_success_amount,
       (SELECT success_amount FROM marts.fct_daily_payments) AS model_success_amount
FROM staging.stg_transactions t LEFT JOIN staging.stg_refunds r ON r.transaction_id = t.transaction_id;
SQL

MAIN_AFTER=$(main_db_counts)
echo "### Main database fintech_analytics after: $MAIN_AFTER"
check "main database untouched" "$MAIN_AFTER" "$MAIN_BEFORE"

echo; echo "### Summary: $PASSED passed, $FAILED failed"
printf '%s\n' "${RESULTS[@]}"
echo "### Log saved to $LOG"
[[ $FAILED -eq 0 ]]
