#!/usr/bin/env bash
# Triggers payments_v2_pipeline through the Airflow CLI, waits for it to finish and prints
# task states with start/end times (in execution order).
#   scripts/v2/trigger_v2_dag.sh            normal run
#   scripts/v2/trigger_v2_dag.sh sync       inject a sync failure before its target commit
#   scripts/v2/trigger_v2_dag.sh dbt        inject a failing dbt test
#   scripts/v2/trigger_v2_dag.sh reconcile  inject a failing reconciliation check
# Exit code: 0 when the run succeeded, 1 otherwise.
set -euo pipefail
cd "$(dirname "$0")/../.."

DAG_ID=payments_v2_pipeline
INJECT=${1:-}
[[ -z "$INJECT" || "$INJECT" == sync || "$INJECT" == dbt || "$INJECT" == reconcile ]] || { echo "usage: $0 [sync|dbt|reconcile]" >&2; exit 2; }
RUN_ID="manual__$(date -u +%Y%m%dT%H%M%SZ)${INJECT:+_inject_${INJECT}}"
if [[ -n "$INJECT" ]]; then CONF="{\"inject_failure\": \"${INJECT}\"}"; else CONF='{}'; fi

af() { docker compose exec -T airflow-scheduler airflow "$@" 2>/dev/null; }
json() { sed -n '/^\[/,$p'; }

af dags trigger "$DAG_ID" --run-id "$RUN_ID" --conf "$CONF" >/dev/null
echo "Triggered $DAG_ID run_id=$RUN_ID conf=$CONF"

state=""
for _ in $(seq 1 300); do
  state=$(af dags list-runs "$DAG_ID" -o json | json | jq -r --arg r "$RUN_ID" '.[] | select(.run_id == $r) | .state')
  [[ "$state" == "success" || "$state" == "failed" ]] && break
  sleep 2
done
echo "Run state: ${state:-unknown}"
af tasks states-for-dag-run "$DAG_ID" "$RUN_ID" -o json | json \
  | jq -r 'sort_by(.start_date // "9999") | .[] | "  task \(.task_id): \(.state)  start=\(.start_date // "-")  end=\(.end_date // "-")"'
echo "Logs: airflow/logs/dag_id=$DAG_ID/run_id=$RUN_ID/"
[[ "$state" == "success" ]]
