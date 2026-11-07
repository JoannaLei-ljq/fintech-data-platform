#!/usr/bin/env bash
# Triggers fintech_demo_pipeline, waits for it to finish and prints task states.
#   scripts/trigger_dag.sh                 normal run
#   scripts/trigger_dag.sh transactions    inject a failure before the transactions batch commits
# Exit code: 0 when the run succeeded, 1 otherwise.
set -euo pipefail
cd "$(dirname "$0")/.."

DAG_ID=fintech_demo_pipeline
FAIL_TABLE=${1:-}
RUN_ID="manual__$(date -u +%Y%m%dT%H%M%SZ)${FAIL_TABLE:+_fail_${FAIL_TABLE}}"
if [[ -n "$FAIL_TABLE" ]]; then CONF="{\"fail_before_commit\": \"${FAIL_TABLE}\"}"; else CONF='{}'; fi

af() { docker compose exec -T airflow-scheduler airflow "$@" 2>/dev/null; }
json() { sed -n '/^\[/,$p'; }   # drop any banner lines before the JSON array

af dags trigger "$DAG_ID" --run-id "$RUN_ID" --conf "$CONF" >/dev/null
echo "Triggered $DAG_ID run_id=$RUN_ID conf=$CONF"

state=""
for _ in $(seq 1 150); do
  state=$(af dags list-runs "$DAG_ID" -o json | json | jq -r --arg r "$RUN_ID" '.[] | select(.run_id == $r) | .state')
  [[ "$state" == "success" || "$state" == "failed" ]] && break
  sleep 2
done
echo "Run state: ${state:-unknown}"
af tasks states-for-dag-run "$DAG_ID" "$RUN_ID" -o json | json \
  | jq -r '.[] | "  task \(.task_id): \(.state)"'
echo "Logs: airflow/logs/dag_id=$DAG_ID/run_id=$RUN_ID/"

[[ "$state" == "success" ]]
