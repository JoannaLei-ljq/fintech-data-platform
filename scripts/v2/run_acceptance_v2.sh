#!/usr/bin/env bash
# End-to-end acceptance for V2, executed through the real Airflow DAG payments_v2_pipeline.
# Uses only v2_oltp / v2_analytics (rebuilt from the seed first). Source changes go through the
# approved write functions as v2_payment_app. Each pipeline run is a real DAG run.
#
#   A  baseline + MVP parity ($150 / 66.67% / $30, then $170 / 75% / $30)
#   B  new payment created Monday, captured Tuesday
#   C  partial refunds (succeeded, failed, still open)
#   D  idempotent re-runs
#   E  failure injection (sync, dbt, reconciliation): the published marts and their publication
#      record never change; then recovery
#   F  events arriving in raw out of business order (a long-running writer)
#
# Requires the Docker stack to be running (docker compose up -d). Logs to
# docs/evidence/acceptance_v2_<UTC timestamp>.log; exits non-zero if any check fails.
set -uo pipefail
cd "$(dirname "$0")/../.."

mkdir -p docs/evidence
LOG="docs/evidence/acceptance_v2_$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee "$LOG") 2>&1

PASSED=0; FAILED=0; RESULTS=()
check() {  # check <label> <actual> <expected>
  if [[ "$2" == "$3" ]]; then PASSED=$((PASSED + 1)); RESULTS+=("PASS  $1  ($2)")
  else FAILED=$((FAILED + 1)); RESULTS+=("FAIL  $1  (got '$2', expected '$3')"); fi
  echo "  [$([[ "$2" == "$3" ]] && echo PASS || echo FAIL)] $1: $2"
}
psql_db() { docker compose exec -T postgres sh -c 'psql -X -q -At -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$0"' "$1"; }
oltp() { psql_db v2_oltp; }
wh() { psql_db v2_analytics; }
app() { { echo "SET ROLE v2_payment_app;"; cat; } | oltp; }
q() { wh <<< "$1"; }
qo() { oltp <<< "$1"; }
run_dag() {  # run_dag [sync|dbt] -> sets RUN (output) and RUN_STATE
  RUN=$(./scripts/v2/trigger_v2_dag.sh "${1:-}"); echo "$RUN"
  RUN_STATE=$(grep -o 'Run state: .*' <<<"$RUN" | sed 's/Run state: //')
}
task_state() { sed -n "s/^  task $1: \([a-z_]*\).*/\1/p" <<<"$RUN"; }
task_order() { sed -n 's/^  task \([a-z0-9_]*\): .*/\1/p' <<<"$RUN" | paste -sd ' ' -; }
ordered_in_time() {  # each task's end <= next task's start (ISO timestamps compare as strings)
  sed -n 's/.*start=\([^ ]*\)  end=\([^ ]*\)/\1 \2/p' <<<"$RUN" | awk 'NR > 1 && $1 < prev_end { bad = 1 } { prev_end = $2 } END { print bad ? "overlap" : "sequential" }'
}
daily() { q "SELECT $2 FROM marts.fct_daily_payments WHERE report_date_utc = '$1' AND currency = 'USD'"; }
total() { q "SELECT coalesce(sum($1), 0) FROM marts.fct_daily_payments WHERE currency = 'USD'"; }
digest() {  # fingerprint of every financial row the marts expose plus raw row counts
  q "SELECT md5(string_agg(t, '|' ORDER BY t)) FROM (
       SELECT row(d.*)::text AS t FROM marts.fct_daily_payments d
       UNION ALL SELECT row(transaction_id, status, amount_minor, succeeded_refund_amount_minor, pending_refund_amount_minor,
                            failed_refund_amount_minor, refund_request_count)::text FROM marts.fct_payments
       UNION ALL SELECT row(m.*)::text FROM marts.fct_merchant_performance m
       UNION ALL SELECT 'raw ' || (SELECT count(*) FROM raw.transactions) || ' ' || (SELECT count(*) FROM raw.payment_events)
                        || ' ' || (SELECT count(*) FROM raw.refunds)) x"
}
source_vs_raw_counts() {
  local out=""
  for t in currencies users merchants transactions payment_events refunds; do
    out+="$t=$(qo "SELECT count(*) FROM $t")/$(q "SELECT count(*) FROM raw.$t") "
  done
  echo "${out% }"
}
run_id() { sed -n 's/^Triggered payments_v2_pipeline run_id=\([^ ]*\) .*/\1/p' <<<"$RUN"; }
publication() { q "SELECT coalesce((SELECT publication_id || ' ' || pipeline_run_id FROM marts.current_publication), 'none')"; }
candidate_matches_published() {
  q "SELECT bool_and(same) FROM (
       SELECT NOT EXISTS (SELECT * FROM marts_candidate.fct_payments EXCEPT ALL SELECT * FROM marts.fct_payments)
          AND NOT EXISTS (SELECT * FROM marts.fct_payments EXCEPT ALL SELECT * FROM marts_candidate.fct_payments) AS same
       UNION ALL SELECT NOT EXISTS (SELECT * FROM marts_candidate.fct_daily_payments EXCEPT ALL SELECT * FROM marts.fct_daily_payments)
          AND NOT EXISTS (SELECT * FROM marts.fct_daily_payments EXCEPT ALL SELECT * FROM marts_candidate.fct_daily_payments)
       UNION ALL SELECT NOT EXISTS (SELECT * FROM marts_candidate.fct_merchant_performance EXCEPT ALL SELECT * FROM marts.fct_merchant_performance)
          AND NOT EXISTS (SELECT * FROM marts.fct_merchant_performance EXCEPT ALL SELECT * FROM marts_candidate.fct_merchant_performance)) x"
}
pipeline_ok() {  # pipeline_ok <label>
  check "$1 run state" "$RUN_STATE" "success"
  check "$1 task order" "$(task_order)" "v2_sync dbt_build reconcile publish report"
  check "$1 tasks ran sequentially" "$(ordered_in_time)" "sequential"
  check "$1 published by this run" "$(publication | cut -d' ' -f2)" "$(run_id)"
  check "$1 published marts = validated candidate" "$(candidate_matches_published)" "t"
}

echo "### V2 acceptance run $(date -u +%FT%TZ) on $(git branch --show-current) @ $(git rev-parse --short HEAD)"
curl -sf http://127.0.0.1:8080/api/v2/monitor/health >/dev/null || { echo "Airflow is not running: docker compose up -d"; exit 1; }
echo "### Airflow DAGs:"; docker compose exec -T airflow-scheduler airflow dags list -o plain 2>/dev/null | grep -E "payments_v2|fintech_demo" | awk '{print "  " $1}'
PROTECTED_BEFORE=$(./scripts/fingerprint_protected_dbs.sh | md5sum | cut -c1-12)
echo "### Protected databases fingerprint before: $PROTECTED_BEFORE"

echo; echo "### Reset: rebuild v2_oltp / v2_analytics from the seed"
./scripts/v2/reset_v2.sh | grep -E "applied|provisioned" | tail -3

# --- A ------------------------------------------------------------------------------------------
echo; echo "### Scenario A: baseline"
run_dag; pipeline_ok "A"
check "A source vs raw row counts" "$(source_vs_raw_counts)" \
  "currencies=7/7 users=3/3 merchants=2/2 transactions=4/4 payment_events=9/9 refunds=2/2"
check "A fct_payments rows" "$(q 'SELECT count(*) FROM marts.fct_payments')" "4"
check "A 2026-10-15 cohort captured / rate" "$(daily 2026-10-15 "cohort_captured_amount_minor || ' ' || cohort_success_rate_pct")" "15000 66.67"
check "A 2026-10-15 outcome captured / rate" "$(daily 2026-10-15 "captured_amount_minor || ' ' || outcome_success_rate_pct")" "15000 66.67"
check "A 2026-10-15 succeeded refunds" "$(daily 2026-10-15 succeeded_refund_amount_minor)" "3000"
check "A synthetic capture times flagged" "$(daily 2026-10-15 estimated_capture_time_count)" "2"

echo; echo "### Scenario A2: MVP parity after the pending payment 3004 completes (same day)"
app <<'SQL'
SELECT outcome FROM record_payment_event('a0000000-0000-4000-8000-000000000001', 3004, 2, 'payment.authorized', '2026-10-15 15:05+00', 'processor', 2000, 'USD');
SELECT outcome FROM record_payment_event('a0000000-0000-4000-8000-000000000002', 3004, 3, 'payment.captured', '2026-10-15 15:06+00', 'processor', 2000, 'USD');
SQL
run_dag; pipeline_ok "A2"
check "A2 2026-10-15 cohort captured / rate" "$(daily 2026-10-15 "cohort_captured_amount_minor || ' ' || cohort_success_rate_pct")" "17000 75.00"
check "A2 2026-10-15 outcome captured / rate" "$(daily 2026-10-15 "captured_amount_minor || ' ' || outcome_success_rate_pct")" "17000 75.00"
check "A2 2026-10-15 succeeded refunds" "$(daily 2026-10-15 succeeded_refund_amount_minor)" "3000"

# --- B ------------------------------------------------------------------------------------------
echo; echo "### Scenario B: new payment 6001, created Monday 2026-11-02, captured Tuesday 2026-11-03"
CAPTURED_BEFORE=$(total captured_amount_minor); PAYMENTS_BEFORE=$(q 'SELECT count(*) FROM marts.fct_payments')
app <<'SQL'
SELECT outcome FROM record_payment_event('b0000000-0000-4000-8000-000000000001', 6001, 1, 'payment.created', '2026-11-02 22:00+00', 'api', 4200, 'USD', NULL, 2, 102);
SELECT outcome FROM record_payment_event('b0000000-0000-4000-8000-000000000002', 6001, 2, 'payment.authorized', '2026-11-02 22:01+00', 'processor', 4200, 'USD');
SELECT outcome FROM record_payment_event('b0000000-0000-4000-8000-000000000003', 6001, 3, 'payment.captured', '2026-11-03 08:00+00', 'processor', 4200, 'USD');
SQL
run_dag; pipeline_ok "B"
check "B 6001 in raw (transaction, events)" "$(q "SELECT (SELECT status FROM raw.transactions WHERE transaction_id = 6001) || ' ' || (SELECT count(*) FROM raw.payment_events WHERE transaction_id = 6001)")" "captured 3"
check "B 6001 in staging" "$(q "SELECT status FROM staging.stg_transactions WHERE transaction_id = 6001")" "captured"
check "B 6001 in fct_payments" "$(q "SELECT status || ' ' || event_derived_status || ' ' || amount_minor FROM marts.fct_payments WHERE transaction_id = 6001")" "captured captured 4200"
check "B Monday: cohort counts it, capture volume does not" "$(daily 2026-11-02 "created_payment_count || ' ' || cohort_captured_amount_minor || ' ' || captured_amount_minor")" "1 4200 0"
check "B Tuesday: capture volume on the capture date" "$(daily 2026-11-03 "created_payment_count || ' ' || captured_count || ' ' || captured_amount_minor")" "0 1 4200"
check "B captured volume +4200 exactly once" "$(( $(total captured_amount_minor) - CAPTURED_BEFORE ))" "4200"
check "B payments +1" "$(( $(q 'SELECT count(*) FROM marts.fct_payments') - PAYMENTS_BEFORE ))" "1"

# --- C ------------------------------------------------------------------------------------------
echo; echo "### Scenario C: partial refunds on 6001 (1000 succeeds, 500 fails, 700 stays open)"
REFUNDS_BEFORE=$(total succeeded_refund_amount_minor); CAPTURED_BEFORE=$(total captured_amount_minor)
R1=$(app <<< "SELECT refund_id FROM request_refund('acc-r1', 6001, 1000, 'USD', 'partial_return');")
R2=$(app <<< "SELECT refund_id FROM request_refund('acc-r2', 6001, 500, 'USD', 'partial_return');")
R3=$(app <<< "SELECT refund_id FROM request_refund('acc-r3', 6001, 700, 'USD', 'goodwill');")
app <<< "SELECT refund_status FROM complete_refund($R1, 'succeeded'); SELECT refund_status FROM complete_refund($R2, 'failed', 'processor_declined');"
TODAY=$(qo "SELECT (succeeded_at AT TIME ZONE 'UTC')::date FROM refunds WHERE refund_id = $R1")
run_dag; pipeline_ok "C"
check "C succeeded refund volume +1000 (failed and open excluded)" "$(( $(total succeeded_refund_amount_minor) - REFUNDS_BEFORE ))" "1000"
check "C refund on its succeeded_at date ($TODAY)" "$(daily "$TODAY" succeeded_refund_amount_minor)" "1000"
check "C nothing on the capture date" "$(daily 2026-11-03 succeeded_refund_amount_minor)" "0"
check "C original amount unchanged; refunds beside it" "$(q "SELECT amount_minor || ' ' || succeeded_refund_amount_minor || ' ' || failed_refund_amount_minor || ' ' || pending_refund_amount_minor || ' ' || remaining_refundable_amount_minor FROM marts.fct_payments WHERE transaction_id = 6001")" "4200 1000 500 700 2500"
check "C captured volume unchanged" "$(total captured_amount_minor)" "$CAPTURED_BEFORE"
check "C payment status unchanged" "$(q "SELECT status FROM marts.fct_payments WHERE transaction_id = 6001")" "captured"

# --- D ------------------------------------------------------------------------------------------
echo; echo "### Scenario D: two more runs without source changes"
DIGEST_BEFORE=$(digest)
run_dag; pipeline_ok "D1"
check "D1 sync read 0 rows" "$(q "SELECT (SELECT sum(value::int) FROM jsonb_each_text(rows_read)) FROM meta.sync_runs WHERE status = 'succeeded' ORDER BY run_id DESC LIMIT 1")" "0"
run_dag; pipeline_ok "D2"
check "D financial digest identical after re-runs" "$(digest)" "$DIGEST_BEFORE"
check "D no duplicate payments/events/refunds" "$(q "SELECT (SELECT count(*) - count(DISTINCT transaction_id) FROM marts.fct_payments) + (SELECT count(*) - count(DISTINCT event_id) FROM raw.payment_events) + (SELECT count(*) - count(DISTINCT refund_id) FROM raw.refunds)")" "0"

# --- E ------------------------------------------------------------------------------------------
echo; echo "### Scenario E: the open refund (700) succeeds; then sync, dbt and reconciliation failures, and recovery"
app <<< "SELECT refund_status FROM complete_refund($R3, 'succeeded');"
REFUNDS_BEFORE=$(total succeeded_refund_amount_minor); DIGEST_BEFORE=$(digest); PUB_BEFORE=$(publication)
W_BEFORE=$(q "SELECT watermark FROM meta.sync_state")
run_dag sync
check "E1 run state (sync failure injected)" "$RUN_STATE" "failed"
check "E1 task states" "$(task_state v2_sync) $(task_state dbt_build) $(task_state reconcile) $(task_state publish) $(task_state report)" \
  "failed upstream_failed upstream_failed upstream_failed upstream_failed"
check "E1 publication unchanged" "$(publication)" "$PUB_BEFORE"
check "E1 checkpoint unchanged" "$(q "SELECT watermark FROM meta.sync_state")" "$W_BEFORE"
check "E1 raw refund still requested (rolled back)" "$(q "SELECT status FROM raw.refunds WHERE refund_id = $R3")" "requested"
check "E1 marts unchanged" "$(digest)" "$DIGEST_BEFORE"
check "E1 failure recorded in meta.sync_runs" "$(q "SELECT status || ' ' || error_class FROM meta.sync_runs ORDER BY run_id DESC LIMIT 1")" "failed InjectedFailure"
run_dag dbt
check "E2 run state (dbt failure injected)" "$RUN_STATE" "failed"
check "E2 task states" "$(task_state v2_sync) $(task_state dbt_build) $(task_state reconcile) $(task_state publish) $(task_state report)" \
  "success failed upstream_failed upstream_failed upstream_failed"
check "E2 sync itself committed (raw refund succeeded)" "$(q "SELECT status FROM raw.refunds WHERE refund_id = $R3")" "succeeded"
check "E2 candidate rebuilt with the new refund (not published)" "$(q "SELECT succeeded_refund_amount_minor FROM marts_candidate.fct_payments WHERE transaction_id = 6001")" "1700"
check "E2 published marts unchanged" "$(digest)" "$DIGEST_BEFORE"
check "E2 publication unchanged" "$(publication)" "$PUB_BEFORE"
run_dag reconcile
check "E3 run state (reconciliation failure injected)" "$RUN_STATE" "failed"
check "E3 task states" "$(task_state v2_sync) $(task_state dbt_build) $(task_state reconcile) $(task_state publish) $(task_state report)" \
  "success success failed upstream_failed upstream_failed"
check "E3 failed validation recorded for this run" "$(q "SELECT passed || ' ' || pipeline_run_id FROM meta.mart_validations ORDER BY validation_id DESC LIMIT 1")" "false $(run_id)"
check "E3 published marts unchanged" "$(digest)" "$DIGEST_BEFORE"
check "E3 publication unchanged" "$(publication)" "$PUB_BEFORE"
run_dag; pipeline_ok "E4 recovery"
check "E4 succeeded refunds +700 exactly once" "$(( $(total succeeded_refund_amount_minor) - REFUNDS_BEFORE ))" "700"
check "E4 6001 refunds" "$(q "SELECT succeeded_refund_amount_minor || ' ' || pending_refund_amount_minor || ' ' || remaining_refundable_amount_minor FROM marts.fct_payments WHERE transaction_id = 6001")" "1700 0 2500"
check "E4 no duplicates" "$(q "SELECT (SELECT count(*) FROM marts.fct_payments) || ' ' || (SELECT count(*) FROM raw.refunds) || ' ' || (SELECT count(*) FROM raw.payment_events)")" "5 5 14"

# --- F ------------------------------------------------------------------------------------------
echo; echo "### Scenario F: a long-running writer; raw ingestion order differs from business order"
MARK=$(mktemp -u); HOLDER_LOG=$(mktemp)
PYTHONPATH=. .venv/bin/python - "$MARK" > "$HOLDER_LOG" 2>&1 <<'PY' &
import sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path
import psycopg
from dotenv import load_dotenv
load_dotenv(".env")
from sync.v2_sync import SyncCredentials
from payments.events import PaymentEvent, record_payment_event
t = lambda s: datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
with psycopg.connect(SyncCredentials.admin().conninfo("v2_oltp")) as c:
    c.execute("SET ROLE v2_payment_app")
    record_payment_event(c, PaymentEvent(uuid.UUID("f0000000-0000-4000-8000-000000000001"), 6002, 1, "payment.created",
                                         t("2026-11-04 08:00:00"), "api", 1800, "USD", user_id=3, merchant_id=101))
    record_payment_event(c, PaymentEvent(uuid.UUID("f0000000-0000-4000-8000-000000000002"), 6002, 2, "payment.authorized",
                                         t("2026-11-04 08:01:00"), "processor", 1800, "USD"))
    print("holding 6002 (business time 08:00) uncommitted", flush=True)
    deadline = time.time() + 600
    while not Path(sys.argv[1]).exists() and time.time() < deadline:
        time.sleep(0.5)
    c.commit()
    print("committed 6002", flush=True)
PY
HOLDER=$!
for _ in $(seq 1 60); do grep -q holding "$HOLDER_LOG" && break; sleep 0.5; done
echo "  $(cat "$HOLDER_LOG")"
app <<'SQL'
SELECT outcome FROM record_payment_event('f0000000-0000-4000-8000-000000000011', 6003, 1, 'payment.created', '2026-11-04 12:00+00', 'api', 900, 'USD', NULL, 1, 102);
SELECT outcome FROM record_payment_event('f0000000-0000-4000-8000-000000000012', 6003, 2, 'payment.authorized', '2026-11-04 12:01+00', 'processor', 900, 'USD');
SELECT outcome FROM record_payment_event('f0000000-0000-4000-8000-000000000013', 6003, 3, 'payment.captured', '2026-11-04 12:05+00', 'processor', 900, 'USD');
SQL
run_dag; pipeline_ok "F1"
check "F1 raw has the later payment 6003, not the open 6002" "$(q "SELECT (SELECT count(*) FROM raw.payment_events WHERE transaction_id = 6003) || ' ' || (SELECT count(*) FROM raw.payment_events WHERE transaction_id = 6002)")" "3 0"
check "F1 watermark held by the open writer (lag > 0)" "$(q "SELECT watermark_lag_xids > 0 FROM meta.sync_runs WHERE status = 'succeeded' ORDER BY run_id DESC LIMIT 1")" "t"
touch "$MARK"; wait "$HOLDER"; echo "  $(tail -1 "$HOLDER_LOG")"; rm -f "$MARK" "$HOLDER_LOG"
app <<< "SELECT outcome FROM record_payment_event('f0000000-0000-4000-8000-000000000003', 6002, 3, 'payment.captured', '2026-11-04 08:10+00', 'processor', 1800, 'USD');"
run_dag; pipeline_ok "F2"
check "F2 6002 ingested after 6003 although it happened earlier" "$(q "SELECT (SELECT min(_run_id) FROM raw.payment_events WHERE transaction_id = 6002) > (SELECT max(_run_id) FROM raw.payment_events WHERE transaction_id = 6003) AND (SELECT min(occurred_at) FROM raw.payment_events WHERE transaction_id = 6002) < (SELECT min(occurred_at) FROM raw.payment_events WHERE transaction_id = 6003)")" "t"
check "F2 rows re-read because of the long writer" "$(q "SELECT rows_reread > 0 FROM meta.sync_runs WHERE status = 'succeeded' ORDER BY run_id DESC LIMIT 1")" "t"
check "F2 6002 state from event_sequence" "$(q "SELECT status || ' ' || event_derived_status || ' ' || last_event_sequence FROM marts.fct_payments WHERE transaction_id = 6002")" "captured captured 3"
check "F2 event issues" "$(q 'SELECT count(*) FROM intermediate.int_payment_event_issues')" "0"
check "F2 2026-11-04 cohort / capture" "$(daily 2026-11-04 "created_payment_count || ' ' || captured_count || ' ' || captured_amount_minor")" "2 2 2700"

# --- final --------------------------------------------------------------------------------------
echo; echo "### Final KPIs (USD, minor units)"
q "SELECT report_date_utc || '  created=' || created_payment_count || ' cohort_captured=' || cohort_captured_amount_minor || ' cohort%=' || coalesce(cohort_success_rate_pct::text, '-') || ' captured=' || captured_amount_minor || ' outcome%=' || coalesce(outcome_success_rate_pct::text, '-') || ' refunds=' || succeeded_refund_amount_minor || ' net=' || net_captured_amount_minor FROM marts.fct_daily_payments ORDER BY report_date_utc" | sed 's/^/  /'
echo "### Sync runs"
q "SELECT run_id || ' ' || mode || ' ' || status || ' read=' || coalesce((SELECT sum(value::int) FROM jsonb_each_text(rows_read))::text, '-') || ' reread=' || coalesce(rows_reread::text, '-') || ' dup_events=' || coalesce(duplicate_events::text, '-') || ' ' || coalesce(error_class, '') FROM meta.sync_runs ORDER BY run_id" | sed 's/^/  /'
PROTECTED_AFTER=$(./scripts/fingerprint_protected_dbs.sh | md5sum | cut -c1-12)
check "protected databases unchanged" "$PROTECTED_AFTER" "$PROTECTED_BEFORE"

echo; echo "### Summary: $PASSED passed, $FAILED failed"
printf '%s\n' "${RESULTS[@]}"
echo "### Log saved to $LOG"
[[ $FAILED -eq 0 ]]
