"""V2 pipeline (write -> audit -> publish):
v2_oltp -> sync -> v2_analytics.raw -> dbt -> marts_candidate -> reconcile -> publish -> marts -> report.

Orchestrates existing components only. One identity, this DAG run's run_id, ties them together:
  v2_sync    python -m sync.v2_sync       least-privilege login V2_SYNC_USER (from .env), never a superuser
  dbt_build  dbt build in dbt/v2          builds marts_candidate; records the build manifest with run_id
  reconcile  python -m sync.v2_reconcile  OLTP <-> raw <-> candidate marts; records the validation and
                                          the candidate fingerprint for run_id; fails the run on a mismatch
  publish    python -m sync.v2_publish    copies the candidate into marts in ONE transaction, only if the
                                          dbt results, build manifest, validation and current candidate
                                          all belong to run_id and the sync run has not moved on
  report     python -m sync.v2_report     publication, sync metrics, dbt results, published KPIs, duration

Each task runs only if the previous one succeeded. A failed sync, dbt build or reconciliation
never reaches publish, so the published marts and their publication record stay exactly as
the last successful run left them. max_active_runs=1 and max_active_tasks=1 keep runs from
overlapping; the sync and the publish also take advisory locks. A failed run is safe to
re-trigger: the sync resumes from its last committed checkpoint and dbt rebuilds the candidate.

Manual by default. Optional schedule: set V2_PIPELINE_SCHEDULE (a cron expression or preset,
for example "@hourly") in .env and restart the Airflow services.

Run conf (demo / acceptance only): {"inject_failure": "sync"} fails the sync just before its
target commit; "dbt" fails one dbt test on purpose; "reconcile" adds a failing reconciliation check.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PROJECT_DIR = "/opt/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_ARTIFACTS = "/tmp/dbt-v2"

# Explicit V2 databases for every task (never the MVP or protected ones).
V2_ENV = {
    "V2_OLTP_DB": "v2_oltp",
    "V2_ANALYTICS_DB": "v2_analytics",
    "DBT_V2_DBNAME": "v2_analytics",
}

with DAG(
    dag_id="payments_v2_pipeline",
    description="V2: transaction-ID watermark sync, dbt build (dbt/v2), reconciliation, report",
    schedule=os.getenv("V2_PIPELINE_SCHEDULE") or None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=15)},
    params={
        "inject_failure": Param(
            "",
            type="string",
            enum=["", "sync", "dbt", "reconcile"],
            description="Demo only: fail the sync before commit, one dbt test, or the reconciliation",
        ),
    },
    tags=["fintech", "v2"],
) as dag:
    v2_sync = BashOperator(
        task_id="v2_sync",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m sync.v2_sync --source v2_oltp --target v2_analytics"
            "{% if params.inject_failure == 'sync' %} --fail-before-commit{% endif %}"
        ),
        env=V2_ENV,
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_DIR}/dbt/v2 && {DBT_BIN} --no-use-colors build --profiles-dir .. "
            f"--target-path {DBT_ARTIFACTS}/target --log-path {DBT_ARTIFACTS}/logs"
            " --vars '{v2_pipeline_run_id: \"{{ run_id }}\", "
            "v2_inject_test_failure: {{ 'true' if params.inject_failure == 'dbt' else 'false' }}}'"
        ),
        env=V2_ENV,
        append_env=True,
    )

    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m sync.v2_reconcile --marts-schema marts_candidate "
            "--pipeline-run-id '{{ run_id }}' --record"
            "{% if params.inject_failure == 'reconcile' %} --inject-failure{% endif %}"
        ),
        env=V2_ENV,
        append_env=True,
    )

    publish = BashOperator(
        task_id="publish",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m sync.v2_publish --pipeline-run-id '{{{{ run_id }}}}' "
            f"--run-results {DBT_ARTIFACTS}/target/run_results.json"
        ),
        env=V2_ENV,
        append_env=True,
    )

    report = BashOperator(
        task_id="report",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m sync.v2_report "
            f"--dbt-run-results {DBT_ARTIFACTS}/target/run_results.json "
            "--started-at '{{ dag_run.start_date.isoformat() }}'"
        ),
        env=V2_ENV,
        append_env=True,
    )

    v2_sync >> dbt_build >> reconcile >> publish >> report
