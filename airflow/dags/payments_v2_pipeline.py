"""V2 pipeline: v2_oltp -> (sync) -> v2_analytics.raw -> (dbt) -> marts -> reconcile -> report.

Orchestrates existing components only:
  v2_sync    python -m sync.v2_sync      least-privilege login V2_SYNC_USER (from .env), never a superuser
  dbt_build  dbt build in dbt/v2         profile fintech_v2, target v2, database v2_analytics
  reconcile  python -m sync.v2_reconcile OLTP <-> raw <-> marts, read-only; fails the run on a mismatch
  report     python -m sync.v2_report    sync metrics, dbt results, KPIs, duration

Each task runs only if the previous one succeeded, so a failed sync never reaches dbt and a
failed dbt build or reconciliation never produces a "successful" report. max_active_runs=1 and
max_active_tasks=1 keep runs from overlapping (the sync also takes an advisory lock).
A failed run is safe to re-trigger: the sync resumes from its last committed checkpoint and
dbt rebuilds the marts in full.

Manual by default. Optional schedule: set V2_PIPELINE_SCHEDULE (a cron expression or preset,
for example "@hourly") in .env and restart the Airflow services.

Run conf (demo / acceptance only): {"inject_failure": "sync"} fails the sync just before its
target commit; {"inject_failure": "dbt"} fails one dbt test on purpose.
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
            enum=["", "sync", "dbt"],
            description="Demo only: make the sync fail before commit, or fail one dbt test",
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
            "{% if params.inject_failure == 'dbt' %} --vars '{v2_inject_test_failure: true}'{% endif %}"
        ),
        env=V2_ENV,
        append_env=True,
    )

    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"cd {PROJECT_DIR} && python -m sync.v2_reconcile",
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

    v2_sync >> dbt_build >> reconcile >> report
