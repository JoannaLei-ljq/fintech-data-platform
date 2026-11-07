"""Demo pipeline: incremental sync (demo_oltp -> demo_analytics.raw) then `dbt build`.

Trigger manually. Pass {"fail_before_commit": "<table>"} as run conf to inject a
failure just before the sync commits that table's batch.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PROJECT_DIR = "/opt/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"

# Point every task at the isolated demo databases, never the main ones.
DEMO_ENV = {
    "OLTP_DB": "demo_oltp",
    "ANALYTICS_DB": "demo_analytics",
    "DBT_DBNAME": "demo_analytics",
}

with DAG(
    dag_id="fintech_demo_pipeline",
    description="Incremental sync of the demo OLTP database, then dbt build",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    params={
        "fail_before_commit": Param(
            "",
            type="string",
            enum=["", "users", "merchants", "transactions", "refunds"],
            description="Demo only: table whose sync batch fails just before commit",
        ),
    },
    tags=["fintech", "demo"],
) as dag:
    incremental_sync = BashOperator(
        task_id="incremental_sync",
        bash_command=(
            f"cd {PROJECT_DIR} && python -m sync.incremental_sync"
            "{% if params.fail_before_commit %}"
            " --fail-before-commit {{ params.fail_before_commit }}"
            "{% endif %}"
        ),
        env=DEMO_ENV,
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # Keep dbt artifacts inside the container so host and Airflow runs never collide.
        bash_command=(
            f"cd {PROJECT_DIR}/dbt && {DBT_BIN} --no-use-colors build --profiles-dir . "
            "--target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
        ),
        env=DEMO_ENV,
        append_env=True,
    )

    report = BashOperator(
        task_id="report",
        bash_command=f"cd {PROJECT_DIR} && python -m sync.report",
        env=DEMO_ENV,
        append_env=True,
    )

    incremental_sync >> dbt_build >> report
