FROM apache/airflow:3.3.2-python3.12

# Sync job dependencies go into Airflow's own environment. Re-pinning apache-airflow
# keeps pip from silently upgrading or downgrading Airflow itself.
COPY requirements.txt /tmp/requirements-sync.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-sync.txt

# dbt lives in its own venv so its dependency pins cannot clash with Airflow's.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
