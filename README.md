# Fintech Data Platform (MVP)

A local batch pipeline for payment data:

```
demo_oltp (Postgres)  ──incremental sync──▶  demo_analytics.raw  ──dbt──▶  staging views  ──▶  marts.fct_daily_payments
        ▲                 (Python, upsert +          (replicas +              (tests: keys,           (success amount,
        │                  checkpoint in one         meta.sync_checkpoints)     relationships,          success rate,
  business writes          transaction)                                         refund checks)          refund amount)
                          └──────────── orchestrated by Airflow DAG fintech_demo_pipeline ────────────┘
```

| Component | Version | Where |
|---|---|---|
| PostgreSQL | 16 (alpine) | `127.0.0.1:5434`, container `fintech-postgres` |
| Airflow | 3.3.2, LocalExecutor | UI at `http://127.0.0.1:8080` |
| dbt | dbt-core 1.12.5 + dbt-postgres 1.11.0 | `dbt/`, runs inside the Airflow image |
| Python sync | psycopg 3 | `sync/incremental_sync.py` |

## Databases

One Postgres instance holds isolated databases:

| Database | Purpose |
|---|---|
| `fintech_oltp`, `fintech_analytics` | The original step-1 data. The demo never touches them. |
| `demo_oltp`, `demo_analytics` | Demo environment. Dropped and recreated by `scripts/reset_demo.sh`. |
| `airflow` | Airflow metadata. |

Both demo databases use the same DDL as the original ones (`docker/postgres/init/schema/`).

## Demo data

The demo uses four USD payments and two refunds, all on **2026-10-15 UTC**:

| Payment | Amount | Status | Refunds |
|---|---|---|---|
| 3001 | $100 | succeeded | $10 succeeded, $20 succeeded |
| 3002 | $50 | succeeded | |
| 3003 | $30 | failed | |
| 3004 | $20 | pending, later set to succeeded | |

## Metric definitions (`marts.fct_daily_payments`)

The table has one row per UTC day and currency. Its primary key is `summary_id = "<date>|<currency>"`.

- **success_amount:** sum of succeeded payments, counted on the UTC day the payment was created.
- **success_rate_pct:** succeeded / (succeeded + failed). Pending payments are not yet decided, so they are excluded from both the numerator and the denominator.
- **refund_amount:** sum of succeeded refunds, counted on the UTC day the refund was created.
- Payments and refunds are aggregated **separately**, then joined on day and currency. A row-level payment-to-refund join would count the $100 payment once per refund. The acceptance log shows the wrong result such a join gives: $270 instead of $170.

| State | success_amount | success_rate_pct | refund_amount |
|---|---|---|---|
| Initial | 150.00 | 66.67 | 30.00 |
| After 3004 becomes succeeded | 170.00 | 75.00 | 30.00 |

## Run it

All commands run from the project root. Credentials and Airflow secrets live in `.env`; copy `.env.example` and fill it in for a fresh checkout.

```bash
docker compose up -d --wait            # Postgres + Airflow (first run builds the image, ~3 min)
./scripts/reset_demo.sh                # (re)create demo_oltp / demo_analytics with the seed data
./scripts/trigger_dag.sh               # trigger the DAG, wait, print task states
./scripts/check_demo.sh                # row counts, the $20 payment, checkpoints, daily summary
```

Failure injection and recovery:

```bash
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d demo_oltp -c "UPDATE transactions SET status = '\''succeeded'\'' WHERE transaction_id = 3004"'
./scripts/trigger_dag.sh transactions  # sync raises just before committing transactions → run fails
./scripts/check_demo.sh                # 3004 still pending in raw, checkpoint unchanged, metrics unchanged
./scripts/trigger_dag.sh               # recovery run
./scripts/check_demo.sh                # 170.00 / 75.00 / 30.00, still 4 transactions
```

Full acceptance run. It resets the demo, performs all of the steps above through Airflow, asserts every expected value, and writes the log to `docs/evidence/`:

```bash
./scripts/run_acceptance.sh
```

Airflow UI: <http://127.0.0.1:8080>. Log in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env`. Open the DAG **fintech_demo_pipeline**. To inject a failure from the UI, use *Trigger* with the config `{"fail_before_commit": "transactions"}`.

Other useful commands:

```bash
docker compose exec airflow-scheduler airflow dags list-runs fintech_demo_pipeline
docker compose exec airflow-scheduler bash -c 'cd /opt/project/dbt && /home/airflow/dbt-venv/bin/dbt test --profiles-dir . --target-path /tmp/dbt/target'
docker compose stop                     # stop everything, keep data
docker compose up -d --wait postgres    # Postgres only, without Airflow
```

## Safety guards

- `fintech_oltp` and `fintech_analytics` are protected (`tools/db_guard.py`).
- The sync refuses to write to `fintech_analytics` unless `FINTECH_ALLOW_PROTECTED_DB_WRITES=1` is set for that one command.
- V2 migrations accept only `v2_*` databases.
- `scripts/fingerprint_protected_dbs.sh` prints a read-only fingerprint of the protected databases, to compare before and after any work.

## How the sync works

- For each table, rows are read from the source in `(updated_at, pk)` order, strictly after the stored cursor `(last_updated_at, last_pk)`.
- Rows are upserted by primary key into `raw.<table>`. An older `updated_at` never overwrites a newer one.
- Each batch's upsert and its checkpoint update commit in **one transaction** in the analytics database. `--fail-before-commit <table>` raises after both statements have executed but before the commit, which demonstrates that they roll back together.
- A source trigger refreshes `updated_at` on every UPDATE. That is how the `pending → succeeded` change is picked up, while `created_at` keeps the metric on the original day.

## dbt tests (22, all part of `dbt build`)

- **Primary keys:** `unique` + `not_null` on every staging key and on `fct_daily_payments.summary_id`.
- **Relationships:** transactions → users and merchants; refunds → transactions.
- **Accepted values:** payment and refund status.
- **`assert_refund_amounts_valid`:**
  - every refund amount is positive;
  - succeeded refunds never exceed the payment amount;
  - succeeded refunds only exist on succeeded payments.
- **`assert_payments_not_double_counted`:** the daily summary equals payments and refunds summed straight from staging, with no join between them.

## Evidence

- `docs/evidence/acceptance_*.log` contains the full acceptance run (27 checks).
- `docs/evidence/negative_refund_test.log` shows the refund check failing on a deliberately bad refund, then passing again after cleanup.

## Known limitations

- **Hard deletes:** a row hard-deleted in the source is not propagated.
- **Late commits:** a long source transaction that commits after a sync, with an earlier `updated_at`, could be missed. A lookback window or CDC would close that gap.
- **Full rebuild:** `fct_daily_payments` is rebuilt as a full table on every run. That is fine at this size; an incremental model would be the next step.
- **Single superuser:** Airflow and dbt connect with the Postgres superuser from `.env`. This is acceptable for a local demo, not for shared environments.
