# 3-minute demo script (V2)

Before the demo:
```bash
docker compose up -d --wait
```
```bash
./scripts/v2/reset_v2.sh
```
Open a terminal in the project root, and open the Airflow UI at <http://127.0.0.1:8080> in a browser tab.

| Time | Do | Say |
|---|---|---|
| 0:00–0:25 | Show the diagram at the top of `README.md` | "Payments are written to Postgres only through database functions that enforce the lifecycle and refund rules. A Python sync copies them into an analytics database, dbt builds the marts, and Airflow orchestrates write → audit → publish." |
| 0:25–0:55 | `./scripts/v2/trigger_v2_dag.sh`, then show the run in the Airflow grid view | "Five tasks: sync, dbt build into a candidate schema, an independent reconciliation, publish, report. Nothing reaches the published marts until the same run's build and reconciliation have both passed." |
| 0:55–1:20 | Open the `report` task's log | "Publication 1. On the demo day: **$150 captured, 66.67% success rate, $30 refunded**. The `*` marks a day whose timestamps came from migrated MVP history: they're estimates, and the data says so." |
| 1:20–1:45 | Run **Capture 3004** (below), then `./scripts/v2/trigger_v2_dag.sh` | "The pending $20 payment is authorized and captured through the write function. The next run publishes **$170, 75%, $30**." |
| 1:45–2:20 | `./scripts/v2/trigger_v2_dag.sh dbt`, then run **Current publication** (below) | "I inject a failing dbt test. The run fails at `dbt_build`; reconcile, publish and report never run. The published marts and their publication record are unchanged: consumers never see a half-built result." |
| 2:20–2:40 | `./scripts/v2/trigger_v2_dag.sh`, then **Current publication** again | "Re-running is the recovery: the sync resumes from its committed checkpoint, and a new publication appears." |
| 2:40–3:00 | Open `docs/evidence/acceptance_v2_20261109T212452Z.log` | "The whole flow, plus late-committing writers, partial refunds and every failure point, is scripted: 112 checks through real Airflow runs. 313 pytest tests and 91 dbt tests run as well." |

**Capture 3004:**
```bash
docker compose exec -T postgres sh -c 'psql -X -q -U "$POSTGRES_USER" -d v2_oltp' <<'SQL'
SET ROLE v2_payment_app;
SELECT outcome FROM record_payment_event(gen_random_uuid(), 3004, 2, 'payment.authorized', '2026-10-15 15:05+00', 'processor', 2000, 'USD');
SELECT outcome FROM record_payment_event(gen_random_uuid(), 3004, 3, 'payment.captured', '2026-10-15 15:06+00', 'processor', 2000, 'USD');
SQL
```

**Current publication:**
```bash
docker compose exec -T postgres sh -c 'psql -X -U "$POSTGRES_USER" -d v2_analytics -c "SELECT publication_id, pipeline_run_id, published_at FROM marts.current_publication"'
```

If time is short, show the end of the acceptance log instead: its summary lists every scenario and check.

The original MVP has its own scripted run: `./scripts/run_acceptance.sh` (27 checks, about 50 seconds).
