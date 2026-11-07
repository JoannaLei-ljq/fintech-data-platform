# 2–3 minute demo script

Before the demo, run `docker compose up -d --wait` and `./scripts/reset_demo.sh`. Open a terminal in the project root, and open the Airflow UI at <http://127.0.0.1:8080> in a browser tab.

| Time | Do | Say |
|---|---|---|
| 0:00–0:20 | Show the diagram at the top of `README.md` | "OLTP Postgres, an incremental Python sync into an analytics database, dbt models and tests, all orchestrated by one Airflow DAG. Everything runs locally in Docker." |
| 0:20–0:40 | `cat sql/demo/seed_demo.sql` (scroll to the inserts) | "Four USD payments on one day: $100 and $50 succeeded, $30 failed, $20 pending. The $100 payment has two refunds, $10 and $20." |
| 0:40–1:05 | `./scripts/trigger_dag.sh`, then show the run in the Airflow grid view | "The DAG runs the sync, then `dbt build` with 22 tests, then a report." |
| 1:05–1:25 | `./scripts/check_demo.sh` | "**$150, 66.67%, $30.** Pending is excluded from the success rate. The two refunds don't double-count the $100 payment. A naive join would say $250, and a dbt test guards against exactly that." |
| 1:25–1:40 | `./scripts/trigger_dag.sh`, then `./scripts/check_demo.sh` | "Re-run: zero rows synced. Same counts, same checkpoint, same metrics. The pipeline is idempotent." |
| 1:40–2:15 | Run the `UPDATE ... 3004 ... succeeded` from the README, then `./scripts/trigger_dag.sh transactions`, then `./scripts/check_demo.sh` | "The $20 payment succeeds. I inject a failure right before the sync commits. The run fails and dbt never runs. In the analytics database, 3004 is still pending and the checkpoint has not moved: data and checkpoint rolled back together." |
| 2:15–2:45 | `./scripts/trigger_dag.sh`, then `./scripts/check_demo.sh` | "Recovery run: one row synced, still 4 transactions. The original day now shows **$170, 75%, $30**, because the metric follows the payment's creation date, not the update time." |
| 2:45–3:00 | `ls docs/evidence/` | "The whole sequence is scripted in `run_acceptance.sh`, which asserts 27 checks and keeps the log as evidence." |

If time is short, run `./scripts/run_acceptance.sh` live (about 50 seconds) and walk through the summary at the end of its output.
