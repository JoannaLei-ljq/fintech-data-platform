# Evidence

Output from real runs (test suites, dbt builds, Airflow DAG runs), saved as plain-text logs without local paths or secrets. Start with the two final ones.

## Final results
| File | What it shows |
|---|---|
| [`acceptance_v2_20261109T212452Z.log`](acceptance_v2_20261109T212452Z.log) | End-to-end acceptance through Airflow from a clean commit: **112 checks passed, 0 failed** across 12 real DAG runs. Covers baseline and MVP parity, a payment captured on a later day, partial refunds, idempotent re-runs, injected sync / dbt / reconciliation failures (the published marts never change), and a late-committing writer. |
| [`v2_release_fixes.log`](v2_release_fixes.log) | Final verification: 313 pytest tests, V2 `dbt build` (PASS=106), the acceptance summary, the publications and validations it recorded, the MVP regression (27/27), and an unchanged protected-database fingerprint. |

## History (one log per build stage)
| File | Stage |
|---|---|
| [`v2_phase1_steps0-3.log`](v2_phase1_steps0-3.log) | Safety guards, versioned migrations, integer minor units, payment lifecycle and event history |
| [`v2_phase1_step4.log`](v2_phase1_step4.log) | Payment event write path |
| [`v2_phase1_step5.log`](v2_phase1_step5.log) | Refund processing and financial invariants |
| [`v2_phase1_step6.log`](v2_phase1_step6.log) | Transaction-ID watermark sync, including the timestamp-vs-xid comparison |
| [`v2_phase1_step6_least_privilege.log`](v2_phase1_step6_least_privilege.log) | Sync running as a least-privilege role |
| [`v2_step7_dbt.log`](v2_step7_dbt.log) | dbt analytics and data quality |
| [`v2_step8_airflow.log`](v2_step8_airflow.log) | Airflow orchestration (before the release fixes) |
| [`acceptance_v2_20261109T190131Z.log`](acceptance_v2_20261109T190131Z.log) | First V2 acceptance run (72 checks, before the release fixes) |
| [`acceptance_20261103T211728Z.log`](acceptance_20261103T211728Z.log), [`acceptance_20261107T225803Z.log`](acceptance_20261107T225803Z.log) | Original MVP acceptance runs (27 checks) |
| [`negative_refund_test.log`](negative_refund_test.log) | MVP negative test: an over-refund is caught by a dbt test |
