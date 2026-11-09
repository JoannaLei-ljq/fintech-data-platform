# V2 Phase 1: approved design and implementation notes

Status: Steps 0–6 implemented on `v2-phase1` (pushed). Steps 7–8 implemented on `v2-analytics-orchestration` (local).
The MVP is frozen at tag `v0.1.0-mvp`.

## Isolation and safety

| Database | Role in V2 |
|---|---|
| `fintech_oltp`, `fintech_analytics` | Protected. Never written by V2 code or tests (`tools/db_guard.py`). |
| `demo_oltp`, `demo_analytics` | MVP regression only (`scripts/run_acceptance.sh`). |
| `v2_oltp`, `v2_analytics` | V2 development databases, built only by migrations (`scripts/v2/reset_v2.sh`). |
| `v2_test_*` | Throwaway databases created and dropped by pytest. |

- **Migrations** refuse any database that does not start with `v2_`. There is no override.
- **The MVP sync** refuses a protected target unless `FINTECH_ALLOW_PROTECTED_DB_WRITES=1` is set explicitly.
- **Both** verify `current_database()` right after connecting.
- **Fingerprints:** `scripts/fingerprint_protected_dbs.sh` takes a read-only fingerprint of the protected databases, to diff before and after any work.

## Event ordering: two separate concerns

Ordering is handled in two distinct places, and they will be tested separately.

1. **OLTP sequence enforcement (write time, Step 4, implemented).** The producer supplies `event_id` and `event_sequence`.
   - `record_payment_event()` accepts an event only if its sequence is exactly `last_event_sequence + 1` for that payment.
   - It rejects gaps as retryable errors (`PE003`), and treats a replay of the same `event_id` with the same payload as a no-op.
   - It does not buffer early events: the producer retries them. Buffering and reordering belong downstream only.
   - So `payment_events` in OLTP never contains gaps, and commit order equals sequence order for each payment.
   - Step 3 provides only the structural guarantees: unique `(transaction_id, event_sequence)`, sequence 1 is always `payment.created`, and append-only rows.
2. **Downstream out-of-order processing (analytics, Steps 6–7).**
   - Events can reach `v2_analytics.raw` in any order, and more than once: snapshot boundaries, lookback re-reads and replays all cause this.
   - Analytics must derive state by `(transaction_id, event_sequence)` and dedupe by `event_id`. It never uses `received_at`, `_synced_at` or load order.
   - dbt unit tests with shuffled and duplicated inputs will prove this.

## Money

- `currencies(currency_code, minor_units, name)` follows ISO 4217 minor units. Not every currency has 2 decimal places: JPY and KRW have 0; KWD and BHD have 3.
- Amounts are stored as `amount_minor bigint`, and the decimal amount is derived as `amount_minor / 10^minor_units`.
- **Conversion never rounds.** If an amount is not exactly representable in its currency's minor units (for example JPY 1500.50), the migration aborts and nothing is applied.
- The legacy `amount numeric(18,2)` column is dropped once converted. The `legacy_transactions` and `legacy_refunds` views expose the original decimal amounts for older consumers.

## Refund attribution (future refund analytics)

- Succeeded refund amounts will be attributed to the refund's **`succeeded_at`** date, not the date it was requested.
- Requested amounts belong to `requested_at`.
- These columns exist since Step 5 (`V006`); the analytics models that use them come later.

## Payment status mapping for existing data

| MVP status | V2 status | Rationale |
|---|---|---|
| `succeeded` | `captured` | Money moved. Synthetic events: created → authorized → captured. |
| `failed` | `failed` | Synthetic events: created → failed, with `failure_code = 'legacy_unknown'`. |
| `pending` | `created` | Approved 2026-11-07. In the MVP, `pending` only meant "not yet decided" (README, `fct_daily_payments`), and `pending → succeeded` was a single jump. No authorization is recorded anywhere, and the demo row 3004 was never updated after insert (`updated_at = created_at`). Mapping to `authorized` would invent an event, so only `payment.created` is backfilled. Metric parity is unaffected, because V2 `pending_count = created + authorized`. |

The MVP never recorded *when* authorization, capture or failure happened, only when the row last changed (`updated_at`). That timestamp is the latest moment those steps could have happened, so it is used for `authorized_at`, `captured_at` and `failed_at`. All backfilled events carry `source = 'migration_backfill'`, so they can be told apart from real ones.

## Implementation log

### Step 0: safety
- Tagged `v0.1.0-mvp` and created branch `v2-phase1`.
- Added `tools/db_guard.py` and the sync guard, `scripts/fingerprint_protected_dbs.sh`, and pytest scaffolding.

### Step 1: migrations
- `tools/migrate.py` runs each migration in its own transaction together with its `schema_migrations` row.
- Applied files are re-verified by SHA-256 checksum on every run, and an advisory lock serialises concurrent runs.
- `V001__baseline` is a frozen copy of the MVP DDL. A catalog-level test proves it equals the MVP schema.
- `scripts/v2/reset_v2.sh` builds `v2_oltp` as V001, then the MVP demo seed, then the remaining migrations; it also builds `v2_analytics`.

### Step 2: money (`V002__currencies_and_minor_units`)
- Adds `currencies` with ISO 4217 minor units, plus `amount_minor` on transactions and refunds. A refund uses its payment's currency.
- Pre-checks abort the migration on unknown currencies or amounts that aren't exactly representable. A round-trip assertion runs before `amount` is dropped.
- The `set_updated_at` triggers are disabled during the backfill, so `updated_at` is preserved.
- Adds the currency foreign key and `amount_minor > 0` checks, `minor_to_major()`, and the `legacy_transactions` / `legacy_refunds` views.

### Step 3: lifecycle (`V003__transaction_lifecycle`, `V004__payment_events`)
- **V003** maps statuses (see the table above) and adds `last_event_sequence`, `failure_code`, `authorized_at`, `captured_at` and `failed_at`. It also adds these CHECK constraints:
  - status vocabulary;
  - `failure_code` present if and only if the status is failed;
  - status ↔ sequence (created 1, authorized 2, captured 3, failed 2 or 3);
  - status ↔ timestamps;
  - timestamp order.
- **V003** also adds an immutability trigger on `transaction_id`, `user_id`, `merchant_id`, `amount_minor`, `currency` and `created_at`; foreign-key indexes; and `legacy_transactions` mapping back to the MVP vocabulary (created and authorized → `pending`).
- **V004** adds `payment_events`, with:
  - a UUID primary key;
  - unique `(transaction_id, event_sequence)`;
  - sequence 1 ⇔ `payment.created`;
  - amount and currency only on non-failed events;
  - `failure_code` only on failed events;
  - `source` and `occurred_at`, with `received_at` defaulting to `clock_timestamp()`;
  - append-only triggers covering UPDATE, DELETE and TRUNCATE.
- **Backfill:** 9 events for the 4 demo payments. IDs are deterministic UUID v5 values, derived from the payment and its sequence number. A final assertion aborts the migration if history and current state disagree for any payment.

### Deviations from the approved design
1. **Step 3 is split into two migrations,** V003 (state) and V004 (history), to keep each one reviewable.
2. **Foreign-key indexes were added in V003.** This addresses audit finding R14; it wasn't listed in Step 3.
3. **Backfilled timestamps are approximate.** Lifecycle timestamps use `updated_at` as an upper bound, and backfilled `received_at` is the migration time (the true ingestion time).
4. **The MVP sync now refuses `fintech_analytics` as a target** unless `FINTECH_ALLOW_PROTECTED_DB_WRITES=1` is set (Step 0 guard).

### Step 4: payment event write path (`V005__payment_event_write_path`)

**`record_payment_event()`** (PL/pgSQL, `SECURITY DEFINER`, fixed `search_path`) runs in the caller's transaction:

1. Validates the payload. Each event type has its required and forbidden fields, and `source` must be `api` or `processor`, because `migration_backfill` is reserved.
2. Locks the payment row with `SELECT … FOR UPDATE`.
   - A `payment.created` event first inserts the row with `ON CONFLICT DO NOTHING`.
   - A concurrent creator of the same payment therefore waits, then sees the committed row.
3. Looks up `event_id` *after* taking the lock:
   - identical payload → `duplicate`, no writes;
   - any difference → `PE006`.
4. Checks the sequence:
   - an already-used number → `PE004`;
   - a gap → `PE003`, retryable.
5. Checks the transition. Only these are legal: created → authorized, created → failed, authorized → captured, authorized → failed. Anything else → `PE005`.
6. Checks the amount: authorized and captured events must carry the original amount and currency, because partial amounts aren't supported. `occurred_at` must not be earlier than the previous lifecycle step.
7. Inserts the event and updates `transactions` together. Any error rolls back everything, including a payment row inserted in step 2.

| SQLSTATE | Meaning | Python (`payments/events.py`) |
|---|---|---|
| PE001 | invalid payload | `InvalidEvent` |
| PE002 | unknown payment | `UnknownPayment` |
| PE003 | sequence gap (retryable) | `SequenceGap` |
| PE004 | sequence already used | `SequenceConflict` |
| PE005 | illegal transition | `IllegalTransition` |
| PE006 | `event_id` reused with a different payload | `EventIdConflict` |

**Bypass prevention (privileges first, triggers second).**

*Roles* (both are cluster-wide NOLOGIN group roles, created only if missing, and refused if they carry elevated attributes):
- **`v2_payment_app`:**
  - has `SELECT` on the payment, reference and legacy tables, plus `EXECUTE` on the function;
  - has no `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE` anywhere;
  - is not a member of the writer role.
- **`v2_payment_writer`:**
  - owns the function;
  - holds only the column privileges it uses (it can `UPDATE` lifecycle columns only, never identity or amount);
  - has no `DELETE` or `TRUNCATE`.

*Other privileges:*
- `PUBLIC` has no `EXECUTE` on the function and no `CREATE` on schema `public`.
- Deployed services should log in as a role that is a member of `v2_payment_app`.

*Commit-time triggers:*
- Deferred constraint triggers on both tables re-check every touched payment at commit:
  - its history is contiguous;
  - its history is one of the five legal paths;
  - its last event matches the current state;
  - the created amount matches.
- This also catches direct writes by the table owner.
- The Step 3 append-only and immutability triggers remain.

**Concurrency.**
- One row lock per call, so different payments never block each other, and the function has no lock-ordering deadlocks.
- Designed for READ COMMITTED, where each statement sees rows committed before it.
- Under REPEATABLE READ or SERIALIZABLE, a concurrent change to the same payment surfaces as a serialization failure, and the caller should retry.

**Tests.** The Step 3 tests are now pinned to V004, because they insert events directly to test the structural constraints, which V005 rejects at commit. `test_write_path.py` covers the following at the head version:
- legal paths, replays, conflicts, gaps, illegal transitions and invalid payloads;
- rollback of partial writes and caller-transaction atomicity;
- concurrency: identical, competing, create-race, cross-payment `event_id` race, lock wait, and shuffled producers;
- privileges, both via `SET ROLE` and via a real temporary login role;
- commit-time checks on owner writes.

### Remaining gaps after Step 4
- **A superuser can still bypass the protections.** It can disable triggers, set `session_replication_role = replica`, or write as owner while keeping state and history consistent. Privileges and the commit-time checks bound this, but don't remove it.
- **The table owner (the migration user) can still write directly,** as long as the result is a legal, consistent history. This is intended for repairs. Using it requires the owner's credentials, which application code must not have.

### Step 5: refund processing (`V006__refund_processing`)

**Model.**
- **Statuses:** `requested → succeeded` or `requested → failed`. Both outcomes are terminal.
- **New columns on `refunds`:**
  - `currency`, which must equal the payment's currency;
  - `idempotency_key`, unique, 1–200 printable characters;
  - `requested_at`, `succeeded_at`, `failed_at` and `failure_code`.
- **CHECK constraints** tie the timestamps and `failure_code` to the status.
- **New refund IDs** come from a sequence that continues after the existing IDs.
- **The payment is never changed by a refund.** A fully refunded payment stays `captured`; refunded totals are derived from `refunds`.

**Existing rows.**
- Statuses map as pending → requested, succeeded → succeeded, failed → failed (with `failure_code = 'legacy_unknown'`).
- `requested_at = created_at`.
- `succeeded_at` and `failed_at` use `updated_at`. That makes them synthetic upper bounds, as in the Step 3 backfill.
- Idempotency keys are `legacy-refund:<id>`.
- `legacy_refunds` maps back to the MVP vocabulary and still matches the MVP data exactly.

**Write functions** (`SECURITY DEFINER`, owned by `v2_payment_writer`, `EXECUTE` granted only to `v2_payment_app`):
- **`request_refund(key, payment, amount, currency, reason)`:**
  1. Locks the payment row (`FOR UPDATE`).
  2. Looks up the idempotency key after taking the lock:
     - same request → `duplicate` with the existing refund and its current status;
     - otherwise → `RF006`.
  3. Requires a `captured` payment (`RF003`) and the payment's currency (`RF004`).
  4. Requires *requested + succeeded + this amount ≤ the payment amount*, so pending requests reserve capacity (`RF005`).
- **`complete_refund(refund, succeeded|failed, failure_code, completed_at)`:**
  - Locks the payment, then the refund. That is the same lock order as requests, so the two can't deadlock.
  - Allows only `requested → succeeded` or `requested → failed`.
  - Replaying the same outcome returns `duplicate`. A different outcome after a terminal status → `RF007`.
  - A failed refund releases its reservation.
- **Error codes:** `RF001` invalid input, `RF002` unknown payment or refund. Python: `payments/refunds.py`.

**Protections below the functions.**
- The application role has no write privileges on `refunds` and no access to the sequence.
- **Triggers:**
  - refund identity, amount, currency, key, reason and request time are immutable;
  - terminal statuses can't change;
  - `DELETE` and `TRUNCATE` are refused.
- **A deferred commit-time check, per payment:**
  - refunds exist only on `captured` payments;
  - their currencies match the payment's;
  - requested + succeeded ≤ the payment amount.

  It also catches direct writes by the table owner.
- The currency rule is a commit-time check rather than a composite foreign key. A composite key would need `UNIQUE (transaction_id, currency)` on `transactions`, and that extra index broke `record_payment_event()`'s concurrent-create handling (caught by the Step 4 tests).

**Concurrency.**
- Every refund decision for a payment is serialized on that payment's row lock. Different payments never block each other.
- Refund functions and payment events for the *same* payment wait for each other briefly.
- Sequences are not transactional, so refund IDs can have gaps after rollbacks.

### Step 6: V2 incremental sync (`V007__sync_watermark`, analytics `V002__v2_raw_and_sync_meta`, `sync/v2_sync.py`)

**Why not timestamps.** A cursor on `updated_at` (set to the transaction's start time) or `received_at` (insert time) assumes rows become visible in timestamp order. They become visible at commit, so a transaction that commits late is skipped for good. This was reproduced, then measured: across 5 concurrent fuzz runs, a timestamp cursor left 221 rows missing or stale, and the transaction-ID watermark left 0. A lookback window only narrows the gap.

**Source side (V007).**
- **`sync_xid xid8`:**
  - added to `users`, `merchants`, `currencies`, `transactions`, `refunds` and `payment_events`, with an index on each;
  - stamped by a `BEFORE INSERT OR UPDATE` trigger with `pg_current_xact_id()`, the top-level ID of the writing transaction;
  - a trigger rather than a column default, so an explicit value can't override it;
  - not the tuple's own `xmin`: inside savepoints and the write functions' exception blocks, that holds a subtransaction ID;
  - rows that already existed keep `'0'` and are read by the first full sync.
- **`sync_source_identity`:**
  - holds an explicit `generation_id` that every rebuild creates fresh;
  - `rotate_sync_source_generation(reason)` must be called after any restore from a backup or copy.

**Algorithm (one run).**
1. **Serialize.** Take a session advisory lock on the target. A concurrent attempt is rejected and recorded as `rejected`. Runs left `running` by a dead process are marked `abandoned`.
2. **Open one source snapshot.** `REPEATABLE READ READ ONLY`. Its first statement fixes snapshot S and reads the source identity. Every table is read from S, so a child row is never visible without its parent.
3. **Check it is safe to continue.** The run refuses (`blocked`) if:
   - the state is already blocked;
   - any identity field changed (generation, system identifier, database OID, name);
   - the stored watermark or snapshot is ahead of S. That check is `W <= pg_snapshot_xmin(S)` and `pg_snapshot_xmax(previous) <= pg_snapshot_xmax(S)`, evaluated by PostgreSQL.
4. **Read.** Every row with `sync_xid >= W`, as `xid8` comparisons in SQL. Transaction IDs are never compared in Python.
5. **Write, in one target transaction:**
   - upsert mutable tables unconditionally (committed runs are applied in order; no `updated_at` or `sync_xid` comparison);
   - insert new events and check every re-read event against its stored copy;
   - store `W := pg_snapshot_xmin(S)`, S itself, the identity and the run's metrics.

   Any error rolls all of it back, and the run row records the failure in a separate transaction.

**Why nothing is missed.** For any row version visible in S, written by transaction X:
- If X ≥ W, this run reads it.
- If X < W, X had finished at the previous snapshot, so the version was visible there and was read by an earlier run (induction).
- A transaction unfinished at S has an ID ≥ `pg_snapshot_xmin(S)`, the new W.
- A transaction that first writes after S gets an ID ≥ `pg_snapshot_xmax(S)`.
- `xmax` is one past the highest *completed* ID. So an in-progress ID at or above `xmax` isn't listed in `pg_snapshot_xip`. It is still ≥ `xmin`, which is what matters (`test_xid8_and_snapshot_boundary_semantics`).

**Why committed runs are applied in order.** A newer row version can carry *both* an older `updated_at` and a lower `sync_xid` than the version already synced. Example: a transaction that started earlier, or got its ID earlier, updates the row after another transaction committed. Comparing either value would keep the stale version. Runs are serialized and each snapshot is later than the last, so the last committed run holds the newest version.

**Events.**
- `raw.payment_events` is insert-only (a trigger refuses `UPDATE` and `DELETE`).
- A re-read event must equal the stored copy, and a different event must not claim a stored `(transaction_id, event_sequence)`.
- Either case fails the run with `EventConflict`, records the conflict in `meta.sync_conflicts`, and leaves data and checkpoint unchanged.
- Ingestion order is snapshot order. Business order comes from `event_sequence`, downstream in Step 7.

**Source identity and stale checkpoints.**
- The four identity fields catch an in-place rebuild (new generation and OID) and a `rotate_sync_source_generation()` call.
- They don't catch every restore. A restore that copies the generation row into a database with the same name, OID and cluster would look identical. That's why the procedure must rotate the generation, and why the watermark-ahead check (`W <= pg_snapshot_xmin(S)`) also catches a transaction counter that went backwards.
- Once blocked, the sync stays blocked until an explicit `--reinitialize`. That truncates `raw.*` and reloads from W = 0, in one transaction.
- A target with raw data but no state is refused too.

**Runbook.** See *Step 6 hardening* below for the full source-restore procedure. Day to day:
- **Reprocess:** `--lookback-runs N` re-reads the windows of the last N successful runs. It's idempotent; duplicate events are counted, not reinserted.
- **Inject a failure:** `--fail-before-commit` raises after all target writes.

**Monitoring** (`meta.sync_runs`):
- `rows_read`, `rows_inserted` and `rows_updated` per table;
- `rows_reread`: rows already visible in the previous snapshot, counted with `pg_visible_in_snapshot`;
- `duplicate_events`;
- `watermark_lag_xids`: snapshot xmax − xmin, a reporting-only numeric conversion;
- `oldest_open_writer_secs`: the oldest open writing transaction at the source;
- `error_class` and `error_message`.

An open writer holds W at its ID, and each run then re-reads everything since. The metrics expose this. The fix belongs at the source: `idle_in_transaction_session_timeout` and alerting.

**What is guaranteed, and under which assumptions.**
- **Guarantee:** after a successful run, `raw.*` holds the latest state of every source row committed before that run's snapshot.
- **Assumptions:**
  - every write goes through tables that have the `sync_xid` trigger, and no one disables triggers (only a superuser can);
  - the sync reads the primary database;
  - runs use this engine, so they are serialized;
  - restores follow the runbook.
- **Not captured:**
  - hard deletes (the V2 payment tables refuse deletes; users and merchants don't);
  - intermediate versions of mutable rows that changed more than once between runs. Payment history is still complete through the append-only `payment_events`, and refund timelines through `requested_at` and `succeeded_at`.
- **Compared with log-based CDC:** CDC would stream every committed change, including deletes, in commit order. It needs `wal_level = logical`, a replication slot (unread WAL can fill the disk) and a consumer, and psycopg 3 doesn't implement that protocol.

**Analytics V002.** It replaces the MVP-shaped `raw.*` tables and `meta.sync_checkpoints` in `v2_analytics` with V2-shaped ones, and refuses to run if they hold data. The MVP sync never targets `v2_analytics`.

### Step 6 hardening: least-privilege sync (`V008__sync_role`, analytics `V003__sync_role_grants`)

Normal sync runs no longer need a superuser. `python -m sync.v2_sync` connects as `V2_SYNC_USER` (default `v2_sync_login`), using `V2_SYNC_PASSWORD` from `.env`. The engine refuses a superuser connection unless `allow_superuser` / `--as-admin` is given explicitly.

**Roles.** All are cluster-wide. NOLOGIN roles are created only if missing, and refused if they carry LOGIN or elevated attributes.

| Role | Kind | Purpose |
|---|---|---|
| `v2_sync` | NOLOGIN group | Holds every sync privilege; is a member of nothing |
| `v2_sync_login` | LOGIN | Member of `v2_sync` only. Created by `scripts/v2/provision_sync_login.sh`, which `reset_v2.sh` calls. The password is generated into `.env` if missing and never printed |
| `v2_sync_stats` | NOLOGIN | Member of `pg_read_all_stats`. Owns one helper; nobody is granted it |

**What the sync needs, and how it gets it.**

| Need | Source of the privilege |
|---|---|
| `SELECT` on the 6 synced tables and `sync_source_identity` (`v2_oltp`) | Granted to `v2_sync` |
| `pg_current_snapshot`, `pg_snapshot_*`, `pg_visible_in_snapshot` | Executable by `PUBLIC` by default |
| `pg_control_system()` (system identifier for the identity check) | Executable by `PUBLIC` in PostgreSQL 16 (verified as an unprivileged role), so identity verification needs no grant and no privileged helper |
| Age of the oldest open writing transaction (monitoring only) | Other users' `xact_start` is hidden from unprivileged roles (`backend_xid` is not). The `SECURITY DEFINER` function `sync_oldest_open_writer_secs()` (owner `v2_sync_stats`, `search_path = pg_catalog, pg_temp`, no `PUBLIC` execute) returns just that number for the current database |
| Target writes (`v2_analytics`) | `SELECT`/`INSERT`/`UPDATE` on the mutable `raw.*` tables, `meta.sync_state` and `meta.sync_runs`; `SELECT`/`INSERT` on `raw.payment_events` and `meta.sync_conflicts`; the two sequences; `CONNECT` and `TEMPORARY` |

**Rejected alternatives for the monitoring metric:**
- Granting `pg_read_all_stats` to the sync role itself would expose every session's query text across the whole cluster, including the protected databases.
- A helper owned by a superuser would run custom code with superuser rights.

**Not granted to the sync role:**
- **On the source:** any write; the payment and refund write functions; `rotate_sync_source_generation()`.
- **On the target:** `DELETE` or `TRUNCATE` anywhere; `UPDATE` on raw events; DDL; disabling triggers.
- **Membership:** none in the payment roles, `v2_sync_stats` or `pg_read_all_stats`.

Reinitialization truncates `raw.*`, so it's an **operator procedure** run with the owner's credentials (`--as-admin`). The sync role is refused with `ReinitializationNotPermitted`.

**Protected databases.**
- The sync role holds no privilege on any table in `fintech_oltp` or `fintech_analytics` (tested with read-only probes).
- The engine also refuses any non-`v2_` database before connecting.
- **Remaining gap:** like every role, it can still *open a connection* to those databases and read their system catalogs, because PostgreSQL grants `CONNECT` to `PUBLIC` by default. Closing that requires either `REVOKE CONNECT ... FROM PUBLIC` on the protected databases, or a `pg_hba.conf` reject rule. Both change protected or cluster configuration, so they're left for an explicit decision.
- **Also open:** inside the container, `pg_hba` trusts local connections. That's a property of the Docker image setup, outside the sync.

**Source restore: limitation and mandatory runbook.**
- **What's detected automatically:**
  - **In-place rebuild:** a new `generation_id` and database OID.
  - **Rotated generation.**
  - **Backwards transaction counter:** a restore into a cluster whose counter is behind the stored watermark (`W <= pg_snapshot_xmin(S)` fails).
  - **Replaced server:** a different system identifier.
- **What isn't:** a restore that copies the `sync_source_identity` row into a database that keeps the same name, OID and server, with a transaction counter already past the stored watermark. That looks exactly like the original. Rows restored with old `sync_xid` values below W would then be skipped silently. **Generation rotation is therefore mandatory, not optional.**
- **After any restore, copy or point-in-time recovery of `v2_oltp`, in order:**
  1. Keep application writers off until step 3 is done.
  2. As the owner: `SELECT rotate_sync_source_generation('restored from <backup id> at <time>');`
  3. Run a sync as usual. It must stop with `SourceIdentityChanged`, the state becomes `blocked`, and the attempt is recorded in `meta.sync_runs`. If it does *not* stop, the rotation didn't happen: stop and investigate.
  4. As the operator: `python -m sync.v2_sync --as-admin --reinitialize`. This truncates `raw.*` and reloads everything from the restored source in one transaction.
  5. Resume normal runs (least-privilege login).
- **Rebuilding with `scripts/v2/reset_v2.sh`** recreates both databases. It gets a new generation, an empty target and a freshly provisioned login automatically.
- **A blocked target never resumes on its own.** Every run is refused until step 4, even if the original source comes back.

### Step 7: dbt analytics and data quality (`dbt/v2`)

**Isolation.**
- **Why a separate dbt project:** V2 is its own project in `dbt/v2/` (`fintech_v2`) rather than a folder in the MVP project. dbt requires model names to be unique within a project, and V2 reuses the MVP's names (`stg_users`, `fct_daily_payments`, …). The MVP project's `model-paths: ["models"]` never sees `dbt/v2`, so MVP behavior is unchanged: its acceptance run still reports exactly 27 dbt nodes.
- **Profile:** V2 uses its own profile, `fintech_v2`, in `dbt/profiles.yml`. Its only target is `v2`.
  - The database is `v2_analytics`, or `DBT_V2_DBNAME` for a disposable `v2_test_*` database.
  - Any name that doesn't start with `v2_` becomes `refused_non_v2_database`, so dbt can't connect anywhere else. This was tested with `postgres`, `demo_analytics` and `fintech_analytics`.
  - An `on-run-start` guard (`assert_v2_target`) fails loudly as a second check.
- **dbt's connection:** dbt runs as the database owner, like the MVP project. Only the sync runs least-privilege.

**Lineage.**
```
raw.currencies ─────── stg_currencies ─────────────────────────────────────────┐
raw.users ──────────── stg_users                                                │
raw.merchants ──────── stg_merchants ──────────────────────────────┐            │
raw.transactions ───── stg_transactions ──┬─ int_payment_event_issues (expect 0) │
raw.payment_events ─── stg_payment_events ┴─ int_payment_state_from_events ─┐    │
raw.refunds ────────── stg_refunds ─┬─ int_refund_totals ───────────────────┴─ fct_payments ─┬─ fct_daily_payments
                                    └─ int_refund_completions ───────────────────────────────┴─ fct_merchant_performance
```

| Model | Grain | Purpose |
|---|---|---|
| `stg_*` (6 views) | 1:1 with `raw.*` | Explicit columns and renames; UTC dates (`*_date_utc`); `occurred_at` and `received_at` kept apart; `amount_minor` untouched |
| `int_payment_state_from_events` | One row per payment with events | State derived from events ordered by `(transaction_id, event_sequence)` only. Exact duplicates collapse. Conflicts, duplicate sequences, gaps and illegal paths are **flagged** |
| `int_payment_event_issues` | One row per problem | Turns those flags into rows, plus state, last-sequence and amount mismatches against `transactions`, payments without events, and events without payments. Expected to be empty |
| `int_refund_totals` | One row per payment with refunds | Requested (all), pending, succeeded and failed counts and amounts, aggregated **before** any join |
| `int_refund_completions` | One row per succeeded refund | Dated by `succeeded_at`, with the payment's merchant |
| `fct_payments` (built in `marts_candidate`, published to `marts`) | One row per payment | Original amount, status, event-derived status, lifecycle times, refund measures, remaining refundable amount, net captured amount, `captured_at_is_estimate` |
| `fct_daily_payments` | `report_date_utc` + `currency` | Cohort metrics, outcome metrics, refunds, net |
| `fct_merchant_performance` | `merchant_id` + `report_date_utc` + `currency` | Created, captured, failed, outcome success rate, refunds, net, refund rate |

**KPI definitions.**
- **Money:** every amount is integer minor units of the row's `currency`; `minor_units` comes from `currencies`. Currencies are never summed together, since there are no exchange rates. Every mart grain includes `currency`.

| KPI | Definition | Date basis |
|---|---|---|
| Captured payment volume | `captured_amount_minor` = sum of `amount_minor` over payments whose status is `captured` | **Capture date** (`captured_at`, UTC). The cohort variant `cohort_captured_amount_minor` uses the **creation date** |
| Payment success rate | `captured / (captured + failed)`; in-flight (`created`, `authorized`) excluded; NULL when the denominator is 0 | `outcome_success_rate_pct`: captures and failures on their **outcome dates**. `cohort_success_rate_pct`: payments **created** that day, by current status. The two bases are never mixed in one ratio |
| Successful refund volume | `succeeded_refund_amount_minor` = sum over refunds with status `succeeded`. Requested (open) and failed refunds never count | **`succeeded_at` date** |
| Net captured volume | `captured_amount_minor − succeeded_refund_amount_minor` | Both on event dates, so it's the day's money in minus money out and can be negative |
| Refund rate (merchant) | `refunded_of_captured_amount_minor / captured_amount_minor` for payments captured that day, with refunds succeeded so far; NULL if nothing was captured | Cohort by **capture date**. It grows as later refunds succeed |
| Remaining refundable (per payment) | `amount − pending − succeeded` for captured payments, else 0 | The same rule `request_refund()` enforces |

**Comparison with the MVP.**
- **The MVP's metrics** used the payment creation day for payment metrics and the refund *creation* day for refunds.
- **On the demo scenario both V2 bases reproduce the MVP figures,** because every demo event happens on 2026-10-15:
  - baseline: $150, 66.67% and $30 (`cohort_*` and `outcome_*`);
  - once 3004 is captured on 2026-10-15: $170, 75% and $30.
- **They diverge as soon as a payment is captured on a later day:**
  - the cohort basis keeps it on the creation day;
  - the outcome basis moves it to the capture day;
  - refunds always move to the `succeeded_at` day.

  Tested in `test_dbt_v2.py`.

**Synthetic backfill.**
- Capture, authorization and failure times for migrated MVP payments are upper bounds (`updated_at`), not observations.
- `fct_payments.captured_at_is_estimate` and `fct_daily_payments.estimated_capture_time_count` expose this.
- Use these timestamps only to date the MVP rows, never to measure processing latency.

**Data quality.** 81 data tests and 6 unit tests run in every `dbt build`:
- **Structure:** unique and not-null primary keys; relationships to users, merchants, currencies and payments; accepted statuses, event types and sources; `minor_units` in 0–4; positive amounts; unique `(transaction_id, event_sequence)`.
- **Events** (`assert_no_payment_event_issues`): contiguity, legal paths (so nothing follows a terminal state), conflicting event IDs, derived state equal to `transactions.status` and `last_event_sequence`, amount and currency consistency.
- **Refunds:** currency equals the payment's; requested + succeeded ≤ the payment amount, and refunds only on captured payments; terminal timestamps consistent with the status; refunded volume counts succeeded refunds only, in every layer.
- **Analytics:** one row per payment; unique daily and merchant grains; payment counts, statuses and captured amounts reconcile, per currency, between staging, `fct_payments`, both daily bases and the merchant mart; staging stays 1:1 with raw.
- **Unit tests:**
  - shuffled, duplicated, out-of-order arrival;
  - gaps, illegal transitions, transitions after a terminal state, conflicting IDs, duplicate sequences;
  - synthetic capture times;
  - partial, pending and failed refunds;
  - no payment fan-out over refunds;
  - created Monday / captured Tuesday;
  - a refund requested Wednesday and succeeded Friday;
  - zero denominators;
  - USD and JPY kept apart.
- **Negative integration tests** (`tests/v2/test_dbt_v2.py`): each corrupts `raw.*` in a disposable database (state mismatch, gap after a terminal state, over-refund, refund on a failed payment, currency mismatch, inconsistent refund timestamps, payment without events) and requires the matching dbt test to fail.
- `assert_injected_failure` fails only with `--vars '{v2_inject_test_failure: true}'`, for failure demos.

**Reconciliation across systems.** OLTP ↔ raw is checked outside dbt, because the two databases are separate. That check is in Step 8 (`sync/v2_reconcile.py`).

**Limitations.**
- **Full rebuilds:** marts are rebuilt in full on every run. That's fine at demo scale, but late events would need an incremental strategy with an event-date lookback later.
- **Currency:** there's no FX, so no cross-currency totals.
- **Partial captures** are not modeled. That's a Phase 1 rule enforced in OLTP.

### Step 8: Airflow orchestration and end-to-end acceptance

**DAG `payments_v2_pipeline`** (`airflow/dags/payments_v2_pipeline.py`). This is write → audit → publish, added as release fix 1:
```
v2_sync ──▶ dbt_build (marts_candidate) ──▶ reconcile (candidate, recorded) ──▶ publish (marts) ──▶ report
```

| Task | Runs | Notes |
|---|---|---|
| `v2_sync` | `python -m sync.v2_sync --source v2_oltp --target v2_analytics` | Least-privilege login `V2_SYNC_USER` from `.env`; a superuser is refused. Logs the run ID, rows read, inserted, updated and re-read, duplicate events, watermark and lag |
| `dbt_build` | `dbt build` in `dbt/v2` (profile `fintech_v2`, target `v2`, `DBT_V2_DBNAME=v2_analytics`, `--vars v2_pipeline_run_id=<run_id>`) | Builds the **candidate** marts in `marts_candidate`; consumers never read them. An `on-run-start` hook first records the build manifest (`marts_candidate.mart_build_info`: pipeline run, dbt invocation, sync run). Artifacts in `/tmp/dbt-v2` inside the container, so it never collides with the MVP's `/tmp/dbt` |
| `reconcile` | `python -m sync.v2_reconcile --marts-schema marts_candidate --pipeline-run-id <run_id> --record` | Reads only the data; writes one row to `meta.mart_validations` with the candidate's content fingerprint. Checks the build manifest belongs to this run and to the latest sync run. Every OLTP row whose current version was visible to the last sync snapshot must be identical in raw (rows changed after it are reported as pending, not as errors). Payment counts, statuses, captured amounts and succeeded refunds must agree per currency between raw, `fct_payments`, both daily bases and the merchant mart, and with OLTP when nothing is pending. One row per payment; no event issues. Any FAIL fails the task |
| `publish` | `python -m sync.v2_publish --pipeline-run-id <run_id>` | Copies the three candidate marts into `marts` and records the publication, in **one transaction**, only after re-verifying every binding (see below) |
| `report` | `python -m sync.v2_report` | The current publication, sync run metrics, dbt results by node type, daily KPIs in major units (exact `Decimal` scaling), pipeline duration. Prints no credentials |

**Run behavior:**
- **Explicit V2 environment:** every task gets `V2_OLTP_DB`, `V2_ANALYTICS_DB` and `DBT_V2_DBNAME`.
- **Upstream failures stop the run:** each task runs only if the previous one succeeded. A failed sync never reaches dbt, and a failed dbt build or reconciliation never produces a report.
- **No overlap:** `max_active_runs=1` and `max_active_tasks=1`, plus the sync's own advisory lock.
- **Retries:** `retries=0`, so failures are visible. Re-triggering is the recovery: the sync resumes from its last committed checkpoint and dbt rebuilds the marts in full.
- **Timeouts:** `dagrun_timeout` 30 minutes, task `execution_timeout` 15 minutes.
- **Schedule:** none (manual). Set `V2_PIPELINE_SCHEDULE` for a recurring one.
- **Failure injection** (run conf `inject_failure`, demos only):
  - `sync`: the sync raises after its writes, before commit;
  - `dbt`: the singular test `assert_injected_failure` fails.

**End-to-end acceptance** (`scripts/v2/run_acceptance_v2.sh`, 72 checks, every pipeline run is a real DAG run). The script rebuilds the V2 databases from the seed, then:

| Scenario | What it does and checks |
|---|---|
| A | **Baseline:** source = raw row counts, then $150 / 66.67% / $30. **A2:** capture 3004 on the same day, giving $170 / 75% / $30. Both on the cohort and the outcome basis |
| B | **New payment** created Monday and captured Tuesday through `record_payment_event`. It appears in raw, staging and marts. The cohort counts it on Monday and captured volume on Tuesday, exactly once |
| C | **Refunds:** 1000 succeeded, 500 failed, 700 left open. Refunded volume rises by exactly 1000, on the `succeeded_at` date. The original amount, status and captured volume are unchanged, and the remaining refundable amount is 2500 |
| D | **Two re-runs without changes:** the sync reads 0 rows, the financial digest of all marts is identical, and there are no duplicates |
| E | **Sync failure:** dbt, reconcile and report never run, and the checkpoint, raw and marts are unchanged. **dbt failure:** reconcile and report never run. **Recovery** run: refunds rise by exactly 700 |
| F | **Long-running writer:** a transaction holds payment 6002 (business time 08:00) open while 6003 (12:00) is captured and synced. 6002 is ingested by a later run (out of business order), the watermark lag and re-reads are recorded, and the state derived from `event_sequence` is correct |
| End | Protected-database fingerprint unchanged |

Note on scenario C: `request_refund` stamps `requested_at` with the database clock, so refunds created by the acceptance run carry today's date. Cross-day refund attribution (requested Wednesday, succeeded Friday) is covered by the dbt unit test `ut_daily_dates_follow_each_metric_basis`.

**Publication (release fix 1).**

*Why it's needed:* a failed `dbt build` can leave marts partly rebuilt. In dbt, a model waits for a test only if it is downstream of every model the test reads. So the event-integrity test and the mart reconciliation tests cannot stop marts from being written, and a failing test on one mart skips only its children. dbt therefore writes only the candidate schema, and `sync/v2_publish.py` alone writes `marts`.

*How it binds everything to one run:*
- **Run identity:** one pipeline run ID (the Airflow `run_id`) connects:
  - dbt's `run_results.json`: same invocation, `--vars` for this run, every node passing, all three marts built;
  - the build manifest, which is written before any model reads raw;
  - a passing validation of that pipeline run, dbt invocation and sync run;
  - the candidate's content: its fingerprint must equal the validated one.
- **Staleness:** the sync state must still be active, with the candidate's sync run as its latest run. A sync that committed after the build is refused, and so are stale `run_results.json` files and candidates changed after reconciliation.
- **No double publication:** a pipeline run or dbt invocation can be published only once.

*The publish transaction:*
1. `pg_advisory_xact_lock` serializes publishers.
2. `LOCK … IN SHARE MODE` on the candidate tables stops a concurrent dbt build from replacing them.
3. `SELECT meta.sync_state … FOR SHARE` stops a concurrent sync from committing.
4. Every binding above is verified.
5. For each mart: `DELETE` the published rows, then `INSERT … SELECT` from the candidate. If a mart's columns changed, it is recreated first, and that is refused if any view depends on it.
6. The `meta.mart_publications` row is inserted, then the transaction commits.

Any failure, at any point before the commit, leaves all three published marts and the publication metadata unchanged. A failed sync, dbt build or reconciliation never even reaches publish.

*Why copying instead of renaming:* the published tables keep their identity, so dependent views and grants survive, and readers are never blocked. Readers take `ACCESS SHARE` locks, while publish needs only `ROW EXCLUSIVE` on the published tables. Renaming would have needed an exclusive lock and could show an empty table to an older REPEATABLE READ snapshot. The cost is a full copy each run (dead rows for vacuum), which is fine at this scale.

*What consumers see:*
- **One SQL statement** sees exactly one publication of all three marts.
- **Several READ COMMITTED statements** can each see a different publication.
- **A consistent multi-query view** needs one REPEATABLE READ transaction. Reading `marts.current_publication` in that same transaction tells you which publication it is.
- **Staging and intermediate are internal views** over raw: they are always live and are not validated results.

*Evidence:*
- `tests/v2/test_v2_publish.py`, 14 tests:
  - rollback after the first mart was copied;
  - concurrent publishers;
  - a REPEATABLE READ reader keeps its publication and does not block;
  - every single statement sees one complete publication while publications change;
  - refusals for another run's results, stale results from an earlier invocation, missing or failed validation, a candidate changed after validation, a sync after the build, a failed dbt build, and non-`v2_` databases;
  - views and grants survive;
  - a column change is refused while a view depends on the mart.
- The acceptance script checks, on every successful run, that the run published its own candidate. After injected sync, dbt and reconciliation failures, it checks that the published marts and publication are unchanged.

**Limitations:**
- **Coupling:** the DAG calls the CLIs inside the Airflow image (LocalExecutor). The dbt `run_results.json` that `publish` and `report` read relies on all tasks running in the same container. If it doesn't exist, publish is refused, which is safe.
- **Credentials:** dbt, reconciliation and publish use the owner's credentials. Reconciliation reads in read-only sessions, and writes only its validation row.
