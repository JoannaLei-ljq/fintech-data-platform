# V2 Phase 1: approved design and implementation notes

Status: Steps 0–3 approved and implemented on branch `v2-phase1`. Steps 4–8 still need approval.
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

1. **OLTP sequence enforcement (write time, Step 4).** The producer supplies `event_id` and `event_sequence`.
   - The write function will accept an event only if its sequence is exactly `last_event_sequence + 1` for that payment.
   - It will reject gaps as retryable errors, and treat a replay of the same `event_id` as a no-op.
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
- These columns arrive with the refund schema in Step 5. Until then, refunds keep their MVP status vocabulary.

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

### Known gaps until Step 4
- **Transitions aren't enforced on direct updates yet.** A direct `UPDATE transactions` can still move a payment to any state allowed by the CHECK constraints without writing a matching event. Likewise, direct `INSERT`s into `payment_events` can leave sequence gaps. Contiguity, legal transitions, idempotent replays and keeping state and history in sync all belong to the Step 4 write path.
- **Refunds still use the MVP vocabulary** (pending, succeeded, failed) and have no currency or idempotency key. That arrives in Step 5.
- **`v2_analytics` is still at the V001 baseline.** Its schema changes come with the sync engine in Step 6.
