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
| `pending` | *see Step 3 decision below* | |

## Implementation log

See the sections below, which are filled in as each step lands.
