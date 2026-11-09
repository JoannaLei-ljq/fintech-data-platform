-- Refunds: requested -> succeeded | failed. Only succeeded refunds are completed money
-- movements; they are attributed to succeeded_at.
select
    refund_id,
    transaction_id,
    currency,
    amount_minor,
    status,
    reason,
    idempotency_key,
    failure_code,
    requested_at,
    succeeded_at,
    failed_at,
    -- Provenance (refunds.record_source, OLTP V009). Migrated MVP refunds: requested_at is the
    -- MVP's recorded created_at; succeeded_at / failed_at are synthetic upper bounds and
    -- failure_code 'legacy_unknown' is synthetic. API refunds: recorded by the system.
    record_source,
    record_source = 'migration_backfill'                                   as is_migrated,
    case when record_source = 'migration_backfill' then 'mvp_recorded'
         else 'database_clock' end                                         as requested_at_provenance,
    case when status = 'requested' then null
         when record_source = 'migration_backfill' then 'synthetic_upper_bound'
         else 'recorded' end                                               as completed_at_provenance,
    record_source = 'migration_backfill' and status = 'failed'             as failure_code_is_synthetic,
    created_at,
    updated_at,
    {{ utc_date('requested_at') }} as requested_date_utc,
    {{ utc_date('succeeded_at') }} as succeeded_date_utc,
    {{ utc_date('failed_at') }}    as failed_date_utc
from {{ source('raw', 'refunds') }}
