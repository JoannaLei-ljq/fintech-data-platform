-- Append-only payment history. Business order is (transaction_id, event_sequence) only:
-- occurred_at is the producer's event time, received_at the OLTP ingestion time, and
-- sync_run_id the warehouse load; none of them orders events.
select
    event_id,
    transaction_id,
    event_sequence,
    event_type,
    replace(event_type, 'payment.', '')     as lifecycle_status,
    amount_minor,
    currency,
    failure_code,
    source                                  as event_source,
    source = 'migration_backfill'           as is_backfilled,
    occurred_at,
    -- Provenance: backfilled created events carry the MVP's recorded created_at; every other
    -- backfilled event time is a synthetic upper bound (the MVP's updated_at), never observed.
    case when source <> 'migration_backfill' then 'producer_reported'
         when event_type = 'payment.created' then 'mvp_recorded'
         else 'synthetic_upper_bound' end    as occurred_at_provenance,
    received_at,
    case when source = 'migration_backfill' then 'migration_run_time'
         else 'oltp_ingestion' end           as received_at_provenance,
    {{ utc_date('occurred_at') }}           as occurred_date_utc,
    _run_id                                 as sync_run_id
from {{ source('raw', 'payment_events') }}
