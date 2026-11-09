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
    created_at,
    updated_at,
    {{ utc_date('requested_at') }} as requested_date_utc,
    {{ utc_date('succeeded_at') }} as succeeded_date_utc,
    {{ utc_date('failed_at') }}    as failed_date_utc
from {{ source('raw', 'refunds') }}
