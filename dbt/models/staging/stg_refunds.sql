select
    refund_id,
    transaction_id,
    amount,
    reason,
    status,
    created_at,
    updated_at,
    (created_at at time zone 'UTC')::date as created_date_utc
from {{ source('raw', 'refunds') }}
