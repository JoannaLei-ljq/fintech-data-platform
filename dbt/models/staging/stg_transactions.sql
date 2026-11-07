select
    transaction_id,
    user_id,
    merchant_id,
    amount,
    currency,
    status,
    created_at,
    updated_at,
    -- Daily reporting is on the UTC calendar day the payment was created.
    (created_at at time zone 'UTC')::date as created_date_utc
from {{ source('raw', 'transactions') }}
