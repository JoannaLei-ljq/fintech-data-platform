-- Current state of each payment, as maintained by record_payment_event() in v2_oltp.
-- created_at is the payment.created event time; authorized_at / captured_at / failed_at are the
-- event times of those steps (for migrated MVP rows they are synthetic upper bounds).
select
    transaction_id,
    user_id,
    merchant_id,
    currency,
    amount_minor,
    status,
    last_event_sequence,
    failure_code,
    created_at,
    authorized_at,
    captured_at,
    failed_at,
    updated_at,
    {{ utc_date('created_at') }}  as created_date_utc,
    {{ utc_date('captured_at') }} as captured_date_utc,
    {{ utc_date('failed_at') }}   as failed_date_utc
from {{ source('raw', 'transactions') }}
