-- One row per payment that has refunds, in that payment's currency (exact integer minor units).
-- Aggregated BEFORE joining to payments, so a payment with several refunds is never repeated.
--   requested_*  every refund request ever made, whatever its outcome
--   pending_*    requests still open (status requested): they reserve refundable capacity
--   succeeded_*  completed refunds: the only refunded money
--   failed_*     failed requests: they released their reservation and refund nothing
select
    transaction_id,
    currency,
    count(*)                                                              as refund_request_count,
    count(*) filter (where status = 'requested')                          as pending_refund_count,
    count(*) filter (where status = 'succeeded')                          as succeeded_refund_count,
    count(*) filter (where status = 'failed')                             as failed_refund_count,
    sum(amount_minor)                                                     as requested_refund_amount_minor,
    coalesce(sum(amount_minor) filter (where status = 'requested'), 0)    as pending_refund_amount_minor,
    coalesce(sum(amount_minor) filter (where status = 'succeeded'), 0)    as succeeded_refund_amount_minor,
    coalesce(sum(amount_minor) filter (where status = 'failed'), 0)       as failed_refund_amount_minor,
    max(succeeded_at)                                                     as last_succeeded_at
from {{ ref('stg_refunds') }}
group by transaction_id, currency
