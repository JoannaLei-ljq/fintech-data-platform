-- One row per SUCCEEDED refund, dated by succeeded_at (the business date of refunded money),
-- with the payment's merchant for merchant reporting. Requested and failed refunds are excluded.
select
    r.refund_id,
    r.transaction_id,
    p.merchant_id,
    r.currency,
    r.amount_minor,
    r.requested_at,
    r.succeeded_at,
    r.requested_date_utc,
    r.succeeded_date_utc
from {{ ref('stg_refunds') }} as r
join {{ ref('stg_transactions') }} as p
  on p.transaction_id = r.transaction_id
where r.status = 'succeeded'
