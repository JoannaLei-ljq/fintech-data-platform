select r.refund_id, r.currency as refund_currency, p.currency as payment_currency
from {{ ref('stg_refunds') }} r
join {{ ref('stg_transactions') }} p using (transaction_id)
where r.currency <> p.currency
