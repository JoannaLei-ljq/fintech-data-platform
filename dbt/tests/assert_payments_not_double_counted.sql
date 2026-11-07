-- The daily summary must match payments and refunds summed straight from staging,
-- with no join between them. A row-level payment-to-refund join would repeat the
-- $100 payment once per refund and break this equality.
with expected_payments as (
    select created_date_utc as summary_date, currency,
           coalesce(sum(amount) filter (where status = 'succeeded'), 0) as success_amount
    from {{ ref('stg_transactions') }}
    group by 1, 2
),

expected_refunds as (
    select r.created_date_utc as summary_date, t.currency,
           coalesce(sum(r.amount) filter (where r.status = 'succeeded'), 0) as refund_amount
    from {{ ref('stg_refunds') }} as r
    join {{ ref('stg_transactions') }} as t using (transaction_id)
    group by 1, 2
)

select f.summary_id, f.success_amount, e.success_amount as expected_success_amount,
       f.refund_amount, er.refund_amount as expected_refund_amount
from {{ ref('fct_daily_payments') }} as f
left join expected_payments as e
  on e.summary_date = f.summary_date and e.currency = f.currency
left join expected_refunds as er
  on er.summary_date = f.summary_date and er.currency = f.currency
where f.success_amount is distinct from coalesce(e.success_amount, 0)
   or f.refund_amount  is distinct from coalesce(er.refund_amount, 0)
