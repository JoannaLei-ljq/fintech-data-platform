-- Anti-pattern kept for the demo: joining refunds to payments row by row and then
-- summing payment amounts counts the $100 payment once per refund.
-- Compiled with `dbt compile`; compare its result with fct_daily_payments.
select
    t.created_date_utc as summary_date,
    sum(t.amount) filter (where t.status = 'succeeded') as naive_success_amount
from {{ ref('stg_transactions') }} as t
left join {{ ref('stg_refunds') }} as r
  on r.transaction_id = t.transaction_id
group by 1
