-- Payment counts and captured amounts agree, per currency, between staging and every mart,
-- whatever the date basis (a re-dated metric must still add up to the same total).
with truth as (
    select currency,
           count(*)                                                as payments,
           count(*) filter (where status = 'captured')             as captured,
           count(*) filter (where status = 'failed')               as failed,
           coalesce(sum(amount_minor) filter (where status = 'captured'), 0) as captured_amount,
           sum(amount_minor)                                       as amount
    from {{ ref('stg_transactions') }} group by currency
),
layers as (
    select 'fct_payments' as layer, currency, count(*) as payments,
           count(*) filter (where is_captured) as captured, count(*) filter (where is_failed) as failed,
           coalesce(sum(amount_minor) filter (where is_captured), 0) as captured_amount, sum(amount_minor) as amount
    from {{ ref('fct_payments') }} group by currency
    union all
    select 'fct_daily_payments (cohort)', currency, sum(created_payment_count), sum(cohort_captured_count),
           sum(cohort_failed_count), sum(cohort_captured_amount_minor), null
    from {{ ref('fct_daily_payments') }} group by currency
    union all
    select 'fct_daily_payments (outcome)', currency, null, sum(captured_count), sum(failed_count),
           sum(captured_amount_minor), null
    from {{ ref('fct_daily_payments') }} group by currency
    union all
    select 'fct_merchant_performance', currency, sum(created_payment_count), sum(captured_count), sum(failed_count),
           sum(captured_amount_minor), null
    from {{ ref('fct_merchant_performance') }} group by currency
)
select l.*, t.payments as expected_payments, t.captured as expected_captured, t.failed as expected_failed,
       t.captured_amount as expected_captured_amount, t.amount as expected_amount
from layers l
full outer join truth t using (currency)
where l.layer is null
   or (l.payments is not null and l.payments <> t.payments)
   or l.captured <> t.captured
   or l.failed <> t.failed
   or l.captured_amount <> t.captured_amount
   or (l.amount is not null and l.amount <> t.amount)
