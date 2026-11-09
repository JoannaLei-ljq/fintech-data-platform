-- Refunded volume in the marts equals SUCCEEDED refunds only (requested and failed excluded),
-- per currency, in every layer that reports it.
with truth as (
    select currency, coalesce(sum(amount_minor), 0) as amount, count(*) as refunds
    from {{ ref('stg_refunds') }} where status = 'succeeded' group by currency
),
layers as (
    select 'int_refund_totals' as layer, currency, sum(succeeded_refund_amount_minor) as amount, sum(succeeded_refund_count) as refunds
    from {{ ref('int_refund_totals') }} group by currency
    union all
    select 'fct_payments', currency, sum(succeeded_refund_amount_minor), sum(succeeded_refund_count)
    from {{ ref('fct_payments') }} group by currency
    union all
    select 'fct_daily_payments', currency, sum(succeeded_refund_amount_minor), sum(succeeded_refund_count)
    from {{ ref('fct_daily_payments') }} group by currency
    union all
    select 'fct_merchant_performance', currency, sum(succeeded_refund_amount_minor), sum(succeeded_refund_count)
    from {{ ref('fct_merchant_performance') }} group by currency
)
select l.layer, l.currency, l.amount, t.amount as expected_amount, l.refunds, t.refunds as expected_refunds
from layers l
left join truth t using (currency)
where coalesce(l.amount, 0) <> coalesce(t.amount, 0) or coalesce(l.refunds, 0) <> coalesce(t.refunds, 0)
