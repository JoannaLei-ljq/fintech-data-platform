-- Refund amount checks. Returns offending rows; the test passes when it returns none.
--   1. every refund amount is positive
--   2. succeeded refunds never exceed the original payment amount
--   3. a succeeded refund only exists for a succeeded payment
with refund_totals as (
    select
        transaction_id,
        sum(amount) filter (where status = 'succeeded') as succeeded_refund_amount,
        count(*)    filter (where status = 'succeeded') as succeeded_refund_count
    from {{ ref('stg_refunds') }}
    group by 1
)

select r.refund_id, r.transaction_id, 'non_positive_amount' as problem
from {{ ref('stg_refunds') }} as r
where r.amount <= 0

union all

select null, t.transaction_id, 'refunds_exceed_payment'
from refund_totals as rt
join {{ ref('stg_transactions') }} as t using (transaction_id)
where rt.succeeded_refund_amount > t.amount

union all

select null, t.transaction_id, 'refund_on_unsuccessful_payment'
from refund_totals as rt
join {{ ref('stg_transactions') }} as t using (transaction_id)
where rt.succeeded_refund_count > 0
  and t.status <> 'succeeded'
