-- Open (requested) + succeeded refunds never exceed the payment amount, and refunds exist only
-- on captured payments. Failed refunds released their reservation, so they do not count.
select p.transaction_id, p.status, p.amount_minor,
       sum(r.amount_minor) filter (where r.status in ('requested', 'succeeded')) as reserved_minor
from {{ ref('stg_transactions') }} p
join {{ ref('stg_refunds') }} r using (transaction_id)
group by p.transaction_id, p.status, p.amount_minor
having p.status <> 'captured'
    or coalesce(sum(r.amount_minor) filter (where r.status in ('requested', 'succeeded')), 0) > p.amount_minor
