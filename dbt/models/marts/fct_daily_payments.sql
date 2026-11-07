-- One row per UTC day and currency.
--   success_amount   : sum of succeeded payment amounts, by payment created day
--   success_rate_pct : succeeded / (succeeded + failed); pending is not yet decided, so excluded
--   refund_amount    : sum of succeeded refunds, by refund created day
--
-- Payments and refunds are aggregated separately and joined on (day, currency).
-- Joining refunds to payments row-by-row first would repeat a payment once per
-- refund (the $100 payment with two refunds would count as $200).

with payments as (

    select
        created_date_utc                                           as summary_date,
        currency,
        count(*)                                                   as payment_count,
        count(*) filter (where status = 'succeeded')               as succeeded_count,
        count(*) filter (where status = 'failed')                  as failed_count,
        count(*) filter (where status = 'pending')                 as pending_count,
        coalesce(sum(amount) filter (where status = 'succeeded'), 0) as success_amount
    from {{ ref('stg_transactions') }}
    group by 1, 2

),

refunds as (

    -- Each refund matches exactly one payment (many-to-one), so this join only
    -- picks up the currency and cannot fan out refund rows.
    select
        r.created_date_utc                                           as summary_date,
        t.currency,
        count(*) filter (where r.status = 'succeeded')               as refund_count,
        coalesce(sum(r.amount) filter (where r.status = 'succeeded'), 0) as refund_amount
    from {{ ref('stg_refunds') }} as r
    join {{ ref('stg_transactions') }} as t
      on t.transaction_id = r.transaction_id
    group by 1, 2

)

select
    coalesce(p.summary_date, r.summary_date)::text || '|' || coalesce(p.currency, r.currency)
                                                    as summary_id,
    coalesce(p.summary_date, r.summary_date)        as summary_date,
    coalesce(p.currency, r.currency)                as currency,
    coalesce(p.payment_count, 0)                    as payment_count,
    coalesce(p.succeeded_count, 0)                  as succeeded_count,
    coalesce(p.failed_count, 0)                     as failed_count,
    coalesce(p.pending_count, 0)                    as pending_count,
    coalesce(p.success_amount, 0)::numeric(18, 2)   as success_amount,
    case
        when coalesce(p.succeeded_count, 0) + coalesce(p.failed_count, 0) = 0 then null
        else round(100.0 * p.succeeded_count / (p.succeeded_count + p.failed_count), 2)
    end::numeric(5, 2)                              as success_rate_pct,
    coalesce(r.refund_count, 0)                     as refund_count,
    coalesce(r.refund_amount, 0)::numeric(18, 2)    as refund_amount,
    (coalesce(p.success_amount, 0) - coalesce(r.refund_amount, 0))::numeric(18, 2)
                                                    as net_amount
from payments as p
full outer join refunds as r
  on r.summary_date = p.summary_date
 and r.currency = p.currency
