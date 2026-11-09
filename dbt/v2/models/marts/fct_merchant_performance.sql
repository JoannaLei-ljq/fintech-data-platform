-- Grain: one row per (merchant_id, report_date_utc, currency). Currencies are never combined.
--
--   created_payment_count       payments created on the date
--   captured_* / failed_count   on the capture / failure date (outcome basis, as in
--                               fct_daily_payments); outcome_success_rate_pct = captured / (captured + failed)
--   succeeded_refund_*          refunds on their succeeded_at date
--   net_captured_amount_minor   captured_amount_minor - succeeded_refund_amount_minor (same date)
--   refund_rate_pct             cohort by capture date: of the money captured on this date, the
--                               share refunded so far = refunded_of_captured_amount_minor / captured_amount_minor
--                               (changes as later refunds succeed; NULL when nothing was captured)
--   estimated_*_count / has_estimated_dates   how many of the row's dates are synthetic (migrated MVP)

with payments as (
    select * from {{ ref('fct_payments') }}
),

created as (
    select merchant_id, created_date_utc as report_date_utc, currency, count(*) as created_payment_count
    from payments group by 1, 2, 3
),

captures as (
    select
        merchant_id, captured_date_utc as report_date_utc, currency,
        count(*)                                     as captured_count,
        sum(amount_minor)                            as captured_amount_minor,
        sum(succeeded_refund_amount_minor)           as refunded_of_captured_amount_minor,
        count(*) filter (where captured_at_is_estimate) as estimated_capture_time_count
    from payments where is_captured group by 1, 2, 3
),

failures as (
    select merchant_id, failed_date_utc as report_date_utc, currency, count(*) as failed_count,
           count(*) filter (where failed_at_is_estimate) as estimated_failure_time_count
    from payments where is_failed group by 1, 2, 3
),

refunds as (
    select
        merchant_id, succeeded_date_utc as report_date_utc, currency,
        count(*)            as succeeded_refund_count,
        sum(amount_minor)   as succeeded_refund_amount_minor,
        count(*) filter (where succeeded_at_is_estimate) as estimated_refund_time_count
    from {{ ref('int_refund_completions') }} group by 1, 2, 3
),

grain as (
    select merchant_id, report_date_utc, currency from created
    union select merchant_id, report_date_utc, currency from captures
    union select merchant_id, report_date_utc, currency from failures
    union select merchant_id, report_date_utc, currency from refunds
)

select
    g.merchant_id,
    m.merchant_name,
    g.report_date_utc,
    g.currency,
    coalesce(c.created_payment_count, 0)                                as created_payment_count,
    coalesce(k.captured_count, 0)                                       as captured_count,
    coalesce(k.captured_amount_minor, 0)                                as captured_amount_minor,
    coalesce(f.failed_count, 0)                                         as failed_count,
    {{ safe_pct('coalesce(k.captured_count, 0)',
                'coalesce(k.captured_count, 0) + coalesce(f.failed_count, 0)') }}
                                                                        as outcome_success_rate_pct,
    coalesce(r.succeeded_refund_count, 0)                               as succeeded_refund_count,
    coalesce(r.succeeded_refund_amount_minor, 0)                        as succeeded_refund_amount_minor,
    coalesce(k.captured_amount_minor, 0) - coalesce(r.succeeded_refund_amount_minor, 0)
                                                                        as net_captured_amount_minor,
    coalesce(k.refunded_of_captured_amount_minor, 0)                    as refunded_of_captured_amount_minor,
    {{ safe_pct('coalesce(k.refunded_of_captured_amount_minor, 0)', 'coalesce(k.captured_amount_minor, 0)') }}
                                                                        as refund_rate_pct,
    coalesce(k.estimated_capture_time_count, 0)                         as estimated_capture_time_count,
    coalesce(f.estimated_failure_time_count, 0)                         as estimated_failure_time_count,
    coalesce(r.estimated_refund_time_count, 0)                          as estimated_refund_time_count,
    coalesce(k.estimated_capture_time_count, 0) + coalesce(f.estimated_failure_time_count, 0)
        + coalesce(r.estimated_refund_time_count, 0) > 0                as has_estimated_dates
from grain as g
left join created  as c on (c.merchant_id, c.report_date_utc, c.currency) = (g.merchant_id, g.report_date_utc, g.currency)
left join captures as k on (k.merchant_id, k.report_date_utc, k.currency) = (g.merchant_id, g.report_date_utc, g.currency)
left join failures as f on (f.merchant_id, f.report_date_utc, f.currency) = (g.merchant_id, g.report_date_utc, g.currency)
left join refunds  as r on (r.merchant_id, r.report_date_utc, r.currency) = (g.merchant_id, g.report_date_utc, g.currency)
left join {{ ref('stg_merchants') }} as m on m.merchant_id = g.merchant_id
