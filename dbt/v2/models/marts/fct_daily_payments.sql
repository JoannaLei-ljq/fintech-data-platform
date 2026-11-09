-- Grain: one row per (report_date_utc, currency). Currencies are never added together.
--
-- Two explicitly separate date bases (see docs/v2/PHASE1_DESIGN.md, Step 7):
--   cohort_*   payments CREATED on report_date_utc, by their current status
--              cohort_success_rate_pct = captured / (captured + failed) within that cohort
--   outcome    captured_* on the capture date, failed_count on the failure date,
--              outcome_success_rate_pct = captured / (captured + failed) on that date
--   refunds    succeeded refunds on their succeeded_at date (requested/failed excluded)
--   net_captured_amount_minor = captured_amount_minor - succeeded_refund_amount_minor
--              (money in minus money out on that date; both on event dates)
-- Rates are NULL when the denominator is zero; in-flight payments are excluded from rates.
-- estimated_*_count say how many of the row's captures, failures and refunds are dated by a
-- synthetic (migrated MVP) timestamp; has_estimated_dates is true if any are. Those rows are
-- still counted, but their dates are not verified processor timestamps.

with payments as (
    select * from {{ ref('fct_payments') }}
),

cohort as (
    select
        created_date_utc                                        as report_date_utc,
        currency,
        count(*)                                                as created_payment_count,
        count(*) filter (where is_captured)                     as cohort_captured_count,
        count(*) filter (where is_failed)                       as cohort_failed_count,
        count(*) filter (where is_in_flight)                    as cohort_in_flight_count,
        coalesce(sum(amount_minor) filter (where is_captured), 0) as cohort_captured_amount_minor
    from payments
    group by 1, 2
),

captures as (
    select
        captured_date_utc                                       as report_date_utc,
        currency,
        count(*)                                                as captured_count,
        sum(amount_minor)                                       as captured_amount_minor,
        count(*) filter (where captured_at_is_estimate)         as estimated_capture_time_count
    from payments
    where is_captured
    group by 1, 2
),

failures as (
    select failed_date_utc as report_date_utc, currency, count(*) as failed_count,
           count(*) filter (where failed_at_is_estimate) as estimated_failure_time_count
    from payments
    where is_failed
    group by 1, 2
),

refunds as (
    select
        succeeded_date_utc                                      as report_date_utc,
        currency,
        count(*)                                                as succeeded_refund_count,
        sum(amount_minor)                                       as succeeded_refund_amount_minor,
        count(*) filter (where succeeded_at_is_estimate)        as estimated_refund_time_count
    from {{ ref('int_refund_completions') }}
    group by 1, 2
),

grain as (
    select report_date_utc, currency from cohort
    union select report_date_utc, currency from captures
    union select report_date_utc, currency from failures
    union select report_date_utc, currency from refunds
)

select
    g.report_date_utc::text || '|' || g.currency                       as report_id,
    g.report_date_utc,
    g.currency,
    cur.minor_units,
    coalesce(c.created_payment_count, 0)                                as created_payment_count,
    coalesce(c.cohort_captured_count, 0)                                as cohort_captured_count,
    coalesce(c.cohort_failed_count, 0)                                  as cohort_failed_count,
    coalesce(c.cohort_in_flight_count, 0)                               as cohort_in_flight_count,
    coalesce(c.cohort_captured_amount_minor, 0)                         as cohort_captured_amount_minor,
    {{ safe_pct('coalesce(c.cohort_captured_count, 0)',
                'coalesce(c.cohort_captured_count, 0) + coalesce(c.cohort_failed_count, 0)') }}
                                                                        as cohort_success_rate_pct,
    coalesce(k.captured_count, 0)                                       as captured_count,
    coalesce(k.captured_amount_minor, 0)                                as captured_amount_minor,
    coalesce(k.estimated_capture_time_count, 0)                         as estimated_capture_time_count,
    coalesce(f.failed_count, 0)                                         as failed_count,
    {{ safe_pct('coalesce(k.captured_count, 0)',
                'coalesce(k.captured_count, 0) + coalesce(f.failed_count, 0)') }}
                                                                        as outcome_success_rate_pct,
    coalesce(r.succeeded_refund_count, 0)                               as succeeded_refund_count,
    coalesce(r.succeeded_refund_amount_minor, 0)                        as succeeded_refund_amount_minor,
    coalesce(k.captured_amount_minor, 0) - coalesce(r.succeeded_refund_amount_minor, 0)
                                                                        as net_captured_amount_minor,
    coalesce(f.estimated_failure_time_count, 0)                         as estimated_failure_time_count,
    coalesce(r.estimated_refund_time_count, 0)                          as estimated_refund_time_count,
    coalesce(k.estimated_capture_time_count, 0) + coalesce(f.estimated_failure_time_count, 0)
        + coalesce(r.estimated_refund_time_count, 0) > 0                as has_estimated_dates
from grain as g
left join cohort   as c on c.report_date_utc = g.report_date_utc and c.currency = g.currency
left join captures as k on k.report_date_utc = g.report_date_utc and k.currency = g.currency
left join failures as f on f.report_date_utc = g.report_date_utc and f.currency = g.currency
left join refunds  as r on r.report_date_utc = g.report_date_utc and r.currency = g.currency
left join {{ ref('stg_currencies') }} as cur on cur.currency_code = g.currency
