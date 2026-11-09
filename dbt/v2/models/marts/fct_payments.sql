-- Grain: one row per payment (transaction_id). Amounts are integer minor units of `currency`.
--
-- amount_minor is the original payment amount and never changes; refunds are reported beside
-- it, aggregated to one row per payment first (int_refund_totals), so nothing fans out.
-- captured_at is the capture event time. For payments migrated from the MVP it is a synthetic
-- upper bound (the MVP never recorded it): captured_at_is_estimate flags those rows.

with payments as (
    select * from {{ ref('stg_transactions') }}
),

events as (
    select * from {{ ref('int_payment_state_from_events') }}
),

refunds as (
    select * from {{ ref('int_refund_totals') }}
)

select
    p.transaction_id,
    p.user_id,
    p.merchant_id,
    p.currency,
    p.amount_minor,
    p.status,
    e.derived_status                                            as event_derived_status,
    coalesce(e.derived_status = p.status, false)                as status_matches_events,
    p.last_event_sequence,
    p.failure_code,
    p.created_at,
    p.created_date_utc,
    p.authorized_at,
    p.captured_at,
    p.captured_date_utc,
    p.failed_at,
    p.failed_date_utc,
    coalesce(e.captured_by_backfill, false)                     as captured_at_is_estimate,
    coalesce(e.has_backfilled_events, false)                    as has_backfilled_events,
    p.status = 'captured'                                       as is_captured,
    p.status = 'failed'                                         as is_failed,
    p.status in ('created', 'authorized')                       as is_in_flight,
    coalesce(r.refund_request_count, 0)                         as refund_request_count,
    coalesce(r.pending_refund_count, 0)                         as pending_refund_count,
    coalesce(r.succeeded_refund_count, 0)                       as succeeded_refund_count,
    coalesce(r.failed_refund_count, 0)                          as failed_refund_count,
    coalesce(r.requested_refund_amount_minor, 0)                as requested_refund_amount_minor,
    coalesce(r.pending_refund_amount_minor, 0)                  as pending_refund_amount_minor,
    coalesce(r.succeeded_refund_amount_minor, 0)                as succeeded_refund_amount_minor,
    coalesce(r.failed_refund_amount_minor, 0)                   as failed_refund_amount_minor,
    -- Same rule as request_refund(): open requests reserve capacity, failed ones do not.
    case when p.status = 'captured'
         then p.amount_minor - coalesce(r.pending_refund_amount_minor, 0)
                             - coalesce(r.succeeded_refund_amount_minor, 0)
         else 0 end                                             as remaining_refundable_amount_minor,
    case when p.status = 'captured'
         then p.amount_minor - coalesce(r.succeeded_refund_amount_minor, 0)
         else 0 end                                             as net_captured_amount_minor,
    p.status = 'captured'
        and coalesce(r.succeeded_refund_amount_minor, 0) = p.amount_minor as is_fully_refunded
from payments as p
left join events as e  on e.transaction_id = p.transaction_id
left join refunds as r on r.transaction_id = p.transaction_id
