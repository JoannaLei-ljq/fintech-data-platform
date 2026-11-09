-- One row per payment that has events: its state derived from the event stream alone.
--
-- Order is (transaction_id, event_sequence) and nothing else: not occurred_at, received_at,
-- sync_run_id or load order. Exact duplicates of an event (same event_id, same payload) are
-- collapsed; anything else is kept and FLAGGED, never repaired:
--   has_conflicting_event_ids  one event_id with more than one payload
--   has_duplicate_sequences    one (payment, sequence) claimed by more than one event
--   is_contiguous              sequences are exactly 1..n
--   is_valid_lifecycle         the path is one of the five legal paths (which also means no
--                              transition follows a terminal state)
-- int_payment_event_issues turns these flags into rows, and a singular test fails on any row.

with events as (

    -- Collapse exact duplicates only. sync_run_id/received_at are not part of an event's payload.
    select distinct
        event_id, transaction_id, event_sequence, lifecycle_status, amount_minor, currency,
        failure_code, event_source, occurred_at
    from {{ ref('stg_payment_events') }}

),

ordered as (

    select
        *,
        row_number() over (partition by transaction_id order by event_sequence desc, event_id desc) as rn_last
    from events

),

summary as (

    select
        transaction_id,
        count(*)                                                       as event_rows,
        count(distinct event_id)                                       as event_count,
        count(distinct event_sequence)                                 as distinct_sequences,
        min(event_sequence)                                            as first_sequence,
        max(event_sequence)                                            as max_sequence,
        string_agg(lifecycle_status, ',' order by event_sequence, event_id) as lifecycle_path,
        bool_or(event_source = 'migration_backfill')                   as has_backfilled_events,
        min(occurred_at) filter (where lifecycle_status = 'created')    as created_occurred_at,
        min(occurred_at) filter (where lifecycle_status = 'authorized') as authorized_occurred_at,
        min(occurred_at) filter (where lifecycle_status = 'captured')   as captured_occurred_at,
        min(occurred_at) filter (where lifecycle_status = 'failed')     as failed_occurred_at,
        bool_or(lifecycle_status = 'captured' and event_source = 'migration_backfill')
                                                                       as captured_by_backfill,
        bool_or(lifecycle_status = 'authorized' and event_source = 'migration_backfill')
                                                                       as authorized_by_backfill,
        bool_or(lifecycle_status = 'failed' and event_source = 'migration_backfill')
                                                                       as failed_by_backfill,
        bool_or(lifecycle_status = 'created' and event_source = 'migration_backfill')
                                                                       as created_by_backfill
    from events
    group by transaction_id

)

select
    s.transaction_id,
    l.lifecycle_status                                   as derived_status,
    l.event_sequence                                     as derived_last_sequence,
    s.event_count,
    s.first_sequence,
    s.max_sequence,
    s.lifecycle_path,
    s.event_rows > s.event_count                         as has_conflicting_event_ids,
    s.distinct_sequences < s.event_count                 as has_duplicate_sequences,
    (s.first_sequence = 1 and s.max_sequence = s.event_count
        and s.distinct_sequences = s.event_count)        as is_contiguous,
    s.lifecycle_path in {{ legal_lifecycle_paths() }}    as is_valid_lifecycle,
    s.has_backfilled_events,
    coalesce(s.captured_by_backfill, false)              as captured_by_backfill,
    coalesce(s.authorized_by_backfill, false)            as authorized_by_backfill,
    coalesce(s.failed_by_backfill, false)                as failed_by_backfill,
    coalesce(s.created_by_backfill, false)               as created_by_backfill,
    s.created_occurred_at,
    s.authorized_occurred_at,
    s.captured_occurred_at,
    s.failed_occurred_at
from summary as s
join ordered as l
  on l.transaction_id = s.transaction_id
 and l.rn_last = 1
