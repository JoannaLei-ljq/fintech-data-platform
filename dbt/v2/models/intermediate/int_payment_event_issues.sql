-- One row per detected problem in the payment event history or its reconciliation with the
-- current payment state. Expected to be empty; the singular test assert_no_payment_event_issues
-- fails on any row. Nothing here repairs data.

with state as (
    select * from {{ ref('int_payment_state_from_events') }}
),

payments as (
    select * from {{ ref('stg_transactions') }}
),

events as (
    select * from {{ ref('stg_payment_events') }}
)

select transaction_id, 'conflicting_event_id' as issue_type,
       'an event_id appears with more than one payload' as detail
from state where has_conflicting_event_ids

union all
select transaction_id, 'duplicate_sequence', 'a (payment, event_sequence) has more than one event'
from state where has_duplicate_sequences

union all
select transaction_id, 'sequence_gap',
       'sequences ' || first_sequence || '..' || max_sequence || ' for ' || event_count || ' events'
from state where not is_contiguous

union all
select transaction_id, 'invalid_lifecycle', 'path ' || lifecycle_path
from state where not is_valid_lifecycle

union all
select p.transaction_id, 'state_mismatch',
       'transactions.status ' || p.status || ' but events end in ' || s.derived_status
from payments p join state s using (transaction_id)
where p.status <> s.derived_status

union all
select p.transaction_id, 'last_sequence_mismatch',
       'transactions.last_event_sequence ' || p.last_event_sequence || ' but last event is ' || s.derived_last_sequence
from payments p join state s using (transaction_id)
where p.last_event_sequence <> s.derived_last_sequence

union all
select p.transaction_id, 'payment_without_events', 'no payment events'
from payments p left join state s using (transaction_id)
where s.transaction_id is null

union all
select e.transaction_id, 'event_without_payment', 'event ' || e.event_id || ' has no payment row'
from events e left join payments p using (transaction_id)
where p.transaction_id is null

union all
select e.transaction_id, 'event_amount_mismatch',
       'event ' || e.event_id || ' carries ' || e.amount_minor || ' ' || e.currency
from events e join payments p using (transaction_id)
where e.amount_minor is not null
  and (e.amount_minor <> p.amount_minor or e.currency <> p.currency)
