-- Event history must be contiguous, legal (no transition after a terminal state), free of
-- conflicting event ids, and its final state must match transactions.status and
-- last_event_sequence. Any row in int_payment_event_issues fails the build.
select * from {{ ref('int_payment_event_issues') }}
