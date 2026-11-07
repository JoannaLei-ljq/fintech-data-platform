-- V004: append-only payment lifecycle history, backfilled for existing payments.
--
-- Ordering is by event_sequence (per payment, assigned by the producer), never by time.
-- This migration adds only structural guarantees: unique (transaction_id, event_sequence),
-- sequence 1 is always payment.created, rows are append-only. Contiguity, legal transitions
-- and idempotent replays are enforced by the Step 4 write path.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";  -- uuid_generate_v5 for deterministic backfill IDs

CREATE TABLE payment_events (
    event_id       uuid        PRIMARY KEY,
    transaction_id bigint      NOT NULL REFERENCES transactions (transaction_id) ON DELETE RESTRICT,
    event_sequence integer     NOT NULL CHECK (event_sequence >= 1),
    event_type     text        NOT NULL CHECK (event_type IN
                       ('payment.created', 'payment.authorized', 'payment.captured', 'payment.failed')),
    amount_minor   bigint      CHECK (amount_minor > 0),
    currency       char(3)     REFERENCES currencies (currency_code),
    failure_code   text,
    source         text        NOT NULL CHECK (source IN ('api', 'processor', 'migration_backfill')),
    occurred_at    timestamptz NOT NULL,
    received_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT payment_events_transaction_sequence_key UNIQUE (transaction_id, event_sequence),
    CONSTRAINT payment_events_first_is_created_check
        CHECK ((event_sequence = 1) = (event_type = 'payment.created')),
    CONSTRAINT payment_events_amount_check
        CHECK ((event_type = 'payment.failed') = (amount_minor IS NULL)
               AND (amount_minor IS NULL) = (currency IS NULL)),
    CONSTRAINT payment_events_failure_code_check
        CHECK ((event_type = 'payment.failed') = (failure_code IS NOT NULL))
);

CREATE INDEX payment_events_received_at_idx ON payment_events (received_at, event_id);

COMMENT ON TABLE  payment_events IS 'Append-only payment lifecycle history; transactions holds the current state.';
COMMENT ON COLUMN payment_events.event_id IS 'Stable producer-supplied ID; replays of the same event reuse it.';
COMMENT ON COLUMN payment_events.event_sequence IS 'Per-payment order (1 = created). The only ordering key for state.';
COMMENT ON COLUMN payment_events.occurred_at IS 'Event time reported by the producer; not used for ordering.';
COMMENT ON COLUMN payment_events.received_at IS 'Ingestion time in this database.';

CREATE FUNCTION forbid_payment_event_mutation() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'payment_events is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation';
END $$;

CREATE TRIGGER payment_events_append_only
    BEFORE UPDATE OR DELETE ON payment_events
    FOR EACH ROW EXECUTE FUNCTION forbid_payment_event_mutation();

CREATE TRIGGER payment_events_no_truncate
    BEFORE TRUNCATE ON payment_events
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_payment_event_mutation();

-- Deterministic IDs: the same payment and sequence always yield the same event_id,
-- so rebuilding from the same data reproduces identical history.
CREATE FUNCTION backfill_event_id(transaction_id bigint, event_sequence integer) RETURNS uuid
    LANGUAGE sql IMMUTABLE
    RETURN uuid_generate_v5('dd5bf0f7-0faf-4ded-bee2-624ad539726e'::uuid,
                            format('payment_event:%s:%s', transaction_id, event_sequence));

INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type,
                            amount_minor, currency, source, occurred_at)
SELECT backfill_event_id(transaction_id, 1), transaction_id, 1, 'payment.created',
       amount_minor, currency, 'migration_backfill', created_at
FROM transactions;

INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type,
                            amount_minor, currency, source, occurred_at)
SELECT backfill_event_id(transaction_id, 2), transaction_id, 2, 'payment.authorized',
       amount_minor, currency, 'migration_backfill', authorized_at
FROM transactions
WHERE authorized_at IS NOT NULL;

INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type,
                            amount_minor, currency, source, occurred_at)
SELECT backfill_event_id(transaction_id, 3), transaction_id, 3, 'payment.captured',
       amount_minor, currency, 'migration_backfill', captured_at
FROM transactions
WHERE status = 'captured';

INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type,
                            failure_code, source, occurred_at)
SELECT backfill_event_id(transaction_id, last_event_sequence), transaction_id, last_event_sequence,
       'payment.failed', failure_code, 'migration_backfill', failed_at
FROM transactions
WHERE status = 'failed';

-- History and current state must agree for every payment before this migration commits.
DO $$
DECLARE
    problems text;
BEGIN
    WITH history AS (
        SELECT transaction_id, count(*) AS n, max(event_sequence) AS last_seq,
               (array_agg(event_type ORDER BY event_sequence DESC))[1] AS last_type
        FROM payment_events GROUP BY transaction_id
    )
    SELECT string_agg(t.transaction_id::text, ', ') INTO problems
    FROM transactions t
    LEFT JOIN history h USING (transaction_id)
    LEFT JOIN payment_events c ON c.transaction_id = t.transaction_id AND c.event_sequence = 1
    WHERE h.transaction_id IS NULL
       OR h.n <> t.last_event_sequence
       OR h.last_seq <> t.last_event_sequence
       OR h.last_type <> 'payment.' || t.status
       OR c.amount_minor <> t.amount_minor
       OR c.currency <> t.currency;
    IF problems IS NOT NULL THEN
        RAISE EXCEPTION 'V004: event history disagrees with transaction state for: %', problems;
    END IF;
END $$;
