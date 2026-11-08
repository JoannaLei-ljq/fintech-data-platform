-- V005: the payment event write path.
--
-- record_payment_event() is the only way ordinary application code can change payment state.
-- It runs in the caller's transaction, locks the payment row, and then, all or nothing:
--   * treats a replay of an existing event_id with an identical payload as a no-op ('duplicate');
--   * rejects an existing event_id with a different payload           (SQLSTATE PE006);
--   * rejects a sequence number that is already taken                   (PE004);
--   * rejects a gap: anything but last_event_sequence + 1              (PE003, retryable);
--   * rejects an illegal transition                                    (PE005);
--   * otherwise appends the event and updates the current state in transactions.
-- Other errors: PE001 invalid event payload, PE002 unknown payment.
--
-- Legal lifecycle (Phase 1): created -> authorized -> captured; created -> failed;
-- authorized -> failed. captured and failed are terminal.
--
-- Three layers keep transactions and payment_events consistent:
--   1. Privileges (primary control). Application code runs as v2_payment_app, which can read
--      but has no INSERT/UPDATE/DELETE/TRUNCATE on either table; it can only EXECUTE the
--      function. The function is SECURITY DEFINER and owned by v2_payment_writer, which holds
--      only the column privileges the function needs. Both roles are NOLOGIN group roles.
--   2. The function's checks (lock, idempotency, sequence, transition, payload rules).
--   3. Deferred constraint triggers that, at commit, re-check every touched payment: history
--      contiguous, a legal path, and its last event matching the current state. This also
--      catches direct writes by the table owner. Only a superuser can switch triggers off.
--
-- Roles are cluster-wide objects. They are created only if missing and must not carry any
-- elevated attribute; their privileges are granted per database, here in v2_* only.
-- Ordering note: this is OLTP write-time sequence enforcement. Out-of-order handling for
-- events arriving downstream (analytics) is a separate concern for Steps 6-7.

-- 1. Roles ------------------------------------------------------------------------------

DO $$
DECLARE
    r text;
BEGIN
    FOREACH r IN ARRAY ARRAY['v2_payment_writer', 'v2_payment_app'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', r);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r
                   AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls)) THEN
            RAISE EXCEPTION 'V005: role % exists with elevated attributes; refusing to grant to it', r;
        END IF;
    END LOOP;
    IF pg_has_role('v2_payment_app', 'v2_payment_writer', 'MEMBER') THEN
        RAISE EXCEPTION 'V005: v2_payment_app must not be a member of v2_payment_writer';
    END IF;
END $$;

-- 2. Privileges -------------------------------------------------------------------------

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO v2_payment_app, v2_payment_writer;

-- Application role: read everything it needs, write nothing directly.
GRANT SELECT ON users, merchants, currencies, transactions, refunds, payment_events,
                legacy_transactions, legacy_refunds
    TO v2_payment_app;

-- Function owner: exactly the privileges record_payment_event() uses.
GRANT SELECT ON currencies, transactions, payment_events TO v2_payment_writer;
GRANT INSERT (transaction_id, user_id, merchant_id, amount_minor, currency, status,
              last_event_sequence, created_at)
    ON transactions TO v2_payment_writer;
GRANT UPDATE (status, last_event_sequence, failure_code, authorized_at, captured_at, failed_at)
    ON transactions TO v2_payment_writer;
GRANT INSERT ON payment_events TO v2_payment_writer;

-- 3. The write function -----------------------------------------------------------------

CREATE FUNCTION record_payment_event(
    p_event_id       uuid,
    p_transaction_id bigint,
    p_event_sequence integer,
    p_event_type     text,
    p_occurred_at    timestamptz,
    p_source         text,
    p_amount_minor   bigint  DEFAULT NULL,
    p_currency       char(3) DEFAULT NULL,
    p_failure_code   text    DEFAULT NULL,
    p_user_id        bigint  DEFAULT NULL,
    p_merchant_id    bigint  DEFAULT NULL)
RETURNS TABLE (outcome text, transaction_status text, last_event_sequence integer,
               received_at timestamptz)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
#variable_conflict use_column
DECLARE
    inserted     boolean := false;
    tx           transactions%ROWTYPE;
    prior        payment_events%ROWTYPE;
    new_status   text;
    prev_time    timestamptz;
    v_received   timestamptz;
BEGIN
    -- Payload validation (no locks, no writes yet). ---------------------------------------
    IF p_event_id IS NULL OR p_transaction_id IS NULL OR p_event_sequence IS NULL
       OR p_event_type IS NULL OR p_occurred_at IS NULL OR p_source IS NULL THEN
        RAISE EXCEPTION 'event_id, transaction_id, event_sequence, event_type, occurred_at and source are required'
            USING ERRCODE = 'PE001';
    END IF;
    IF p_source NOT IN ('api', 'processor') THEN
        RAISE EXCEPTION 'source must be api or processor, got %', p_source USING ERRCODE = 'PE001';
    END IF;
    IF p_event_sequence < 1 THEN
        RAISE EXCEPTION 'event_sequence must be >= 1' USING ERRCODE = 'PE001';
    END IF;
    CASE p_event_type
        WHEN 'payment.created' THEN
            IF p_event_sequence <> 1 OR p_user_id IS NULL OR p_merchant_id IS NULL
               OR p_amount_minor IS NULL OR p_currency IS NULL OR p_failure_code IS NOT NULL THEN
                RAISE EXCEPTION 'payment.created needs event_sequence 1, user_id, merchant_id, amount_minor and currency, and no failure_code'
                    USING ERRCODE = 'PE001';
            END IF;
            IF p_amount_minor <= 0 THEN
                RAISE EXCEPTION 'amount_minor must be positive' USING ERRCODE = 'PE001';
            END IF;
        WHEN 'payment.authorized', 'payment.captured' THEN
            IF p_amount_minor IS NULL OR p_currency IS NULL OR p_failure_code IS NOT NULL
               OR p_user_id IS NOT NULL OR p_merchant_id IS NOT NULL THEN
                RAISE EXCEPTION '% needs amount_minor and currency, and no failure_code, user_id or merchant_id',
                    p_event_type USING ERRCODE = 'PE001';
            END IF;
        WHEN 'payment.failed' THEN
            IF nullif(btrim(p_failure_code), '') IS NULL OR p_amount_minor IS NOT NULL
               OR p_currency IS NOT NULL OR p_user_id IS NOT NULL OR p_merchant_id IS NOT NULL THEN
                RAISE EXCEPTION 'payment.failed needs a failure_code and no amount_minor, currency, user_id or merchant_id'
                    USING ERRCODE = 'PE001';
            END IF;
        ELSE
            RAISE EXCEPTION 'unknown event_type %', p_event_type USING ERRCODE = 'PE001';
    END CASE;

    -- Lock the payment row. A created event inserts it first; a concurrent creator of the
    -- same payment waits here for us and then sees our committed row. --------------------
    IF p_event_type = 'payment.created' THEN
        IF NOT EXISTS (SELECT 1 FROM currencies WHERE currency_code = p_currency) THEN
            RAISE EXCEPTION 'unknown currency %', p_currency USING ERRCODE = 'PE001';
        END IF;
        INSERT INTO transactions AS t (transaction_id, user_id, merchant_id, amount_minor, currency,
                                       status, last_event_sequence, created_at)
        VALUES (p_transaction_id, p_user_id, p_merchant_id, p_amount_minor, p_currency,
                'created', 1, p_occurred_at)
        ON CONFLICT (transaction_id) DO NOTHING;
        inserted := FOUND;  -- false: the payment already exists (a replay or a conflict)
    END IF;

    SELECT * INTO tx FROM transactions t WHERE t.transaction_id = p_transaction_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown payment %', p_transaction_id USING ERRCODE = 'PE002';
    END IF;

    -- Idempotency: same event_id. Read after the lock, so a concurrent identical submission
    -- that committed first is visible here. -------------------------------------------------
    SELECT * INTO prior FROM payment_events e WHERE e.event_id = p_event_id;
    IF FOUND THEN
        IF prior.transaction_id = p_transaction_id
           AND prior.event_sequence = p_event_sequence
           AND prior.event_type = p_event_type
           AND prior.amount_minor IS NOT DISTINCT FROM p_amount_minor
           AND prior.currency IS NOT DISTINCT FROM p_currency
           AND prior.failure_code IS NOT DISTINCT FROM p_failure_code
           AND prior.source = p_source
           AND prior.occurred_at = p_occurred_at
           AND (p_event_type <> 'payment.created'
                OR (tx.user_id = p_user_id AND tx.merchant_id = p_merchant_id)) THEN
            RETURN QUERY SELECT 'duplicate'::text, tx.status, tx.last_event_sequence, prior.received_at;
            RETURN;
        END IF;
        RAISE EXCEPTION 'event_id % was already recorded with a different payload', p_event_id
            USING ERRCODE = 'PE006';
    END IF;

    -- Contiguous per-payment sequence (a new payment's created event is sequence 1 by now). -----
    IF NOT inserted AND p_event_sequence <= tx.last_event_sequence THEN
        RAISE EXCEPTION 'payment %: event_sequence % is already recorded (last is %)',
            p_transaction_id, p_event_sequence, tx.last_event_sequence USING ERRCODE = 'PE004';
    END IF;
    IF p_event_sequence > tx.last_event_sequence + 1 THEN
        RAISE EXCEPTION 'payment %: event_sequence % leaves a gap (expected %)',
            p_transaction_id, p_event_sequence, tx.last_event_sequence + 1
            USING ERRCODE = 'PE003', HINT = 'retry after the missing events are recorded';
    END IF;

    -- Legal transitions. ------------------------------------------------------------------------
    new_status := substr(p_event_type, length('payment.') + 1);
    IF NOT (inserted
         OR (tx.status = 'created'    AND new_status IN ('authorized', 'failed'))
         OR (tx.status = 'authorized' AND new_status IN ('captured', 'failed'))) THEN
        RAISE EXCEPTION 'payment %: illegal transition % -> %', p_transaction_id, tx.status, new_status
            USING ERRCODE = 'PE005';
    END IF;

    -- Payload rules that depend on the payment. ---------------------------------------------------
    IF new_status IN ('authorized', 'captured')
       AND (p_amount_minor <> tx.amount_minor OR p_currency <> tx.currency) THEN
        RAISE EXCEPTION 'payment %: % amount must equal the original % % (partial amounts are not supported)',
            p_transaction_id, p_event_type, tx.amount_minor, tx.currency USING ERRCODE = 'PE001';
    END IF;
    prev_time := greatest(tx.created_at, tx.authorized_at);
    IF p_occurred_at < prev_time THEN
        RAISE EXCEPTION 'payment %: occurred_at % is earlier than the previous lifecycle step (%)',
            p_transaction_id, p_occurred_at, prev_time USING ERRCODE = 'PE001';
    END IF;

    -- Write history and state together. --------------------------------------------------------------
    BEGIN
        INSERT INTO payment_events (event_id, transaction_id, event_sequence, event_type, amount_minor,
                                    currency, failure_code, source, occurred_at)
        VALUES (p_event_id, p_transaction_id, p_event_sequence, p_event_type, p_amount_minor,
                p_currency, p_failure_code, p_source, p_occurred_at)
        RETURNING payment_events.received_at INTO v_received;
    EXCEPTION WHEN unique_violation THEN
        -- Only reachable when another payment claimed this event_id concurrently.
        RAISE EXCEPTION 'event_id % was already recorded with a different payload', p_event_id
            USING ERRCODE = 'PE006';
    END;

    IF new_status <> 'created' THEN
        UPDATE transactions t SET
            status              = new_status,
            last_event_sequence = p_event_sequence,
            authorized_at       = CASE WHEN new_status = 'authorized' THEN p_occurred_at ELSE t.authorized_at END,
            captured_at         = CASE WHEN new_status = 'captured'   THEN p_occurred_at ELSE t.captured_at END,
            failed_at           = CASE WHEN new_status = 'failed'     THEN p_occurred_at ELSE t.failed_at END,
            failure_code        = CASE WHEN new_status = 'failed'     THEN p_failure_code ELSE t.failure_code END
        WHERE t.transaction_id = p_transaction_id;
    END IF;

    RETURN QUERY SELECT 'recorded'::text, new_status, p_event_sequence, v_received;
END
$fn$;

ALTER FUNCTION record_payment_event(uuid, bigint, integer, text, timestamptz, text, bigint, char, text, bigint, bigint)
    OWNER TO v2_payment_writer;
REVOKE ALL ON FUNCTION record_payment_event(uuid, bigint, integer, text, timestamptz, text, bigint, char, text, bigint, bigint)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION record_payment_event(uuid, bigint, integer, text, timestamptz, text, bigint, char, text, bigint, bigint)
    TO v2_payment_app;

COMMENT ON FUNCTION record_payment_event(uuid, bigint, integer, text, timestamptz, text, bigint, char, text, bigint, bigint) IS
    'The payment event write path: appends one lifecycle event and updates transactions atomically.';

-- 4. History check at commit (also covers direct writes by the table owner) --------------

CREATE FUNCTION payment_history_problem(p_transaction_id bigint) RETURNS text
    LANGUAGE sql STABLE
    SET search_path = public, pg_temp
AS $$
    WITH h AS (
        SELECT count(*) AS n, max(event_sequence) AS last_seq,
               string_agg(substr(event_type, 9), ',' ORDER BY event_sequence) AS path
        FROM payment_events WHERE transaction_id = p_transaction_id
    )
    SELECT CASE
        WHEN t.transaction_id IS NULL THEN NULL       -- no such payment (nothing to check)
        WHEN h.n = 0 THEN 'has no events'
        WHEN h.n <> h.last_seq THEN 'event sequence has gaps'
        WHEN h.path NOT IN ('created', 'created,authorized', 'created,authorized,captured',
                            'created,failed', 'created,authorized,failed')
            THEN 'history ' || h.path || ' is not a legal lifecycle'
        WHEN h.last_seq <> t.last_event_sequence OR split_part(h.path, ',', h.n::int) <> t.status
            THEN 'state ' || t.status || '/' || t.last_event_sequence
                 || ' does not match its last event (' || h.path || ')'
        WHEN c.amount_minor <> t.amount_minor OR c.currency <> t.currency
            THEN 'amount differs from its payment.created event'
    END
    FROM h
    LEFT JOIN transactions t ON t.transaction_id = p_transaction_id
    LEFT JOIN payment_events c ON c.transaction_id = p_transaction_id AND c.event_sequence = 1
$$;

CREATE FUNCTION check_payment_history() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = public, pg_temp
AS $$
DECLARE
    problem text := payment_history_problem(NEW.transaction_id);
BEGIN
    IF problem IS NOT NULL THEN
        RAISE EXCEPTION 'payment %: %', NEW.transaction_id, problem
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER transactions_history_consistent
    AFTER INSERT OR UPDATE ON transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_payment_history();

CREATE CONSTRAINT TRIGGER payment_events_history_consistent
    AFTER INSERT ON payment_events
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_payment_history();

-- Existing data (the backfill) must already satisfy the check.
DO $$
DECLARE
    problems text;
BEGIN
    SELECT string_agg(transaction_id || ': ' || payment_history_problem(transaction_id), '; ')
    INTO problems
    FROM transactions
    WHERE payment_history_problem(transaction_id) IS NOT NULL;
    IF problems IS NOT NULL THEN
        RAISE EXCEPTION 'V005: existing payment history fails the new checks: %', problems;
    END IF;
END $$;
