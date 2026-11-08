-- V006: refund processing and financial invariants.
--
-- Refund model: requested -> succeeded | requested -> failed (both terminal). A refund carries
-- its payment's currency, a stable idempotency key, and lifecycle timestamps. Refunds never
-- change the payment: its amount and status stay as they are (a refunded payment stays
-- captured; refunded totals are derived from refunds).
--
-- Write functions (the only application write path, same security model as V005):
--   request_refund()  locks the payment row, then reserves capacity: requested + succeeded
--                     refunds may never exceed the payment's original amount.
--   complete_refund() locks the payment row, then the refund, and moves it to a terminal
--                     status. A failed refund releases its reservation.
-- SQLSTATEs: RF001 invalid request, RF002 unknown payment or refund, RF003 payment not
-- refundable (not captured), RF004 currency mismatch, RF005 amount exceeds the refundable
-- remainder, RF006 idempotency key reused with a different request, RF007 illegal refund
-- status change.
--
-- Existing rows (MVP vocabulary): pending -> requested, succeeded -> succeeded,
-- failed -> failed (failure_code 'legacy_unknown'). The MVP recorded no completion time,
-- so succeeded_at / failed_at use updated_at, the latest moment it could have happened
-- (synthetic, like the Step 3 backfill). requested_at = created_at.
-- Refund analytics will attribute succeeded amounts to succeeded_at (design decision).

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM refunds WHERE status NOT IN ('pending', 'succeeded', 'failed')) THEN
        RAISE EXCEPTION 'V006: unexpected MVP refund status found';
    END IF;
END $$;

-- 1. Columns and backfill -----------------------------------------------------------------

ALTER TABLE refunds DROP CONSTRAINT refunds_status_check;

ALTER TABLE refunds
    ADD COLUMN currency        char(3),
    ADD COLUMN idempotency_key text,
    ADD COLUMN requested_at    timestamptz,
    ADD COLUMN succeeded_at    timestamptz,
    ADD COLUMN failed_at       timestamptz,
    ADD COLUMN failure_code    text;

ALTER TABLE refunds DISABLE TRIGGER refunds_set_updated_at;

-- Right-hand sides all read the pre-update status.
UPDATE refunds r SET
    currency        = t.currency,
    idempotency_key = 'legacy-refund:' || r.refund_id,
    requested_at    = r.created_at,
    status          = CASE r.status WHEN 'pending' THEN 'requested' ELSE r.status END,
    succeeded_at    = CASE WHEN r.status = 'succeeded' THEN r.updated_at END,
    failed_at       = CASE WHEN r.status = 'failed'    THEN r.updated_at END,
    failure_code    = CASE WHEN r.status = 'failed'    THEN 'legacy_unknown' END
FROM transactions t
WHERE t.transaction_id = r.transaction_id;

ALTER TABLE refunds ENABLE TRIGGER refunds_set_updated_at;

-- 2. Constraints ---------------------------------------------------------------------------

-- The refund currency must equal its payment's currency. That is checked at commit (section 4)
-- and in request_refund(), not with a composite foreign key: the extra UNIQUE (transaction_id,
-- currency) on transactions that a composite key needs would break the concurrent-create
-- handling of record_payment_event() (ON CONFLICT (transaction_id) covers only the primary key).

ALTER TABLE refunds
    ALTER COLUMN currency        SET NOT NULL,
    ALTER COLUMN idempotency_key SET NOT NULL,
    ALTER COLUMN requested_at    SET NOT NULL,
    ALTER COLUMN requested_at    SET DEFAULT clock_timestamp(),
    ADD CONSTRAINT refunds_currency_fkey FOREIGN KEY (currency) REFERENCES currencies (currency_code),
    ADD CONSTRAINT refunds_idempotency_key_key UNIQUE (idempotency_key),
    ADD CONSTRAINT refunds_idempotency_key_check CHECK (idempotency_key ~ '^[!-~]{1,200}$'),
    ADD CONSTRAINT refunds_status_check CHECK (status IN ('requested', 'succeeded', 'failed')),
    ADD CONSTRAINT refunds_lifecycle_check CHECK (
        CASE status
            WHEN 'requested' THEN succeeded_at IS NULL AND failed_at IS NULL AND failure_code IS NULL
            WHEN 'succeeded' THEN succeeded_at IS NOT NULL AND failed_at IS NULL AND failure_code IS NULL
            WHEN 'failed'    THEN failed_at IS NOT NULL AND succeeded_at IS NULL
                                  AND nullif(btrim(failure_code), '') IS NOT NULL
        END),
    ADD CONSTRAINT refunds_lifecycle_order_check CHECK (
        (succeeded_at IS NULL OR succeeded_at >= requested_at)
        AND (failed_at IS NULL OR failed_at >= requested_at)),
    ADD CONSTRAINT refunds_reason_check CHECK (btrim(reason) <> '');

COMMENT ON COLUMN refunds.idempotency_key IS 'Client-supplied key; a retry with the same key and request returns the same refund.';
COMMENT ON COLUMN refunds.succeeded_at IS 'Attribution date for succeeded refund amounts in analytics.';

-- New refund IDs (the MVP seed supplied its own).
CREATE SEQUENCE refunds_refund_id_seq OWNED BY refunds.refund_id;
SELECT setval('refunds_refund_id_seq', coalesce((SELECT max(refund_id) FROM refunds), 0) + 1, false);
ALTER TABLE refunds ALTER COLUMN refund_id SET DEFAULT nextval('refunds_refund_id_seq');

-- 3. Immutability, terminal statuses, no deletes --------------------------------------------

CREATE FUNCTION guard_refund_change() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'refunds are financial records: % is not allowed', TG_OP
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.refund_id       IS DISTINCT FROM OLD.refund_id
       OR NEW.transaction_id  IS DISTINCT FROM OLD.transaction_id
       OR NEW.amount_minor    IS DISTINCT FROM OLD.amount_minor
       OR NEW.currency        IS DISTINCT FROM OLD.currency
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.reason          IS DISTINCT FROM OLD.reason
       OR NEW.requested_at    IS DISTINCT FROM OLD.requested_at
       OR NEW.created_at      IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'refund %: refund_id, transaction_id, amount_minor, currency, idempotency_key, reason, requested_at and created_at are immutable',
            OLD.refund_id USING ERRCODE = 'restrict_violation';
    END IF;
    IF OLD.status IN ('succeeded', 'failed')
       AND (NEW.status, NEW.succeeded_at, NEW.failed_at, NEW.failure_code)
           IS DISTINCT FROM (OLD.status, OLD.succeeded_at, OLD.failed_at, OLD.failure_code) THEN
        RAISE EXCEPTION 'refund %: status % is terminal', OLD.refund_id, OLD.status
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER refunds_guard_change
    BEFORE UPDATE OR DELETE ON refunds
    FOR EACH ROW EXECUTE FUNCTION guard_refund_change();

CREATE TRIGGER refunds_no_truncate
    BEFORE TRUNCATE ON refunds
    FOR EACH STATEMENT EXECUTE FUNCTION guard_refund_change();

-- 4. Financial invariants at commit (also covers direct writes by the table owner) ----------

CREATE FUNCTION refund_problem(p_transaction_id bigint) RETURNS text
    LANGUAGE sql STABLE
    SET search_path = public, pg_temp
AS $$
    SELECT CASE
        WHEN t.status <> 'captured' THEN 'has refunds but is ' || t.status
        WHEN r.other_currency THEN 'has a refund whose currency differs from the payment currency '
                                   || t.currency
        WHEN r.reserved > t.amount_minor
            THEN 'requested + succeeded refunds (' || r.reserved || ') exceed the payment amount ('
                 || t.amount_minor || ')'
    END
    FROM transactions t
    JOIN (SELECT sum(amount_minor) FILTER (WHERE status IN ('requested', 'succeeded')) AS reserved,
                 bool_or(currency IS DISTINCT FROM (SELECT currency FROM transactions
                                                    WHERE transaction_id = p_transaction_id)) AS other_currency
          FROM refunds WHERE transaction_id = p_transaction_id) r ON true
    WHERE t.transaction_id = p_transaction_id
      AND EXISTS (SELECT 1 FROM refunds WHERE transaction_id = p_transaction_id)
$$;

CREATE FUNCTION check_refund_invariants() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = public, pg_temp
AS $$
DECLARE
    problem text := refund_problem(NEW.transaction_id);
BEGIN
    IF problem IS NOT NULL THEN
        RAISE EXCEPTION 'payment %: %', NEW.transaction_id, problem
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER refunds_invariants
    AFTER INSERT OR UPDATE ON refunds
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_refund_invariants();

DO $$
DECLARE
    problems text;
BEGIN
    SELECT string_agg(transaction_id || ': ' || refund_problem(transaction_id), '; ')
    INTO problems
    FROM (SELECT DISTINCT transaction_id FROM refunds) r
    WHERE refund_problem(transaction_id) IS NOT NULL;
    IF problems IS NOT NULL THEN
        RAISE EXCEPTION 'V006: existing refunds break the invariants: %', problems;
    END IF;
END $$;

-- 5. Write functions -----------------------------------------------------------------------

CREATE FUNCTION request_refund(
    p_idempotency_key text,
    p_transaction_id  bigint,
    p_amount_minor    bigint,
    p_currency        char(3),
    p_reason          text)
RETURNS TABLE (outcome text, refund_id bigint, refund_status text, refundable_minor bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
#variable_conflict use_column
DECLARE
    tx       transactions%ROWTYPE;
    prior    refunds%ROWTYPE;
    reserved bigint;
    new_id   bigint;
BEGIN
    IF p_idempotency_key IS NULL OR p_idempotency_key !~ '^[!-~]{1,200}$' THEN
        RAISE EXCEPTION 'idempotency_key must be 1-200 printable characters without spaces'
            USING ERRCODE = 'RF001';
    END IF;
    IF p_transaction_id IS NULL OR p_amount_minor IS NULL OR p_currency IS NULL
       OR nullif(btrim(p_reason), '') IS NULL THEN
        RAISE EXCEPTION 'transaction_id, amount_minor, currency and reason are required' USING ERRCODE = 'RF001';
    END IF;
    IF p_amount_minor <= 0 THEN
        RAISE EXCEPTION 'amount_minor must be positive' USING ERRCODE = 'RF001';
    END IF;

    -- Serialize every refund decision for this payment.
    SELECT * INTO tx FROM transactions t WHERE t.transaction_id = p_transaction_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown payment %', p_transaction_id USING ERRCODE = 'RF002';
    END IF;

    SELECT coalesce(sum(r.amount_minor), 0) INTO reserved
    FROM refunds r WHERE r.transaction_id = p_transaction_id AND r.status IN ('requested', 'succeeded');

    -- Idempotency: read after the lock, so a concurrent identical request that committed first
    -- is visible here.
    SELECT * INTO prior FROM refunds r WHERE r.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF prior.transaction_id = p_transaction_id AND prior.amount_minor = p_amount_minor
           AND prior.currency = p_currency AND prior.reason = p_reason THEN
            RETURN QUERY SELECT 'duplicate'::text, prior.refund_id, prior.status, tx.amount_minor - reserved;
            RETURN;
        END IF;
        RAISE EXCEPTION 'idempotency_key % was already used for a different refund request', p_idempotency_key
            USING ERRCODE = 'RF006';
    END IF;

    IF tx.status <> 'captured' THEN
        RAISE EXCEPTION 'payment % is %, only captured payments can be refunded', p_transaction_id, tx.status
            USING ERRCODE = 'RF003';
    END IF;
    IF p_currency <> tx.currency THEN
        RAISE EXCEPTION 'refund currency % does not match payment currency %', p_currency, tx.currency
            USING ERRCODE = 'RF004';
    END IF;
    IF reserved + p_amount_minor > tx.amount_minor THEN
        RAISE EXCEPTION 'payment %: refund of % exceeds the refundable remainder % (amount %, requested or succeeded %)',
            p_transaction_id, p_amount_minor, tx.amount_minor - reserved, tx.amount_minor, reserved
            USING ERRCODE = 'RF005';
    END IF;

    BEGIN
        INSERT INTO refunds (transaction_id, amount_minor, currency, reason, status, idempotency_key)
        VALUES (p_transaction_id, p_amount_minor, p_currency, p_reason, 'requested', p_idempotency_key)
        RETURNING refunds.refund_id INTO new_id;
    EXCEPTION WHEN unique_violation THEN
        -- Only reachable when a request for another payment claimed this key concurrently.
        RAISE EXCEPTION 'idempotency_key % was already used for a different refund request', p_idempotency_key
            USING ERRCODE = 'RF006';
    END;

    RETURN QUERY SELECT 'recorded'::text, new_id, 'requested'::text, tx.amount_minor - reserved - p_amount_minor;
END
$fn$;

CREATE FUNCTION complete_refund(
    p_refund_id    bigint,
    p_status       text,
    p_failure_code text        DEFAULT NULL,
    p_completed_at timestamptz DEFAULT NULL)
RETURNS TABLE (outcome text, refund_id bigint, refund_status text, refundable_minor bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
#variable_conflict use_column
DECLARE
    tx_id     bigint;
    tx        transactions%ROWTYPE;
    rf        refunds%ROWTYPE;
    done_at   timestamptz;
    stored_at timestamptz;
BEGIN
    IF p_refund_id IS NULL OR p_status IS NULL OR p_status NOT IN ('succeeded', 'failed') THEN
        RAISE EXCEPTION 'refund_id and a status of succeeded or failed are required' USING ERRCODE = 'RF001';
    END IF;
    IF (p_status = 'failed') <> (nullif(btrim(p_failure_code), '') IS NOT NULL) THEN
        RAISE EXCEPTION 'failure_code is required for failed refunds and not allowed otherwise'
            USING ERRCODE = 'RF001';
    END IF;

    -- Same lock order as request_refund: the payment first, then the refund.
    SELECT r.transaction_id INTO tx_id FROM refunds r WHERE r.refund_id = p_refund_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown refund %', p_refund_id USING ERRCODE = 'RF002';
    END IF;
    SELECT * INTO tx FROM transactions t WHERE t.transaction_id = tx_id FOR UPDATE;
    SELECT * INTO rf FROM refunds r WHERE r.refund_id = p_refund_id FOR UPDATE;

    IF rf.status <> 'requested' THEN
        stored_at := coalesce(rf.succeeded_at, rf.failed_at);
        IF rf.status = p_status AND rf.failure_code IS NOT DISTINCT FROM p_failure_code
           AND (p_completed_at IS NULL OR p_completed_at = stored_at) THEN
            RETURN QUERY SELECT 'duplicate'::text, rf.refund_id, rf.status,
                tx.amount_minor - (SELECT coalesce(sum(x.amount_minor), 0)::bigint FROM refunds x
                                   WHERE x.transaction_id = tx.transaction_id
                                     AND x.status IN ('requested', 'succeeded'));
            RETURN;
        END IF;
        RAISE EXCEPTION 'refund % is already % and cannot change', p_refund_id, rf.status
            USING ERRCODE = 'RF007';
    END IF;

    done_at := coalesce(p_completed_at, clock_timestamp());
    IF done_at < rf.requested_at THEN
        RAISE EXCEPTION 'refund %: completion time % is earlier than its request (%)',
            p_refund_id, done_at, rf.requested_at USING ERRCODE = 'RF001';
    END IF;

    UPDATE refunds r SET
        status       = p_status,
        succeeded_at = CASE WHEN p_status = 'succeeded' THEN done_at END,
        failed_at    = CASE WHEN p_status = 'failed'    THEN done_at END,
        failure_code = CASE WHEN p_status = 'failed'    THEN p_failure_code END
    WHERE r.refund_id = p_refund_id;

    RETURN QUERY SELECT 'recorded'::text, rf.refund_id, p_status,
        tx.amount_minor - (SELECT coalesce(sum(x.amount_minor), 0)::bigint FROM refunds x
                           WHERE x.transaction_id = tx.transaction_id
                             AND x.status IN ('requested', 'succeeded'));
END
$fn$;

COMMENT ON FUNCTION request_refund(text, bigint, bigint, char, text) IS
    'Refund write path: reserves refundable capacity for a captured payment, idempotent by key.';
COMMENT ON FUNCTION complete_refund(bigint, text, text, timestamptz) IS
    'Refund write path: moves a requested refund to succeeded or failed (failed releases its reservation).';

-- 6. Privileges (same model as V005) --------------------------------------------------------

GRANT SELECT ON refunds TO v2_payment_writer;
GRANT INSERT (transaction_id, amount_minor, currency, reason, status, idempotency_key)
    ON refunds TO v2_payment_writer;
GRANT UPDATE (status, succeeded_at, failed_at, failure_code) ON refunds TO v2_payment_writer;
GRANT USAGE ON SEQUENCE refunds_refund_id_seq TO v2_payment_writer;

ALTER FUNCTION request_refund(text, bigint, bigint, char, text) OWNER TO v2_payment_writer;
ALTER FUNCTION complete_refund(bigint, text, text, timestamptz) OWNER TO v2_payment_writer;
REVOKE ALL ON FUNCTION request_refund(text, bigint, bigint, char, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION complete_refund(bigint, text, text, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION request_refund(text, bigint, bigint, char, text) TO v2_payment_app;
GRANT EXECUTE ON FUNCTION complete_refund(bigint, text, text, timestamptz) TO v2_payment_app;

-- 7. Legacy consumers keep the MVP vocabulary and columns ----------------------------------

CREATE OR REPLACE VIEW legacy_refunds AS
SELECT r.refund_id, r.transaction_id,
       minor_to_major(r.amount_minor, c.minor_units) AS amount,
       r.reason,
       CASE r.status WHEN 'requested' THEN 'pending' ELSE r.status END AS status,
       r.created_at, r.updated_at
FROM refunds r
JOIN currencies c ON c.currency_code = r.currency;
