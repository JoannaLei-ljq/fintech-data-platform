-- V003: payment lifecycle state on transactions (current state; history arrives in V004).
--
-- Status mapping for existing rows, approved 2026-11-07 (docs/v2/PHASE1_DESIGN.md):
--   succeeded -> captured   (synthetic history: created, authorized, captured)
--   failed    -> failed     (synthetic history: created, failed; failure_code 'legacy_unknown')
--   pending   -> created    (MVP pending only meant "not yet decided"; no authorization was recorded,
--                            so none is invented)
-- The MVP never recorded when authorization, capture or failure happened, only when the row last
-- changed (updated_at). That is the latest moment those steps could have happened, so it is used
-- for authorized_at / captured_at / failed_at. updated_at itself is preserved.
-- Legal-transition enforcement is NOT added here; it belongs to the Step 4 write path.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM transactions WHERE status NOT IN ('pending', 'succeeded', 'failed')) THEN
        RAISE EXCEPTION 'V003: unexpected MVP transaction status found';
    END IF;
END $$;

ALTER TABLE transactions DROP CONSTRAINT transactions_status_check;

ALTER TABLE transactions
    ADD COLUMN last_event_sequence integer,
    ADD COLUMN failure_code        text,
    ADD COLUMN authorized_at       timestamptz,
    ADD COLUMN captured_at         timestamptz,
    ADD COLUMN failed_at           timestamptz;

ALTER TABLE transactions DISABLE TRIGGER transactions_set_updated_at;

-- Right-hand sides all read the pre-update status.
UPDATE transactions SET
    status = CASE status WHEN 'succeeded' THEN 'captured'
                         WHEN 'failed'    THEN 'failed'
                         WHEN 'pending'   THEN 'created' END,
    last_event_sequence = CASE status WHEN 'succeeded' THEN 3 WHEN 'failed' THEN 2 WHEN 'pending' THEN 1 END,
    authorized_at = CASE WHEN status = 'succeeded' THEN updated_at END,
    captured_at   = CASE WHEN status = 'succeeded' THEN updated_at END,
    failed_at     = CASE WHEN status = 'failed'    THEN updated_at END,
    failure_code  = CASE WHEN status = 'failed'    THEN 'legacy_unknown' END;

ALTER TABLE transactions ENABLE TRIGGER transactions_set_updated_at;

ALTER TABLE transactions
    ALTER COLUMN last_event_sequence SET NOT NULL,
    ADD CONSTRAINT transactions_status_check
        CHECK (status IN ('created', 'authorized', 'captured', 'failed')),
    ADD CONSTRAINT transactions_failure_code_check
        CHECK ((status = 'failed') = (failure_code IS NOT NULL)),
    -- Phase 1 paths are fixed: created(1) -> authorized(2) -> captured(3), with failed after
    -- created (2) or after authorized (3).
    ADD CONSTRAINT transactions_status_sequence_check CHECK (
        CASE status
            WHEN 'created'    THEN last_event_sequence = 1
            WHEN 'authorized' THEN last_event_sequence = 2
            WHEN 'captured'   THEN last_event_sequence = 3
            WHEN 'failed'     THEN last_event_sequence IN (2, 3)
                                   AND (last_event_sequence = 3) = (authorized_at IS NOT NULL)
        END),
    ADD CONSTRAINT transactions_lifecycle_timestamps_check CHECK (
        CASE status
            WHEN 'created'    THEN authorized_at IS NULL AND captured_at IS NULL AND failed_at IS NULL
            WHEN 'authorized' THEN authorized_at IS NOT NULL AND captured_at IS NULL AND failed_at IS NULL
            WHEN 'captured'   THEN authorized_at IS NOT NULL AND captured_at IS NOT NULL AND failed_at IS NULL
            WHEN 'failed'     THEN captured_at IS NULL AND failed_at IS NOT NULL
        END),
    ADD CONSTRAINT transactions_lifecycle_order_check CHECK (
        (authorized_at IS NULL OR authorized_at >= created_at)
        AND (captured_at IS NULL OR captured_at >= authorized_at)
        AND (failed_at IS NULL OR failed_at >= coalesce(authorized_at, created_at)));

-- The original amount and the payment's identity never change after creation.
CREATE FUNCTION forbid_transaction_identity_change() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.transaction_id IS DISTINCT FROM OLD.transaction_id
       OR NEW.user_id      IS DISTINCT FROM OLD.user_id
       OR NEW.merchant_id  IS DISTINCT FROM OLD.merchant_id
       OR NEW.amount_minor IS DISTINCT FROM OLD.amount_minor
       OR NEW.currency     IS DISTINCT FROM OLD.currency
       OR NEW.created_at   IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'transaction %: transaction_id, user_id, merchant_id, amount_minor, currency and created_at are immutable',
            OLD.transaction_id USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER transactions_immutable_identity
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_transaction_identity_change();

-- Foreign-key lookups (missing in the MVP).
CREATE INDEX transactions_user_id_idx     ON transactions (user_id);
CREATE INDEX transactions_merchant_id_idx ON transactions (merchant_id);
CREATE INDEX refunds_transaction_id_idx   ON refunds (transaction_id);

-- Legacy consumers keep seeing the MVP vocabulary.
CREATE OR REPLACE VIEW legacy_transactions AS
SELECT t.transaction_id, t.user_id, t.merchant_id,
       minor_to_major(t.amount_minor, c.minor_units) AS amount,
       t.currency,
       CASE t.status WHEN 'captured' THEN 'succeeded'
                     WHEN 'failed'   THEN 'failed'
                     ELSE 'pending' END AS status,
       t.created_at, t.updated_at
FROM transactions t
JOIN currencies c ON c.currency_code = t.currency;
