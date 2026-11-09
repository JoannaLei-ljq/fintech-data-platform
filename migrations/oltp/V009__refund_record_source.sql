-- V009: explicit provenance for refunds (record_source), so analytics never has to infer
-- "migrated from the MVP" from the idempotency-key naming convention.
--
--   record_source = 'migration_backfill'  created by V006 from an MVP refund: its succeeded_at /
--                                         failed_at are synthetic upper bounds (the MVP's
--                                         updated_at) and failure_code 'legacy_unknown' is synthetic
--   record_source = 'api'                 created through request_refund() (the default)
--
-- Migrated rows are identified by the exact key V006 gave them ('legacy-refund:' || refund_id);
-- the migration aborts rather than guess if any other key uses that prefix. No timestamp, amount,
-- status or other historical value is changed: only the new column is set. updated_at is
-- preserved; sync_xid is re-stamped on purpose so the next sync carries the column into raw.
-- The write functions are unchanged: request_refund() does not list the column, so new refunds
-- get the default, and the writer role holds no privilege on it. The value is immutable, and
-- new rows can never claim to be migrated history.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM refunds
               WHERE idempotency_key LIKE 'legacy-refund:%' AND idempotency_key <> 'legacy-refund:' || refund_id) THEN
        RAISE EXCEPTION 'V009: refunds with a legacy-refund: key that V006 did not create; refusing to guess provenance';
    END IF;
END $$;

ALTER TABLE refunds
    ADD COLUMN record_source text NOT NULL DEFAULT 'api'
        CONSTRAINT refunds_record_source_check CHECK (record_source IN ('api', 'migration_backfill'));
COMMENT ON COLUMN refunds.record_source IS
    'migration_backfill: migrated MVP refund (completion times are synthetic upper bounds); api: created via request_refund().';

-- Run V006's deferred refund invariants at statement end here, so no trigger events are pending
-- when the updated_at trigger is re-enabled below.
SET CONSTRAINTS refunds_invariants IMMEDIATE;
ALTER TABLE refunds DISABLE TRIGGER refunds_set_updated_at;
UPDATE refunds
   SET record_source = CASE WHEN idempotency_key = 'legacy-refund:' || refund_id THEN 'migration_backfill' ELSE 'api' END;
ALTER TABLE refunds ENABLE TRIGGER refunds_set_updated_at;

CREATE FUNCTION guard_refund_record_source() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.record_source = 'migration_backfill' THEN
        RAISE EXCEPTION 'refund %: record_source migration_backfill is reserved for migrated history', NEW.refund_id
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.record_source IS DISTINCT FROM OLD.record_source THEN
        RAISE EXCEPTION 'refund %: record_source is immutable', OLD.refund_id USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER refunds_record_source_guard
    BEFORE INSERT OR UPDATE ON refunds
    FOR EACH ROW EXECUTE FUNCTION guard_refund_record_source();
