-- V007: source-side support for the V2 incremental sync (transaction-ID watermark polling).
--
-- 1. sync_xid xid8 on every synced table, stamped by a BEFORE INSERT OR UPDATE trigger with
--    pg_current_xact_id(): the TOP-LEVEL id of the writing transaction (also inside savepoints
--    and PL/pgSQL exception blocks, where the tuple's own xmin would be a subtransaction id).
--    A trigger, not a default, so an explicit value cannot override it. Rows that exist when
--    this runs keep the constant default '0' and are read by the first (full) sync.
-- 2. sync_source_identity: one row holding an explicit source generation id. It is created
--    with every rebuild of the database (scripts/v2/reset_v2.sh) and MUST be rotated with
--    rotate_sync_source_generation() after any restore from a backup or copy. The sync
--    refuses to continue incrementally when it changes.
--
-- The sync reads one REPEATABLE READ snapshot S, takes rows with sync_xid >= W (its stored
-- watermark) and then stores W := pg_snapshot_xmin(S). All comparisons use xid8 operators and
-- the pg_snapshot functions; see docs/v2/PHASE1_DESIGN.md (Step 6) for the argument and limits.
-- No write function or earlier migration is changed.

CREATE FUNCTION set_sync_xid() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.sync_xid := pg_current_xact_id();
    RETURN NEW;
END $$;

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['users', 'merchants', 'currencies', 'transactions', 'refunds', 'payment_events'] LOOP
        -- A constant default adds the column without rewriting the table or firing any trigger
        -- (payment_events stays untouched by its append-only trigger).
        EXECUTE format('ALTER TABLE %I ADD COLUMN sync_xid xid8 NOT NULL DEFAULT ''0''', t);
        EXECUTE format('COMMENT ON COLUMN %I.sync_xid IS %L', t,
                       'Top-level id of the last transaction that wrote this row (set by trigger; used by the sync).');
        EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %I '
                       'FOR EACH ROW EXECUTE FUNCTION set_sync_xid()', t || '_set_sync_xid', t);
        EXECUTE format('CREATE INDEX %I ON %I (sync_xid)', t || '_sync_xid_idx', t);
    END LOOP;
END $$;

CREATE TABLE sync_source_identity (
    singleton     boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    generation_id uuid        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    reason        text        NOT NULL
);
COMMENT ON TABLE sync_source_identity IS
    'Source generation for the V2 sync. New on every rebuild; rotate after every restore.';

INSERT INTO sync_source_identity (generation_id, reason)
VALUES (gen_random_uuid(), 'created by migration V007');

CREATE FUNCTION rotate_sync_source_generation(p_reason text) RETURNS uuid
    LANGUAGE sql
    SET search_path = public, pg_temp
BEGIN ATOMIC
    UPDATE sync_source_identity
       SET generation_id = gen_random_uuid(), created_at = clock_timestamp(), reason = p_reason
     WHERE singleton
    RETURNING generation_id;
END;

REVOKE ALL ON FUNCTION rotate_sync_source_generation(text) FROM PUBLIC;
REVOKE ALL ON sync_source_identity FROM PUBLIC;
