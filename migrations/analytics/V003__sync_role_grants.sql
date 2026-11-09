-- V003 (analytics): least-privilege write access for the V2 sync role on the target.
--
-- v2_sync may: read and append raw rows, upsert mutable raw rows, maintain meta.sync_state,
-- meta.sync_runs and meta.sync_conflicts, and create temporary staging tables.
-- v2_sync may not: delete or truncate anything, update or delete raw.payment_events, alter or
-- drop tables, or disable triggers. Explicit reinitialization (TRUNCATE of raw.*) is therefore
-- an operator procedure run with the owner's credentials (python -m sync.v2_sync --as-admin
-- --reinitialize), never part of normal execution.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'v2_sync') THEN
        CREATE ROLE v2_sync NOLOGIN;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'v2_sync'
               AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls OR rolcanlogin)) THEN
        RAISE EXCEPTION 'V003: role v2_sync exists with LOGIN or elevated attributes; refusing to grant to it';
    END IF;
    EXECUTE format('GRANT CONNECT, TEMPORARY ON DATABASE %I TO v2_sync', current_database());
END $$;

GRANT USAGE ON SCHEMA raw, meta TO v2_sync;
GRANT SELECT, INSERT, UPDATE ON raw.users, raw.merchants, raw.currencies, raw.transactions, raw.refunds TO v2_sync;
GRANT SELECT, INSERT ON raw.payment_events TO v2_sync;
GRANT SELECT, INSERT, UPDATE ON meta.sync_state, meta.sync_runs TO v2_sync;
GRANT SELECT, INSERT ON meta.sync_conflicts TO v2_sync;
GRANT USAGE ON SEQUENCE meta.sync_runs_run_id_seq, meta.sync_conflicts_conflict_id_seq TO v2_sync;
