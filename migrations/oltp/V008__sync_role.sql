-- V008: least-privilege access for the V2 sync on the source (no superuser needed).
--
-- Roles (cluster-wide NOLOGIN group roles; created only if missing, refused if elevated):
--   v2_sync        privileges of the sync process. Deployed sync logins are members of it
--                  (scripts/v2/provision_sync_login.sh); it never logs in itself.
--   v2_sync_stats  member of pg_read_all_stats; owns the one monitoring helper below. It
--                  cannot log in, and no role is granted membership in it.
--
-- What the sync needs on the source, and how it gets it:
--   * SELECT on the six synced tables and sync_source_identity          -> granted below
--   * pg_current_snapshot / pg_snapshot_* / pg_visible_in_snapshot       -> PUBLIC by default
--   * pg_control_system() (system identifier for the identity check)     -> PUBLIC by default in
--     PostgreSQL 16; no grant or helper needed
--   * the age of the oldest open writing transaction (monitoring only)   -> other users'
--     xact_start is hidden from unprivileged roles (backend_xid is not), so the narrow SECURITY
--     DEFINER helper sync_oldest_open_writer_secs() returns that single number for this
--     database only. Alternative rejected: granting pg_read_all_stats to the sync role itself
--     would expose every session's query text cluster-wide, including the protected databases.
-- Nothing is granted for writing: v2_sync cannot modify any source table or call the payment
-- or refund write functions.

DO $$
DECLARE
    r text;
BEGIN
    FOREACH r IN ARRAY ARRAY['v2_sync', 'v2_sync_stats'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', r);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r
                   AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls OR rolcanlogin)) THEN
            RAISE EXCEPTION 'V008: role % exists with LOGIN or elevated attributes; refusing to grant to it', r;
        END IF;
    END LOOP;
    IF pg_has_role('v2_sync', 'v2_payment_app', 'MEMBER') OR pg_has_role('v2_sync', 'v2_payment_writer', 'MEMBER')
       OR pg_has_role('v2_sync', 'v2_sync_stats', 'MEMBER') OR pg_has_role('v2_sync', 'pg_read_all_stats', 'MEMBER') THEN
        RAISE EXCEPTION 'V008: v2_sync must not be a member of a payment, stats or monitoring role';
    END IF;
    IF NOT pg_has_role('v2_sync_stats', 'pg_read_all_stats', 'MEMBER') THEN
        GRANT pg_read_all_stats TO v2_sync_stats;
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO v2_sync', current_database());
END $$;

GRANT USAGE ON SCHEMA public TO v2_sync;
GRANT SELECT ON users, merchants, currencies, transactions, refunds, payment_events, sync_source_identity
    TO v2_sync;

CREATE FUNCTION sync_oldest_open_writer_secs() RETURNS numeric
    LANGUAGE sql STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp
AS $$
    SELECT extract(epoch FROM clock_timestamp() - min(xact_start))::numeric(12, 3)
    FROM pg_stat_activity
    WHERE datname = current_database() AND backend_xid IS NOT NULL AND pid <> pg_backend_pid()
$$;

ALTER FUNCTION sync_oldest_open_writer_secs() OWNER TO v2_sync_stats;
REVOKE ALL ON FUNCTION sync_oldest_open_writer_secs() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sync_oldest_open_writer_secs() TO v2_sync;
COMMENT ON FUNCTION sync_oldest_open_writer_secs() IS
    'Monitoring only: age in seconds of the oldest open writing transaction in this database (NULL if none).';
