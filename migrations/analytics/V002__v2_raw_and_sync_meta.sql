-- V002 (analytics): raw tables shaped like the V2 OLTP schema, plus V2 sync bookkeeping.
--
-- The V001 raw tables have the MVP shape (decimal amounts, MVP statuses) and are loaded only
-- by the MVP sync, which runs against the demo_* / fintech_* databases, never v2_analytics.
-- They are replaced here; the migration refuses to run if any of them holds data.
--
-- raw.*: 1:1 copies of source rows (latest state), plus
--   _sync_xid   the source row's sync_xid when it was read (lineage/debugging only: never used
--               to decide which version wins; committed sync runs are applied in order)
--   _run_id     the meta.sync_runs row that last wrote it
--   _synced_at  when that run wrote it
-- raw.payment_events is insert-only: a re-read event must match the stored one exactly.

DO $$
DECLARE
    t text;
    n bigint;
BEGIN
    FOREACH t IN ARRAY ARRAY['raw.users', 'raw.merchants', 'raw.transactions', 'raw.refunds',
                             'meta.sync_checkpoints'] LOOP
        EXECUTE format('SELECT count(*) FROM %s', t) INTO n;
        IF n > 0 THEN
            RAISE EXCEPTION 'V002: % holds % rows; refusing to replace the MVP-shaped tables', t, n;
        END IF;
    END LOOP;
END $$;

DROP TABLE raw.users, raw.merchants, raw.transactions, raw.refunds, meta.sync_checkpoints;

CREATE TABLE raw.users (
    user_id     bigint      PRIMARY KEY,
    email       text        NOT NULL,
    full_name   text        NOT NULL,
    country     char(2)     NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    _sync_xid   xid8        NOT NULL,
    _run_id     bigint      NOT NULL,
    _synced_at  timestamptz NOT NULL
);

CREATE TABLE raw.merchants (
    merchant_id   bigint      PRIMARY KEY,
    merchant_name text        NOT NULL,
    category      text        NOT NULL,
    country       char(2)     NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    _sync_xid     xid8        NOT NULL,
    _run_id       bigint      NOT NULL,
    _synced_at    timestamptz NOT NULL
);

CREATE TABLE raw.currencies (
    currency_code char(3)     PRIMARY KEY,
    minor_units   smallint    NOT NULL,
    name          text        NOT NULL,
    _sync_xid     xid8        NOT NULL,
    _run_id       bigint      NOT NULL,
    _synced_at    timestamptz NOT NULL
);

CREATE TABLE raw.transactions (
    transaction_id      bigint      PRIMARY KEY,
    user_id             bigint      NOT NULL,
    merchant_id         bigint      NOT NULL,
    amount_minor        bigint      NOT NULL,
    currency            char(3)     NOT NULL,
    status              text        NOT NULL,
    last_event_sequence integer     NOT NULL,
    failure_code        text,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    authorized_at       timestamptz,
    captured_at         timestamptz,
    failed_at           timestamptz,
    _sync_xid           xid8        NOT NULL,
    _run_id             bigint      NOT NULL,
    _synced_at          timestamptz NOT NULL
);

CREATE TABLE raw.refunds (
    refund_id       bigint      PRIMARY KEY,
    transaction_id  bigint      NOT NULL,
    amount_minor    bigint      NOT NULL,
    currency        char(3)     NOT NULL,
    reason          text        NOT NULL,
    status          text        NOT NULL,
    idempotency_key text        NOT NULL,
    failure_code    text,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    requested_at    timestamptz NOT NULL,
    succeeded_at    timestamptz,
    failed_at       timestamptz,
    _sync_xid       xid8        NOT NULL,
    _run_id         bigint      NOT NULL,
    _synced_at      timestamptz NOT NULL
);

CREATE TABLE raw.payment_events (
    event_id       uuid        PRIMARY KEY,
    transaction_id bigint      NOT NULL,
    event_sequence integer     NOT NULL,
    event_type     text        NOT NULL,
    amount_minor   bigint,
    currency       char(3),
    failure_code   text,
    source         text        NOT NULL,
    occurred_at    timestamptz NOT NULL,
    received_at    timestamptz NOT NULL,
    _sync_xid      xid8        NOT NULL,
    _run_id        bigint      NOT NULL,
    _synced_at     timestamptz NOT NULL,
    -- A different event claiming a stored (payment, sequence) is a conflict, never a silent overwrite.
    CONSTRAINT raw_payment_events_transaction_sequence_key UNIQUE (transaction_id, event_sequence)
);

CREATE FUNCTION raw.forbid_event_mutation() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'raw.payment_events is insert-only: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation';
END $$;

CREATE TRIGGER payment_events_insert_only
    BEFORE UPDATE OR DELETE ON raw.payment_events
    FOR EACH ROW EXECUTE FUNCTION raw.forbid_event_mutation();
-- TRUNCATE stays possible: it is how an explicit reinitialization replaces raw data.

-- Sync bookkeeping ---------------------------------------------------------------------------

CREATE TABLE meta.sync_state (
    source_name              text        PRIMARY KEY,
    source_generation        uuid        NOT NULL,
    source_system_identifier bigint      NOT NULL,
    source_database_oid      oid         NOT NULL,
    source_database_name     text        NOT NULL,
    watermark                xid8        NOT NULL,   -- pg_snapshot_xmin of the last committed run's snapshot
    last_snapshot            pg_snapshot NOT NULL,   -- that snapshot, for re-read accounting and lineage checks
    last_run_id              bigint      NOT NULL,
    status                   text        NOT NULL CHECK (status IN ('active', 'blocked')),
    blocked_reason           text,
    updated_at               timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((status = 'blocked') = (blocked_reason IS NOT NULL))
);

CREATE TABLE meta.sync_runs (
    run_id                   bigserial   PRIMARY KEY,
    source_name              text        NOT NULL,
    mode                     text        NOT NULL CHECK (mode IN ('initial', 'incremental', 'reinitialize')),
    status                   text        NOT NULL
                             CHECK (status IN ('running', 'succeeded', 'failed', 'blocked', 'rejected', 'abandoned')),
    started_at               timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at              timestamptz,
    lookback_runs            integer     NOT NULL DEFAULT 0,
    source_generation        uuid,
    watermark_from           xid8,
    watermark_to             xid8,
    snapshot                 pg_snapshot,
    rows_read                jsonb,
    rows_inserted            jsonb,
    rows_updated             jsonb,
    rows_reread              bigint,     -- rows already visible in the previous run's snapshot
    duplicate_events         bigint,     -- re-read events that matched the stored copy
    watermark_lag_xids       numeric,    -- snapshot xmax - xmin: transaction ids the watermark trails by
    oldest_open_writer_secs  numeric,    -- age of the oldest open writing transaction at the source
    error_class              text,
    error_message            text
);

CREATE INDEX sync_runs_status_idx ON meta.sync_runs (source_name, status, run_id);

CREATE TABLE meta.sync_conflicts (
    conflict_id  bigserial   PRIMARY KEY,
    run_id       bigint      NOT NULL REFERENCES meta.sync_runs (run_id),
    table_name   text        NOT NULL,
    row_key      text        NOT NULL,
    stored       jsonb,
    incoming     jsonb       NOT NULL,
    detected_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);
