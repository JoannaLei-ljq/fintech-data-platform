-- V004 (analytics): write-audit-publish for the V2 marts.
--
-- dbt builds the marts into marts_candidate (never read by consumers). After the same
-- pipeline run's dbt build and reconciliation both pass, sync/v2_publish.py copies the three
-- marts into marts and records the publication, all in ONE transaction:
--   marts.fct_payments, marts.fct_daily_payments, marts.fct_merchant_performance
-- Copying (DELETE + INSERT) instead of renaming keeps the published tables' identity, so
-- dependent views and grants survive, readers are never blocked, and each SQL statement sees
-- either the previous or the new publication in full (MVCC). A consumer that issues several
-- statements and needs them to agree must use one REPEATABLE READ transaction (and can read
-- marts.current_publication in it to know which publication it sees).
--
-- meta.mart_validations   one row per reconciliation of a candidate (pass or fail), with the
--                         candidate's content fingerprint and the run identities it checked
-- meta.mart_publications  one row per publication, bound to exactly one passing validation,
--                         one dbt invocation, one sync run and one pipeline run
-- Owner-only: the v2_sync role gets nothing here.

CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE meta.mart_validations (
    validation_id         bigserial   PRIMARY KEY,
    pipeline_run_id       text        NOT NULL,
    dbt_invocation_id     uuid        NOT NULL,
    sync_run_id           bigint      NOT NULL REFERENCES meta.sync_runs (run_id),
    candidate_fingerprint text        NOT NULL,
    passed                boolean     NOT NULL,
    checks                integer     NOT NULL,
    failed                integer     NOT NULL,
    summary               jsonb       NOT NULL,
    validated_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (passed = (failed = 0))
);

CREATE TABLE meta.mart_publications (
    publication_id          bigserial   PRIMARY KEY,
    pipeline_run_id         text        NOT NULL UNIQUE,
    dbt_invocation_id       uuid        NOT NULL UNIQUE,
    sync_run_id             bigint      NOT NULL REFERENCES meta.sync_runs (run_id),
    validation_id           bigint      NOT NULL UNIQUE REFERENCES meta.mart_validations (validation_id),
    candidate_fingerprint   text        NOT NULL,
    row_counts              jsonb       NOT NULL,
    previous_publication_id bigint      REFERENCES meta.mart_publications (publication_id),
    published_at            timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Which publication the published marts currently hold (read it in the same REPEATABLE READ
-- transaction as the marts to know exactly what you are looking at).
CREATE VIEW marts.current_publication AS
SELECT publication_id, pipeline_run_id, dbt_invocation_id, sync_run_id, validation_id, row_counts, published_at
FROM meta.mart_publications
ORDER BY publication_id DESC
LIMIT 1;
