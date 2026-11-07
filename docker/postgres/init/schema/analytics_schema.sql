-- raw: 1:1 replicas of source tables, loaded by sync/incremental_sync.py
CREATE SCHEMA raw;
-- meta: pipeline bookkeeping (checkpoints)
CREATE SCHEMA meta;

CREATE TABLE raw.users (
    user_id     bigint PRIMARY KEY,
    email       text        NOT NULL,
    full_name   text        NOT NULL,
    country     char(2)     NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    _synced_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE raw.merchants (
    merchant_id   bigint PRIMARY KEY,
    merchant_name text        NOT NULL,
    category      text        NOT NULL,
    country       char(2)     NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    _synced_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE raw.transactions (
    transaction_id bigint PRIMARY KEY,
    user_id        bigint        NOT NULL,
    merchant_id    bigint        NOT NULL,
    amount         numeric(18,2) NOT NULL,
    currency       char(3)       NOT NULL,
    status         text          NOT NULL,
    created_at     timestamptz   NOT NULL,
    updated_at     timestamptz   NOT NULL,
    _synced_at     timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE raw.refunds (
    refund_id      bigint PRIMARY KEY,
    transaction_id bigint        NOT NULL,
    amount         numeric(18,2) NOT NULL,
    reason         text          NOT NULL,
    status         text          NOT NULL,
    created_at     timestamptz   NOT NULL,
    updated_at     timestamptz   NOT NULL,
    _synced_at     timestamptz   NOT NULL DEFAULT now()
);

-- One row per source table. The cursor is (last_updated_at, last_pk) so rows
-- sharing the same updated_at are neither skipped nor re-read forever.
CREATE TABLE meta.sync_checkpoints (
    table_name       text        PRIMARY KEY,
    last_updated_at  timestamptz NOT NULL,
    last_pk          bigint      NOT NULL,
    rows_last_run    integer     NOT NULL DEFAULT 0,
    checkpointed_at  timestamptz NOT NULL DEFAULT now()
);
