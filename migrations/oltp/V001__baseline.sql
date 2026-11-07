-- V001 baseline: the exact MVP OLTP schema (docker/postgres/init/schema/oltp_schema.sql at v0.1.0-mvp).
-- Frozen: never edit an applied migration; add a new version instead.

-- Keeps updated_at current on every UPDATE; incremental sync relies on it.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE users (
    user_id     bigint PRIMARY KEY,
    email       text        NOT NULL UNIQUE,
    full_name   text        NOT NULL,
    country     char(2)     NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE merchants (
    merchant_id   bigint PRIMARY KEY,
    merchant_name text        NOT NULL,
    category      text        NOT NULL,
    country       char(2)     NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    transaction_id bigint PRIMARY KEY,
    user_id        bigint        NOT NULL REFERENCES users (user_id),
    merchant_id    bigint        NOT NULL REFERENCES merchants (merchant_id),
    amount         numeric(18,2) NOT NULL CHECK (amount > 0),
    currency       char(3)       NOT NULL,
    status         text          NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
    created_at     timestamptz   NOT NULL DEFAULT now(),
    updated_at     timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE refunds (
    refund_id      bigint PRIMARY KEY,
    transaction_id bigint        NOT NULL REFERENCES transactions (transaction_id),
    amount         numeric(18,2) NOT NULL CHECK (amount > 0),
    reason         text          NOT NULL,
    status         text          NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
    created_at     timestamptz   NOT NULL DEFAULT now(),
    updated_at     timestamptz   NOT NULL DEFAULT now()
);

-- Composite (updated_at, pk) indexes back the sync cursor query.
CREATE INDEX users_updated_at_idx        ON users        (updated_at, user_id);
CREATE INDEX merchants_updated_at_idx    ON merchants    (updated_at, merchant_id);
CREATE INDEX transactions_updated_at_idx ON transactions (updated_at, transaction_id);
CREATE INDEX refunds_updated_at_idx      ON refunds      (updated_at, refund_id);

CREATE TRIGGER users_set_updated_at        BEFORE UPDATE ON users        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER merchants_set_updated_at    BEFORE UPDATE ON merchants    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER transactions_set_updated_at BEFORE UPDATE ON transactions FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER refunds_set_updated_at      BEFORE UPDATE ON refunds      FOR EACH ROW EXECUTE FUNCTION set_updated_at();
