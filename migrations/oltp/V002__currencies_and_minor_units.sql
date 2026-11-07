-- V002: currency reference data and integer minor-unit amounts.
--
-- Amounts move from numeric(18,2) to amount_minor bigint, scaled by each currency's
-- ISO 4217 minor units (JPY/KRW 0, USD/EUR/GBP 2, KWD/BHD 3). Conversion never rounds:
-- an unknown currency or an amount that is not exactly representable aborts the whole
-- migration. Backfill does not touch updated_at, because it is not a business change.

CREATE TABLE currencies (
    currency_code char(3)  PRIMARY KEY CHECK (currency_code ~ '^[A-Z]{3}$'),
    minor_units   smallint NOT NULL CHECK (minor_units BETWEEN 0 AND 4),
    name          text     NOT NULL
);

INSERT INTO currencies (currency_code, minor_units, name) VALUES
    ('USD', 2, 'US Dollar'),
    ('EUR', 2, 'Euro'),
    ('GBP', 2, 'Pound Sterling'),
    ('JPY', 0, 'Yen'),
    ('KRW', 0, 'Won'),
    ('KWD', 3, 'Kuwaiti Dinar'),
    ('BHD', 3, 'Bahraini Dinar');

DO $$
DECLARE
    problems text;
BEGIN
    SELECT string_agg(DISTINCT t.currency, ', ') INTO problems
    FROM transactions t
    WHERE NOT EXISTS (SELECT 1 FROM currencies c WHERE c.currency_code = t.currency);
    IF problems IS NOT NULL THEN
        RAISE EXCEPTION 'V002: unknown currency code(s) in transactions: %', problems;
    END IF;

    SELECT string_agg(format('transaction %s: %s %s', t.transaction_id, t.amount, t.currency), '; ')
      INTO problems
    FROM transactions t
    JOIN currencies c ON c.currency_code = t.currency
    WHERE t.amount * power(10::numeric, c.minor_units) <> trunc(t.amount * power(10::numeric, c.minor_units));
    IF problems IS NOT NULL THEN
        RAISE EXCEPTION 'V002: amounts not exactly representable in minor units: %', problems;
    END IF;

    SELECT string_agg(format('refund %s: %s %s', r.refund_id, r.amount, t.currency), '; ')
      INTO problems
    FROM refunds r
    JOIN transactions t ON t.transaction_id = r.transaction_id
    JOIN currencies c ON c.currency_code = t.currency
    WHERE r.amount * power(10::numeric, c.minor_units) <> trunc(r.amount * power(10::numeric, c.minor_units));
    IF problems IS NOT NULL THEN
        RAISE EXCEPTION 'V002: refund amounts not exactly representable in minor units: %', problems;
    END IF;
END $$;

ALTER TABLE transactions DISABLE TRIGGER transactions_set_updated_at;
ALTER TABLE refunds      DISABLE TRIGGER refunds_set_updated_at;

ALTER TABLE transactions ADD COLUMN amount_minor bigint;
UPDATE transactions t
   SET amount_minor = (t.amount * power(10::numeric, c.minor_units))::bigint
  FROM currencies c
 WHERE c.currency_code = t.currency;

-- A refund is always in its payment's currency.
ALTER TABLE refunds ADD COLUMN amount_minor bigint;
UPDATE refunds r
   SET amount_minor = (r.amount * power(10::numeric, c.minor_units))::bigint
  FROM transactions t
  JOIN currencies c ON c.currency_code = t.currency
 WHERE t.transaction_id = r.transaction_id;

ALTER TABLE transactions ENABLE TRIGGER transactions_set_updated_at;
ALTER TABLE refunds      ENABLE TRIGGER refunds_set_updated_at;

-- Round-trip proof before the decimal columns are dropped.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM transactions t JOIN currencies c ON c.currency_code = t.currency
        WHERE t.amount_minor IS NULL OR t.amount_minor / power(10::numeric, c.minor_units) <> t.amount
        UNION ALL
        SELECT 1 FROM refunds r
        JOIN transactions t ON t.transaction_id = r.transaction_id
        JOIN currencies c ON c.currency_code = t.currency
        WHERE r.amount_minor IS NULL OR r.amount_minor / power(10::numeric, c.minor_units) <> r.amount
    ) THEN
        RAISE EXCEPTION 'V002: minor-unit conversion does not round-trip';
    END IF;
END $$;

ALTER TABLE transactions
    ALTER COLUMN amount_minor SET NOT NULL,
    ADD CONSTRAINT transactions_amount_minor_check CHECK (amount_minor > 0),
    ADD CONSTRAINT transactions_currency_fkey FOREIGN KEY (currency) REFERENCES currencies (currency_code),
    DROP COLUMN amount;

ALTER TABLE refunds
    ALTER COLUMN amount_minor SET NOT NULL,
    ADD CONSTRAINT refunds_amount_minor_check CHECK (amount_minor > 0),
    DROP COLUMN amount;

-- Decimal amount in the currency's own scale, e.g. 1500 JPY -> 1500, 12340 KWD minor -> 12.340.
CREATE FUNCTION minor_to_major(amount_minor bigint, minor_units smallint) RETURNS numeric
    LANGUAGE sql IMMUTABLE STRICT
    RETURN round(amount_minor / power(10::numeric, minor_units), minor_units);

-- Compatibility views with the MVP column layout and decimal amounts.
CREATE VIEW legacy_transactions AS
SELECT t.transaction_id, t.user_id, t.merchant_id,
       minor_to_major(t.amount_minor, c.minor_units) AS amount,
       t.currency, t.status, t.created_at, t.updated_at
FROM transactions t
JOIN currencies c ON c.currency_code = t.currency;

CREATE VIEW legacy_refunds AS
SELECT r.refund_id, r.transaction_id,
       minor_to_major(r.amount_minor, c.minor_units) AS amount,
       r.reason, r.status, r.created_at, r.updated_at
FROM refunds r
JOIN transactions t ON t.transaction_id = r.transaction_id
JOIN currencies c ON c.currency_code = t.currency;
