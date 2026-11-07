-- Demo dataset for demo_oltp. Everything lands on the same UTC day (2026-10-15)
-- so the daily summary has exactly one row.
--
-- Expected fct_daily_payments for 2026-10-15 (USD):
--   success_amount  = 100 + 50          = 150.00
--   success_rate    = 2 / (2 + 1 failed) = 66.67%   (pending is excluded)
--   refund_amount   = 10 + 20           =  30.00
INSERT INTO users (user_id, email, full_name, country, created_at, updated_at) VALUES
    (1, 'alice@example.com', 'Alice Chen', 'US', '2026-10-01 09:00:00+00', '2026-10-01 09:00:00+00'),
    (2, 'bob@example.com',   'Bob Martin', 'US', '2026-10-02 10:00:00+00', '2026-10-02 10:00:00+00'),
    (3, 'carol@example.com', 'Carol Wang', 'US', '2026-10-03 11:00:00+00', '2026-10-03 11:00:00+00');

INSERT INTO merchants (merchant_id, merchant_name, category, country, created_at, updated_at) VALUES
    (101, 'Blue Bottle Coffee', 'food_and_beverage', 'US', '2026-09-01 08:00:00+00', '2026-09-01 08:00:00+00'),
    (102, 'Northwind Airlines', 'travel',            'US', '2026-09-02 08:00:00+00', '2026-09-02 08:00:00+00');

INSERT INTO transactions (transaction_id, user_id, merchant_id, amount, currency, status, created_at, updated_at) VALUES
    (3001, 1, 102, 100.00, 'USD', 'succeeded', '2026-10-15 09:00:00+00', '2026-10-15 09:00:05+00'),
    (3002, 2, 101,  50.00, 'USD', 'succeeded', '2026-10-15 10:30:00+00', '2026-10-15 10:30:04+00'),
    (3003, 3, 101,  30.00, 'USD', 'failed',    '2026-10-15 12:00:00+00', '2026-10-15 12:00:03+00'),
    (3004, 1, 101,  20.00, 'USD', 'pending',   '2026-10-15 15:00:00+00', '2026-10-15 15:00:00+00');

-- Two successful partial refunds against the same $100 payment.
INSERT INTO refunds (refund_id, transaction_id, amount, reason, status, created_at, updated_at) VALUES
    (7001, 3001, 10.00, 'partial_downgrade', 'succeeded', '2026-10-15 18:00:00+00', '2026-10-15 18:00:10+00'),
    (7002, 3001, 20.00, 'service_credit',    'succeeded', '2026-10-15 20:00:00+00', '2026-10-15 20:00:12+00');
