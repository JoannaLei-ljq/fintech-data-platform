\connect fintech_oltp

-- Fixed seed data with explicit timestamps so every run is reproducible.
INSERT INTO users (user_id, email, full_name, country, created_at, updated_at) VALUES
    (1, 'alice@example.com', 'Alice Chen',  'US', '2026-01-05 09:00:00+00', '2026-01-05 09:00:00+00'),
    (2, 'bob@example.com',   'Bob Martin',  'GB', '2026-01-06 10:30:00+00', '2026-01-06 10:30:00+00'),
    (3, 'carol@example.com', 'Carol Wang',  'SG', '2026-01-07 14:15:00+00', '2026-01-07 14:15:00+00');

INSERT INTO merchants (merchant_id, merchant_name, category, country, created_at, updated_at) VALUES
    (101, 'Blue Bottle Coffee', 'food_and_beverage', 'US', '2026-01-01 08:00:00+00', '2026-01-01 08:00:00+00'),
    (102, 'Northwind Airlines', 'travel',            'GB', '2026-01-02 08:00:00+00', '2026-01-02 08:00:00+00');

INSERT INTO transactions (transaction_id, user_id, merchant_id, amount, currency, status, created_at, updated_at) VALUES
    (1001, 1, 101,   12.50, 'USD', 'succeeded', '2026-02-01 08:15:00+00', '2026-02-01 08:15:05+00'),
    (1002, 2, 102,  480.00, 'GBP', 'succeeded', '2026-02-01 11:40:00+00', '2026-02-01 11:40:07+00'),
    (1003, 3, 101,    6.75, 'USD', 'failed',    '2026-02-02 07:05:00+00', '2026-02-02 07:05:02+00'),
    (1004, 1, 102, 1250.00, 'USD', 'succeeded', '2026-02-03 16:20:00+00', '2026-02-03 16:20:09+00');

INSERT INTO refunds (refund_id, transaction_id, amount, reason, status, created_at, updated_at) VALUES
    (5001, 1002, 480.00, 'flight_cancelled',  'succeeded', '2026-02-05 09:00:00+00', '2026-02-05 09:00:30+00'),
    (5002, 1004, 250.00, 'partial_downgrade', 'pending',   '2026-02-06 13:45:00+00', '2026-02-06 13:45:00+00');
