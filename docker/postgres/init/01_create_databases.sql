-- Two isolated databases on one local Postgres instance:
--   fintech_oltp      : simulated business (source) database
--   fintech_analytics : analytics (warehouse) database
CREATE DATABASE fintech_oltp;
CREATE DATABASE fintech_analytics;
