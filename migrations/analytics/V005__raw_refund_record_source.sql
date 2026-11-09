-- V005 (analytics): carry refunds.record_source (OLTP V009) into raw.refunds.
-- Nullable: rows loaded before this column existed are filled by the next sync (V009 re-stamps
-- every refund's sync_xid). The existing v2_sync table-level grants cover the new column.
ALTER TABLE raw.refunds ADD COLUMN record_source text;
