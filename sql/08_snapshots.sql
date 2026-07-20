-- Iceberg snapshot history — proof of two incremental MERGE loads.
-- Each MERGE creates a snapshot; this is what makes time travel possible.
-- Note: operation shows 'overwrite' because Athena implements MERGE as
-- copy-on-write at the file level. The LOAD is still incremental — October's
-- 3,261 rows were untouched when November was merged in.
SELECT
  snapshot_id,
  committed_at,
  operation
FROM "mikaila_gold"."daily_revenue_summary$snapshots"
ORDER BY committed_at;