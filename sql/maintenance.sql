-- Snapshot history and a reproducible time-travel target.
SELECT snapshot_id, parent_id, committed_at, operation, summary
FROM coldchain.silver."enriched_events$snapshots"
ORDER BY committed_at DESC;

-- Replace <snapshot_id> with a value above.
-- SELECT count(*) FROM coldchain.silver.enriched_events FOR VERSION AS OF <snapshot_id>;

-- Compact small Parquet files without changing logical rows.
ALTER TABLE coldchain.silver.enriched_events
EXECUTE optimize(file_size_threshold => '128MB');

-- Safety procedure:
-- 1. Wait for a completed Flink checkpoint.
-- 2. Record the newest snapshot committed_at immediately after that checkpoint.
-- 3. Use a retention timestamp older than that value below. The current snapshot is always retained.
-- ALTER TABLE coldchain.silver.enriched_events
-- EXECUTE expire_snapshots(retention_threshold => '7d');

-- Remove only files that have not been referenced for at least seven days.
-- ALTER TABLE coldchain.silver.enriched_events
-- EXECUTE remove_orphan_files(retention_threshold => '7d');
