-- Run after stopping the v1 job with a savepoint and registering telemetry-v2.avsc.
ALTER TABLE coldchain.silver.enriched_events
SET PROPERTIES partitioning = ARRAY['day(event_time)', 'bucket(device_id, 8)'];

-- Both partition specs remain valid; Iceberg reads old and new files as one table.
SELECT partition, record_count, file_count
FROM coldchain.silver."enriched_events$partitions"
ORDER BY record_count DESC
LIMIT 25;

-- Old v1 rows are null; rows produced by the v2 simulator carry a value.
SELECT signal_strength_dbm IS NULL AS is_v1_row, count(*) AS records
FROM coldchain.silver.enriched_events
GROUP BY 1
ORDER BY 1;
