SELECT count(*) AS raw_rows,
       count(DISTINCT ROW(source_topic, source_partition, source_offset)) AS unique_offsets
FROM coldchain.bronze.raw_events;

SELECT count(*) AS enriched_rows,
       count(DISTINCT event_id) AS unique_event_ids
FROM coldchain.silver.enriched_events;

SELECT disposition, error_code, count(*) AS records
FROM coldchain.silver.rejected_events
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

SELECT count(*) AS hourly_rows,
       sum(event_count) AS aggregated_events,
       sum(breach_count) AS breaches
FROM coldchain.gold.shipment_hourly_metrics;

SELECT table_schema, table_name
FROM coldchain.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name;
