-- Shipment hours with the most temperature exposure.
SELECT shipment_id,
       window_start,
       temperature_min,
       temperature_max,
       temperature_avg,
       breach_count,
       event_count
FROM coldchain.gold.shipment_hourly_metrics
WHERE breach_count > 0
ORDER BY breach_count DESC, window_start DESC
LIMIT 50;

-- GPS gaps longer than ten simulated minutes.
WITH locations AS (
    SELECT shipment_id,
           device_id,
           event_time,
           latitude,
           longitude,
           lag(event_time) OVER (PARTITION BY device_id ORDER BY event_time) AS previous_event_time
    FROM coldchain.silver.enriched_events
    WHERE event_type = 'location'
)
SELECT shipment_id,
       device_id,
       previous_event_time,
       event_time,
       date_diff('minute', previous_event_time, event_time) AS gap_minutes,
       latitude,
       longitude
FROM locations
WHERE date_diff('minute', previous_event_time, event_time) > 10
ORDER BY gap_minutes DESC;

-- Rejection rate by source and disposition.
WITH raw AS (
    SELECT source_topic, count(*) AS raw_count
    FROM coldchain.bronze.raw_events
    GROUP BY 1
), rejected AS (
    SELECT source_topic, disposition, count(*) AS rejected_count
    FROM coldchain.silver.rejected_events
    GROUP BY 1, 2
)
SELECT r.source_topic,
       x.disposition,
       x.rejected_count,
       r.raw_count,
       round(100.0 * x.rejected_count / r.raw_count, 3) AS rejected_percent
FROM raw r
JOIN rejected x USING (source_topic)
ORDER BY rejected_percent DESC;

-- Current shipment/device state from the effective-dated history.
SELECT device_id,
       shipment_id,
       vehicle_id,
       origin,
       destination,
       cargo_type,
       status,
       min_temperature_c,
       max_temperature_c,
       effective_from
FROM (
    SELECT *, row_number() OVER (
        PARTITION BY device_id ORDER BY effective_from DESC, metadata_version DESC
    ) AS version_rank
    FROM coldchain.silver.device_metadata_history
)
WHERE version_rank = 1
ORDER BY shipment_id;
