# Architecture and data flow

## Runtime topology

```mermaid
flowchart LR
    S[Python simulator] -->|Avro, device_id key| K[(Kafka KRaft)]
    SR[Schema Registry<br/>FULL_TRANSITIVE] --- K
    K -->|raw bytes + Kafka coordinates| F[Flink application]
    F -->|DLQ / late / breaches| K
    F -->|checkpoint commits| P[Polaris REST catalog]
    P --> PG[(PostgreSQL)]
    F -->|Parquet + ZSTD| M[(MinIO)]
    T[Trino] --> P
    T --> M
    PR[Prometheus] --> F
    PR --> KE[Kafka exporter]
    G[Grafana] --> PR
```

Flink reads all four source topics through one raw source so the first durable representation can
preserve topic, partition, offset, Kafka timestamp, key, schema ID, and exact payload. Decode errors
therefore do not prevent bronze capture. Valid sensor records proceed through keyed deduplication;
metadata is routed to a broadcast-like keyed temporal history and selected by `effective_from <=
event_time` with the greatest matching version. The simulator uses event time as Kafka `CreateTime`,
allowing the source to generate split-aware watermarks with 60 seconds of disorder and 30-second
idle-partition detection even during accelerated replay.

## Processing sequence

```mermaid
flowchart TD
    R[Raw Kafka record] --> B[bronze.raw_events]
    R --> W[Source watermark from Kafka CreateTime]
    W --> D{Avro decode and logical validation}
    D -->|invalid| X[silver.rejected_events + DLQ]
    D -->|metadata| H[silver.device_metadata_history]
    D -->|sensor/location/detection| U{event_id seen in 24h?}
    U -->|yes| DM[duplicate counter]
    U -->|no| E[event-time metadata enrichment]
    E -->|no applicable version| X
    E --> L{over 5m beyond watermark?}
    L -->|yes| LT[rejected table + late topic]
    L -->|no| S[silver.enriched_events]
    S --> A[hourly event-time aggregation]
    A --> G[gold.shipment_hourly_metrics]
    S --> Q{temperature outside shipment bounds?}
    Q -->|yes| BA[breach-alerts topic]
```

## Delivery semantics

- The Kafka source participates in Flink checkpoints and reads committed transactions.
- Iceberg sink commits become visible only after successful checkpoints and are exactly once for
  each individual table.
- Kafka alert/DLQ/late sinks use unique transactional ID prefixes and exactly-once delivery.
- A checkpoint can commit several independent sinks, but those table commits do not form an atomic
  transaction across all bronze, silver, and gold tables. Reconciliation tolerates short-lived
  visibility skew and checks the final converged state.
- `bronze.raw_events` is append-only. Its `(topic, partition, offset)` uniqueness is an acceptance
  invariant rather than an update key.
