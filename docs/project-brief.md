# Project brief

## 90-second technical pitch

This project is a local streaming lakehouse for refrigerated-shipment telemetry. A deterministic
Python simulator publishes Avro temperature, humidity, battery, GPS, door, shock, and
effective-dated metadata records into Kafka. It deliberately injects malformed, duplicate,
out-of-order, and too-late records so correctness can be demonstrated under failure conditions.

The Java Flink pipeline first preserves the exact Kafka bytes and coordinates in an Iceberg bronze
table. It then decodes defensively, validates the data contract, deduplicates with 24-hour keyed
state, and joins events to the metadata version effective at the payload event time. Watermarks are
assigned after that temporal join, so buffered events still enter the right late-data decision and
hourly window. Valid records, explicit rejection dispositions, hourly shipment aggregates, and
breach alerts are committed through checkpoint-aware sinks.

Polaris provides the Iceberg REST catalog, MinIO stores Parquet/ZSTD data and Flink state, and
Trino queries the same tables. Every simulator run has a unique namespace and an atomic manifest
of confirmed Kafka offsets. Verification uses those offsets to prove every delivery appears once
in bronze, exactly once in a terminal silver state, and contributes correctly to finalized gold
windows. The project also includes checkpoint recovery, schema and partition evolution, metrics,
and retained evidence workflows.

## Measured-result templates

Use these only after a dated pack in [`docs/evidence`](evidence/README.md) contains the matching
measurements:

> Built a Kafka/Flink/Iceberg streaming lakehouse that sustained **[measured events/s]** for
> **[measured duration]**, exactly reconciled **[delivered records]** Kafka deliveries, and drained
> consumer lag in **[measured seconds]**.

> Implemented checkpointed event-time processing with temporal metadata enrichment,
> deduplication, late-data quarantine, and Iceberg upserts; recovered from a forced TaskManager
> loss in **[measured seconds]** with **[measured RPO]** and **[measured duplicate count]**
> duplicate outputs.
