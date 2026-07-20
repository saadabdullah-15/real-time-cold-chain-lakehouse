# Design decisions and limitations

## Why Flink 2.1 and Iceberg 1.11

Iceberg 1.11 directly supports Flink 2.1, avoiding an attractive-looking but brittle mix of engine
and runtime adapters. The job uses Java 17 and the external Kafka connector line built for Flink
2.1. Application mode gives the portfolio one job per cluster and makes job restoration arguments
explicit.

## Why Polaris instead of a Hive metastore

Polaris exposes the Iceberg REST protocol to both Flink and Trino. PostgreSQL stores catalog state,
while table metadata and data stay in MinIO. This makes the catalog boundary visible and maps more
cleanly to managed REST-catalog architectures than embedding a catalog in either engine.

## Why preserve raw bytes before decoding

Schema Registry deserializers normally fail the consumer callback for malformed data. The source
instead retains raw Kafka bytes, extracts a schema ID only when the Confluent framing is present,
and decodes inside a guarded process function. That creates an auditable replay source and prevents
poison records from causing a restart loop.

## Temporal metadata, not latest metadata

Shipment thresholds and routes change. Joining every late event to the latest device record would
rewrite history incorrectly. The enrichment function stores versions per device and chooses the
greatest `effective_from` no later than the event timestamp. It briefly buffers events when
metadata has not arrived yet, then rejects records with no valid historical version.

## Event time and late data

A 60-second bounded-out-of-order watermark covers ordinary disorder. Hourly windows can update for
five more minutes. An event older than the watermark minus that allowance is quarantined. Because
the watermark already trails the observed maximum by 60 seconds, the effective boundary is based
on event-time progress, not wall-clock delay.

The simulator writes each record's event time as Kafka `CreateTime`, and Flink assigns watermarks
at the Kafka source so each split advances independently before the heterogeneous streams branch.
Partitions idle after 30 seconds and stop holding back the combined watermark. Accelerated profiles
can move simulated time ahead of wall time, so the local broker permits timestamps up to 48 hours
in the future; this allowance is scoped to the prototype and covers the one-day demo.

## MinIO distribution

The community repository became source-only and was archived. The image in this project compiles
the pinned `RELEASE.2025-10-15T17-29-55Z` security release with Go rather than consuming an old
`latest` binary image. The first build is consequently slower and requires network access.

## Known prototype limitations

- One Kafka broker/controller, one JobManager, and one TaskManager are not highly available.
- Credentials are generated local-development secrets; traffic is not encrypted.
- No cross-table transaction exists. Consumers must account for checkpoint-scale visibility skew.
- The temporal history is held in keyed Flink state. A very large fleet needs sizing, compaction,
  and possibly a different temporal-join representation.
- The last open event-time window does not close until watermark progress occurs; dashboards should
  distinguish finalized and still-updating periods.
- Metrics names can change between Flink releases. The dashboard is pinned with the runtime and
  should be reviewed as part of upgrades.
