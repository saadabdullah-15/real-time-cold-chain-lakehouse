# Real-Time Cold-Chain Sensor Data Lakehouse

A production-inspired data-engineering reference project that turns deterministic
refrigerated-shipment events into queryable Apache Iceberg tables while exercising event time,
bad-data isolation, deduplication, temporal enrichment, recovery, and schema evolution.

```text
Python simulator -> Kafka + Avro -> Java Flink -> Iceberg REST / Polaris -> MinIO
                                            |                              |
                                            +-> alert/DLQ topics            +-> Trino
                                                                             |
                                                               Prometheus <-+-> Grafana
```

The default demo emits 100,000 simulated sensor events at 500 events/second. Every run writes a
manifest that is reconciled against Iceberg by an automated verifier. Treat the workload size as a
simulated business volume; only publish a throughput claim after running the benchmark on a named
machine and retaining its evidence.

## What this demonstrates

- Kafka 4.2.1 in single-node KRaft mode, keyed/partitioned topics, compacted metadata, Avro
  contracts, and `FULL_TRANSITIVE` compatibility.
- Flink 2.1.3 on Java 17 with defensive decoding, validation, 24-hour stateful deduplication,
  60-second watermarks, idle partitions, five-minute allowed lateness, temporal metadata lookup,
  threshold breaches, and hourly event-time windows.
- Iceberg 1.11 format-v2 tables in bronze/silver/gold layers, Parquet/ZSTD, exactly-once sink
  commits, upserted metrics, schema/partition evolution, snapshots, time travel, and maintenance.
- Polaris 1.6.0 backed by PostgreSQL, MinIO built from the final security-fixed community source
  tag, and Trino 483 using the same REST catalog and warehouse.
- Provisioned Prometheus 3.13.1 and Grafana OSS 13.0.3 dashboards for throughput, lag, data
  quality, breaches, checkpoints, restarts, and backpressure.

Iceberg sinks commit exactly once per table at completed Flink checkpoints. Commits to different
tables are deliberately documented as independent; they are not one atomic multi-table
transaction.

## Prerequisites

- Docker Desktop with Compose v2
- At least 4 CPUs and 10 GB memory assigned to Docker
- PowerShell 7 (included on Windows; available as `pwsh` elsewhere)

No local Java, Maven, Python, or Make installation is required for the documented workflow.
All service ports bind only to `127.0.0.1`. On first use, the script creates an ignored `.env` and
replaces template passwords with generated local credentials.

## Quick start

```powershell
pwsh ./scripts/coldchain.ps1 demo
```

The first run builds MinIO from its pinned source tag and downloads all images and Maven
dependencies, so it is substantially slower than later runs. The demo command starts the stack,
waits for Flink and Trino, emits the events, waits for checkpoint-backed commits, and runs the
reconciliation assertions.

Useful commands:

```powershell
pwsh ./scripts/coldchain.ps1 up
pwsh ./scripts/coldchain.ps1 status
pwsh ./scripts/coldchain.ps1 verify
pwsh ./scripts/coldchain.ps1 failure-drill
pwsh ./scripts/coldchain.ps1 test
pwsh ./scripts/coldchain.ps1 down
pwsh ./scripts/coldchain.ps1 reset       # requires typing RESET
```

Local endpoints:

| Service | URL |
|---|---|
| Flink | http://127.0.0.1:8081 |
| Trino | http://127.0.0.1:8080 |
| Schema Registry | http://127.0.0.1:8085 |
| MinIO Console | http://127.0.0.1:9001 |
| Prometheus | http://127.0.0.1:9090 |
| Grafana | http://127.0.0.1:3000 |

Use the generated values in `.env` to sign into MinIO and Grafana.

## Repository map

| Path | Purpose |
|---|---|
| `simulator/` | Deterministic profiles, faults, Avro serialization, and Kafka publishing |
| `schemas/` | Avro v1 contracts and additive telemetry v2 |
| `flink-job/` | Java pipeline, stateful functions, Iceberg/Kafka sinks, and unit tests |
| `tools/` | Idempotent infrastructure bootstrap, schema-v2 registration, and reconciliation |
| `sql/` | Analytics, verification, evolution, time-travel, and maintenance queries |
| `infrastructure/` | Source/container builds, Trino config, Prometheus, and Grafana provisioning |
| `scripts/coldchain.ps1` | Reproducible operator interface |
| `docs/` | Architecture, decisions, walkthrough, evolution, troubleshooting, and cloud mapping |

## Profiles and fault controls

| Profile | Workload | Default rate |
|---|---:|---:|
| `baseline` | 100k events across one simulated day | real-time, about 1.16/s |
| `demo` | 100k events across one simulated day | 500/s |
| `benchmark` | 300k events across five simulated minutes | 1,000/s |

The defaults inject 1% duplicates, 0.5% invalid records, 5% out-of-order records, and 1% records
over five minutes late. Override `--events`, `--rate`, `--seed`, `--devices`, each fault rate,
`--event-time-span-seconds`, and `--schema-version` through the simulator CLI.

## Querying

Run the included analytics with Trino's containerized CLI:

```powershell
docker compose --env-file .env exec -T trino trino --file /sql/analytics.sql
docker compose --env-file .env exec -T trino trino --file /sql/verification.sql
```

See [the demo walkthrough](docs/demo-walkthrough.md) for the end-to-end operating flow and
[schema evolution](docs/evolution.md) for the savepoint-backed v1-to-v2 deployment.

## Verification status

The fast test suites verify simulator determinism and faults, contract structure, validation,
temporal enrichment, breach rules, and aggregation. The `demo` workflow waits for a checkpoint
triggered after its manifest before running end-to-end reconciliation, preventing a false failure
against records that have not committed yet. Throughput and recovery claims remain gated by
machine-specific retained evidence; see [benchmark evidence](docs/benchmark.md).

## Design boundary

This is a single-machine reference implementation. TLS, multi-broker/high-availability Kafka,
Kubernetes, cloud deployment, ML, and a business UI are outside its scope. The equivalent AWS
services and production changes are mapped in [AWS mapping](docs/aws-mapping.md).

Upstream references: [Iceberg engine compatibility](https://iceberg.apache.org/multi-engine-support/),
[Iceberg Flink writes](https://iceberg.apache.org/docs/latest/flink-writes/),
[Polaris](https://polaris.apache.org/), [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html),
and [the pinned MinIO security release](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z).
