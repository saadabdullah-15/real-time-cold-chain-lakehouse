# Interview demo walkthrough

## 1. Start and generate

```powershell
pwsh ./scripts/coldchain.ps1 demo
```

Explain that the simulator first publishes effective-dated metadata, then a deterministic mix of
telemetry, locations, and detections. The same seed produces the same event values and fault
choices. It writes `data/manifest.json`, which is the independent source of expected counts.

## 2. Show operating behavior

Open Flink at http://127.0.0.1:8081 and point out:

- one application job and stable operator UIDs;
- checkpoint duration, data size, and completed/failed counts;
- the keyed deduplication and event-time/window operators;
- backpressure rather than merely container CPU.

Open Grafana at http://127.0.0.1:3000 and show throughput, Kafka lag returning to zero, injected
invalid/duplicate/late counts, breach rate, checkpoints, restarts, and backpressure.

## 3. Prove the lakehouse invariants

```powershell
pwsh ./scripts/coldchain.ps1 verify
docker compose --env-file .env exec -T trino trino --file /sql/verification.sql
docker compose --env-file .env exec -T trino trino --file /sql/analytics.sql
```

The automated verifier does more than check that tables are non-empty: it compares the simulator
manifest to raw rows, proves Kafka-coordinate uniqueness, reconciles metadata and invalid rows,
proves enriched event-ID uniqueness, bounds late/enriched counts, and requires gold output.

## 4. Demonstrate recovery

```powershell
pwsh ./scripts/coldchain.ps1 failure-drill
```

This starts the five-minute benchmark producer, kills the TaskManager during ingestion, recreates
it, waits for the producer, and reruns reconciliation. In Flink, show the restart and the completed
checkpoint used for recovery. In Trino, show that source offsets and event IDs remain unique.

## 5. Demonstrate Iceberg capabilities

Follow [evolution.md](evolution.md), then use `sql/maintenance.sql` to show snapshot metadata, a
version-as-of query, compaction, and the guarded expiration procedure. Emphasize that partition
evolution does not rewrite old files: Iceberg plans both specs.

## Suggested interview close

Discuss what changes in production: multi-AZ Kafka, managed Flink checkpoints in S3, IAM instead of
static keys, TLS and secrets management, independent Trino workers, catalog HA, alerting SLOs, and
capacity tests against actual cardinality/state size. Keep workload and measured-capacity claims
separate.
