# Cold-chain lakehouse evidence — YYYY-MM-DD

Status: **INCOMPLETE — do not cite these placeholders**

## Environment

| Field | Value |
|---|---|
| Commit SHA / clean worktree | `[required]` |
| CPU / physical RAM | `[required]` |
| OS / Docker version | `[required]` |
| Docker CPU / RAM allocation | `[required]` |

## Workloads

| Workload | Run ID | Attempted / delivered | Achieved publish rate | Reconciliation |
|---|---|---:|---:|---|
| Demo 1 | `[required]` | `[required]` | `[required]` | `[required]` |
| Demo 2 | `[required]` | `[required]` | `[required]` | `[required]` |
| Benchmark | `[required]` | `[required]` | `[required]` | `[required]` |
| Failure drill | `[required]` | `[required]` | `[required]` | `[required]` |

## Reliability and evolution

| Field | Result |
|---|---|
| Completed / failed checkpoints | `[required]` |
| Peak / final Kafka lag | `[required]` |
| Lag drain time | `[required]` |
| TaskManager recovery / RPO | `[required]` |
| V1 / V2 schema IDs | `[required]` |
| Savepoint restore | `[required]` |
| Old-null / new-populated v2 rows | `[required]` |
| Partition and snapshot evidence | `[required]` |

All values above must be backed by files in this evidence directory.
