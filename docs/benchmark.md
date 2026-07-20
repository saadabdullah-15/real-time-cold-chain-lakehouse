# Benchmark and résumé evidence

No throughput result should be copied to a résumé merely because the simulator is configured for
1,000 events/second. Configuration is demand; a benchmark is evidence.

## Required run

1. Record CPU model, RAM, Docker CPU/memory allocation, OS, Docker version, and commit SHA.
2. Start from an empty, protected reset.
3. Run the benchmark producer for 300,000 events at 1,000/s.
4. Confirm zero failed checkpoints, no producer delivery errors, exact raw reconciliation, unique
   enriched IDs, and Kafka lag returning to zero within two minutes.
5. Save `data/failure-manifest.json`, verifier output, relevant Prometheus exports/screenshots, and
   the Flink checkpoint/job JSON.

The `failure-drill` command covers the same workload while injecting a TaskManager loss. For a
clean capacity measurement, run the benchmark simulator without the kill, then `verify` against a
separately named manifest.

## Evidence record

| Field | Result |
|---|---|
| Commit | _not measured_ |
| Machine / Docker allocation | _not measured_ |
| Produced / reconciled rows | _not measured_ |
| Sustained input rate | _not measured_ |
| Peak and final Kafka lag | _not measured_ |
| Lag drain time | _not measured_ |
| Completed / failed checkpoints | _not measured_ |
| Recovery outcome | _not measured_ |

## Résumé bullet gate

After the evidence table is complete, a defensible bullet can use the actual values:

> Built a Kafka/Flink/Iceberg cold-chain lakehouse that sustained **[measured rate] events/s** for
> **[duration]**, reconciled **[row count]** source records exactly, and recovered from a forced
> TaskManager failure with **[recovery time]** downtime and zero duplicate Iceberg output.

Until those placeholders are backed by retained artifacts, do not publish the bullet.
