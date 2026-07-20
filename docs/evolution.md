# Schema and partition evolution

The repository starts with `PIPELINE_SCHEMA_VERSION=1`. The Java model can read the optional v2
field, but the initial Iceberg table intentionally omits it. This makes the deployment sequence
observable instead of silently creating the final schema on day one.

## 1. Verify compatibility and register v2

Bootstrap already tests `telemetry-v2.avsc` against the latest v1 subject under
`FULL_TRANSITIVE`. Register it only when the deployment is approved:

```powershell
docker compose --env-file .env --profile tools run --rm --entrypoint python simulator -m tools.register_v2
```

## 2. Stop with a savepoint

Find the running job ID in the Flink UI or CLI:

```powershell
docker compose --env-file .env exec -T jobmanager flink list
docker compose --env-file .env exec -T jobmanager flink stop --savepointPath s3://flink-checkpoints/savepoints <job-id>
```

Copy the returned savepoint path. Edit only the ignored `.env`:

```text
PIPELINE_SCHEMA_VERSION=2
FLINK_RESTORE_PATH=s3://flink-checkpoints/savepoints/<returned-savepoint>
```

Recreate the application cluster:

```powershell
docker compose --env-file .env up -d --force-recreate jobmanager taskmanager
```

The v2 job adds `signal_strength_dbm` if absent before it starts its inserts, and the custom Flink
entrypoint appends `--fromSavepoint` when `FLINK_RESTORE_PATH` is set.

## 3. Produce v2 records

```powershell
docker compose --env-file .env --profile tools run --rm simulator --profile demo --events 10000 --schema-version 2 --manifest data/manifest-v2.json
```

Old rows remain null and new telemetry rows populate the field.

## 4. Evolve partitioning

```powershell
docker compose --env-file .env exec -T trino trino --file /sql/evolution-v2.sql
```

The new spec partitions by event day and `bucket(device_id, 8)`. Existing files retain their old
identity-day spec and remain queryable without rewrite. New writes use the current spec.

## 5. Prove snapshots and time travel

Run the snapshot query in `sql/maintenance.sql`, record a snapshot ID, then substitute it into the
commented `FOR VERSION AS OF` query. Compare counts before/after `optimize`; logical row counts must
match. Expire snapshots only with a cutoff older than the snapshot committed immediately after the
latest completed Flink checkpoint. Iceberg always retains the current snapshot, but this explicit
cutoff also preserves the checkpoint-aligned recovery point.
