# Schema and partition evolution

The repository starts with `PIPELINE_SCHEMA_VERSION=1`. The Java model can read the optional v2
field, but the initial Iceberg table intentionally omits it. This makes the deployment sequence
observable instead of silently creating the final schema on day one.

## 1. Write and verify v1 data

Start from `PIPELINE_SCHEMA_VERSION=1`, run a scoped demo, and retain its manifest:

```powershell
pwsh ./scripts/coldchain.ps1 demo
```

Query `signal_strength_dbm` only after the column is added in the later step. Record the v1
manifest and the current enriched-table snapshot ID.

## 2. Stop with a savepoint

Find the running job ID in the Flink UI or CLI:

```powershell
docker compose --env-file .env exec -T jobmanager flink list
docker compose --env-file .env exec -T jobmanager flink stop --savepointPath s3://flink-checkpoints/savepoints <job-id>
```

Copy the returned savepoint path.

## 3. Verify compatibility and register v2

Bootstrap tests `telemetry-v2.avsc` against the latest v1 subject under `FULL_TRANSITIVE`.
Registration is a separate approval step. The command repeats the compatibility check and refuses
to register an incompatible schema:

```powershell
docker compose --env-file .env --profile tools run --rm --entrypoint python simulator -m tools.register_v2
```

Edit only the ignored `.env`:

```text
PIPELINE_SCHEMA_VERSION=2
FLINK_RESTORE_PATH=s3://flink-checkpoints/savepoints/<returned-savepoint>
```

## 4. Evolve partitioning and restore

While the v1 job is stopped, apply the v2 partition specification:

```powershell
docker compose --env-file .env exec -T trino trino --file /sql/evolution-v2.sql
```

The SQL adds the nullable field and makes the current spec
`event_day, bucket(event_id, 8)`. Both partition source fields are members of the upsert equality
key, which is required for Iceberg upsert writes. Existing files keep their old event-day-only
spec and remain queryable without rewrite. New files use the current spec.

Then recreate the application cluster using the savepoint settings above:

```powershell
docker compose --env-file .env up -d --force-recreate jobmanager taskmanager
```

The custom Flink entrypoint appends `--fromSavepoint` when `FLINK_RESTORE_PATH` is set. Stable
operator UIDs and maximum parallelism allow the keyed deduplication and temporal history state to
be restored.

## 5. Produce and verify v2 records

```powershell
$runId = "run-$([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ'))-$(([guid]::NewGuid().ToString('N')).Substring(0, 8))"
$manifest = "data/runs/$runId/manifest.json"
docker compose --env-file .env --profile tools run --rm simulator `
  --profile demo --events 10000 --schema-version 2 `
  --run-id $runId --manifest $manifest
$env:MANIFEST_PATH = $manifest
pwsh ./scripts/coldchain.ps1 verify
```

Old rows remain null and new telemetry rows populate the field. Verify both groups with the final
query in `sql/evolution-v2.sql`, then record the new snapshot and partition specs.

## 6. Prove snapshots and time travel

Run the snapshot query in `sql/maintenance.sql`, record a snapshot ID, then substitute it into the
commented `FOR VERSION AS OF` query. Compare counts before/after `optimize`; logical row counts must
match. Expire snapshots only with a cutoff older than the snapshot committed immediately after the
latest completed Flink checkpoint. Iceberg always retains the current snapshot, but this explicit
cutoff also preserves the checkpoint-aligned recovery point.
