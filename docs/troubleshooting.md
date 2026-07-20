# Troubleshooting

## Docker is unavailable

`coldchain.ps1` expects Docker Desktop to be running. Check `docker version` and confirm at least 4
CPUs and 10 GB memory are assigned. WSL-based Docker can take a minute after login before its named
pipe is ready.

## First build is slow

MinIO is compiled from the pinned community source release and Flink/Iceberg dependencies are
large. BuildKit and the named Maven volume cache subsequent work. Do not replace the MinIO build
with an old `latest` image merely to make the first run faster.

## Polaris bootstrap fails after manual Compose cleanup

Use `pwsh ./scripts/coldchain.ps1 down`, which stops containers while retaining the completed
bootstrap container and persistent state. `reset` removes both together. If containers were
manually removed while PostgreSQL volumes were retained, use the protected reset and create the
local environment again; do not purge an arbitrary PostgreSQL volume by hand.

## Flink reports no S3 filesystem

Confirm `ENABLE_BUILT_IN_PLUGINS` names `flink-s3-fs-hadoop-2.1.3.jar`, both Flink containers use
the custom image, and the checkpoint bucket exists. Inspect `docker compose logs bootstrap
jobmanager` for the first failing request.

## Trino cannot read MinIO

The native property is `fs.s3.enabled=true` in Trino 483. Also verify the internal endpoint is
`http://minio:9000`, path-style access is enabled, and `.env` credentials match the MinIO service.
The generated catalog file lives only inside the Trino container.

## The final hourly window is missing

Kafka is an unbounded source. Once every partition becomes idle there may be no later watermark to
close the final simulated hour. Earlier hours are finalized and the active hour is still valid
streaming state. Emit a small later event or use a bounded integration source when a test requires
the terminal window specifically.

## Verification times out

Check the Flink checkpoint tab first. Iceberg data becomes visible at successful checkpoints, and
different tables can converge a checkpoint apart. Then inspect Kafka lag and `docker compose logs
taskmanager jobmanager trino`. Increasing the verifier timeout can hide a real failed checkpoint,
so diagnose before changing it.

## Reset warning

`pwsh ./scripts/coldchain.ps1 reset` removes all project Docker volumes and generated manifests
after explicit confirmation. Those data are not recoverable. The repository source and `.env`
remain untouched.
