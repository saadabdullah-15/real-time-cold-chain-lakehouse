from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


class TrinoClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint.rstrip("/")

    def query(self, sql: str) -> tuple[list[str], list[list[Any]]]:
        request = urllib.request.Request(
            f"{self.endpoint}/v1/statement",
            data=sql.encode(),
            method="POST",
            headers={
                "X-Trino-User": "coldchain-verifier",
                "X-Trino-Catalog": "coldchain",
                "Accept": "application/json",
            },
        )
        columns: list[str] = []
        rows: list[list[Any]] = []
        response = self._open(request)
        while True:
            if "error" in response:
                error = response["error"]
                failure = error.get("failureInfo", {})
                detail = failure.get("message") or error.get("message") or str(error)
                raise RuntimeError(f"Trino query failed: {detail}")
            if response.get("columns"):
                columns = [column["name"] for column in response["columns"]]
            rows.extend(response.get("data", []))
            next_uri = response.get("nextUri")
            if not next_uri:
                return columns, rows
            response = self._open(urllib.request.Request(next_uri, method="GET"))

    @staticmethod
    def _open(request: urllib.request.Request) -> dict[str, Any]:
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(body)
                detail = payload.get("error", {}).get("message") or payload.get("message") or body
            except json.JSONDecodeError:
                detail = body
            raise RuntimeError(
                f"Trino HTTP {error.code} for {request.full_url}: {detail[:500]}"
            ) from error
        except urllib.error.URLError as error:
            raise RuntimeError(
                f"Could not reach Trino at {request.full_url}: {error.reason}"
            ) from error

    def scalar(self, sql: str) -> int:
        _, rows = self.query(sql)
        if not rows:
            raise RuntimeError(f"Query returned no rows: {sql}")
        return int(rows[0][0])


def resolve_manifest_path() -> Path:
    configured = os.getenv("MANIFEST_PATH")
    if configured:
        return Path(configured)
    candidates = list(Path("data/runs").glob("*/manifest.json"))
    if not candidates:
        raise RuntimeError("No manifest found; set MANIFEST_PATH or run a workload first")
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Manifest does not exist: {path}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("manifest_version") != 2:
        raise RuntimeError("Verifier requires manifest version 2")
    if manifest.get("status") != "COMPLETE":
        raise RuntimeError(f"Manifest is not complete: {manifest.get('status')}")
    attempted = int(manifest["attempted_total"])
    delivered = int(manifest["delivered_total"])
    if attempted != delivered:
        raise RuntimeError(
            f"Manifest delivery mismatch: {attempted} attempted, {delivered} delivered"
        )
    ranges = manifest.get("offset_ranges", [])
    if sum(int(value["count"]) for value in ranges) != delivered:
        raise RuntimeError("Manifest offset ranges do not reconcile to delivered_total")
    return manifest


def sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def delivered_cte(manifest: dict[str, Any]) -> str:
    rows = [
        "("
        + ", ".join(
            (
                sql_text(str(item["topic"])),
                str(int(item["partition"])),
                str(int(item["start_offset"])),
                str(int(item["end_offset"])),
            )
        )
        + ")"
        for item in manifest["offset_ranges"]
    ]
    if not rows:
        raise RuntimeError("Complete manifest contains no delivered offset ranges")
    return (
        "delivered(source_topic, source_partition, start_offset, end_offset) AS "
        f"(VALUES {', '.join(rows)})"
    )


def scoped_join(alias: str = "source") -> str:
    return (
        f"{alias}.source_topic = delivered.source_topic "
        f"AND {alias}.source_partition = delivered.source_partition "
        f"AND {alias}.source_offset BETWEEN delivered.start_offset "
        "AND delivered.end_offset"
    )


def wait_for_commits(client: TrinoClient, manifest: dict[str, Any], timeout: int = 240) -> None:
    expected = int(manifest["delivered_total"])
    cte = delivered_cte(manifest)
    bronze_query = (
        f"WITH {cte} SELECT count(*) FROM bronze.raw_events source "
        f"JOIN delivered ON {scoped_join()}"
    )
    terminal_query = (
        terminal_sql(manifest)
        + """
        SELECT count(*)
        FROM scoped
        """
    )
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            bronze = client.scalar(bronze_query)
            terminal = client.scalar(terminal_query)
            if bronze >= expected and terminal >= expected:
                return
        except RuntimeError as error:
            last_error = error
        time.sleep(5)
    raise RuntimeError(
        f"Run-scoped bronze and terminal rows did not reach {expected} within {timeout}s; "
        f"last error: {last_error}"
    )


def assert_equal(label: str, actual: int, expected: int) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected:,}, got {actual:,}")
    print(f"PASS {label}: {actual:,}")


def verify_bronze(client: TrinoClient, manifest: dict[str, Any]) -> None:
    expected = int(manifest["delivered_total"])
    cte = delivered_cte(manifest)
    _, rows = client.query(
        f"""
        WITH {cte}
        SELECT count(*) AS rows,
               count(DISTINCT ROW(source.source_topic,
                                  source.source_partition,
                                  source.source_offset)) AS coordinates
        FROM bronze.raw_events source
        JOIN delivered ON {scoped_join()}
        """
    )
    assert_equal("run-scoped bronze rows", int(rows[0][0]), expected)
    assert_equal("unique delivered coordinates in bronze", int(rows[0][1]), expected)


def terminal_sql(manifest: dict[str, Any]) -> str:
    cte = delivered_cte(manifest)
    return f"""
        WITH {cte},
        terminal AS (
            SELECT source_topic, source_partition, source_offset, 'METADATA' AS terminal_type
            FROM silver.device_metadata_history
            UNION ALL
            SELECT source_topic, source_partition, source_offset, 'ENRICHED'
            FROM silver.enriched_events
            UNION ALL
            SELECT source_topic, source_partition, source_offset, disposition
            FROM silver.rejected_events
        ),
        scoped AS (
            SELECT terminal.*
            FROM terminal
            JOIN delivered ON {scoped_join("terminal")}
        )
    """


def verify_terminal_accounting(client: TrinoClient, manifest: dict[str, Any]) -> dict[str, int]:
    expected = int(manifest["delivered_total"])
    prefix = terminal_sql(manifest)
    _, rows = client.query(
        prefix
        + """
        SELECT count(*) AS rows,
               count(DISTINCT ROW(source_topic, source_partition, source_offset)) AS coordinates
        FROM scoped
        """
    )
    assert_equal("terminal silver rows", int(rows[0][0]), expected)
    assert_equal("unique terminal silver coordinates", int(rows[0][1]), expected)

    _, disposition_rows = client.query(
        prefix
        + """
        SELECT terminal_type, count(*)
        FROM scoped
        GROUP BY terminal_type
        ORDER BY terminal_type
        """
    )
    counts = {str(row[0]): int(row[1]) for row in disposition_rows}
    allowed = {
        "METADATA",
        "ENRICHED",
        "INVALID",
        "DUPLICATE",
        "MISSING_METADATA",
        "TOO_LATE",
    }
    unknown = set(counts) - allowed
    if unknown:
        raise AssertionError(f"Unknown terminal dispositions: {sorted(unknown)}")

    counters = manifest["counters"]
    assert_equal("metadata terminal rows", counts.get("METADATA", 0), int(counters["metadata"]))
    expected_invalid = int(counters.get("invalid_logical", 0)) + int(
        counters.get("invalid_malformed", 0)
    )
    assert_equal("invalid terminal rows", counts.get("INVALID", 0), expected_invalid)
    assert_equal(
        "duplicate terminal rows",
        counts.get("DUPLICATE", 0),
        int(counters.get("expected_duplicate_disposition", 0)),
    )
    late_candidates = int(counters.get("too_late", 0))
    actual_late = counts.get("TOO_LATE", 0)
    if late_candidates > 0 and actual_late == 0:
        raise AssertionError("late injection was enabled but no event reached TOO_LATE")
    if actual_late > late_candidates:
        raise AssertionError(
            f"too-late rows exceed generated candidates: {actual_late} > {late_candidates}"
        )
    print(f"PASS too-late terminal rows: {actual_late:,}/{late_candidates:,} candidates")
    return counts


def verify_enriched_identity(client: TrinoClient, manifest: dict[str, Any]) -> None:
    cte = delivered_cte(manifest)
    duplicates = client.scalar(
        f"""
        WITH {cte}, scoped AS (
            SELECT source.*
            FROM silver.enriched_events source
            JOIN delivered ON {scoped_join()}
        )
        SELECT count(*) - count(DISTINCT event_id) FROM scoped
        """
    )
    assert_equal("duplicate enriched event IDs", duplicates, 0)


def verify_finalized_gold(client: TrinoClient, manifest: dict[str, Any]) -> None:
    cte = delivered_cte(manifest)
    run_id = sql_text(str(manifest["run_id"]) + "-shipment-")
    _, rows = client.query(
        f"""
        WITH {cte},
        run_events AS (
            SELECT source.*
            FROM silver.enriched_events source
            JOIN delivered ON {scoped_join()}
        ),
        cutoff AS (
            SELECT max(event_time) - INTERVAL '6' MINUTE AS finalized_before
            FROM run_events
        ),
        recomputed AS (
            SELECT shipment_id,
                   date_trunc('hour', event_time) AS window_start,
                   date_trunc('hour', event_time) + INTERVAL '1' HOUR AS window_end,
                   min(numeric_value) FILTER (
                       WHERE event_type = 'TELEMETRY_TEMPERATURE') AS temperature_min,
                   max(numeric_value) FILTER (
                       WHERE event_type = 'TELEMETRY_TEMPERATURE') AS temperature_max,
                   avg(numeric_value) FILTER (
                       WHERE event_type = 'TELEMETRY_TEMPERATURE') AS temperature_avg,
                   avg(numeric_value) FILTER (
                       WHERE event_type = 'TELEMETRY_HUMIDITY') AS humidity_avg,
                   count_if(event_type = 'DETECTION') AS detection_count,
                   count_if(breach) AS breach_count,
                   count(*) AS event_count,
                   max_by(latitude, ROW(event_time, source_partition, source_offset)) FILTER (
                       WHERE latitude IS NOT NULL AND longitude IS NOT NULL) AS latest_latitude,
                   max_by(longitude, ROW(event_time, source_partition, source_offset)) FILTER (
                       WHERE latitude IS NOT NULL AND longitude IS NOT NULL) AS latest_longitude,
                   max(event_time) AS last_event_time,
                   CAST(date_trunc('hour', event_time) AS DATE) AS window_day
            FROM run_events
            GROUP BY shipment_id, date_trunc('hour', event_time)
            HAVING date_trunc('hour', event_time) + INTERVAL '1' HOUR
                   <= (SELECT finalized_before FROM cutoff)
        ),
        actual AS (
            SELECT *
            FROM gold.shipment_hourly_metrics
            WHERE starts_with(shipment_id, {run_id})
              AND window_end <= (SELECT finalized_before FROM cutoff)
        ),
        compared AS (
            SELECT recomputed.shipment_id AS expected_shipment,
                   actual.shipment_id AS actual_shipment,
                   recomputed.temperature_min AS expected_temperature_min,
                   actual.temperature_min AS actual_temperature_min,
                   recomputed.temperature_max AS expected_temperature_max,
                   actual.temperature_max AS actual_temperature_max,
                   recomputed.temperature_avg AS expected_temperature_avg,
                   actual.temperature_avg AS actual_temperature_avg,
                   recomputed.humidity_avg AS expected_humidity_avg,
                   actual.humidity_avg AS actual_humidity_avg,
                   recomputed.detection_count AS expected_detection_count,
                   actual.detection_count AS actual_detection_count,
                   recomputed.breach_count AS expected_breach_count,
                   actual.breach_count AS actual_breach_count,
                   recomputed.event_count AS expected_event_count,
                   actual.event_count AS actual_event_count,
                   recomputed.latest_latitude AS expected_latitude,
                   actual.latest_latitude AS actual_latitude,
                   recomputed.latest_longitude AS expected_longitude,
                   actual.latest_longitude AS actual_longitude,
                   recomputed.last_event_time AS expected_last_event_time,
                   actual.last_event_time AS actual_last_event_time,
                   recomputed.window_end AS expected_window_end,
                   actual.window_end AS actual_window_end,
                   recomputed.window_day AS expected_window_day,
                   actual.window_day AS actual_window_day,
                   actual.updated_at
            FROM recomputed
            FULL OUTER JOIN actual
              ON recomputed.shipment_id = actual.shipment_id
             AND recomputed.window_start = actual.window_start
        )
        SELECT count(*) AS compared_rows,
               count_if(
                   expected_shipment IS NULL OR actual_shipment IS NULL
                   OR expected_window_end IS DISTINCT FROM actual_window_end
                   OR expected_window_day IS DISTINCT FROM actual_window_day
                   OR expected_detection_count IS DISTINCT FROM actual_detection_count
                   OR expected_breach_count IS DISTINCT FROM actual_breach_count
                   OR expected_event_count IS DISTINCT FROM actual_event_count
                   OR expected_last_event_time IS DISTINCT FROM actual_last_event_time
                   OR (expected_temperature_min IS NULL) <> (actual_temperature_min IS NULL)
                   OR (expected_temperature_min IS NOT NULL AND
                       abs(expected_temperature_min - actual_temperature_min) > 0.000001)
                   OR (expected_temperature_max IS NULL) <> (actual_temperature_max IS NULL)
                   OR (expected_temperature_max IS NOT NULL AND
                       abs(expected_temperature_max - actual_temperature_max) > 0.000001)
                   OR (expected_temperature_avg IS NULL) <> (actual_temperature_avg IS NULL)
                   OR (expected_temperature_avg IS NOT NULL AND
                       abs(expected_temperature_avg - actual_temperature_avg) > 0.000001)
                   OR (expected_humidity_avg IS NULL) <> (actual_humidity_avg IS NULL)
                   OR (expected_humidity_avg IS NOT NULL AND
                       abs(expected_humidity_avg - actual_humidity_avg) > 0.000001)
                   OR (expected_latitude IS NULL) <> (actual_latitude IS NULL)
                   OR (expected_latitude IS NOT NULL AND
                       abs(expected_latitude - actual_latitude) > 0.000001)
                   OR (expected_longitude IS NULL) <> (actual_longitude IS NULL)
                   OR (expected_longitude IS NOT NULL AND
                       abs(expected_longitude - actual_longitude) > 0.000001)
                   OR updated_at IS NULL
               ) AS mismatches
        FROM compared
        """
    )
    compared = int(rows[0][0])
    mismatches = int(rows[0][1])
    if manifest["profile"] != "benchmark" and compared == 0:
        raise AssertionError("No finalized gold windows were available for comparison")
    assert_equal("finalized gold aggregate mismatches", mismatches, 0)
    print(f"PASS finalized gold windows compared: {compared:,}")


def main() -> None:
    manifest_path = resolve_manifest_path()
    manifest = load_manifest(manifest_path)
    client = TrinoClient(os.getenv("TRINO_URL", "http://trino:8080"))

    wait_for_commits(client, manifest)
    verify_bronze(client, manifest)
    counts = verify_terminal_accounting(client, manifest)
    verify_enriched_identity(client, manifest)
    verify_finalized_gold(client, manifest)
    print(
        f"All reconciliation checks passed for {manifest['run_id']} "
        f"({sum(counts.values()):,} delivered records)"
    )


if __name__ == "__main__":
    main()
