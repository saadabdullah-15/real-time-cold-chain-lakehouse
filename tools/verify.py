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
                raise RuntimeError(error.get("message", str(error)))
            if response.get("columns"):
                columns = [column["name"] for column in response["columns"]]
            rows.extend(response.get("data", []))
            next_uri = response.get("nextUri")
            if not next_uri:
                return columns, rows
            response = self._open(urllib.request.Request(next_uri, method="GET"))

    @staticmethod
    def _open(request: urllib.request.Request) -> dict[str, Any]:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read())

    def scalar(self, sql: str) -> int:
        _, rows = self.query(sql)
        if not rows:
            raise RuntimeError(f"Query returned no rows: {sql}")
        return int(rows[0][0])


def wait_for_commits(client: TrinoClient, expected_raw: int, timeout: int = 180) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if client.scalar("SELECT count(*) FROM bronze.raw_events") >= expected_raw:
                return
        except (RuntimeError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(5)
    raise RuntimeError(
        f"Raw table did not reach {expected_raw} rows within {timeout}s; last error: {last_error}"
    )


def assert_equal(label: str, actual: int, expected: int) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected:,}, got {actual:,}")
    print(f"PASS {label}: {actual:,}")


def assert_zero(label: str, actual: int) -> None:
    assert_equal(label, actual, 0)


def main() -> None:
    manifest_path = Path(os.getenv("MANIFEST_PATH", "data/manifest.json"))
    if not manifest_path.exists():
        raise RuntimeError(f"Manifest does not exist: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    counters = manifest["counters"]
    client = TrinoClient(os.getenv("TRINO_URL", "http://trino:8080"))

    expected_raw = int(counters["produced_total"])
    wait_for_commits(client, expected_raw)
    raw = client.scalar("SELECT count(*) FROM bronze.raw_events")
    assert_equal("raw manifest reconciliation", raw, expected_raw)
    assert_zero(
        "raw Kafka-coordinate duplicates",
        client.scalar(
            "SELECT count(*) - count(DISTINCT ROW(source_topic, source_partition, source_offset)) "
            "FROM bronze.raw_events"
        ),
    )
    assert_equal(
        "metadata history reconciliation",
        client.scalar("SELECT count(*) FROM silver.device_metadata_history"),
        int(counters["metadata"]),
    )
    assert_zero(
        "duplicate enriched event IDs",
        client.scalar(
            "SELECT count(*) - count(DISTINCT event_id) FROM silver.enriched_events"
        ),
    )

    expected_invalid = int(counters.get("invalid_logical", 0)) + int(
        counters.get("invalid_malformed", 0)
    )
    assert_equal(
        "invalid record reconciliation",
        client.scalar(
            "SELECT count(*) FROM silver.rejected_events WHERE disposition = 'INVALID'"
        ),
        expected_invalid,
    )
    late = client.scalar(
        "SELECT count(*) FROM silver.rejected_events WHERE disposition = 'TOO_LATE'"
    )
    expected_late = int(counters.get("too_late", 0))
    if expected_late > 0 and late == 0:
        raise AssertionError(
            "late-event injection was configured, but no records reached the quarantine path"
        )
    if late > expected_late:
        raise AssertionError(
            f"too-late records exceed generated candidates: {late} > {expected_late}"
        )
    print(f"PASS too-late quarantine: {late:,}/{expected_late:,} generated candidates")

    enriched = client.scalar("SELECT count(*) FROM silver.enriched_events")
    maximum_enriched = int(manifest["configured_sensor_events"]) - expected_invalid
    if not 0 < enriched <= maximum_enriched:
        raise AssertionError(
            f"enriched count outside expected bounds: {enriched:,} not in 1..{maximum_enriched:,}"
        )
    print(f"PASS enriched row bounds: {enriched:,}")
    if client.scalar("SELECT count(*) FROM gold.shipment_hourly_metrics") <= 0:
        raise AssertionError("gold.shipment_hourly_metrics is empty")
    print("PASS hourly metrics populated")
    print("All cold-chain reconciliation checks passed")


if __name__ == "__main__":
    main()
