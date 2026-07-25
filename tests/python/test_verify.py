from __future__ import annotations

import io
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from tools.verify import (
    TrinoClient,
    delivered_cte,
    load_manifest,
    verify_bronze,
    verify_terminal_accounting,
)


def complete_manifest() -> dict[str, Any]:
    return {
        "manifest_version": 2,
        "run_id": "run-test",
        "status": "COMPLETE",
        "profile": "demo",
        "attempted_total": 4,
        "delivered_total": 4,
        "counters": {
            "metadata": 1,
            "invalid_logical": 1,
            "invalid_malformed": 0,
            "expected_duplicate_disposition": 1,
            "too_late": 0,
        },
        "offset_ranges": [
            {
                "topic": "coldchain.telemetry.v1",
                "partition": 0,
                "start_offset": 10,
                "end_offset": 12,
                "count": 3,
            },
            {
                "topic": "coldchain.device-metadata.v1",
                "partition": 1,
                "start_offset": 20,
                "end_offset": 20,
                "count": 1,
            },
        ],
    }


def test_trino_query_follows_every_page() -> None:
    client = TrinoClient("http://trino:8080")
    responses = iter(
        [
            {
                "columns": [{"name": "value"}],
                "data": [[1]],
                "nextUri": "http://trino:8080/v1/statement/queued/1",
            },
            {
                "data": [[2]],
                "nextUri": "http://trino:8080/v1/statement/executing/1",
            },
            {"data": [[3]]},
        ]
    )
    requested: list[str] = []

    def fake_open(request: urllib.request.Request) -> dict[str, Any]:
        requested.append(request.full_url)
        return next(responses)

    client._open = fake_open  # type: ignore[method-assign]

    columns, rows = client.query("SELECT value")

    assert columns == ["value"]
    assert rows == [[1], [2], [3]]
    assert requested == [
        "http://trino:8080/v1/statement",
        "http://trino:8080/v1/statement/queued/1",
        "http://trino:8080/v1/statement/executing/1",
    ]


def test_trino_query_reports_server_failure_details() -> None:
    client = TrinoClient("http://trino:8080")
    client._open = lambda _request: {  # type: ignore[method-assign]
        "error": {
            "message": "outer",
            "failureInfo": {"message": "column event_typo cannot be resolved"},
        }
    }

    with pytest.raises(RuntimeError, match="column event_typo cannot be resolved"):
        client.query("SELECT event_typo")


def test_trino_http_error_includes_status_url_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    body = json.dumps({"error": {"message": "catalog unavailable"}}).encode()
    failure = urllib.error.HTTPError(
        "http://trino:8080/v1/statement",
        503,
        "Service Unavailable",
        {},
        io.BytesIO(body),
    )
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(failure)
    )

    request = urllib.request.Request("http://trino:8080/v1/statement")
    with pytest.raises(RuntimeError, match=r"HTTP 503.*catalog unavailable"):
        TrinoClient._open(request)


def test_manifest_requires_exact_delivery_ranges(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    manifest = complete_manifest()
    manifest["offset_ranges"][0]["count"] = 2
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RuntimeError, match="offset ranges"):
        load_manifest(path)


def test_delivered_cte_escapes_topics_and_includes_each_range() -> None:
    manifest = complete_manifest()
    manifest["offset_ranges"][0]["topic"] = "topic'quoted"

    sql = delivered_cte(manifest)

    assert "'topic''quoted', 0, 10, 12" in sql
    assert "'coldchain.device-metadata.v1', 1, 20, 20" in sql


class ReconciliationClient:
    def __init__(
        self,
        *,
        bronze_rows: tuple[int, int] = (4, 4),
        terminal_rows: tuple[int, int] = (4, 4),
    ):
        self.bronze_rows = bronze_rows
        self.terminal_rows = terminal_rows

    def query(self, sql: str) -> tuple[list[str], list[list[Any]]]:
        if "bronze.raw_events" in sql:
            return ["rows", "coordinates"], [[*self.bronze_rows]]
        if "GROUP BY terminal_type" in sql:
            return ["terminal_type", "count"], [
                ["DUPLICATE", 1],
                ["ENRICHED", 1],
                ["INVALID", 1],
                ["METADATA", 1],
            ]
        if "FROM scoped" in sql:
            return ["rows", "coordinates"], [[*self.terminal_rows]]
        raise AssertionError(f"Unexpected query: {sql}")


def test_exact_bronze_and_terminal_reconciliation_passes() -> None:
    client = ReconciliationClient()
    manifest = complete_manifest()

    verify_bronze(client, manifest)  # type: ignore[arg-type]
    counts = verify_terminal_accounting(client, manifest)  # type: ignore[arg-type]

    assert counts == {"DUPLICATE": 1, "ENRICHED": 1, "INVALID": 1, "METADATA": 1}


def test_terminal_reconciliation_rejects_coordinate_present_twice() -> None:
    client = ReconciliationClient(terminal_rows=(4, 3))

    with pytest.raises(AssertionError, match="unique terminal silver coordinates"):
        verify_terminal_accounting(client, complete_manifest())  # type: ignore[arg-type]
