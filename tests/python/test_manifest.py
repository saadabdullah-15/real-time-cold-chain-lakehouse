from __future__ import annotations

import json
from pathlib import Path

import pytest

from simulator.models import RunManifest, compress_offsets


def manifest() -> RunManifest:
    return RunManifest(
        run_id="run-test",
        profile="demo",
        seed=42,
        configured_sensor_events=10,
        device_count=2,
        schema_version=1,
        start_time_ms=1_700_000_000_000,
    )


def test_offset_ranges_are_sorted_and_compressed() -> None:
    ranges = compress_offsets(
        {
            ("topic-b", 1): {8, 9},
            ("topic-a", 0): {1, 2, 3, 7},
        }
    )

    assert ranges == [
        {
            "topic": "topic-a",
            "partition": 0,
            "start_offset": 1,
            "end_offset": 3,
            "count": 3,
        },
        {
            "topic": "topic-a",
            "partition": 0,
            "start_offset": 7,
            "end_offset": 7,
            "count": 1,
        },
        {
            "topic": "topic-b",
            "partition": 1,
            "start_offset": 8,
            "end_offset": 9,
            "count": 2,
        },
    ]


def test_manifest_is_written_atomically_only_after_completion(tmp_path: Path) -> None:
    target = tmp_path / "run-test" / "manifest.json"
    active = manifest()

    with pytest.raises(RuntimeError, match="incomplete manifest"):
        active.write_atomic(target)
    assert not target.exists()

    active.record_attempt()
    active.record_delivery("coldchain.telemetry.v1", 0, 12)
    active.complete(0.5)
    active.write_atomic(target)

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["manifest_version"] == 2
    assert payload["status"] == "COMPLETE"
    assert payload["attempted_total"] == payload["delivered_total"] == 1
    assert payload["offset_ranges"][0]["start_offset"] == 12
    assert list(target.parent.glob("*.tmp")) == []


def test_manifest_rejects_delivery_count_mismatch() -> None:
    active = manifest()
    active.record_attempt()

    with pytest.raises(RuntimeError, match="attempted and delivered"):
        active.complete(1.0)
