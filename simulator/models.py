from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

TELEMETRY_TOPIC = "coldchain.telemetry.v1"
LOCATION_TOPIC = "coldchain.location.v1"
DETECTION_TOPIC = "coldchain.detection.v1"
METADATA_TOPIC = "coldchain.device-metadata.v1"


@dataclass(frozen=True)
class GeneratedRecord:
    topic: str
    key: str
    value: dict[str, object] | None
    category: str
    malformed_payload: bytes | None = None
    duplicate: bool = False
    event_time_ms: int | None = None


@dataclass
class RunManifest:
    run_id: str
    profile: str
    seed: int
    configured_sensor_events: int
    device_count: int
    schema_version: int
    start_time_ms: int
    manifest_version: int = 2
    status: str = "RUNNING"
    started_at: str = field(default_factory=lambda: utc_now())
    completed_at: str | None = None
    attempted_total: int = 0
    delivered_total: int = 0
    duration_seconds: float | None = None
    achieved_publish_rate: float | None = None
    counters: dict[str, int] = field(default_factory=dict)
    offset_ranges: list[dict[str, int | str]] = field(default_factory=list)
    _offsets: dict[tuple[str, int], set[int]] = field(default_factory=dict, init=False, repr=False)

    def observe(self, record: GeneratedRecord) -> None:
        self.counters["generated_total"] = self.counters.get("generated_total", 0) + 1
        self.counters[record.topic] = self.counters.get(record.topic, 0) + 1
        self.counters[record.category] = self.counters.get(record.category, 0) + 1
        if record.duplicate:
            self.counters["duplicates"] = self.counters.get("duplicates", 0) + 1
            if not record.category.startswith("invalid_"):
                self.counters["expected_duplicate_disposition"] = (
                    self.counters.get("expected_duplicate_disposition", 0) + 1
                )
        if record.malformed_payload is not None:
            self.counters["malformed"] = self.counters.get("malformed", 0) + 1

    def record_attempt(self) -> None:
        self.attempted_total += 1

    def record_delivery(self, topic: str, partition: int, offset: int) -> None:
        self.delivered_total += 1
        self._offsets.setdefault((topic, partition), set()).add(offset)

    def complete(self, duration_seconds: float) -> None:
        if self.attempted_total != self.delivered_total:
            raise RuntimeError(
                "attempted and delivered counts differ: "
                f"{self.attempted_total} != {self.delivered_total}"
            )
        self.status = "COMPLETE"
        self.completed_at = utc_now()
        self.duration_seconds = round(duration_seconds, 6)
        self.achieved_publish_rate = round(
            self.delivered_total / duration_seconds if duration_seconds > 0 else 0.0,
            3,
        )
        self.offset_ranges = compress_offsets(self._offsets)

    def complete_dry_run(self, duration_seconds: float) -> None:
        self.status = "DRY_RUN"
        self.completed_at = utc_now()
        self.duration_seconds = round(duration_seconds, 6)
        self.achieved_publish_rate = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "run_id": self.run_id,
            "status": self.status,
            "profile": self.profile,
            "seed": self.seed,
            "configured_sensor_events": self.configured_sensor_events,
            "device_count": self.device_count,
            "schema_version": self.schema_version,
            "start_time_ms": self.start_time_ms,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "attempted_total": self.attempted_total,
            "delivered_total": self.delivered_total,
            "duration_seconds": self.duration_seconds,
            "achieved_publish_rate": self.achieved_publish_rate,
            "counters": self.counters,
            "offset_ranges": self.offset_ranges,
        }

    def write_atomic(self, path: Path) -> None:
        if self.status not in {"COMPLETE", "DRY_RUN"}:
            raise RuntimeError(f"refusing to write incomplete manifest with status {self.status}")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        payload = json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"
        try:
            with temporary.open("w", encoding="utf-8", newline="\n") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def compress_offsets(
    offsets: dict[tuple[str, int], set[int]],
) -> list[dict[str, int | str]]:
    ranges: list[dict[str, int | str]] = []
    for (topic, partition), values in sorted(offsets.items()):
        ordered = sorted(values)
        if not ordered:
            continue
        start = previous = ordered[0]
        for current in ordered[1:]:
            if current == previous + 1:
                previous = current
                continue
            ranges.append(offset_range(topic, partition, start, previous))
            start = previous = current
        ranges.append(offset_range(topic, partition, start, previous))
    return ranges


def offset_range(topic: str, partition: int, start: int, end: int) -> dict[str, int | str]:
    return {
        "topic": topic,
        "partition": partition,
        "start_offset": start,
        "end_offset": end,
        "count": end - start + 1,
    }
