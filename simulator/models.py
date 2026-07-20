from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

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
    profile: str
    seed: int
    configured_sensor_events: int
    device_count: int
    schema_version: int
    start_time_ms: int
    counters: dict[str, int] = field(default_factory=dict)

    def observe(self, record: GeneratedRecord) -> None:
        self.counters["produced_total"] = self.counters.get("produced_total", 0) + 1
        self.counters[record.topic] = self.counters.get(record.topic, 0) + 1
        self.counters[record.category] = self.counters.get(record.category, 0) + 1
        if record.duplicate:
            self.counters["duplicates"] = self.counters.get("duplicates", 0) + 1
        if record.malformed_payload is not None:
            self.counters["malformed"] = self.counters.get("malformed", 0) + 1

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2, sort_keys=True), encoding="utf-8")
