from __future__ import annotations

import json
from pathlib import Path

from fastavro import parse_schema

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
COMMON_FIELDS = {"event_id", "device_id", "event_time", "produced_at", "sequence_number"}


def read_schema(name: str) -> dict[str, object]:
    return json.loads((SCHEMA_DIR / name).read_text(encoding="utf-8"))


def test_all_avro_contracts_parse() -> None:
    for path in SCHEMA_DIR.glob("*.avsc"):
        assert parse_schema(read_schema(path.name))["type"] == "record"


def test_event_contracts_have_common_envelope() -> None:
    for name in (
        "telemetry-v1.avsc",
        "location-v1.avsc",
        "detection-v1.avsc",
        "device-metadata-v1.avsc",
    ):
        fields = {field["name"] for field in read_schema(name)["fields"]}
        assert COMMON_FIELDS <= fields


def test_telemetry_v2_is_additive_and_nullable() -> None:
    v1 = read_schema("telemetry-v1.avsc")
    v2 = read_schema("telemetry-v2.avsc")
    v1_fields = {field["name"]: field for field in v1["fields"]}
    v2_fields = {field["name"]: field for field in v2["fields"]}
    assert set(v2_fields) == set(v1_fields) | {"signal_strength_dbm"}
    assert all(v2_fields[name] == field for name, field in v1_fields.items())
    assert v2_fields["signal_strength_dbm"]["type"] == ["null", "int"]
    assert v2_fields["signal_strength_dbm"]["default"] is None
