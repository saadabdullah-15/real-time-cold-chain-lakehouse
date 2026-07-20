from simulator.config import RunConfig
from simulator.generator import ColdChainGenerator
from simulator.models import LOCATION_TOPIC, METADATA_TOPIC, TELEMETRY_TOPIC


def generate(seed: int = 42):
    config = RunConfig.from_profile(
        "demo",
        event_count=100,
        device_count=5,
        seed=seed,
        duplicate_rate=0.1,
        invalid_rate=0.1,
        out_of_order_rate=0.1,
        too_late_rate=0.1,
        start_time_ms=1_700_000_000_000,
    )
    return list(ColdChainGenerator(config).records())


def test_generation_is_deterministic() -> None:
    assert generate() == generate()
    assert generate(41) != generate(42)


def test_generates_metadata_and_sensor_topics() -> None:
    records = generate()
    assert sum(record.topic == METADATA_TOPIC for record in records) == 5
    topics = {record.topic for record in records}
    assert {METADATA_TOPIC, TELEMETRY_TOPIC, LOCATION_TOPIC}.issubset(topics)
    assert len(records) >= 105


def test_duplicate_preserves_event_identity() -> None:
    records = generate()
    duplicates = [record for record in records if record.duplicate]
    assert duplicates
    ids = {
        record.value["event_id"]
        for record in records
        if record.value is not None and not record.duplicate
    }
    assert all(record.value is None or record.value["event_id"] in ids for record in duplicates)
    assert all(record.category != "duplicate" for record in duplicates)


def test_initial_metadata_covers_the_injected_lateness_horizon() -> None:
    config = RunConfig.from_profile(
        "demo",
        event_count=100,
        device_count=3,
        duplicate_rate=0,
        invalid_rate=0,
        out_of_order_rate=0,
        too_late_rate=1,
        start_time_ms=1_700_000_000_000,
    )
    generator = ColdChainGenerator(config)
    effective_from = min(
        int(record.value["effective_from"]) for record in generator.metadata_records()
    )
    event_times = [
        int(record.value["event_time"]) for record in generator.sensor_records()
    ]

    assert min(event_times) >= effective_from


def test_v2_adds_signal_strength_only_to_telemetry() -> None:
    config = RunConfig.from_profile(
        "demo",
        event_count=50,
        device_count=2,
        schema_version=2,
        invalid_rate=0,
        start_time_ms=1_700_000_000_000,
    )
    telemetry = [
        record
        for record in ColdChainGenerator(config).sensor_records()
        if record.topic == TELEMETRY_TOPIC and not record.duplicate
    ]
    assert telemetry
    assert all("signal_strength_dbm" in record.value for record in telemetry)
