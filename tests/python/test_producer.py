from __future__ import annotations

from collections.abc import Callable

import pytest

from simulator.models import TELEMETRY_TOPIC, GeneratedRecord, RunManifest
from simulator.producer import KafkaPublisher


class FakeMessage:
    def __init__(self, topic: str, partition: int, offset: int):
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeProducer:
    def __init__(self, *, callback_error: object | None = None, remaining: int = 0):
        self.callback_error = callback_error
        self.remaining = remaining
        self.offset = 0

    def produce(self, **arguments: object) -> None:
        callback = arguments["on_delivery"]
        assert isinstance(callback, Callable)
        topic = str(arguments["topic"])
        callback(self.callback_error, FakeMessage(topic, 0, self.offset))
        self.offset += 1

    def poll(self, _timeout: float) -> None:
        return None

    def flush(self, _timeout: float) -> int:
        return self.remaining


def manifest() -> RunManifest:
    return RunManifest(
        run_id="run-publisher",
        profile="demo",
        seed=42,
        configured_sensor_events=1,
        device_count=1,
        schema_version=1,
        start_time_ms=1_700_000_000_000,
    )


def record() -> GeneratedRecord:
    return GeneratedRecord(
        topic=TELEMETRY_TOPIC,
        key="run-publisher-device-0000",
        value={
            "event_id": "run-publisher:event",
            "device_id": "run-publisher-device-0000",
            "event_time": 1_700_000_000_000,
        },
        category="on_time",
    )


def serializer(value: object, _context: object) -> bytes:
    assert value is not None
    return b"serialized"


def publisher(producer: FakeProducer) -> KafkaPublisher:
    return KafkaPublisher(
        1,
        1_000_000,
        producer=producer,
        serializers={TELEMETRY_TOPIC: serializer},
    )


def test_success_requires_and_records_confirmed_delivery() -> None:
    active = manifest()
    publisher(FakeProducer()).publish([record()], active)

    assert active.status == "COMPLETE"
    assert active.attempted_total == active.delivered_total == 1
    assert active.offset_ranges[0]["count"] == 1


def test_callback_failure_fails_publication() -> None:
    active = manifest()

    with pytest.raises(RuntimeError, match="delivery failed"):
        publisher(FakeProducer(callback_error="delivery failed")).publish([record()], active)
    assert active.status == "RUNNING"
    assert active.attempted_total == 1
    assert active.delivered_total == 0


def test_flush_timeout_fails_publication() -> None:
    active = manifest()

    with pytest.raises(RuntimeError, match="flush timed out"):
        publisher(FakeProducer(remaining=1)).publish([record()], active)
    assert active.status == "RUNNING"
