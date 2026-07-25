from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from simulator.models import (
    DETECTION_TOPIC,
    LOCATION_TOPIC,
    METADATA_TOPIC,
    TELEMETRY_TOPIC,
    GeneratedRecord,
    RunManifest,
)

SCHEMA_FILES = {
    LOCATION_TOPIC: "location-v1.avsc",
    DETECTION_TOPIC: "detection-v1.avsc",
    METADATA_TOPIC: "device-metadata-v1.avsc",
}


class KafkaPublisher:
    def __init__(
        self,
        schema_version: int,
        rate_per_second: float,
        *,
        producer: Any | None = None,
        serializers: dict[str, Any] | None = None,
    ):
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085")
        self.producer = producer or Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "coldchain-simulator",
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
                "linger.ms": 10,
            }
        )
        if serializers is None:
            registry = SchemaRegistryClient({"url": registry_url})
            schema_dir = Path(
                os.getenv("SCHEMA_DIR", Path(__file__).resolve().parents[1] / "schemas")
            )
            files = dict(SCHEMA_FILES)
            files[TELEMETRY_TOPIC] = f"telemetry-v{schema_version}.avsc"
            serializers = {
                topic: AvroSerializer(
                    registry,
                    (schema_dir / filename).read_text(encoding="utf-8"),
                    lambda value, _: value,
                    {"auto.register.schemas": False},
                )
                for topic, filename in files.items()
            }
        self.serializers = serializers
        self.rate_per_second = rate_per_second
        self.delivery_errors: list[str] = []

    def publish(self, records: Iterable[GeneratedRecord], manifest: RunManifest) -> None:
        started = time.perf_counter()
        sensor_messages = 0
        for record in records:
            manifest.observe(record)
            if record.malformed_payload is not None:
                payload = record.malformed_payload
            else:
                assert record.value is not None
                serializer = self.serializers[record.topic]
                payload = serializer(
                    record.value,
                    SerializationContext(record.topic, MessageField.VALUE),
                )
            event_time_ms = record.event_time_ms
            if event_time_ms is None and record.value is not None:
                value_event_time = record.value.get("event_time")
                if isinstance(value_event_time, int):
                    event_time_ms = value_event_time
            produce_args = {
                "topic": record.topic,
                "key": record.key.encode(),
                "value": payload,
                "on_delivery": lambda error, message, active=manifest: self._on_delivery(
                    error, message, active
                ),
            }
            if event_time_ms is not None:
                produce_args["timestamp"] = event_time_ms
            manifest.record_attempt()
            self.producer.produce(**produce_args)
            self.producer.poll(0)
            if record.topic != METADATA_TOPIC:
                sensor_messages += 1
                target_elapsed = sensor_messages / self.rate_per_second
                remaining = target_elapsed - (time.perf_counter() - started)
                if remaining > 0:
                    time.sleep(min(remaining, 0.05))
        remaining = self.producer.flush(30)
        if remaining:
            self.delivery_errors.append(
                f"producer flush timed out with {remaining} message(s) still queued"
            )
        if self.delivery_errors:
            raise RuntimeError(json.dumps(self.delivery_errors[:10]))
        if manifest.attempted_total != manifest.delivered_total:
            raise RuntimeError(
                "attempted and delivered counts differ: "
                f"{manifest.attempted_total} != {manifest.delivered_total}"
            )
        manifest.complete(time.perf_counter() - started)

    def _on_delivery(self, error: object, message: Any, manifest: RunManifest) -> None:
        if error:
            self.delivery_errors.append(str(error))
            return
        manifest.record_delivery(message.topic(), message.partition(), message.offset())
