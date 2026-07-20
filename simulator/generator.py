from __future__ import annotations

import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

from simulator.config import RunConfig
from simulator.models import (
    DETECTION_TOPIC,
    LOCATION_TOPIC,
    METADATA_TOPIC,
    TELEMETRY_TOPIC,
    GeneratedRecord,
)

ROUTES = (
    ("Berlin", "Paris", 52.5200, 13.4050, 48.8566, 2.3522),
    ("Hamburg", "Amsterdam", 53.5511, 9.9937, 52.3676, 4.9041),
    ("Munich", "Vienna", 48.1351, 11.5820, 48.2082, 16.3738),
    ("Frankfurt", "Prague", 50.1109, 8.6821, 50.0755, 14.4378),
)


class ColdChainGenerator:
    def __init__(self, config: RunConfig):
        config.validate()
        self.config = config
        self.random = random.Random(config.seed)
        self.start_time_ms = config.start_time_ms or int(datetime.now(UTC).timestamp() * 1000)
        self._sequences = {self._device_id(i): 0 for i in range(config.device_count)}
        self._metadata = self._make_metadata()

    @staticmethod
    def _device_id(index: int) -> str:
        return f"device-{index:04d}"

    def _event_id(self) -> str:
        return str(uuid.UUID(int=self.random.getrandbits(128)))

    def _next_sequence(self, device_id: str) -> int:
        self._sequences[device_id] += 1
        return self._sequences[device_id]

    def _make_metadata(self) -> dict[str, dict[str, object]]:
        result: dict[str, dict[str, object]] = {}
        cargo = ("vaccines", 2.0, 8.0), ("fresh-produce", 1.0, 6.0), ("frozen-food", -24.0, -18.0)
        for index in range(self.config.device_count):
            device_id = self._device_id(index)
            route = ROUTES[index % len(ROUTES)]
            cargo_type, min_temp, max_temp = cargo[index % len(cargo)]
            result[device_id] = {
                "route": route,
                "shipment_id": f"shipment-{index:05d}",
                "vehicle_id": f"vehicle-{index % max(1, self.config.device_count // 2):04d}",
                "cargo_type": cargo_type,
                "min_temperature_c": min_temp,
                "max_temperature_c": max_temp,
            }
        return result

    def metadata_records(self) -> Iterator[GeneratedRecord]:
        # Cover the full injected-lateness horizon so accepted historical events
        # can always select the metadata version effective at their event time.
        effective_from = self.start_time_ms - 3_600_000
        for device_id, metadata in self._metadata.items():
            route = metadata["route"]
            assert isinstance(route, tuple)
            yield GeneratedRecord(
                topic=METADATA_TOPIC,
                key=device_id,
                category="metadata",
                value={
                    "event_id": self._event_id(),
                    "device_id": device_id,
                    "event_time": effective_from,
                    "produced_at": self.start_time_ms,
                    "sequence_number": self._next_sequence(device_id),
                    "metadata_version": 1,
                    "effective_from": effective_from,
                    "shipment_id": metadata["shipment_id"],
                    "vehicle_id": metadata["vehicle_id"],
                    "origin": route[0],
                    "destination": route[1],
                    "cargo_type": metadata["cargo_type"],
                    "min_temperature_c": metadata["min_temperature_c"],
                    "max_temperature_c": metadata["max_temperature_c"],
                    "status": "ACTIVE",
                },
            )

    def sensor_records(self) -> Iterator[GeneratedRecord]:
        for index in range(self.config.event_count):
            record = self._sensor_record(index)
            yield record
            if self.random.random() < self.config.duplicate_rate:
                yield GeneratedRecord(
                    topic=record.topic,
                    key=record.key,
                    value=dict(record.value) if record.value is not None else None,
                    category=record.category,
                    malformed_payload=record.malformed_payload,
                    duplicate=True,
                    event_time_ms=record.event_time_ms,
                )

    def records(self) -> Iterator[GeneratedRecord]:
        yield from self.metadata_records()
        yield from self.sensor_records()

    def _sensor_record(self, index: int) -> GeneratedRecord:
        device_index = self.random.randrange(self.config.device_count)
        device_id = self._device_id(device_index)
        produced_at = self.start_time_ms + int(
            index * self.config.event_time_span_seconds * 1000 / self.config.event_count
        )
        event_time = produced_at
        timing_roll = self.random.random()
        timing_category = "on_time"
        if timing_roll < self.config.too_late_rate:
            event_time -= self.random.randint(360, 900) * 1000
            timing_category = "too_late"
        elif timing_roll < self.config.too_late_rate + self.config.out_of_order_rate:
            event_time -= self.random.randint(1, 60) * 1000
            timing_category = "out_of_order"

        roll = self.random.random()
        if roll < 0.70:
            topic, value = TELEMETRY_TOPIC, self._telemetry(device_id, event_time, produced_at)
        elif roll < 0.95:
            topic, value = LOCATION_TOPIC, self._location(
                device_id, device_index, index, event_time, produced_at
            )
        else:
            topic, value = DETECTION_TOPIC, self._detection(device_id, event_time, produced_at)

        if self.random.random() < self.config.invalid_rate:
            if self.random.random() < 0.5:
                return GeneratedRecord(
                    topic=topic,
                    key=device_id,
                    value=None,
                    category="invalid_malformed",
                    malformed_payload=b"not-confluent-avro",
                    event_time_ms=event_time,
                )
            self._make_logically_invalid(topic, value)
            return GeneratedRecord(topic, device_id, value, "invalid_logical")
        return GeneratedRecord(topic, device_id, value, timing_category)

    def _common(self, device_id: str, event_time: int, produced_at: int) -> dict[str, object]:
        return {
            "event_id": self._event_id(),
            "device_id": device_id,
            "event_time": event_time,
            "produced_at": produced_at,
            "sequence_number": self._next_sequence(device_id),
        }

    def _telemetry(self, device_id: str, event_time: int, produced_at: int) -> dict[str, object]:
        value = self._common(device_id, event_time, produced_at)
        measurement = self.random.choices(
            ("TEMPERATURE", "HUMIDITY", "BATTERY"), weights=(0.70, 0.20, 0.10), k=1
        )[0]
        metadata = self._metadata[device_id]
        if measurement == "TEMPERATURE":
            low = float(metadata["min_temperature_c"])
            high = float(metadata["max_temperature_c"])
            center = (low + high) / 2
            reading = center + self.random.gauss(0, max(0.3, (high - low) / 8))
            if self.random.random() < 0.05:
                reading = high + self.random.uniform(1, 5)
            unit = "CELSIUS"
        elif measurement == "HUMIDITY":
            reading, unit = self.random.uniform(35, 80), "PERCENT"
        else:
            reading, unit = self.random.uniform(15, 100), "PERCENT"
        value.update({"measurement_type": measurement, "value": round(reading, 3), "unit": unit})
        if self.config.schema_version == 2:
            value["signal_strength_dbm"] = self.random.randint(-100, -40)
        return value

    def _location(
        self,
        device_id: str,
        device_index: int,
        index: int,
        event_time: int,
        produced_at: int,
    ) -> dict[str, object]:
        value = self._common(device_id, event_time, produced_at)
        route = self._metadata[device_id]["route"]
        assert isinstance(route, tuple)
        progress = ((index + device_index * 17) % 10_000) / 10_000
        value.update(
            {
                "latitude": (
                    route[2] + (route[4] - route[2]) * progress + self.random.gauss(0, 0.002)
                ),
                "longitude": (
                    route[3] + (route[5] - route[3]) * progress + self.random.gauss(0, 0.002)
                ),
                "speed_kph": max(0.0, self.random.gauss(72, 15)),
                "accuracy_m": self.random.uniform(2, 25),
            }
        )
        return value

    def _detection(self, device_id: str, event_time: int, produced_at: int) -> dict[str, object]:
        value = self._common(device_id, event_time, produced_at)
        detection = self.random.choices(
            ("DOOR_OPEN", "DOOR_CLOSED", "SHOCK"), weights=(0.42, 0.42, 0.16), k=1
        )[0]
        magnitude = self.random.uniform(0.5, 8.0) if detection == "SHOCK" else None
        if magnitude and magnitude > 6:
            severity = "CRITICAL"
        elif detection == "DOOR_OPEN":
            severity = "WARNING"
        else:
            severity = "INFO"
        value.update({"detection_type": detection, "severity": severity, "magnitude": magnitude})
        return value

    @staticmethod
    def _make_logically_invalid(topic: str, value: dict[str, object]) -> None:
        if topic == LOCATION_TOPIC:
            value["latitude"] = 200.0
        elif topic == TELEMETRY_TOPIC:
            value["value"] = 10_000.0
        else:
            value["device_id"] = ""
