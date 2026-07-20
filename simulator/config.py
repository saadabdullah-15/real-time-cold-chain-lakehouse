from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Profile:
    name: str
    event_count: int
    rate_per_second: float
    event_time_span_seconds: int


PROFILES = {
    "baseline": Profile("baseline", 100_000, 100_000 / 86_400, 86_400),
    "demo": Profile("demo", 100_000, 500.0, 86_400),
    "benchmark": Profile("benchmark", 300_000, 1_000.0, 300),
}


@dataclass(frozen=True)
class RunConfig:
    profile: str = "demo"
    event_count: int = 100_000
    rate_per_second: float = 500.0
    event_time_span_seconds: int = 86_400
    seed: int = 42
    device_count: int = 500
    duplicate_rate: float = 0.01
    invalid_rate: float = 0.005
    out_of_order_rate: float = 0.05
    too_late_rate: float = 0.01
    schema_version: int = 1
    start_time_ms: int = 0

    @classmethod
    def from_profile(cls, name: str, **overrides: object) -> RunConfig:
        if name not in PROFILES:
            raise ValueError(f"Unknown profile {name!r}; choose from {sorted(PROFILES)}")
        profile = PROFILES[name]
        base = cls(
            profile=profile.name,
            event_count=profile.event_count,
            rate_per_second=profile.rate_per_second,
            event_time_span_seconds=profile.event_time_span_seconds,
        )
        allowed = {field.name for field in cls.__dataclass_fields__.values()}
        unknown = set(overrides) - allowed
        if unknown:
            raise ValueError(f"Unknown configuration fields: {sorted(unknown)}")
        values = {key: value for key, value in overrides.items() if value is not None}
        config = replace(base, **values)
        config.validate()
        return config

    def validate(self) -> None:
        if self.event_count <= 0:
            raise ValueError("event_count must be positive")
        if self.rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        if self.device_count <= 0:
            raise ValueError("device_count must be positive")
        if self.schema_version not in (1, 2):
            raise ValueError("schema_version must be 1 or 2")
        for name in (
            "duplicate_rate",
            "invalid_rate",
            "out_of_order_rate",
            "too_late_rate",
        ):
            value = getattr(self, name)
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.out_of_order_rate + self.too_late_rate > 1:
            raise ValueError("out_of_order_rate + too_late_rate cannot exceed 1")
