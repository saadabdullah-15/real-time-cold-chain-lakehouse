from __future__ import annotations

import argparse
import secrets
from datetime import UTC, datetime
from pathlib import Path

from simulator.config import PROFILES, RunConfig
from simulator.generator import ColdChainGenerator
from simulator.models import RunManifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate deterministic cold-chain Kafka events")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="demo")
    parser.add_argument("--run-id")
    parser.add_argument("--events", type=int)
    parser.add_argument("--rate", type=float)
    parser.add_argument("--event-time-span-seconds", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--devices", type=int, default=500)
    parser.add_argument("--duplicate-rate", type=float, default=0.01)
    parser.add_argument("--invalid-rate", type=float, default=0.005)
    parser.add_argument("--out-of-order-rate", type=float, default=0.05)
    parser.add_argument("--too-late-rate", type=float, default=0.01)
    parser.add_argument("--schema-version", type=int, choices=(1, 2), default=1)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--dry-run", action="store_true", help="Generate and count without Kafka")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_id = args.run_id or default_run_id()
    start_time_ms = int(datetime.now(UTC).timestamp() * 1000)
    config = RunConfig.from_profile(
        args.profile,
        run_id=run_id,
        event_count=args.events,
        rate_per_second=args.rate,
        event_time_span_seconds=args.event_time_span_seconds,
        seed=args.seed,
        device_count=args.devices,
        duplicate_rate=args.duplicate_rate,
        invalid_rate=args.invalid_rate,
        out_of_order_rate=args.out_of_order_rate,
        too_late_rate=args.too_late_rate,
        schema_version=args.schema_version,
        start_time_ms=start_time_ms,
    )
    generator = ColdChainGenerator(config)
    manifest = RunManifest(
        run_id=config.run_id,
        profile=config.profile,
        seed=config.seed,
        configured_sensor_events=config.event_count,
        device_count=config.device_count,
        schema_version=config.schema_version,
        start_time_ms=generator.start_time_ms,
    )
    started = datetime.now(UTC)
    if args.dry_run:
        for record in generator.records():
            manifest.observe(record)
        manifest.complete_dry_run((datetime.now(UTC) - started).total_seconds())
    else:
        from simulator.producer import KafkaPublisher

        KafkaPublisher(config.schema_version, config.rate_per_second).publish(
            generator.records(), manifest
        )
    manifest_path = args.manifest or Path("data") / "runs" / run_id / "manifest.json"
    manifest.write_atomic(manifest_path)
    print(f"Run ID: {run_id}")
    print(f"Delivered {manifest.delivered_total:,} Kafka records")
    print(f"Manifest: {manifest_path.resolve()}")
    return 0


def default_run_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"run-{timestamp}-{secrets.token_hex(4)}"


if __name__ == "__main__":
    raise SystemExit(main())
