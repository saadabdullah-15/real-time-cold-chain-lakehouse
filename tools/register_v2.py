from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from pathlib import Path


def main() -> None:
    registry = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")
    subject = urllib.parse.quote("coldchain.telemetry.v1-value", safe="")
    schema = (Path(__file__).resolve().parents[1] / "schemas" / "telemetry-v2.avsc").read_text(
        encoding="utf-8"
    )
    body = json.dumps({"schemaType": "AVRO", "schema": schema}).encode()
    request = urllib.request.Request(
        f"{registry}/subjects/{subject}/versions",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.loads(response.read())
    print(f"registered telemetry v2 as schema {result['id']}")


if __name__ == "__main__":
    main()
