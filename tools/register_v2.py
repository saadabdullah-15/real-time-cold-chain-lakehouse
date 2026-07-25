from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from pathlib import Path


def registry_request(url: str, body: bytes) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def main() -> None:
    registry = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")
    subject = urllib.parse.quote("coldchain.telemetry.v1-value", safe="")
    schema = (Path(__file__).resolve().parents[1] / "schemas" / "telemetry-v2.avsc").read_text(
        encoding="utf-8"
    )
    body = json.dumps({"schemaType": "AVRO", "schema": schema}).encode()
    compatibility = registry_request(
        f"{registry}/compatibility/subjects/{subject}/versions/latest", body
    )
    if compatibility.get("is_compatible") is not True:
        messages = compatibility.get("messages", [])
        raise RuntimeError(f"telemetry v2 failed FULL_TRANSITIVE compatibility: {messages}")
    print("telemetry v2 passed FULL_TRANSITIVE compatibility")
    result = registry_request(f"{registry}/subjects/{subject}/versions", body)
    print(f"registered telemetry v2 as schema {result['id']}")


if __name__ == "__main__":
    main()
