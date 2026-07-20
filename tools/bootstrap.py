from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

ROOT = Path(__file__).resolve().parents[1]


def setting(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def request_json(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    form: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any] | list[Any]]:
    request_headers = {"Accept": "application/json", **(headers or {})}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        request_headers["Content-Type"] = "application/json"
    elif form is not None:
        data = urllib.parse.urlencode(form).encode()
        request_headers["Content-Type"] = "application/x-www-form-urlencoded"
    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read()
            return response.status, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as error:
        payload = error.read()
        parsed = json.loads(payload) if payload else {}
        return error.code, parsed


def retry(label: str, operation, attempts: int = 60, delay: float = 2.0):
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return operation()
        except Exception as error:  # readiness crosses several different client libraries
            last_error = error
            time.sleep(delay)
    raise RuntimeError(f"Timed out waiting for {label}: {last_error}") from last_error


def bootstrap_buckets() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=setting("MINIO_ENDPOINT"),
        aws_access_key_id=setting("MINIO_ROOT_USER"),
        aws_secret_access_key=setting("MINIO_ROOT_PASSWORD"),
        region_name=setting("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    retry("MinIO", client.list_buckets)
    existing = {bucket["Name"] for bucket in client.list_buckets()["Buckets"]}
    for bucket in (
        setting("MINIO_WAREHOUSE_BUCKET", "coldchain-warehouse"),
        setting("MINIO_CHECKPOINT_BUCKET", "flink-checkpoints"),
    ):
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)
            print(f"created bucket {bucket}")


def bootstrap_topics() -> None:
    admin = AdminClient({"bootstrap.servers": setting("KAFKA_BOOTSTRAP_SERVERS")})
    retry("Kafka", lambda: admin.list_topics(timeout=10))
    specification = json.loads((ROOT / "config" / "topics.json").read_text(encoding="utf-8"))
    existing = set(admin.list_topics(timeout=10).topics)
    topics = [
        NewTopic(item["name"], item["partitions"], 1)
        for item in specification["topics"]
        if item["name"] not in existing
    ]
    for name, future in admin.create_topics(topics).items() if topics else []:
        future.result()
        print(f"created topic {name}")

    resources = []
    for item in specification["topics"]:
        resources.append(
            ConfigResource(
                ResourceType.TOPIC,
                item["name"],
                set_config={
                    "cleanup.policy": item["cleanupPolicy"],
                    "retention.ms": str(item["retentionMs"]),
                },
            )
        )
    for future in admin.alter_configs(resources).values():
        future.result()


def registry_call(path: str, *, method: str = "GET", body: dict[str, Any] | None = None):
    base_url = setting("SCHEMA_REGISTRY_URL").rstrip("/")
    return request_json(f"{base_url}{path}", method=method, body=body)


def bootstrap_schemas() -> None:
    retry(
        "Schema Registry",
        lambda: (
            registry_call("/subjects")
            if registry_call("/subjects")[0] == 200
            else (_ for _ in ()).throw(RuntimeError("not ready"))
        ),
    )
    status, _ = registry_call(
        "/config", method="PUT", body={"compatibility": "FULL_TRANSITIVE"}
    )
    if status not in (200, 201):
        raise RuntimeError(f"Could not configure Schema Registry compatibility: HTTP {status}")

    schemas = {
        "coldchain.telemetry.v1-value": "telemetry-v1.avsc",
        "coldchain.location.v1-value": "location-v1.avsc",
        "coldchain.detection.v1-value": "detection-v1.avsc",
        "coldchain.device-metadata.v1-value": "device-metadata-v1.avsc",
    }
    for subject, filename in schemas.items():
        schema = (ROOT / "schemas" / filename).read_text(encoding="utf-8")
        status, result = registry_call(
            f"/subjects/{urllib.parse.quote(subject, safe='')}/versions",
            method="POST",
            body={"schemaType": "AVRO", "schema": schema},
        )
        if status not in (200, 201):
            raise RuntimeError(f"Could not register {subject}: HTTP {status}: {result}")
        print(f"registered {subject} as schema {result.get('id')}")

    v2 = (ROOT / "schemas" / "telemetry-v2.avsc").read_text(encoding="utf-8")
    status, result = registry_call(
        "/compatibility/subjects/coldchain.telemetry.v1-value/versions/latest",
        method="POST",
        body={"schemaType": "AVRO", "schema": v2},
    )
    if status != 200 or not result.get("is_compatible"):
        raise RuntimeError(f"Telemetry v2 is not FULL_TRANSITIVE compatible: {result}")
    print("verified telemetry v2 FULL_TRANSITIVE compatibility")


def bootstrap_polaris_catalog() -> None:
    base = setting("POLARIS_URL").rstrip("/")
    realm = setting("POLARIS_REALM", "POLARIS")

    def token_request() -> str:
        status, response = request_json(
            f"{base}/api/catalog/v1/oauth/tokens",
            method="POST",
            form={
                "grant_type": "client_credentials",
                "client_id": setting("POLARIS_CLIENT_ID"),
                "client_secret": setting("POLARIS_CLIENT_SECRET"),
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            headers={"Polaris-Realm": realm},
        )
        if status != 200 or "access_token" not in response:
            raise RuntimeError(f"Polaris token endpoint returned HTTP {status}: {response}")
        return str(response["access_token"])

    token = retry("Polaris", token_request)
    headers = {"Authorization": f"Bearer {token}", "Polaris-Realm": realm}
    status, response = request_json(f"{base}/api/management/v1/catalogs", headers=headers)
    if status != 200:
        raise RuntimeError(f"Could not list Polaris catalogs: HTTP {status}: {response}")
    catalog_name = setting("POLARIS_CATALOG", "coldchain")
    existing = {item["name"] for item in response.get("catalogs", [])}
    if catalog_name in existing:
        print(f"catalog {catalog_name} already exists")
        return

    bucket = setting("MINIO_WAREHOUSE_BUCKET", "coldchain-warehouse")
    location = f"s3://{bucket}/warehouse"
    endpoint = setting("MINIO_ENDPOINT")
    body = {
        "catalog": {
            "name": catalog_name,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {"default-base-location": location},
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [location, f"s3://{bucket}/"],
                "endpoint": endpoint,
                "endpointInternal": endpoint,
                "pathStyleAccess": True,
                "stsUnavailable": True,
                "region": setting("MINIO_REGION", "us-east-1"),
            },
        }
    }
    status, response = request_json(
        f"{base}/api/management/v1/catalogs", method="POST", body=body, headers=headers
    )
    if status not in (200, 201):
        raise RuntimeError(f"Could not create Polaris catalog: HTTP {status}: {response}")
    print(f"created Polaris catalog {catalog_name}")


def main() -> None:
    bootstrap_buckets()
    bootstrap_topics()
    bootstrap_schemas()
    bootstrap_polaris_catalog()
    print("cold-chain platform bootstrap complete")


if __name__ == "__main__":
    main()
