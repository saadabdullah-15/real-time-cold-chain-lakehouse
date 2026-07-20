#!/usr/bin/env bash
set -euo pipefail

cat > /etc/trino/catalog/coldchain.properties <<EOF
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
iceberg.rest-catalog.warehouse=${POLARIS_CATALOG}
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
iceberg.rest-catalog.oauth2.server-uri=http://polaris:8181/api/catalog/v1/oauth/tokens
iceberg.rest-catalog.oauth2.token-exchange-enabled=false
iceberg.rest-catalog.vended-credentials-enabled=false
iceberg.rest-catalog.nested-namespace-enabled=true
iceberg.rest-catalog.http-headers=Polaris-Realm: ${POLARIS_REALM}
iceberg.file-format=PARQUET
fs.s3.enabled=true
s3.endpoint=${MINIO_ENDPOINT}
s3.region=${MINIO_REGION}
s3.path-style-access=true
s3.aws-access-key=${MINIO_ROOT_USER}
s3.aws-secret-key=${MINIO_ROOT_PASSWORD}
EOF

exec /usr/lib/trino/bin/run-trino
