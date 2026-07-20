# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.24.8
FROM golang:${GO_VERSION}-bookworm AS build

ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go install github.com/minio/minio@${MINIO_TAG}

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 minio \
    && useradd --system --uid 10001 --gid minio --home /data minio \
    && mkdir -p /data \
    && chown minio:minio /data
COPY --from=build /go/bin/minio /usr/local/bin/minio
USER minio
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
