# syntax=docker/dockerfile:1.7
ARG FLINK_VERSION=2.1.3
FROM maven:3.9.11-eclipse-temurin-17 AS build

WORKDIR /build
COPY flink-job/pom.xml ./pom.xml
RUN --mount=type=cache,target=/root/.m2 set -eu; \
    for attempt in 1 2 3; do \
      mvn -B -Dmaven.wagon.http.retryHandler.count=5 dependency:go-offline && exit 0; \
      echo "Maven dependency download attempt ${attempt} failed; retrying" >&2; \
      sleep 5; \
    done; \
    exit 1
COPY flink-job/src ./src
RUN --mount=type=cache,target=/root/.m2 set -eu; \
    for attempt in 1 2 3; do \
      mvn -B -Dmaven.wagon.http.retryHandler.count=5 package -DskipTests \
      && mkdir -p /runtime-libs \
      && mvn -B -Dmaven.wagon.http.retryHandler.count=5 dependency:copy-dependencies \
         -DincludeArtifactIds=iceberg-flink-runtime-2.1,iceberg-aws-bundle,hadoop-client-api,hadoop-client-runtime \
         -DoutputDirectory=/runtime-libs \
      && exit 0; \
      echo "Maven build attempt ${attempt} failed; retrying" >&2; \
      sleep 5; \
    done; \
    exit 1

FROM flink:${FLINK_VERSION}-java17
ARG FLINK_VERSION=2.1.3
COPY --from=build /build/target/coldchain-flink-job.jar /opt/flink/usrlib/coldchain-flink-job.jar
COPY --from=build /runtime-libs/ /opt/flink/lib/
COPY infrastructure/flink/entrypoint.sh /usr/local/bin/coldchain-flink
RUN test -f /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-${FLINK_VERSION}.jar \
    && chmod 0755 /usr/local/bin/coldchain-flink
ENTRYPOINT ["/usr/local/bin/coldchain-flink"]
