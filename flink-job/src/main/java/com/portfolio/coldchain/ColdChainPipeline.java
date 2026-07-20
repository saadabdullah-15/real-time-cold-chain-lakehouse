package com.portfolio.coldchain;

import com.portfolio.coldchain.kafka.RawKafkaRecordDeserializationSchema;
import com.portfolio.coldchain.model.BreachAlert;
import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.EnrichedEvent;
import com.portfolio.coldchain.model.NormalizedEvent;
import com.portfolio.coldchain.model.RawKafkaRecord;
import com.portfolio.coldchain.model.RejectedEvent;
import com.portfolio.coldchain.model.ShipmentHourlyMetric;
import com.portfolio.coldchain.processing.DecodeAndValidateFunction;
import com.portfolio.coldchain.processing.DeduplicateFunction;
import com.portfolio.coldchain.processing.HourlyAggregateFunction;
import com.portfolio.coldchain.processing.LateEventRouter;
import com.portfolio.coldchain.processing.OutputTags;
import com.portfolio.coldchain.processing.TemporalEnrichmentFunction;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class ColdChainPipeline {
    private static final List<String> INPUT_TOPICS =
            List.of(
                    "coldchain.telemetry.v1",
                    "coldchain.location.v1",
                    "coldchain.detection.v1",
                    "coldchain.device-metadata.v1");

    private ColdChainPipeline() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> env = System.getenv();
        String kafkaBootstrap = env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String schemaRegistry =
                env.getOrDefault("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        execution.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        execution.setParallelism(integer(env, "FLINK_PIPELINE_PARALLELISM", 2));
        execution.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoints = execution.getCheckpointConfig();
        checkpoints.setCheckpointTimeout(120_000);
        checkpoints.setMinPauseBetweenCheckpoints(10_000);
        checkpoints.setMaxConcurrentCheckpoints(1);

        KafkaSource<RawKafkaRecord> source =
                KafkaSource.<RawKafkaRecord>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setGroupId("coldchain-flink-pipeline-v1")
                        .setTopics(INPUT_TOPICS)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializer(new RawKafkaRecordDeserializationSchema())
                        .setProperty("isolation.level", "read_committed")
                        .build();

        WatermarkStrategy<RawKafkaRecord> sourceWatermarks =
                WatermarkStrategy.<RawKafkaRecord>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withIdleness(Duration.ofSeconds(30))
                        .withTimestampAssigner((record, timestamp) -> record.kafkaTimestamp);
        DataStream<RawKafkaRecord> raw =
                execution.fromSource(source, sourceWatermarks, "kafka-raw-source")
                        .uid("kafka-raw-source");

        SingleOutputStreamOperator<NormalizedEvent> decoded =
                raw.process(new DecodeAndValidateFunction(schemaRegistry))
                        .name("decode-and-validate")
                        .uid("decode-and-validate");
        DataStream<DeviceMetadata> metadata = decoded.getSideOutput(OutputTags.METADATA);
        DataStream<RejectedEvent> decodingRejected = decoded.getSideOutput(OutputTags.REJECTED);

        SingleOutputStreamOperator<NormalizedEvent> deduplicated =
                decoded.keyBy(event -> event.eventId)
                        .process(new DeduplicateFunction())
                        .name("deduplicate-event-id")
                        .uid("deduplicate-event-id");

        SingleOutputStreamOperator<EnrichedEvent> enriched =
                deduplicated
                        .keyBy(event -> event.deviceId)
                        .connect(metadata.keyBy(event -> event.deviceId))
                        .process(new TemporalEnrichmentFunction())
                        .name("event-time-metadata-enrichment")
                        .uid("event-time-metadata-enrichment");
        DataStream<RejectedEvent> missingMetadata =
                enriched.getSideOutput(OutputTags.MISSING_METADATA);

        SingleOutputStreamOperator<EnrichedEvent> onTime =
                enriched.keyBy(event -> event.deviceId)
                        .process(new LateEventRouter())
                        .name("late-event-router")
                        .uid("late-event-router");
        DataStream<RejectedEvent> late = onTime.getSideOutput(OutputTags.LATE);
        DataStream<RejectedEvent> allRejected = decodingRejected.union(missingMetadata, late);

        DataStream<ShipmentHourlyMetric> hourly =
                onTime.keyBy(event -> event.shipmentId)
                        .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                        .allowedLateness(Duration.ofMinutes(5))
                        .aggregate(
                                new HourlyAggregateFunction(),
                                new HourlyAggregateFunction.WindowResult())
                        .name("shipment-hourly-metrics")
                        .uid("shipment-hourly-metrics");

        DataStream<BreachAlert> breaches =
                onTime.filter(event -> event.breach)
                        .map(BreachAlert::from)
                        .returns(TypeInformation.of(BreachAlert.class))
                        .name("temperature-breach-alerts")
                        .uid("temperature-breach-alerts");

        StreamTableEnvironment tables = StreamTableEnvironment.create(execution);
        registerViews(tables, raw, metadata, onTime, allRejected, hourly, breaches);
        createCatalog(tables, env);
        boolean schemaV2 = integer(env, "PIPELINE_SCHEMA_VERSION", 1) >= 2;
        createLakehouseTables(tables, schemaV2);
        createKafkaOutputTables(tables, kafkaBootstrap);
        StatementSet inserts = createInserts(tables, schemaV2);
        inserts.execute();
    }

    private static void registerViews(
            StreamTableEnvironment tables,
            DataStream<RawKafkaRecord> raw,
            DataStream<DeviceMetadata> metadata,
            DataStream<EnrichedEvent> enriched,
            DataStream<RejectedEvent> rejected,
            DataStream<ShipmentHourlyMetric> hourly,
            DataStream<BreachAlert> breaches) {
        tables.createTemporaryView("raw_events_view", raw);
        tables.createTemporaryView("metadata_history_view", metadata);
        tables.createTemporaryView("enriched_events_view", enriched);
        tables.createTemporaryView("rejected_events_view", rejected);
        tables.createTemporaryView("hourly_metrics_view", hourly);
        tables.createTemporaryView("breach_alerts_view", breaches);
    }

    private static void createCatalog(StreamTableEnvironment tables, Map<String, String> env) {
        String ddl =
                """
                CREATE CATALOG coldchain WITH (
                  'type'='iceberg',
                  'catalog-type'='rest',
                  'uri'='%s',
                  'warehouse'='%s',
                  'credential'='%s:%s',
                  'scope'='PRINCIPAL_ROLE:ALL',
                  'header.Polaris-Realm'='%s',
                  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
                  's3.endpoint'='%s',
                  's3.path-style-access'='true',
                  's3.access-key-id'='%s',
                  's3.secret-access-key'='%s',
                  's3.region'='%s'
                )
                """
                        .formatted(
                                env.getOrDefault(
                                        "POLARIS_CATALOG_URI", "http://polaris:8181/api/catalog"),
                                env.getOrDefault("POLARIS_CATALOG", "coldchain"),
                                env.getOrDefault("POLARIS_CLIENT_ID", "root"),
                                env.getOrDefault("POLARIS_CLIENT_SECRET", "secret"),
                                env.getOrDefault("POLARIS_REALM", "POLARIS"),
                                env.getOrDefault("MINIO_ENDPOINT", "http://minio:9000"),
                                env.getOrDefault("MINIO_ROOT_USER", "coldchain_admin"),
                                env.getOrDefault("MINIO_ROOT_PASSWORD", "password"),
                                env.getOrDefault("MINIO_REGION", "us-east-1"));
        tables.executeSql(ddl);
    }

    private static void createKafkaOutputTables(
            StreamTableEnvironment tables, String kafkaBootstrap) {
        tables.executeSql(kafkaOutputDdl("dlq_sink", "coldchain.dlq.v1", kafkaBootstrap));
        tables.executeSql(kafkaOutputDdl("late_sink", "coldchain.late.v1", kafkaBootstrap));
        tables.executeSql(
                """
                CREATE TEMPORARY TABLE breach_sink (
                  event_id STRING,
                  device_id STRING,
                  shipment_id STRING,
                  event_time BIGINT,
                  measured_temperature_c DOUBLE,
                  alert_type STRING,
                  emitted_at BIGINT
                ) WITH (
                  'connector'='kafka',
                  'topic'='coldchain.breach-alerts.v1',
                  'properties.bootstrap.servers'='%s',
                  'format'='json',
                  'sink.delivery-guarantee'='exactly-once',
                  'sink.transactional-id-prefix'='coldchain-breach-'
                )
                """
                        .formatted(kafkaBootstrap));
    }

    private static String kafkaOutputDdl(String table, String topic, String kafkaBootstrap) {
        return """
               CREATE TEMPORARY TABLE %s (
                 disposition STRING,
                 error_code STRING,
                 error_message STRING,
                 event_id STRING,
                 device_id STRING,
                 event_time BIGINT,
                 source_topic STRING,
                 source_partition INT,
                 source_offset BIGINT,
                 rejected_at BIGINT
               ) WITH (
                 'connector'='kafka',
                 'topic'='%s',
                 'properties.bootstrap.servers'='%s',
                 'format'='json',
                 'sink.delivery-guarantee'='exactly-once',
                 'sink.transactional-id-prefix'='coldchain-%s-'
               )
               """
                .formatted(table, topic, kafkaBootstrap, table.replace('_', '-'));
    }

    private static void createLakehouseTables(StreamTableEnvironment tables, boolean schemaV2) {
        tables.executeSql("CREATE DATABASE IF NOT EXISTS coldchain.bronze");
        tables.executeSql("CREATE DATABASE IF NOT EXISTS coldchain.silver");
        tables.executeSql("CREATE DATABASE IF NOT EXISTS coldchain.gold");
        tables.executeSql(
                """
                CREATE TABLE IF NOT EXISTS coldchain.bronze.raw_events (
                  source_topic STRING,
                  source_partition INT,
                  source_offset BIGINT,
                  kafka_timestamp TIMESTAMP_LTZ(3),
                  kafka_key BYTES,
                  schema_id INT,
                  payload BYTES,
                  ingested_at TIMESTAMP_LTZ(3),
                  kafka_day DATE
                ) PARTITIONED BY (kafka_day) WITH (
                  'format-version'='2',
                  'write.format.default'='parquet',
                  'write.parquet.compression-codec'='zstd'
                )
                """);
        tables.executeSql(
                """
                CREATE TABLE IF NOT EXISTS coldchain.silver.device_metadata_history (
                  event_id STRING,
                  device_id STRING,
                  metadata_version INT,
                  effective_from TIMESTAMP_LTZ(3),
                  shipment_id STRING,
                  vehicle_id STRING,
                  origin STRING,
                  destination STRING,
                  cargo_type STRING,
                  min_temperature_c DOUBLE,
                  max_temperature_c DOUBLE,
                  status STRING,
                  source_topic STRING,
                  source_partition INT,
                  source_offset BIGINT,
                  ingested_at TIMESTAMP_LTZ(3),
                  PRIMARY KEY (device_id, metadata_version) NOT ENFORCED
                ) WITH (
                  'format-version'='2',
                  'write.format.default'='parquet',
                  'write.parquet.compression-codec'='zstd',
                  'write.upsert.enabled'='true'
                )
                """);
        tables.executeSql(
                """
                CREATE TABLE IF NOT EXISTS coldchain.silver.enriched_events (
                  event_id STRING,
                  device_id STRING,
                  shipment_id STRING,
                  vehicle_id STRING,
                  origin STRING,
                  destination STRING,
                  cargo_type STRING,
                  metadata_version INT,
                  event_time TIMESTAMP_LTZ(3),
                  produced_at TIMESTAMP_LTZ(3),
                  sequence_number BIGINT,
                  event_type STRING,
                  numeric_value DOUBLE,
                  unit STRING,
                  latitude DOUBLE,
                  longitude DOUBLE,
                  speed_kph DOUBLE,
                  accuracy_m DOUBLE,
                  detection_type STRING,
                  severity STRING,
                  magnitude DOUBLE,
                  breach BOOLEAN,
                  source_topic STRING,
                  source_partition INT,
                  source_offset BIGINT,
                  ingested_at TIMESTAMP_LTZ(3),
                  processed_at TIMESTAMP_LTZ(3),
                  event_day DATE,
                  PRIMARY KEY (event_id, event_day) NOT ENFORCED
                ) PARTITIONED BY (event_day) WITH (
                  'format-version'='2',
                  'write.format.default'='parquet',
                  'write.parquet.compression-codec'='zstd',
                  'write.upsert.enabled'='true'
                )
                """);
        if (schemaV2) {
            tables.executeSql(
                    "ALTER TABLE coldchain.silver.enriched_events "
                            + "ADD COLUMN IF NOT EXISTS signal_strength_dbm INT");
        }
        tables.executeSql(
                """
                CREATE TABLE IF NOT EXISTS coldchain.silver.rejected_events (
                  disposition STRING,
                  error_code STRING,
                  error_message STRING,
                  event_id STRING,
                  device_id STRING,
                  event_time TIMESTAMP_LTZ(3),
                  source_topic STRING,
                  source_partition INT,
                  source_offset BIGINT,
                  kafka_timestamp TIMESTAMP_LTZ(3),
                  schema_id INT,
                  payload_base64 STRING,
                  lateness_ms BIGINT,
                  watermark_ms BIGINT,
                  rejected_at TIMESTAMP_LTZ(3),
                  rejected_day DATE
                ) PARTITIONED BY (rejected_day) WITH (
                  'format-version'='2',
                  'write.format.default'='parquet',
                  'write.parquet.compression-codec'='zstd'
                )
                """);
        tables.executeSql(
                """
                CREATE TABLE IF NOT EXISTS coldchain.gold.shipment_hourly_metrics (
                  shipment_id STRING,
                  window_start TIMESTAMP_LTZ(3),
                  window_end TIMESTAMP_LTZ(3),
                  temperature_min DOUBLE,
                  temperature_max DOUBLE,
                  temperature_avg DOUBLE,
                  humidity_avg DOUBLE,
                  detection_count BIGINT,
                  breach_count BIGINT,
                  event_count BIGINT,
                  latest_latitude DOUBLE,
                  latest_longitude DOUBLE,
                  last_event_time TIMESTAMP_LTZ(3),
                  updated_at TIMESTAMP_LTZ(3),
                  window_day DATE,
                  PRIMARY KEY (shipment_id, window_start, window_day) NOT ENFORCED
                ) PARTITIONED BY (window_day) WITH (
                  'format-version'='2',
                  'write.format.default'='parquet',
                  'write.parquet.compression-codec'='zstd',
                  'write.upsert.enabled'='true'
                )
                """);
    }

    private static StatementSet createInserts(StreamTableEnvironment tables, boolean schemaV2) {
        StatementSet inserts = tables.createStatementSet();
        inserts.addInsertSql(
                """
                INSERT INTO coldchain.bronze.raw_events
                SELECT sourceTopic, sourcePartition, sourceOffset,
                       TO_TIMESTAMP_LTZ(kafkaTimestamp, 3), kafkaKey, schemaId, payload,
                       TO_TIMESTAMP_LTZ(ingestedAt, 3),
                       CAST(TO_TIMESTAMP_LTZ(kafkaTimestamp, 3) AS DATE)
                FROM raw_events_view
                """);
        inserts.addInsertSql(
                """
                INSERT INTO coldchain.silver.device_metadata_history
                SELECT eventId, deviceId, metadataVersion,
                       TO_TIMESTAMP_LTZ(effectiveFrom, 3), shipmentId, vehicleId,
                       origin, destination, cargoType, minTemperatureC, maxTemperatureC,
                       status, sourceTopic, sourcePartition, sourceOffset,
                       TO_TIMESTAMP_LTZ(ingestedAt, 3)
                FROM metadata_history_view
                """);
        String enrichedColumns =
                "event_id, device_id, shipment_id, vehicle_id, origin, destination, cargo_type, "
                        + "metadata_version, event_time, produced_at, sequence_number, event_type, "
                        + "numeric_value, unit, latitude, longitude, speed_kph, accuracy_m, "
                        + "detection_type, severity, magnitude, breach, source_topic, "
                        + "source_partition, source_offset, ingested_at, processed_at, event_day";
        String enrichedSelect =
                """
                SELECT eventId, deviceId, shipmentId, vehicleId, origin, destination, cargoType,
                       metadataVersion, TO_TIMESTAMP_LTZ(eventTime, 3),
                       TO_TIMESTAMP_LTZ(producedAt, 3), sequenceNumber, eventType,
                       numericValue, unit, latitude, longitude, speedKph, accuracyM,
                       detectionType, severity, magnitude, breach,
                       sourceTopic, sourcePartition, sourceOffset,
                       TO_TIMESTAMP_LTZ(ingestedAt, 3), TO_TIMESTAMP_LTZ(processedAt, 3),
                       CAST(TO_TIMESTAMP_LTZ(eventTime, 3) AS DATE)
                FROM enriched_events_view
                """;
        if (schemaV2) {
            enrichedColumns += ", signal_strength_dbm";
            enrichedSelect =
                    enrichedSelect.replace(
                            "FROM enriched_events_view", ", signalStrengthDbm FROM enriched_events_view");
        }
        inserts.addInsertSql(
                "INSERT INTO coldchain.silver.enriched_events ("
                        + enrichedColumns
                        + ") "
                        + enrichedSelect);
        inserts.addInsertSql(
                """
                INSERT INTO coldchain.silver.rejected_events
                SELECT disposition, errorCode, errorMessage, eventId, deviceId,
                       CASE WHEN eventTime IS NULL THEN NULL ELSE TO_TIMESTAMP_LTZ(eventTime, 3) END,
                       sourceTopic, sourcePartition, sourceOffset,
                       TO_TIMESTAMP_LTZ(kafkaTimestamp, 3), schemaId, payloadBase64,
                       latenessMs, watermarkMs, TO_TIMESTAMP_LTZ(rejectedAt, 3),
                       CAST(TO_TIMESTAMP_LTZ(rejectedAt, 3) AS DATE)
                FROM rejected_events_view
                """);
        inserts.addInsertSql(
                """
                INSERT INTO coldchain.gold.shipment_hourly_metrics
                SELECT shipmentId, TO_TIMESTAMP_LTZ(windowStart, 3),
                       TO_TIMESTAMP_LTZ(windowEnd, 3), temperatureMin, temperatureMax,
                       temperatureAvg, humidityAvg, detectionCount, breachCount, eventCount,
                       latestLatitude, latestLongitude,
                       CASE WHEN lastEventTime IS NULL THEN NULL ELSE TO_TIMESTAMP_LTZ(lastEventTime, 3) END,
                       TO_TIMESTAMP_LTZ(updatedAt, 3),
                       CAST(TO_TIMESTAMP_LTZ(windowStart, 3) AS DATE)
                FROM hourly_metrics_view
                """);
        inserts.addInsertSql(
                """
                INSERT INTO dlq_sink
                SELECT disposition, errorCode, errorMessage, eventId, deviceId, eventTime,
                       sourceTopic, sourcePartition, sourceOffset, rejectedAt
                FROM rejected_events_view WHERE disposition = 'INVALID'
                """);
        inserts.addInsertSql(
                """
                INSERT INTO late_sink
                SELECT disposition, errorCode, errorMessage, eventId, deviceId, eventTime,
                       sourceTopic, sourcePartition, sourceOffset, rejectedAt
                FROM rejected_events_view WHERE disposition = 'TOO_LATE'
                """);
        inserts.addInsertSql(
                """
                INSERT INTO breach_sink
                SELECT eventId, deviceId, shipmentId, eventTime, measuredTemperatureC,
                       alertType, emittedAt FROM breach_alerts_view
                """);
        return inserts;
    }

    private static int integer(Map<String, String> env, String name, int defaultValue) {
        return Integer.parseInt(env.getOrDefault(name, Integer.toString(defaultValue)));
    }
}
