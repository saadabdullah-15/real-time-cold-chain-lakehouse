package com.portfolio.coldchain.processing;

import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.NormalizedEvent;
import com.portfolio.coldchain.model.RawKafkaRecord;
import java.time.Instant;
import org.apache.avro.generic.GenericRecord;

public final class AvroEventMapper {
    private AvroEventMapper() {}

    public static NormalizedEvent sensor(GenericRecord record, RawKafkaRecord raw) {
        NormalizedEvent target = new NormalizedEvent();
        copyCommon(record, raw, target);
        switch (raw.sourceTopic) {
            case "coldchain.telemetry.v1" -> mapTelemetry(record, target);
            case "coldchain.location.v1" -> mapLocation(record, target);
            case "coldchain.detection.v1" -> mapDetection(record, target);
            default -> throw new IllegalArgumentException("Unsupported source topic " + raw.sourceTopic);
        }
        return target;
    }

    public static DeviceMetadata metadata(GenericRecord record, RawKafkaRecord raw) {
        DeviceMetadata target = new DeviceMetadata();
        target.eventId = text(record, "event_id");
        target.deviceId = text(record, "device_id");
        target.eventTime = timestamp(record, "event_time");
        target.producedAt = timestamp(record, "produced_at");
        target.sequenceNumber = number(record, "sequence_number").longValue();
        target.metadataVersion = number(record, "metadata_version").intValue();
        target.effectiveFrom = timestamp(record, "effective_from");
        target.shipmentId = text(record, "shipment_id");
        target.vehicleId = text(record, "vehicle_id");
        target.origin = text(record, "origin");
        target.destination = text(record, "destination");
        target.cargoType = text(record, "cargo_type");
        target.minTemperatureC = number(record, "min_temperature_c").doubleValue();
        target.maxTemperatureC = number(record, "max_temperature_c").doubleValue();
        target.status = text(record, "status");
        target.sourceTopic = raw.sourceTopic;
        target.sourcePartition = raw.sourcePartition;
        target.sourceOffset = raw.sourceOffset;
        target.ingestedAt = raw.ingestedAt;
        return target;
    }

    private static void copyCommon(
            GenericRecord source, RawKafkaRecord raw, NormalizedEvent target) {
        target.eventId = text(source, "event_id");
        target.deviceId = text(source, "device_id");
        target.eventTime = timestamp(source, "event_time");
        target.producedAt = timestamp(source, "produced_at");
        target.sequenceNumber = number(source, "sequence_number").longValue();
        target.sourceTopic = raw.sourceTopic;
        target.sourcePartition = raw.sourcePartition;
        target.sourceOffset = raw.sourceOffset;
        target.ingestedAt = raw.ingestedAt;
    }

    private static void mapTelemetry(GenericRecord source, NormalizedEvent target) {
        String measurement = text(source, "measurement_type");
        target.eventType = "TELEMETRY_" + measurement;
        target.numericValue = number(source, "value").doubleValue();
        target.unit = text(source, "unit");
        if (source.getSchema().getField("signal_strength_dbm") != null) {
            Object signalStrength = source.get("signal_strength_dbm");
            if (signalStrength instanceof Number value) {
                target.signalStrengthDbm = value.intValue();
            }
        }
    }

    private static void mapLocation(GenericRecord source, NormalizedEvent target) {
        target.eventType = "LOCATION";
        target.latitude = number(source, "latitude").doubleValue();
        target.longitude = number(source, "longitude").doubleValue();
        target.speedKph = number(source, "speed_kph").doubleValue();
        target.accuracyM = number(source, "accuracy_m").doubleValue();
    }

    private static void mapDetection(GenericRecord source, NormalizedEvent target) {
        target.eventType = "DETECTION";
        target.detectionType = text(source, "detection_type");
        target.severity = text(source, "severity");
        Object magnitude = source.get("magnitude");
        if (magnitude instanceof Number value) {
            target.magnitude = value.doubleValue();
        }
    }

    private static String text(GenericRecord record, String field) {
        Object value = record.get(field);
        return value == null ? null : value.toString();
    }

    private static Number number(GenericRecord record, String field) {
        Object value = record.get(field);
        if (value instanceof Number number) {
            return number;
        }
        throw new IllegalArgumentException(field + " is not numeric");
    }

    private static long timestamp(GenericRecord record, String field) {
        Object value = record.get(field);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof Instant instant) {
            return instant.toEpochMilli();
        }
        throw new IllegalArgumentException(field + " is not a supported timestamp");
    }
}
