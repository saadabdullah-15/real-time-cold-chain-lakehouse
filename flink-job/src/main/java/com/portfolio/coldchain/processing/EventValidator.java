package com.portfolio.coldchain.processing;

import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.NormalizedEvent;
import java.util.Set;

public final class EventValidator {
    private static final long MAX_EVENT_TIME_DIFFERENCE_MS = 1_000;
    private static final Set<String> EVENT_TYPES =
            Set.of(
                    "TELEMETRY_TEMPERATURE",
                    "TELEMETRY_HUMIDITY",
                    "TELEMETRY_BATTERY",
                    "LOCATION",
                    "DETECTION");

    private EventValidator() {}

    public static ValidationResult validate(NormalizedEvent event) {
        ValidationResult common = validateCommon(event.eventId, event.deviceId, event.sequenceNumber);
        if (!common.valid()) {
            return common;
        }
        if (!EVENT_TYPES.contains(event.eventType)) {
            return ValidationResult.invalid("UNKNOWN_EVENT_TYPE", "Unsupported event type");
        }
        if (event.eventTime <= 0 || event.producedAt <= 0) {
            return ValidationResult.invalid("INVALID_TIMESTAMP", "Timestamps must be positive");
        }
        ValidationResult contract =
                validateKafkaContract(
                        event.kafkaKey, event.deviceId, event.eventTime, event.kafkaTimestamp);
        if (!contract.valid()) {
            return contract;
        }
        return switch (event.eventType) {
            case "TELEMETRY_TEMPERATURE" ->
                    telemetry(
                            event.numericValue, -50, 100, "temperature", event.unit, "CELSIUS");
            case "TELEMETRY_HUMIDITY", "TELEMETRY_BATTERY" ->
                    telemetry(
                            event.numericValue,
                            0,
                            100,
                            event.eventType.toLowerCase(),
                            event.unit,
                            "PERCENT");
            case "LOCATION" -> validateLocation(event);
            case "DETECTION" ->
                    blank(event.detectionType)
                            ? ValidationResult.invalid(
                                    "INVALID_DETECTION", "Detection type is required")
                            : ValidationResult.ok();
            default -> ValidationResult.invalid("UNKNOWN_EVENT_TYPE", "Unsupported event type");
        };
    }

    public static ValidationResult validate(DeviceMetadata metadata) {
        ValidationResult common =
                validateCommon(metadata.eventId, metadata.deviceId, metadata.sequenceNumber);
        if (!common.valid()) {
            return common;
        }
        if (metadata.metadataVersion <= 0) {
            return ValidationResult.invalid(
                    "INVALID_METADATA_VERSION", "Metadata version must be positive");
        }
        if (metadata.eventTime <= 0
                || metadata.producedAt <= 0
                || metadata.effectiveFrom <= 0
                || blank(metadata.shipmentId)
                || blank(metadata.vehicleId)) {
            return ValidationResult.invalid(
                    "INVALID_METADATA", "Metadata identity and effective timestamp are required");
        }
        if (metadata.minTemperatureC >= metadata.maxTemperatureC) {
            return ValidationResult.invalid(
                    "INVALID_TEMPERATURE_RANGE", "Minimum temperature must be below maximum");
        }
        String expectedKey = metadata.deviceId + ":v" + metadata.metadataVersion;
        if (!expectedKey.equals(metadata.kafkaKey)) {
            return ValidationResult.invalid(
                    "KAFKA_KEY_MISMATCH",
                    "Metadata Kafka key must be <device_id>:v<metadata_version>");
        }
        if (Math.abs(metadata.eventTime - metadata.kafkaTimestamp)
                > MAX_EVENT_TIME_DIFFERENCE_MS) {
            return ValidationResult.invalid(
                    "EVENT_TIME_MISMATCH",
                    "Payload event_time differs from Kafka timestamp by more than one second");
        }
        return ValidationResult.ok();
    }

    private static ValidationResult validateKafkaContract(
            String kafkaKey, String deviceId, long eventTime, long kafkaTimestamp) {
        if (!deviceId.equals(kafkaKey)) {
            return ValidationResult.invalid(
                    "KAFKA_KEY_MISMATCH", "Kafka key must equal payload device_id");
        }
        if (Math.abs(eventTime - kafkaTimestamp) > MAX_EVENT_TIME_DIFFERENCE_MS) {
            return ValidationResult.invalid(
                    "EVENT_TIME_MISMATCH",
                    "Payload event_time differs from Kafka timestamp by more than one second");
        }
        return ValidationResult.ok();
    }

    private static ValidationResult telemetry(
            Double value,
            double minimum,
            double maximum,
            String name,
            String actualUnit,
            String expectedUnit) {
        if (!expectedUnit.equals(actualUnit)) {
            return ValidationResult.invalid(
                    "INVALID_UNIT", name + " must use " + expectedUnit);
        }
        return inRange(value, minimum, maximum, name);
    }

    private static ValidationResult validateLocation(NormalizedEvent event) {
        if (!between(event.latitude, -90, 90) || !between(event.longitude, -180, 180)) {
            return ValidationResult.invalid(
                    "INVALID_COORDINATES", "Latitude or longitude is outside its valid range");
        }
        if (!between(event.speedKph, 0, 250) || !between(event.accuracyM, 0, 10_000)) {
            return ValidationResult.invalid(
                    "INVALID_LOCATION_READING", "Speed or GPS accuracy is outside its valid range");
        }
        return ValidationResult.ok();
    }

    private static ValidationResult inRange(
            Double value, double minimum, double maximum, String name) {
        if (!between(value, minimum, maximum)) {
            return ValidationResult.invalid(
                    "INVALID_MEASUREMENT", name + " is outside its valid range");
        }
        return ValidationResult.ok();
    }

    private static boolean between(Double value, double minimum, double maximum) {
        return value != null
                && Double.isFinite(value)
                && value >= minimum
                && value <= maximum;
    }

    private static ValidationResult validateCommon(
            String eventId, String deviceId, long sequenceNumber) {
        if (blank(eventId) || blank(deviceId)) {
            return ValidationResult.invalid(
                    "MISSING_IDENTITY", "Event and device identifiers are required");
        }
        if (sequenceNumber <= 0) {
            return ValidationResult.invalid(
                    "INVALID_SEQUENCE", "Sequence number must be positive");
        }
        return ValidationResult.ok();
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    public record ValidationResult(boolean valid, String code, String message) {
        public static ValidationResult ok() {
            return new ValidationResult(true, null, null);
        }

        public static ValidationResult invalid(String code, String message) {
            return new ValidationResult(false, code, message);
        }
    }
}
