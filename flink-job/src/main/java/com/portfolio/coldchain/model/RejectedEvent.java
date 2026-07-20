package com.portfolio.coldchain.model;

import java.io.Serializable;
import java.util.Base64;

public class RejectedEvent implements Serializable {
    public String disposition;
    public String errorCode;
    public String errorMessage;
    public String eventId;
    public String deviceId;
    public Long eventTime;
    public String sourceTopic;
    public int sourcePartition;
    public long sourceOffset;
    public long kafkaTimestamp;
    public Integer schemaId;
    public String payloadBase64;
    public Long latenessMs;
    public Long watermarkMs;
    public long rejectedAt;

    public RejectedEvent() {}

    public static RejectedEvent invalid(RawKafkaRecord raw, String code, String message) {
        RejectedEvent event = new RejectedEvent();
        event.disposition = "INVALID";
        event.errorCode = code;
        event.errorMessage = message;
        event.sourceTopic = raw.sourceTopic;
        event.sourcePartition = raw.sourcePartition;
        event.sourceOffset = raw.sourceOffset;
        event.kafkaTimestamp = raw.kafkaTimestamp;
        event.schemaId = raw.schemaId;
        event.payloadBase64 = raw.payload == null ? null : Base64.getEncoder().encodeToString(raw.payload);
        event.rejectedAt = System.currentTimeMillis();
        return event;
    }

    public static RejectedEvent missingMetadata(NormalizedEvent source) {
        RejectedEvent event = fromNormalized(source);
        event.disposition = "INVALID";
        event.errorCode = "MISSING_METADATA";
        event.errorMessage = "No metadata version was effective for this event";
        return event;
    }

    public static RejectedEvent tooLate(EnrichedEvent source, long watermark) {
        RejectedEvent event = new RejectedEvent();
        event.disposition = "TOO_LATE";
        event.errorCode = "EVENT_TIME_TOO_OLD";
        event.errorMessage = "Event exceeded the five-minute allowed-lateness boundary";
        event.eventId = source.eventId;
        event.deviceId = source.deviceId;
        event.eventTime = source.eventTime;
        event.sourceTopic = source.sourceTopic;
        event.sourcePartition = source.sourcePartition;
        event.sourceOffset = source.sourceOffset;
        event.latenessMs = Math.max(0, watermark - source.eventTime);
        event.watermarkMs = watermark;
        event.rejectedAt = System.currentTimeMillis();
        return event;
    }

    private static RejectedEvent fromNormalized(NormalizedEvent source) {
        RejectedEvent event = new RejectedEvent();
        event.eventId = source.eventId;
        event.deviceId = source.deviceId;
        event.eventTime = source.eventTime;
        event.sourceTopic = source.sourceTopic;
        event.sourcePartition = source.sourcePartition;
        event.sourceOffset = source.sourceOffset;
        event.rejectedAt = System.currentTimeMillis();
        return event;
    }
}
