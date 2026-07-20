package com.portfolio.coldchain.model;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class RawKafkaRecord implements Serializable {
    public String sourceTopic;
    public int sourcePartition;
    public long sourceOffset;
    public long kafkaTimestamp;
    public byte[] kafkaKey;
    public Integer schemaId;
    public byte[] payload;
    public long ingestedAt;

    public RawKafkaRecord() {}

    public RawKafkaRecord(
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            long kafkaTimestamp,
            byte[] kafkaKey,
            byte[] payload,
            long ingestedAt) {
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.kafkaTimestamp = kafkaTimestamp;
        this.kafkaKey = kafkaKey;
        this.schemaId = readSchemaId(payload);
        this.payload = payload;
        this.ingestedAt = ingestedAt;
    }

    public static Integer readSchemaId(byte[] bytes) {
        if (bytes == null || bytes.length < 5 || bytes[0] != 0) {
            return null;
        }
        return ByteBuffer.wrap(bytes, 1, 4).getInt();
    }
}
