package com.portfolio.coldchain.kafka;

import com.portfolio.coldchain.model.RawKafkaRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class RawKafkaRecordDeserializationSchema
        implements KafkaRecordDeserializationSchema<RawKafkaRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawKafkaRecord> out) {
        out.collect(
                new RawKafkaRecord(
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.timestamp(),
                        record.key(),
                        record.value(),
                        System.currentTimeMillis()));
    }

    @Override
    public TypeInformation<RawKafkaRecord> getProducedType() {
        return TypeInformation.of(RawKafkaRecord.class);
    }
}
