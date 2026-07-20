package com.portfolio.coldchain.processing;

import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.NormalizedEvent;
import com.portfolio.coldchain.model.RawKafkaRecord;
import com.portfolio.coldchain.model.RejectedEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DecodeAndValidateFunction extends ProcessFunction<RawKafkaRecord, NormalizedEvent> {
    private final String schemaRegistryUrl;
    private transient KafkaAvroDeserializer deserializer;
    private transient Counter invalidCounter;

    public DecodeAndValidateFunction(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(OpenContext openContext) {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("schema.registry.url", schemaRegistryUrl);
        configuration.put("specific.avro.reader", false);
        configuration.put("logical.type.conversion.enabled", false);
        deserializer = new KafkaAvroDeserializer();
        deserializer.configure(configuration, false);
        invalidCounter = getRuntimeContext().getMetricGroup().counter("invalid_records");
    }

    @Override
    public void processElement(RawKafkaRecord raw, Context context, Collector<NormalizedEvent> out) {
        try {
            Object decoded = deserializer.deserialize(raw.sourceTopic, raw.payload);
            if (!(decoded instanceof GenericRecord record)) {
                reject(raw, context, "NOT_AVRO_RECORD", "Payload is not an Avro record");
                return;
            }
            if ("coldchain.device-metadata.v1".equals(raw.sourceTopic)) {
                DeviceMetadata metadata = AvroEventMapper.metadata(record, raw);
                EventValidator.ValidationResult result = EventValidator.validate(metadata);
                if (result.valid()) {
                    context.output(OutputTags.METADATA, metadata);
                } else {
                    reject(raw, context, result.code(), result.message());
                }
                return;
            }
            NormalizedEvent event = AvroEventMapper.sensor(record, raw);
            EventValidator.ValidationResult result = EventValidator.validate(event);
            if (result.valid()) {
                out.collect(event);
            } else {
                RejectedEvent rejected = RejectedEvent.invalid(raw, result.code(), result.message());
                rejected.eventId = event.eventId;
                rejected.deviceId = event.deviceId;
                rejected.eventTime = event.eventTime;
                invalidCounter.inc();
                context.output(OutputTags.REJECTED, rejected);
            }
        } catch (Exception error) {
            reject(raw, context, "AVRO_DECODE_ERROR", concise(error));
        }
    }

    private void reject(RawKafkaRecord raw, Context context, String code, String message) {
        invalidCounter.inc();
        context.output(OutputTags.REJECTED, RejectedEvent.invalid(raw, code, message));
    }

    private static String concise(Exception error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) {
            message = error.getClass().getSimpleName();
        }
        return message.length() <= 500 ? message : message.substring(0, 500);
    }
}
