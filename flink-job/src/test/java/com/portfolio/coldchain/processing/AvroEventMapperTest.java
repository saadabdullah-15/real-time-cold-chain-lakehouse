package com.portfolio.coldchain.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.coldchain.model.NormalizedEvent;
import com.portfolio.coldchain.model.RawKafkaRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class AvroEventMapperTest {
    @Test
    void telemetryV1DoesNotRequireTheV2SignalStrengthField() {
        NormalizedEvent event = AvroEventMapper.sensor(telemetry(false), raw());

        assertThat(event.eventType).isEqualTo("TELEMETRY_TEMPERATURE");
        assertThat(event.signalStrengthDbm).isNull();
    }

    @Test
    void telemetryV2MapsSignalStrengthWhenPresent() {
        NormalizedEvent event = AvroEventMapper.sensor(telemetry(true), raw());

        assertThat(event.signalStrengthDbm).isEqualTo(-78);
    }

    private static GenericRecord telemetry(boolean versionTwo) {
        String signalField =
                versionTwo
                        ? ",{\"name\":\"signal_strength_dbm\",\"type\":[\"null\",\"int\"],\"default\":null}"
                        : "";
        Schema schema =
                new Schema.Parser()
                        .parse(
                                """
                                {"type":"record","name":"Telemetry","fields":[
                                  {"name":"event_id","type":"string"},
                                  {"name":"device_id","type":"string"},
                                  {"name":"event_time","type":"long"},
                                  {"name":"produced_at","type":"long"},
                                  {"name":"sequence_number","type":"long"},
                                  {"name":"measurement_type","type":"string"},
                                  {"name":"value","type":"double"},
                                  {"name":"unit","type":"string"}
                                """
                                        + signalField
                                        + "]}");
        GenericRecord record = new GenericData.Record(schema);
        record.put("event_id", "evt-1");
        record.put("device_id", "device-1");
        record.put("event_time", 1_000L);
        record.put("produced_at", 1_001L);
        record.put("sequence_number", 1L);
        record.put("measurement_type", "TEMPERATURE");
        record.put("value", 4.2D);
        record.put("unit", "C");
        if (versionTwo) {
            record.put("signal_strength_dbm", -78);
        }
        return record;
    }

    private static RawKafkaRecord raw() {
        return new RawKafkaRecord(
                "coldchain.telemetry.v1", 0, 0L, 1_000L, new byte[] {1}, new byte[] {2}, 1_002L);
    }
}
