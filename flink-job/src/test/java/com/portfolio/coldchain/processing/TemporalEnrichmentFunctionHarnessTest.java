package com.portfolio.coldchain.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.EnrichedEvent;
import com.portfolio.coldchain.model.NormalizedEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class TemporalEnrichmentFunctionHarnessTest {
    @Test
    void enrichesWhenMetadataArrivesBeforeTheEvent() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<
                        String, NormalizedEvent, DeviceMetadata, EnrichedEvent>
                harness = harness()) {
            harness.processElement2(metadata(1, 1_000, "shipment-1"), 1_000);
            harness.processElement1(event("event-1", 1_500), 1_500);

            assertThat(harness.extractOutputValues())
                    .singleElement()
                    .satisfies(
                            value -> {
                                assertThat(value.shipmentId).isEqualTo("shipment-1");
                                assertThat(value.eventTime).isEqualTo(1_500);
                            });
        }
    }

    @Test
    void buffersEventThenPreservesItsCanonicalEventTime() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<
                        String, NormalizedEvent, DeviceMetadata, EnrichedEvent>
                harness = harness()) {
            harness.processElement1(event("event-buffered", 3_500), 3_500);
            assertThat(harness.extractOutputValues()).isEmpty();

            harness.processElement2(metadata(2, 3_000, "shipment-new"), 9_999);

            assertThat(harness.extractOutputValues())
                    .singleElement()
                    .satisfies(
                            value -> {
                                assertThat(value.eventId).isEqualTo("event-buffered");
                                assertThat(value.eventTime).isEqualTo(3_500);
                                assertThat(value.shipmentId).isEqualTo("shipment-new");
                            });
        }
    }

    @Test
    void selectsHistoricalMetadataWhenVersionsArriveOutOfOrder() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<
                        String, NormalizedEvent, DeviceMetadata, EnrichedEvent>
                harness = harness()) {
            harness.processElement2(metadata(2, 2_000, "shipment-new"), 2_000);
            harness.processElement2(metadata(1, 1_000, "shipment-old"), 1_000);
            harness.processElement1(event("historical", 1_500), 1_500);

            assertThat(harness.extractOutputValues())
                    .singleElement()
                    .satisfies(
                            value -> {
                                assertThat(value.shipmentId).isEqualTo("shipment-old");
                                assertThat(value.metadataVersion).isEqualTo(1);
                            });
        }
    }

    @Test
    void timerRoutesEventWhenMetadataNeverArrives() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<
                        String, NormalizedEvent, DeviceMetadata, EnrichedEvent>
                harness = harness()) {
            harness.setProcessingTime(0);
            harness.processElement1(event("missing", 1_500), 1_500);
            harness.setProcessingTime(10_000);

            assertThat(harness.extractOutputValues()).isEmpty();
            assertThat(harness.getSideOutput(OutputTags.MISSING_METADATA))
                    .singleElement()
                    .satisfies(
                            record -> {
                                assertThat(record.getValue().eventId).isEqualTo("missing");
                                assertThat(record.getValue().disposition)
                                        .isEqualTo("MISSING_METADATA");
                            });
        }
    }

    private static KeyedTwoInputStreamOperatorTestHarness<
                    String, NormalizedEvent, DeviceMetadata, EnrichedEvent>
            harness() throws Exception {
        return ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new TemporalEnrichmentFunction(),
                event -> event.deviceId,
                metadata -> metadata.deviceId,
                Types.STRING);
    }

    static NormalizedEvent event(String eventId, long eventTime) {
        NormalizedEvent event = new NormalizedEvent();
        event.eventId = eventId;
        event.deviceId = "device-1";
        event.eventTime = eventTime;
        event.eventType = "TELEMETRY_TEMPERATURE";
        event.numericValue = 4.0;
        event.sourceTopic = "coldchain.telemetry.v1";
        return event;
    }

    static DeviceMetadata metadata(
            int metadataVersion, long effectiveFrom, String shipmentId) {
        DeviceMetadata metadata = new DeviceMetadata();
        metadata.deviceId = "device-1";
        metadata.metadataVersion = metadataVersion;
        metadata.effectiveFrom = effectiveFrom;
        metadata.shipmentId = shipmentId;
        metadata.minTemperatureC = 2.0;
        metadata.maxTemperatureC = 8.0;
        return metadata;
    }
}
