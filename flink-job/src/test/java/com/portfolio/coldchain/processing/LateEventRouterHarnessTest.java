package com.portfolio.coldchain.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.coldchain.model.EnrichedEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class LateEventRouterHarnessTest {
    @Test
    void acceptsExactBoundaryAndRejectsOneMillisecondBeyondIt() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, EnrichedEvent> harness =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        new LateEventRouter(), value -> value.deviceId, Types.STRING)) {
            long watermark = 1_000_000;
            harness.processWatermark(watermark);
            EnrichedEvent boundary =
                    event("boundary", watermark - LateEventRouter.ALLOWED_LATENESS_MS);
            EnrichedEvent beyond =
                    event("beyond", watermark - LateEventRouter.ALLOWED_LATENESS_MS - 1);

            harness.processElement(boundary, boundary.eventTime);
            harness.processElement(beyond, beyond.eventTime);

            assertThat(harness.extractOutputValues())
                    .extracting(value -> value.eventId)
                    .containsExactly("boundary");
            assertThat(harness.getSideOutput(OutputTags.LATE))
                    .singleElement()
                    .satisfies(
                            record -> {
                                assertThat(record.getValue().eventId).isEqualTo("beyond");
                                assertThat(record.getValue().latenessMs)
                                        .isEqualTo(LateEventRouter.ALLOWED_LATENESS_MS + 1);
                                assertThat(record.getValue().watermarkMs).isEqualTo(watermark);
                            });
        }
    }

    private static EnrichedEvent event(String id, long eventTime) {
        EnrichedEvent event = new EnrichedEvent();
        event.eventId = id;
        event.deviceId = "device-1";
        event.eventTime = eventTime;
        event.sourceTopic = "coldchain.telemetry.v1";
        return event;
    }
}
