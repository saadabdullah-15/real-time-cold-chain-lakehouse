package com.portfolio.coldchain.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.coldchain.model.NormalizedEvent;
import com.portfolio.coldchain.model.RejectedEvent;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class DeduplicateFunctionHarnessTest {
    @Test
    void routesDuplicateWithItsKafkaCoordinates() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<
                        String, NormalizedEvent, NormalizedEvent>
                harness = harness()) {
            NormalizedEvent first = event("event-1", 10);
            NormalizedEvent duplicate = event("event-1", 11);

            harness.processElement(first, 1_000);
            harness.processElement(duplicate, 1_001);

            assertThat(harness.extractOutputValues())
                    .extracting(value -> value.sourceOffset)
                    .containsExactly(10L);
            assertThat(harness.getSideOutput(OutputTags.DUPLICATE))
                    .singleElement()
                    .extracting(StreamRecord::getValue)
                    .satisfies(
                            rejected -> {
                                assertThat(rejected.disposition).isEqualTo("DUPLICATE");
                                assertThat(rejected.errorCode).isEqualTo("DUPLICATE_EVENT_ID");
                                assertThat(rejected.sourceOffset).isEqualTo(11);
                            });
        }
    }

    @Test
    void restoresSeenStateFromSnapshot() throws Exception {
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<
                        String, NormalizedEvent, NormalizedEvent>
                original = harness()) {
            original.processElement(event("event-restore", 20), 1_000);
            snapshot = original.snapshot(1, 2_000);
        }

        try (KeyedOneInputStreamOperatorTestHarness<
                        String, NormalizedEvent, NormalizedEvent>
                restored =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                new KeyedProcessOperator<>(new DeduplicateFunction()),
                                value -> value.eventId,
                                Types.STRING)) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processElement(event("event-restore", 21), 3_000);

            assertThat(restored.extractOutputValues()).isEmpty();
            assertThat(restored.getSideOutput(OutputTags.DUPLICATE)).hasSize(1);
        }
    }

    @Test
    void acceptsEventAgainAfterTtlExpires() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<
                        String, NormalizedEvent, NormalizedEvent>
                harness = harness()) {
            NormalizedEvent first = event("event-expired", 30);
            NormalizedEvent afterTtl = event("event-expired", 31);

            harness.setProcessingTime(0);
            harness.setStateTtlProcessingTime(0);
            harness.processElement(first, 1_000);
            long afterExpiry = Duration.ofHours(25).toMillis();
            harness.setProcessingTime(afterExpiry);
            harness.setStateTtlProcessingTime(afterExpiry);
            harness.processElement(afterTtl, 2_000);

            assertThat(harness.extractOutputValues())
                    .extracting(value -> value.sourceOffset)
                    .containsExactly(30L, 31L);
            assertThat(harness.getSideOutput(OutputTags.DUPLICATE)).isNullOrEmpty();
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<
                    String, NormalizedEvent, NormalizedEvent>
            harness() throws Exception {
        return ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new DeduplicateFunction(), value -> value.eventId, Types.STRING);
    }

    private static NormalizedEvent event(String eventId, long offset) {
        NormalizedEvent event = new NormalizedEvent();
        event.eventId = eventId;
        event.deviceId = "device-1";
        event.eventTime = 1_000;
        event.sourceTopic = "coldchain.telemetry.v1";
        event.sourcePartition = 2;
        event.sourceOffset = offset;
        event.kafkaTimestamp = 1_000;
        return event;
    }
}
