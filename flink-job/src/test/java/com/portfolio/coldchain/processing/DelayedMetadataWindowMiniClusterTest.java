package com.portfolio.coldchain.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.EnrichedEvent;
import com.portfolio.coldchain.model.NormalizedEvent;
import com.portfolio.coldchain.model.ShipmentHourlyMetric;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class DelayedMetadataWindowMiniClusterTest {
    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @Test
    void delayedMetadataStillPlacesEventInPayloadTimeWindow() throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        long eventTime = 3_600_000 + 15_000;
        NormalizedEvent event =
                TemporalEnrichmentFunctionHarnessTest.event("event-delayed", eventTime);
        DeviceMetadata metadata =
                TemporalEnrichmentFunctionHarnessTest.metadata(1, 0, "shipment-1");

        DataStream<EnrichedEvent> enriched =
                environment
                        .fromData(event)
                        .keyBy(value -> value.deviceId)
                        .connect(
                                environment
                                        .fromData(metadata)
                                        .map(new DelayedMetadata())
                                        .keyBy(value -> value.deviceId))
                        .process(new TemporalEnrichmentFunction())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<EnrichedEvent>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(60))
                                        .withTimestampAssigner(
                                                (value, previousTimestamp) -> value.eventTime));

        DataStream<ShipmentHourlyMetric> metrics =
                enriched
                        .keyBy(value -> value.shipmentId)
                        .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                        .aggregate(
                                new HourlyAggregateFunction(),
                                new HourlyAggregateFunction.WindowResult());

        ShipmentHourlyMetric result = metrics.executeAndCollect(1).get(0);

        assertThat(result.windowStart).isEqualTo(3_600_000);
        assertThat(result.windowEnd).isEqualTo(7_200_000);
        assertThat(result.eventCount).isEqualTo(1);
        assertThat(result.lastEventTime).isEqualTo(eventTime);
    }

    private static final class DelayedMetadata implements MapFunction<DeviceMetadata, DeviceMetadata> {
        @Override
        public DeviceMetadata map(DeviceMetadata value) throws InterruptedException {
            Thread.sleep(100);
            return value;
        }
    }
}
