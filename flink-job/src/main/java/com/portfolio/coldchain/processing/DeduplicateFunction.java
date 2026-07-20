package com.portfolio.coldchain.processing;

import com.portfolio.coldchain.model.NormalizedEvent;
import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DeduplicateFunction
        extends KeyedProcessFunction<String, NormalizedEvent, NormalizedEvent> {
    private transient ValueState<Boolean> seen;
    private transient Counter duplicateCounter;

    @Override
    public void open(OpenContext openContext) throws Exception {
        StateTtlConfig ttl =
                StateTtlConfig.newBuilder(Duration.ofHours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("seen-event-id", Boolean.class);
        descriptor.enableTimeToLive(ttl);
        seen = getRuntimeContext().getState(descriptor);
        duplicateCounter = getRuntimeContext().getMetricGroup().counter("duplicate_records");
    }

    @Override
    public void processElement(
            NormalizedEvent event, Context context, Collector<NormalizedEvent> out) throws Exception {
        if (Boolean.TRUE.equals(seen.value())) {
            duplicateCounter.inc();
            return;
        }
        seen.update(true);
        out.collect(event);
    }
}
