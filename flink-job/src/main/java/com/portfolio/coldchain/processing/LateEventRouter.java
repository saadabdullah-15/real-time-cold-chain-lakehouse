package com.portfolio.coldchain.processing;

import com.portfolio.coldchain.model.EnrichedEvent;
import com.portfolio.coldchain.model.RejectedEvent;
import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LateEventRouter extends KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent> {
    public static final long ALLOWED_LATENESS_MS = Duration.ofMinutes(5).toMillis();
    private transient Counter lateCounter;

    @Override
    public void open(OpenContext openContext) {
        lateCounter = getRuntimeContext().getMetricGroup().counter("too_late_records");
    }

    @Override
    public void processElement(EnrichedEvent event, Context context, Collector<EnrichedEvent> out) {
        long watermark = context.timerService().currentWatermark();
        if (watermark != Long.MIN_VALUE && event.eventTime < watermark - ALLOWED_LATENESS_MS) {
            lateCounter.inc();
            context.output(OutputTags.LATE, RejectedEvent.tooLate(event, watermark));
            return;
        }
        out.collect(event);
    }
}
