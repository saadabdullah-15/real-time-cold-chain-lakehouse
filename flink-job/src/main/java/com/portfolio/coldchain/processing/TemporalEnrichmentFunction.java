package com.portfolio.coldchain.processing;

import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.EnrichedEvent;
import com.portfolio.coldchain.model.NormalizedEvent;
import com.portfolio.coldchain.model.RejectedEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class TemporalEnrichmentFunction
        extends KeyedCoProcessFunction<
                String, NormalizedEvent, DeviceMetadata, EnrichedEvent> {
    private static final long METADATA_WAIT_MS = 10_000;

    private transient MapState<Integer, DeviceMetadata> metadataHistory;
    private transient ListState<NormalizedEvent> pendingEvents;
    private transient ValueState<Long> pendingTimer;
    private transient Counter missingMetadataCounter;

    @Override
    public void open(OpenContext openContext) throws Exception {
        metadataHistory =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        "metadata-history", Integer.class, DeviceMetadata.class));
        pendingEvents =
                getRuntimeContext()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "pending-events", NormalizedEvent.class));
        pendingTimer =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("pending-timer", Long.class));
        missingMetadataCounter =
                getRuntimeContext().getMetricGroup().counter("missing_metadata_records");
    }

    @Override
    public void processElement1(
            NormalizedEvent event, Context context, Collector<EnrichedEvent> out) throws Exception {
        DeviceMetadata metadata = selectMetadata(metadataHistory.values(), event.eventTime);
        if (metadata != null) {
            out.collect(enrich(event, metadata));
            return;
        }
        pendingEvents.add(event);
        if (pendingTimer.value() == null) {
            long timer = context.timerService().currentProcessingTime() + METADATA_WAIT_MS;
            context.timerService().registerProcessingTimeTimer(timer);
            pendingTimer.update(timer);
        }
    }

    @Override
    public void processElement2(
            DeviceMetadata metadata, Context context, Collector<EnrichedEvent> out) throws Exception {
        metadataHistory.put(metadata.metadataVersion, metadata);
        List<NormalizedEvent> unresolved = new ArrayList<>();
        for (NormalizedEvent event : pendingEvents.get()) {
            DeviceMetadata selected = selectMetadata(metadataHistory.values(), event.eventTime);
            if (selected == null) {
                unresolved.add(event);
            } else {
                out.collect(enrich(event, selected));
            }
        }
        pendingEvents.update(unresolved);
    }

    @Override
    public void onTimer(
            long timestamp, OnTimerContext context, Collector<EnrichedEvent> out) throws Exception {
        for (NormalizedEvent event : pendingEvents.get()) {
            DeviceMetadata metadata = selectMetadata(metadataHistory.values(), event.eventTime);
            if (metadata == null) {
                missingMetadataCounter.inc();
                context.output(OutputTags.MISSING_METADATA, RejectedEvent.missingMetadata(event));
            } else {
                out.collect(enrich(event, metadata));
            }
        }
        pendingEvents.clear();
        pendingTimer.clear();
    }

    public static DeviceMetadata selectMetadata(
            Iterable<DeviceMetadata> metadata, long eventTime) {
        DeviceMetadata selected = null;
        for (DeviceMetadata candidate : metadata) {
            if (candidate.effectiveFrom <= eventTime
                    && (selected == null
                            || candidate.effectiveFrom > selected.effectiveFrom
                            || (candidate.effectiveFrom == selected.effectiveFrom
                                    && candidate.metadataVersion > selected.metadataVersion))) {
                selected = candidate;
            }
        }
        return selected;
    }

    public static EnrichedEvent enrich(NormalizedEvent source, DeviceMetadata metadata) {
        EnrichedEvent target = new EnrichedEvent();
        target.eventId = source.eventId;
        target.deviceId = source.deviceId;
        target.shipmentId = metadata.shipmentId;
        target.vehicleId = metadata.vehicleId;
        target.origin = metadata.origin;
        target.destination = metadata.destination;
        target.cargoType = metadata.cargoType;
        target.metadataVersion = metadata.metadataVersion;
        target.eventTime = source.eventTime;
        target.producedAt = source.producedAt;
        target.sequenceNumber = source.sequenceNumber;
        target.eventType = source.eventType;
        target.numericValue = source.numericValue;
        target.unit = source.unit;
        target.latitude = source.latitude;
        target.longitude = source.longitude;
        target.speedKph = source.speedKph;
        target.accuracyM = source.accuracyM;
        target.detectionType = source.detectionType;
        target.severity = source.severity;
        target.magnitude = source.magnitude;
        target.signalStrengthDbm = source.signalStrengthDbm;
        target.breach =
                "TELEMETRY_TEMPERATURE".equals(source.eventType)
                        && source.numericValue != null
                        && (source.numericValue < metadata.minTemperatureC
                                || source.numericValue > metadata.maxTemperatureC);
        target.sourceTopic = source.sourceTopic;
        target.sourcePartition = source.sourcePartition;
        target.sourceOffset = source.sourceOffset;
        target.ingestedAt = source.ingestedAt;
        target.processedAt = System.currentTimeMillis();
        return target;
    }

    public static List<DeviceMetadata> sortedHistory(Iterable<DeviceMetadata> history) {
        List<DeviceMetadata> result = new ArrayList<>();
        history.forEach(result::add);
        result.sort(Comparator.comparingLong(value -> value.effectiveFrom));
        return result;
    }
}
