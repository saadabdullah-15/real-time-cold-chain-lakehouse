package com.portfolio.coldchain.processing;

import com.portfolio.coldchain.model.EnrichedEvent;
import com.portfolio.coldchain.model.ShipmentHourlyMetric;
import java.io.Serializable;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyAggregateFunction
        implements AggregateFunction<
                EnrichedEvent,
                HourlyAggregateFunction.Accumulator,
                HourlyAggregateFunction.Accumulator> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(EnrichedEvent event, Accumulator value) {
        value.shipmentId = event.shipmentId;
        value.eventCount++;
        if (event.breach) {
            value.breachCount++;
        }
        if ("DETECTION".equals(event.eventType)) {
            value.detectionCount++;
        } else if ("TELEMETRY_TEMPERATURE".equals(event.eventType)
                && event.numericValue != null) {
            value.temperatureCount++;
            value.temperatureSum += event.numericValue;
            value.temperatureMin = Math.min(value.temperatureMin, event.numericValue);
            value.temperatureMax = Math.max(value.temperatureMax, event.numericValue);
        } else if ("TELEMETRY_HUMIDITY".equals(event.eventType)
                && event.numericValue != null) {
            value.humidityCount++;
            value.humiditySum += event.numericValue;
        }
        if (event.latitude != null
                && event.longitude != null
                && event.eventTime >= value.lastLocationEventTime) {
            value.lastLocationEventTime = event.eventTime;
            value.latestLatitude = event.latitude;
            value.latestLongitude = event.longitude;
        }
        value.lastEventTime = Math.max(value.lastEventTime, event.eventTime);
        return value;
    }

    @Override
    public Accumulator getResult(Accumulator accumulator) {
        return accumulator;
    }

    @Override
    public Accumulator merge(Accumulator left, Accumulator right) {
        left.eventCount += right.eventCount;
        left.breachCount += right.breachCount;
        left.detectionCount += right.detectionCount;
        left.temperatureCount += right.temperatureCount;
        left.temperatureSum += right.temperatureSum;
        left.temperatureMin = Math.min(left.temperatureMin, right.temperatureMin);
        left.temperatureMax = Math.max(left.temperatureMax, right.temperatureMax);
        left.humidityCount += right.humidityCount;
        left.humiditySum += right.humiditySum;
        if (right.lastLocationEventTime >= left.lastLocationEventTime) {
            left.lastLocationEventTime = right.lastLocationEventTime;
            left.latestLatitude = right.latestLatitude;
            left.latestLongitude = right.latestLongitude;
        }
        left.lastEventTime = Math.max(left.lastEventTime, right.lastEventTime);
        return left;
    }

    public static class Accumulator implements Serializable {
        public String shipmentId;
        public long eventCount;
        public long breachCount;
        public long detectionCount;
        public long temperatureCount;
        public double temperatureSum;
        public double temperatureMin = Double.POSITIVE_INFINITY;
        public double temperatureMax = Double.NEGATIVE_INFINITY;
        public long humidityCount;
        public double humiditySum;
        public Double latestLatitude;
        public Double latestLongitude;
        public long lastLocationEventTime = Long.MIN_VALUE;
        public long lastEventTime = Long.MIN_VALUE;
    }

    public static class WindowResult
            extends ProcessWindowFunction<Accumulator, ShipmentHourlyMetric, String, TimeWindow> {
        @Override
        public void process(
                String shipmentId,
                Context context,
                Iterable<Accumulator> values,
                Collector<ShipmentHourlyMetric> out) {
            Accumulator value = values.iterator().next();
            ShipmentHourlyMetric metric = new ShipmentHourlyMetric();
            metric.shipmentId = shipmentId;
            metric.windowStart = context.window().getStart();
            metric.windowEnd = context.window().getEnd();
            if (value.temperatureCount > 0) {
                metric.temperatureMin = value.temperatureMin;
                metric.temperatureMax = value.temperatureMax;
                metric.temperatureAvg = value.temperatureSum / value.temperatureCount;
            }
            if (value.humidityCount > 0) {
                metric.humidityAvg = value.humiditySum / value.humidityCount;
            }
            metric.detectionCount = value.detectionCount;
            metric.breachCount = value.breachCount;
            metric.eventCount = value.eventCount;
            metric.latestLatitude = value.latestLatitude;
            metric.latestLongitude = value.latestLongitude;
            metric.lastEventTime =
                    value.lastEventTime == Long.MIN_VALUE ? null : value.lastEventTime;
            metric.updatedAt = System.currentTimeMillis();
            out.collect(metric);
        }
    }
}
