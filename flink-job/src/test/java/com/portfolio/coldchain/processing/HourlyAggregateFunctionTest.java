package com.portfolio.coldchain.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.coldchain.model.EnrichedEvent;
import org.junit.jupiter.api.Test;

class HourlyAggregateFunctionTest {
    @Test
    void aggregatesTemperatureHumidityBreachesAndLocation() {
        HourlyAggregateFunction function = new HourlyAggregateFunction();
        HourlyAggregateFunction.Accumulator accumulator = function.createAccumulator();
        function.add(event("TELEMETRY_TEMPERATURE", 4.0, false, 1_000), accumulator);
        function.add(event("TELEMETRY_TEMPERATURE", 10.0, true, 2_000), accumulator);
        function.add(event("TELEMETRY_HUMIDITY", 60.0, false, 3_000), accumulator);
        EnrichedEvent location = event("LOCATION", null, false, 4_000);
        location.latitude = 52.5;
        location.longitude = 13.4;
        function.add(location, accumulator);

        assertThat(accumulator.eventCount).isEqualTo(4);
        assertThat(accumulator.temperatureMin).isEqualTo(4.0);
        assertThat(accumulator.temperatureMax).isEqualTo(10.0);
        assertThat(accumulator.temperatureSum / accumulator.temperatureCount).isEqualTo(7.0);
        assertThat(accumulator.humiditySum / accumulator.humidityCount).isEqualTo(60.0);
        assertThat(accumulator.breachCount).isEqualTo(1);
        assertThat(accumulator.latestLatitude).isEqualTo(52.5);
    }

    private static EnrichedEvent event(
            String eventType, Double value, boolean breach, long eventTime) {
        EnrichedEvent event = new EnrichedEvent();
        event.shipmentId = "shipment-1";
        event.eventType = eventType;
        event.numericValue = value;
        event.breach = breach;
        event.eventTime = eventTime;
        return event;
    }
}
