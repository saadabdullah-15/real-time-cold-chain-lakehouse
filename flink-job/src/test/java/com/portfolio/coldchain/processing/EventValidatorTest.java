package com.portfolio.coldchain.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.coldchain.model.NormalizedEvent;
import org.junit.jupiter.api.Test;

class EventValidatorTest {
    @Test
    void acceptsValidTemperature() {
        NormalizedEvent event = temperature(5.5);
        assertThat(EventValidator.validate(event).valid()).isTrue();
    }

    @Test
    void rejectsInjectedInvalidTemperature() {
        NormalizedEvent event = temperature(10_000);
        assertThat(EventValidator.validate(event).code()).isEqualTo("INVALID_MEASUREMENT");
    }

    @Test
    void rejectsInvalidCoordinates() {
        NormalizedEvent event = temperature(5.5);
        event.eventType = "LOCATION";
        event.latitude = 200.0;
        event.longitude = 13.4;
        event.speedKph = 70.0;
        event.accuracyM = 5.0;
        assertThat(EventValidator.validate(event).code()).isEqualTo("INVALID_COORDINATES");
    }

    private static NormalizedEvent temperature(double value) {
        NormalizedEvent event = new NormalizedEvent();
        event.eventId = "event-1";
        event.deviceId = "device-1";
        event.sequenceNumber = 1;
        event.eventTime = 1_700_000_000_000L;
        event.producedAt = 1_700_000_000_100L;
        event.eventType = "TELEMETRY_TEMPERATURE";
        event.numericValue = value;
        event.unit = "CELSIUS";
        return event;
    }
}
