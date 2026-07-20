package com.portfolio.coldchain.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.EnrichedEvent;
import com.portfolio.coldchain.model.NormalizedEvent;
import java.util.List;
import org.junit.jupiter.api.Test;

class TemporalEnrichmentFunctionTest {
    @Test
    void selectsMetadataEffectiveAtEventTime() {
        DeviceMetadata v1 = metadata(1, 1_000, "shipment-old");
        DeviceMetadata v2 = metadata(2, 2_000, "shipment-new");

        assertThat(TemporalEnrichmentFunction.selectMetadata(List.of(v1, v2), 1_500))
                .isSameAs(v1);
        assertThat(TemporalEnrichmentFunction.selectMetadata(List.of(v1, v2), 2_500))
                .isSameAs(v2);
    }

    @Test
    void flagsTemperatureOutsideShipmentThresholds() {
        DeviceMetadata metadata = metadata(1, 1_000, "shipment-1");
        metadata.minTemperatureC = 2;
        metadata.maxTemperatureC = 8;
        NormalizedEvent event = new NormalizedEvent();
        event.eventId = "event-1";
        event.deviceId = "device-1";
        event.eventTime = 1_500;
        event.eventType = "TELEMETRY_TEMPERATURE";
        event.numericValue = 12.0;

        EnrichedEvent result = TemporalEnrichmentFunction.enrich(event, metadata);
        assertThat(result.breach).isTrue();
        assertThat(result.shipmentId).isEqualTo("shipment-1");
    }

    private static DeviceMetadata metadata(int version, long effectiveFrom, String shipment) {
        DeviceMetadata result = new DeviceMetadata();
        result.deviceId = "device-1";
        result.metadataVersion = version;
        result.effectiveFrom = effectiveFrom;
        result.shipmentId = shipment;
        return result;
    }
}
