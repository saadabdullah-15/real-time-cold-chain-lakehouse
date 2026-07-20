package com.portfolio.coldchain.model;

import java.io.Serializable;

public class BreachAlert implements Serializable {
    public String eventId;
    public String deviceId;
    public String shipmentId;
    public long eventTime;
    public double measuredTemperatureC;
    public String alertType;
    public long emittedAt;

    public BreachAlert() {}

    public static BreachAlert from(EnrichedEvent event) {
        BreachAlert alert = new BreachAlert();
        alert.eventId = event.eventId;
        alert.deviceId = event.deviceId;
        alert.shipmentId = event.shipmentId;
        alert.eventTime = event.eventTime;
        alert.measuredTemperatureC = event.numericValue;
        alert.alertType = "TEMPERATURE_RANGE_BREACH";
        alert.emittedAt = System.currentTimeMillis();
        return alert;
    }
}
