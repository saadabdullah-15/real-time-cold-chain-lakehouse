package com.portfolio.coldchain.model;

import java.io.Serializable;

public class ShipmentHourlyMetric implements Serializable {
    public String shipmentId;
    public long windowStart;
    public long windowEnd;
    public Double temperatureMin;
    public Double temperatureMax;
    public Double temperatureAvg;
    public Double humidityAvg;
    public long detectionCount;
    public long breachCount;
    public long eventCount;
    public Double latestLatitude;
    public Double latestLongitude;
    public Long lastEventTime;
    public long updatedAt;

    public ShipmentHourlyMetric() {}
}
