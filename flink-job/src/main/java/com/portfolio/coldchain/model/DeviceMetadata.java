package com.portfolio.coldchain.model;

import java.io.Serializable;

public class DeviceMetadata implements Serializable {
    public String eventId;
    public String deviceId;
    public long eventTime;
    public long producedAt;
    public long sequenceNumber;
    public int metadataVersion;
    public long effectiveFrom;
    public String shipmentId;
    public String vehicleId;
    public String origin;
    public String destination;
    public String cargoType;
    public double minTemperatureC;
    public double maxTemperatureC;
    public String status;
    public String sourceTopic;
    public int sourcePartition;
    public long sourceOffset;
    public long ingestedAt;

    public DeviceMetadata() {}
}
