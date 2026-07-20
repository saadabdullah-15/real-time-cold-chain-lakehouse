package com.portfolio.coldchain.model;

import java.io.Serializable;

public class EnrichedEvent implements Serializable {
    public String eventId;
    public String deviceId;
    public String shipmentId;
    public String vehicleId;
    public String origin;
    public String destination;
    public String cargoType;
    public int metadataVersion;
    public long eventTime;
    public long producedAt;
    public long sequenceNumber;
    public String eventType;
    public Double numericValue;
    public String unit;
    public Double latitude;
    public Double longitude;
    public Double speedKph;
    public Double accuracyM;
    public String detectionType;
    public String severity;
    public Double magnitude;
    public Integer signalStrengthDbm;
    public boolean breach;
    public String sourceTopic;
    public int sourcePartition;
    public long sourceOffset;
    public long ingestedAt;
    public long processedAt;

    public EnrichedEvent() {}
}
