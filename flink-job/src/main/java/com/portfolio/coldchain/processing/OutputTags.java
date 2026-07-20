package com.portfolio.coldchain.processing;

import com.portfolio.coldchain.model.DeviceMetadata;
import com.portfolio.coldchain.model.RejectedEvent;
import org.apache.flink.util.OutputTag;

public final class OutputTags {
    public static final OutputTag<DeviceMetadata> METADATA = new OutputTag<>("metadata") {};
    public static final OutputTag<RejectedEvent> REJECTED = new OutputTag<>("rejected") {};
    public static final OutputTag<RejectedEvent> MISSING_METADATA =
            new OutputTag<>("missing-metadata") {};
    public static final OutputTag<RejectedEvent> LATE = new OutputTag<>("late") {};

    private OutputTags() {}
}
