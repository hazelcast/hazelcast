package com.hazelcast.internal.management.dto;

import java.util.Map;

/**
 * A DTO that describes event sent to Management Center.
 *
 * @see com.hazelcast.internal.management.events.Event
 */
public class MCEventDTO {

    private final long timestamp;
    private final int type;
    private final Map<String, String> data;

    public MCEventDTO(long timestamp, int type, Map<String, String> data) {
        this.timestamp = timestamp;
        this.type = type;
        this.data = data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getType() {
        return type;
    }

    public Map<String, String> getData() {
        return data;
    }

}
