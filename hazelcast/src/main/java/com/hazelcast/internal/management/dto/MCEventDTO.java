package com.hazelcast.internal.management.dto;

import com.hazelcast.internal.management.events.Event;

/**
 * A DTO that describes event sent to Management Center.
 *
 * @see com.hazelcast.internal.management.events.Event
 */
public class MCEventDTO {

    private final long timestamp;
    private final int type;
    private final String dataJson;

    public MCEventDTO(long timestamp, int type, String dataJson) {
        this.timestamp = timestamp;
        this.type = type;
        this.dataJson = dataJson;
    }

    public static MCEventDTO fromEvent(Event event) {
        return new MCEventDTO(event.getTimestamp(), event.getType().getCode(), event.toJson().toString());
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getType() {
        return type;
    }

    public String getDataJson() {
        return dataJson;
    }

}
