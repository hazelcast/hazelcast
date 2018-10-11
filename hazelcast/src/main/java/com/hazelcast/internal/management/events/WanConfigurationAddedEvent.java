package com.hazelcast.internal.management.events;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.events.EventMetadata.EventType;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.WAN_CONFIGURATION_ADDED;

public class WanConfigurationAddedEvent extends AbstractEventBase {
    private final String wanConfigName;

    public WanConfigurationAddedEvent(String wanConfigName) {
        this.wanConfigName = wanConfigName;
    }

    @Override
    public EventType getType() {
        return WAN_CONFIGURATION_ADDED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("wanConfigName", wanConfigName);
        return json;
    }
}
