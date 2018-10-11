package com.hazelcast.internal.management.events;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.events.EventMetadata.EventType;

import static com.hazelcast.internal.management.events.EventMetadata.EventType.ADD_WAN_CONFIGURATION_IGNORED;

public class AddWanConfigIgnoredEvent extends AbstractEventBase {
    private final String wanConfigName;
    private final String reason;

    public static AddWanConfigIgnoredEvent alreadyExists(String wanConfigName) {
        return new AddWanConfigIgnoredEvent(wanConfigName,
                "A WAN replication config already exists with the given name.");
    }

    public static AddWanConfigIgnoredEvent enterpriseOnly(String wanConfigName) {
        return new AddWanConfigIgnoredEvent(wanConfigName,
                "Adding new WAN replication config is supported for enterprise clusters only.");
    }

    private AddWanConfigIgnoredEvent(String wanConfigName, String reason) {
        this.wanConfigName = wanConfigName;
        this.reason = reason;
    }

    @Override
    public EventType getType() {
        return ADD_WAN_CONFIGURATION_IGNORED;
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("wanConfigName", wanConfigName);
        json.add("reason", reason);
        return json;
    }
}
