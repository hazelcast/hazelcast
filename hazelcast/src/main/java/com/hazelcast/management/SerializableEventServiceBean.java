package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.spi.EventService;

import static com.hazelcast.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.EventServiceMBean}.
 */
public class SerializableEventServiceBean implements JsonSerializable {

    private int eventThreadCount;
    private int eventQueueCapacity;
    private int eventQueueSize;

    public SerializableEventServiceBean() {
    }

    public SerializableEventServiceBean(EventService es) {
        this.eventThreadCount = es.getEventThreadCount();
        this.eventQueueCapacity = es.getEventQueueCapacity();
        this.eventQueueSize = es.getEventQueueSize();
    }

    public int getEventThreadCount() {
        return eventThreadCount;
    }

    public void setEventThreadCount(int eventThreadCount) {
        this.eventThreadCount = eventThreadCount;
    }

    public int getEventQueueCapacity() {
        return eventQueueCapacity;
    }

    public void setEventQueueCapacity(int eventQueueCapacity) {
        this.eventQueueCapacity = eventQueueCapacity;
    }

    public int getEventQueueSize() {
        return eventQueueSize;
    }

    public void setEventQueueSize(int eventQueueSize) {
        this.eventQueueSize = eventQueueSize;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("eventThreadCount", eventThreadCount);
        root.add("eventQueueCapacity", eventQueueCapacity);
        root.add("eventQueueSize", eventQueueSize);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        eventThreadCount = getInt(json, "eventThreadCount", -1);
        eventQueueCapacity = getInt(json, "eventQueueCapacity", -1);
        eventQueueSize = getInt(json, "eventQueueSize", -1);

    }
}
