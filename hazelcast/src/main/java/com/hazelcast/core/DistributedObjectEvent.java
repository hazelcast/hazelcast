package com.hazelcast.core;

/**
 * DistributedObjectEvent is fired when a {@link DistributedObject}
 * is created or destroyed cluster-wide.
 *
 * @see DistributedObject
 * @see DistributedObjectListener
 */
public class DistributedObjectEvent {

    private EventType eventType;

    private String serviceName;

    private DistributedObject distributedObject;

    public DistributedObjectEvent(EventType eventType, String serviceName, DistributedObject distributedObject) {
        this.eventType = eventType;
        this.serviceName = serviceName;
        this.distributedObject = distributedObject;
    }

    /**
     * Returns service name of related DistributedObject
     *
     * @return service name of DistributedObject
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Returns type of this event; one of {@link EventType#CREATED} or {@link EventType#DESTROYED}
     *
     * @return eventType
     */
    public EventType getEventType() {
        return eventType;
    }

    /**
     * Returns identifier of related DistributedObject
     *
     * @return identifier of DistributedObject
     */
    public Object getObjectId() {
        return distributedObject.getId();
    }

    /**
     * Returns DistributedObject instance
     *
     * @return DistributedObject
     */
    public DistributedObject getDistributedObject() {
        return distributedObject;
    }

    public enum EventType {
        CREATED, DESTROYED
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DistributedObjectEvent{");
        sb.append("eventType=").append(eventType);
        sb.append(", serviceName='").append(serviceName).append('\'');
        sb.append(", distributedObject=").append(distributedObject);
        sb.append('}');
        return sb.toString();
    }
}
