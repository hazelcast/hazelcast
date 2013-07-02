package com.hazelcast.core;

/**
 * DistributedObjectEvent is fired when a {@link DistributedObject}
 * is created or destroyed cluster-wide.
 *
 * @see DistributedObject
 * @see DistributedObjectListener
 */
public interface DistributedObjectEvent {

    /**
     * Returns service name of related DistributedObject
     *
     * @return service name of DistributedObject
     */
    String getServiceName();

    /**
     * Returns type of this event; one of {@link EventType#CREATED} or {@link EventType#DESTROYED}
     *
     * @return eventType
     */
    EventType getEventType();

    /**
     * Returns identifier of related DistributedObject
     *
     * @return identifier of DistributedObject
     */
    Object getObjectId();

    /**
     * Returns DistributedObject instance
     *
     * @return DistributedObject
     */
    DistributedObject getDistributedObject();

    public enum EventType {
        CREATED, DESTROYED
    }
}
