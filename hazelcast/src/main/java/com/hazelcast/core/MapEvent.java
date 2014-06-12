package com.hazelcast.core;

/**
 * Map events common contract.
 */
public interface MapEvent {

    /**
     * Returns the member fired this event.
     *
     * @return the member fired this event.
     */
    Member getMember();

    /**
     * Return the event type
     *
     * @return event type
     */
    EntryEventType getEventType();

    /**
     * Returns the name of the map for this event.
     *
     * @return name of the map.
     */
    String getName();

}
