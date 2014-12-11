package com.hazelcast.core;

/**
 * Map events common contract.
 */
public interface IMapEvent {

    /**
     * Returns the member that fired this event.
     *
     * @return the member that fired this event.
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
     * @return name of the map for this event.
     */
    String getName();

}
