package com.hazelcast.core;

/**
 * Used for map-wide events like {@link com.hazelcast.core.EntryEventType#EVICT_ALL}
 * and {@link  com.hazelcast.core.EntryEventType#CLEAR_ALL}.
 */
public class MapEvent extends AbstractIMapEvent {

    private static final long serialVersionUID = -4948640313865667023L;

    /**
     * Number of entries affected by this event.
     */
    private final int numberOfEntriesAffected;

    public MapEvent(Object source, Member member, int eventType, int numberOfEntriesAffected) {
        super(source, member, eventType);
        this.numberOfEntriesAffected = numberOfEntriesAffected;
    }

    /**
     * Returns the number of entries affected by this event.
     *
     * @return number of entries affected by this event
     */
    public int getNumberOfEntriesAffected() {
        return numberOfEntriesAffected;
    }


    @Override
    public String toString() {
        return "MapEvent{"
                + super.toString()
                + ", numberOfEntriesAffected=" + numberOfEntriesAffected
                + '}';

    }
}
