package com.hazelcast.core;

import java.util.EventObject;

/**
 * The abstract class for a map event {@link com.hazelcast.core.IMapEvent}.
 */
public abstract class AbstractIMapEvent extends EventObject implements IMapEvent {

    protected final String name;

    private final EntryEventType entryEventType;

    private final Member member;

    /**
     * Constructs a prototypical map Event.
     *
     * @param source    The object on which the Event initially occurred.
     * @param member    The interface to the cluster member (node).
     * @param eventType The event type as an enum {@link EntryEventType} integer.
     * @throws IllegalArgumentException if source is null.
     */
    public AbstractIMapEvent(Object source, Member member, int eventType) {
        super(source);
        this.name = (String) source;
        this.member = member;
        this.entryEventType = EntryEventType.getByType(eventType);
    }


    /**
     * Returns the object on which the event initially occurred.
     *
     * @return The object on which the event initially occurred.
     */
    @Override
    public Object getSource() {
        return name;
    }

    /**
     * Returns the member that fired this event.
     *
     * @return The member that fired this event.
     */
    @Override
    public Member getMember() {
        return member;
    }

    /**
     * Returns the event type {@link EntryEventType}.
     *
     * @return The event type {@link EntryEventType}.
     */
    @Override
    public EntryEventType getEventType() {
        return entryEventType;
    }

    /**
     * Returns the name of the map for this event.
     *
     * @return The name of the map for this event.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns a String representation of this event.
     *
     * @return A String representation of this event.
     */
    @Override
    public String toString() {
        return String.format("entryEventType=%s, member=%s, name='%s'",
                entryEventType, member, name);
    }
}
