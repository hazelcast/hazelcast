package com.hazelcast.core;

import java.util.EventObject;

/**
 * Abstract map event.
 */
public abstract class AbstractIMapEvent extends EventObject implements IMapEvent {

    protected final String name;

    private final EntryEventType entryEventType;

    private final Member member;

    /**
     * Constructs a prototypical Event.
     *
     * @param source    The object on which the Event initially occurred.
     * @param member    Member node.
     * @param eventType Type of event as an integer.{@link EntryEventType}
     * @throws IllegalArgumentException if source is null.
     */
    public AbstractIMapEvent(Object source, Member member, int eventType) {
        super(source);
        this.name = (String) source;
        this.member = member;
        this.entryEventType = EntryEventType.getByType(eventType);
    }


    @Override
    public Object getSource() {
        return name;
    }

    /**
     * Returns the member fired this event.
     *
     * @return the member fired this event.
     */
    @Override
    public Member getMember() {
        return member;
    }

    /**
     * Return the event type
     *
     * @return event type
     */
    @Override
    public EntryEventType getEventType() {
        return entryEventType;
    }

    /**
     * Returns the name of the map for this event.
     *
     * @return name of the map.
     */
    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return String.format("entryEventType=%s, member=%s, name='%s'",
                entryEventType, member, name);
    }
}
