/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.EntryEventType;

import java.util.EventObject;

/**
 * The abstract class for a map event {@link IMapEvent}.
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
