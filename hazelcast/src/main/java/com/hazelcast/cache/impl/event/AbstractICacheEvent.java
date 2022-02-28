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

package com.hazelcast.cache.impl.event;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.cluster.Member;

import java.util.EventObject;

/**
 * The abstract class for a JCache event {@link com.hazelcast.cache.impl.event.ICacheEvent}.
 * @since 3.6
 */
public abstract class AbstractICacheEvent extends EventObject implements ICacheEvent {

    protected final String name;

    private final CacheEventType cacheEventType;

    private final Member member;

    /**
     * Constructs a prototypical Cache Event.
     *
     * @param source    The object on which the Event initially occurred.
     * @param member    The interface to the cluster member (node).
     * @param eventType The event type as an enum {@link EntryEventType} integer.
     * @throws IllegalArgumentException if source is null.
     */
    public AbstractICacheEvent(Object source, Member member, int eventType) {
        super(source);
        this.name = (String) source;
        this.member = member;
        this.cacheEventType = CacheEventType.getByType(eventType);
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
    public CacheEventType getEventType() {
        return cacheEventType;
    }

    /**
     * Returns the name of the cache for this event.
     *
     * @return The name of the cache for this event.
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
                cacheEventType, member, name);
    }
}
