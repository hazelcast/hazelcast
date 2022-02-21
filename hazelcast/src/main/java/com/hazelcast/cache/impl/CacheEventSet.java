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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * <p>Internal Set wrapper of {@link CacheEventData} items used during publishing and dispatching events.</p>
 *
 * Multiple event objects is required via iterator to handle cache events
 * by {@link javax.cache.event.CacheEntryListener}. This implementation  serves that purpose
 * as a {@link com.hazelcast.cache.impl.CacheEventData} set.
 *
 * @see com.hazelcast.cache.impl.CacheService#publishEvent(String, CacheEventSet, int)
 * @see com.hazelcast.cache.impl.CacheService#dispatchEvent(Object, CacheEventListener)
 */
public class CacheEventSet
        implements IdentifiedDataSerializable {

    private CacheEventType eventType;
    private Set<CacheEventData> events;
    private int completionId;

    public CacheEventSet() {
    }

    public CacheEventSet(CacheEventType eventType, Set<CacheEventData> events) {
        this.eventType = eventType;
        this.events = events;
    }

    public CacheEventSet(CacheEventType eventType, Set<CacheEventData> events, int completionId) {
        this.eventType = eventType;
        this.events = events;
        this.completionId = completionId;
    }

    public CacheEventSet(CacheEventType eventType, int completionId) {
        this.eventType = eventType;
        this.completionId = completionId;
    }

    /**
     * @return Set of CacheEventData
     */
    public Set<CacheEventData> getEvents() {
        return events;
    }

    /**
     * @return Event type
     */
    public CacheEventType getEventType() {
        return eventType;
    }

    /**
     * @return completion ID for sync listeners
     */
    public int getCompletionId() {
        return completionId;
    }

    /**
     * Helper method for adding multiple CacheEventData into this Set
     * @param cacheEventData event data representing a single event's data.
     * @see CacheEventData
     */
    public void addEventData(CacheEventData cacheEventData) {
        if (events == null) {
            events = new HashSet<CacheEventData>();
        }
        this.events.add(cacheEventData);
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(eventType.getType());
        out.writeInt(completionId);
        out.writeInt(events.size());
        for (CacheEventData ced : events) {
            out.writeObject(ced);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        eventType = CacheEventType.getByType(in.readInt());
        completionId = in.readInt();
        final int size = in.readInt();
        events = createHashSet(size);
        for (int i = 0; i < size; i++) {
            CacheEventData ced = in.readObject();
            events.add(ced);
        }
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_EVENT_DATA_SET;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }
}
