/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CacheEventSet
        implements IdentifiedDataSerializable {

    private CacheEventType eventType;
    private Set<CacheEventData> events;

    public CacheEventSet() {
    }

    public CacheEventSet(CacheEventType eventType, Set<CacheEventData> events) {
        this.eventType = eventType;
        this.events = events;
    }

    public CacheEventSet(CacheEventType eventType) {
        this.eventType = eventType;
    }

    public Set<CacheEventData> getEvents() {
        return events;
    }

    public CacheEventType getEventType() {
        return eventType;
    }

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
        out.writeInt(events.size());
        for (CacheEventData ced : events) {
            out.writeObject(ced);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        eventType = CacheEventType.getByType(in.readInt());
        final int size = in.readInt();
        events = new HashSet<CacheEventData>(size);
        for (int i = 0; i < size; i++) {
            CacheEventData ced = in.readObject();
            events.add(ced);
        }
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CACHE_EVENT_DATA_SET;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }
}
