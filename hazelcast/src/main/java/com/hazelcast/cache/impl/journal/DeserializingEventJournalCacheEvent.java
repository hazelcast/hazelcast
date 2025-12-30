/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import java.io.IOException;

//@SuppressFBWarnings(value = "EQ_DOESNT_OVERRIDE_EQUALS",
//        justification = "equality is checked by serialised data in superclass, not deserialised instances in this class")
public class DeserializingEventJournalCacheEvent<K, V>
        extends InternalEventJournalCacheEvent
        implements EventJournalCacheEvent<K, V>, HazelcastInstanceAware {
    private SerializationService serializationService;

    private K objectKey;
    private V objectNewValue;
    private V objectOldValue;
    private boolean lostEventsDetected;

    public DeserializingEventJournalCacheEvent() {
    }

    public DeserializingEventJournalCacheEvent(
            SerializationService serializationService,
            InternalEventJournalCacheEvent je,
            boolean lostEventsDetected
    ) {
        super(je.getDataKey(), je.getDataNewValue(), je.getDataOldValue(), je.getEventType());
        this.serializationService = serializationService;
        this.lostEventsDetected = lostEventsDetected;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.EVENT_JOURNAL_DESERIALIZING_CACHE_EVENT;
    }

    @Override
    public K getKey() {
        if (objectKey == null && dataKey != null) {
            objectKey = serializationService.toObject(dataKey);
        }
        return objectKey;
    }

    @Override
    public V getNewValue() {
        if (objectNewValue == null && dataNewValue != null) {
            objectNewValue = serializationService.toObject(dataNewValue);
        }
        return objectNewValue;
    }

    @Override
    public V getOldValue() {
        if (objectOldValue == null && dataOldValue != null) {
            objectOldValue = serializationService.toObject(dataOldValue);
        }
        return objectOldValue;
    }

    @Override
    public CacheEventType getType() {
        return CacheEventType.getByType(eventType);
    }

    @Override
    public boolean isAfterLostEvents() {
        return lostEventsDetected;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventType = in.readInt();
        dataKey = IOUtil.readData(in);
        dataNewValue = IOUtil.readData(in);
        dataOldValue = IOUtil.readData(in);
        // Provides compatibility between the new client version (5.7) and an older member.
        // This is required to ensure compatibility when a new cluster reads the remote journal
        // from an older cluster.
        // This compatibility works only if each entry is serialized individually.
        // See {@link Sources#remoteCacheJournal} and similar methods in Sources.
        try {
            lostEventsDetected = in.readBoolean();
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(eventType);
        IOUtil.writeData(out, toData(dataKey, objectKey));
        IOUtil.writeData(out, toData(dataNewValue, objectNewValue));
        IOUtil.writeData(out, toData(dataOldValue, objectOldValue));
        out.writeBoolean(lostEventsDetected);
    }

    private Data toData(Data data, Object o) {
        return o != null ? serializationService.toData(o) : data;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        serializationService = ((SerializationServiceSupport) hazelcastInstance).getSerializationService();
    }
}
