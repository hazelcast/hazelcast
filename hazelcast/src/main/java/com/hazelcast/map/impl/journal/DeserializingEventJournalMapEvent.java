/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.journal;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.spi.serialization.SerializationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

@SuppressFBWarnings(value = "EQ_DOESNT_OVERRIDE_EQUALS",
        justification = "equality is checked by serialised data in superclass, not deserialised instances in this class")
public class DeserializingEventJournalMapEvent<K, V>
        extends InternalEventJournalMapEvent
        implements EventJournalMapEvent<K, V>, HazelcastInstanceAware {
    private SerializationService serializationService;
    private K objectKey;
    private V objectNewValue;
    private V objectOldValue;

    public DeserializingEventJournalMapEvent() {
    }

    public DeserializingEventJournalMapEvent(SerializationService serializationService, InternalEventJournalMapEvent je) {
        super(je.getDataKey(), je.getDataNewValue(), je.getDataOldValue(), je.getEventType());
        this.serializationService = serializationService;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.EVENT_JOURNAL_DESERIALIZING_MAP_EVENT;
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
    public EntryEventType getType() {
        return EntryEventType.getByType(eventType);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(eventType);
        out.writeData(toData(dataKey, objectKey));
        out.writeData(toData(dataNewValue, objectNewValue));
        out.writeData(toData(dataOldValue, objectOldValue));
    }

    private Data toData(Data data, Object o) {
        return o != null ? serializationService.toData(o) : data;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        serializationService = ((SerializationServiceSupport) hazelcastInstance).getSerializationService();
    }
}
