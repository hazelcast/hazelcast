/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.journal;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;
import java.util.Map.Entry;

public class DeserializingEntry<K, V> implements Entry<K, V>, HazelcastInstanceAware, IdentifiedDataSerializable {
    private Data dataKey;
    private Data dataValue;

    private transient K key;
    private transient V value;
    private transient SerializationService serializationService;

    DeserializingEntry() { }

    public DeserializingEntry(Data dataKey, Data dataValue) {
        this.dataKey = dataKey;
        this.dataValue = dataValue;
    }

    @Override
    public K getKey() {
        if (key == null && dataKey != null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
    }

    @Override
    public V getValue() {
        if (value == null && dataValue != null) {
            value = serializationService.toObject(dataValue);
        }
        return value;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return debugInfo(key, dataKey) + "=" + debugInfo(value, dataValue);
    }

    private String debugInfo(Object deserialized, Data serialized) {
        if (deserialized != null) {
            return deserialized.toString();
        }
        if (serialized == null) {
            return "{serialized, null}";
        }
        return "{serialized, " + serialized.totalSize() + " bytes}";
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        serializationService = ((SerializationServiceSupport) hazelcastInstance).getSerializationService();
    }

    @Override
    public int getFactoryId() {
        return EventJournalDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EventJournalDataSerializerHook.DESERIALIZING_ENTRY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeData(toData(key, dataKey));
        out.writeData(toData(value, dataValue));
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataKey = in.readData();
        dataValue = in.readData();
    }

    private Data toData(Object value, Data defaultValue) {
        return value != null ? serializationService.toData(value) : defaultValue;
    }
}
