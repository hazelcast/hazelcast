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

package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;

/**
 * {@link EventData} which is used only for the subscriber end of a query cache
 * and only for entry based events.
 * For this reason, it is not sent over the wire and is used locally to query cache.
 * <p>
 * Throws {@link UnsupportedOperationException} if one tries to serialize an instance of this class.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
@BinaryInterface
public class LocalEntryEventData<K, V> implements EventData {

    private K key;
    private V value;
    private V oldValue;
    private String source;
    private int eventType;
    private Data keyData;
    private Data valueData;
    private Data oldValueData;
    private final SerializationService serializationService;
    private final int partitionId;

    public LocalEntryEventData(SerializationService serializationService, String source,
                               int eventType, Object key, Object oldValue, Object value, int partitionId) {
        this.serializationService = serializationService;
        this.partitionId = partitionId;

        if (key instanceof Data) {
            this.keyData = (Data) key;
        } else {
            this.key = (K) key;
        }

        if (value instanceof Data) {
            this.valueData = (Data) value;
        } else {
            this.value = (V) value;
        }

        if (oldValue instanceof Data) {
            this.oldValueData = (Data) oldValue;
        } else {
            this.oldValue = (V) oldValue;
        }

        this.source = source;
        this.eventType = eventType;
    }

    public V getValue() {
        if (value == null && serializationService != null) {
            value = serializationService.toObject(valueData);
        }
        return value;
    }

    public V getOldValue() {
        if (oldValue == null && serializationService != null) {
            oldValue = serializationService.toObject(oldValueData);
        }
        return oldValue;
    }

    public K getKey() {
        if (key == null && serializationService != null) {
            key = serializationService.toObject(keyData);
        }
        return key;
    }

    public Data getKeyData() {
        if (keyData == null && serializationService != null) {
            keyData = serializationService.toData(key);
        }
        return keyData;
    }

    public Data getValueData() {
        if (valueData == null && serializationService != null) {
            valueData = serializationService.toData(value);
        }
        return valueData;
    }

    public Data getOldValueData() {
        if (oldValueData == null && serializationService != null) {
            oldValueData = serializationService.toData(oldValue);
        }
        return oldValueData;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getMapName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Address getCaller() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getEventType() {
        return eventType;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public LocalEntryEventData<K, V> cloneWithoutValue() {
        Object key = this.key != null ? this.key : this.keyData;
        return new LocalEntryEventData<>(serializationService, source,
                eventType, key, null, null, partitionId);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "LocalEntryEventData{"
                + "eventType=" + eventType
                + ", key=" + getKey()
                + ", source='" + source + '\''
                + '}';
    }
}
