/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Implementation of {@link Entry} which lazily deserializes keyData and valueData on access.
 * This class is not thread-safe.
 * <p>
 * @param <K> key
 * @param <V> value
 */
public final class ImmutableLazyEntry<K, V> implements Entry<K, V>, HazelcastInstanceAware, IdentifiedDataSerializable {

    private Data keyData;
    private Data valueData;

    private transient K key;
    private transient V value;
    private transient SerializationService serializationService;

    ImmutableLazyEntry() {
    }

    public ImmutableLazyEntry(Data keyData, Data valueData, SerializationService serializationService) {
        this.keyData = keyData;
        this.valueData = valueData;
        this.serializationService = serializationService;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        serializationService = ((SerializationServiceSupport) hazelcastInstance).getSerializationService();
    }

    @Override
    public K getKey() {
        if (key == null && keyData != null) {
            key = serializationService.toObject(keyData);
        }
        return key;
    }

    public Data getKeyData() {
        return keyData;
    }

    @Override
    public V getValue() {
        if (value == null && valueData != null) {
            value = serializationService.toObject(valueData);
        }
        return value;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException(
                "ImmutableLazyEntry does not support setValue, create a new Map.Entry instead."
        );
    }

    public Data getValueData() {
        return valueData;
    }

    @Override
    public int getFactoryId() {
        return UtilCollectionSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return UtilCollectionSerializerHook.IMMUTABLE_LAZY_ENTRY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, keyData);
        IOUtil.writeData(out, valueData);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        keyData = IOUtil.readData(in);
        valueData = IOUtil.readData(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ImmutableLazyEntry) {
            ImmutableLazyEntry<?, ?> that = (ImmutableLazyEntry<?, ?>) o;
            return Objects.equals(keyData, that.keyData) && Objects.equals(valueData, that.valueData);
        } else if (o instanceof Entry) {
            Entry<?, ?> that = (Entry<?, ?>) o;
            return Objects.equals(getKey(), that.getKey()) && Objects.equals(getValue(), that.getValue());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyData, valueData);
    }
}
