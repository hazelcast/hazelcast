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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation of {@link MergingEntry}.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractMergingEntryImpl<K, V, T extends AbstractMergingEntryImpl<K, V, T>>
        implements MergingEntry<K, V>, SerializationServiceAware, IdentifiedDataSerializable {

    private K key;
    private V value;

    private transient SerializationService serializationService;

    public AbstractMergingEntryImpl() {
    }

    public AbstractMergingEntryImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public K getRawKey() {
        return key;
    }

    @Override
    public K getKey() {
        return serializationService.toObject(key);
    }

    public T setKey(K key) {
        this.key = key;
        return (T) this;
    }

    @Override
    public V getRawValue() {
        return value;
    }

    @Override
    public V getValue() {
        return serializationService.toObject(value);
    }

    public T setValue(V value) {
        this.value = value;
        return (T) this;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, key);
        IOUtil.writeObject(out, value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readObject(in);
        value = IOUtil.readObject(in);
    }

    @Override
    public int getFactoryId() {
        return SplitBrainDataSerializerHook.F_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractMergingEntryImpl)) {
            return false;
        }

        AbstractMergingEntryImpl<?, ?, ?> that = (AbstractMergingEntryImpl<?, ?, ?>) o;
        if (!Objects.equals(key, that.key)) {
            return false;
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MergingEntry{"
                + "key=" + key
                + ", value=" + value
                + '}';
    }
}
