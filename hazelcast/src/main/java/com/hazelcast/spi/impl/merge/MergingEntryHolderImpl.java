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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.CreationTimeHolder;
import com.hazelcast.spi.merge.MergingEntryHolder;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;

/**
 * Implementation of {@link MergingEntryHolder}.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 * @since 3.10
 */
public class MergingEntryHolderImpl<K, V> implements MergingEntryHolder<K, V>, CreationTimeHolder,
        IdentifiedDataSerializable {

    private K key;
    private V value;
    private long creationTime;

    private transient SerializationService serializationService;

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public Object getDeserializedKey() {
        return serializationService.toObject(key);
    }

    public MergingEntryHolderImpl<K, V> setKey(K key) {
        this.key = key;
        return this;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public Object getDeserializedValue() {
        return serializationService.toObject(value);
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public MergingEntryHolderImpl<K, V> setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public MergingEntryHolderImpl<K, V> setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, key);
        IOUtil.writeObject(out, value);
        out.writeLong(creationTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readObject(in);
        value = IOUtil.readObject(in);
        creationTime = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return SplitBrainDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SplitBrainDataSerializerHook.KEY_MERGE_DATA_HOLDER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MergingEntryHolderImpl)) {
            return false;
        }

        MergingEntryHolderImpl<?, ?> that = (MergingEntryHolderImpl<?, ?>) o;
        if (creationTime != that.creationTime) {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "MergeDataHolder{"
                + "key=" + key
                + ", value=" + value
                + ", creationTime=" + creationTime
                + '}';
    }
}
