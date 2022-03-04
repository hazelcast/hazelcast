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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MultiMapMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * Implementation of {@link MultiMapMergeTypes}.
 *
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public class MultiMapMergingEntryImpl<K, V> implements MultiMapMergeTypes<K, V>, SerializationServiceAware,
                                                       IdentifiedDataSerializable {

    private Data key;
    private Collection<Object> value;
    private long creationTime = -1;
    private long expirationTime = -1;
    private long hits = -1;
    private long lastAccessTime = -1;
    private long lastUpdateTime = -1;

    private transient SerializationService serializationService;

    public MultiMapMergingEntryImpl() {
    }

    public MultiMapMergingEntryImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Data getRawKey() {
        return key;
    }

    @Override
    public K getKey() {
        return serializationService.toObject(key);
    }

    public MultiMapMergingEntryImpl<K, V> setKey(Data key) {
        this.key = key;
        return this;
    }

    @Override
    public Collection<Object> getRawValue() {
        return value;
    }

    @Override
    public Collection<V> getValue() {
        Collection<Object> deserializedValues = new ArrayList<>(value.size());
        for (Object aValue : value) {
            deserializedValues.add(serializationService.toObject(aValue));
        }
        return (Collection<V>) deserializedValues;
    }

    public MultiMapMergingEntryImpl<K, V> setValues(Collection<Object> values) {
        this.value = values;
        return this;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public MultiMapMergingEntryImpl<K, V> setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    @Override
    public long getHits() {
        return hits;
    }

    public MultiMapMergingEntryImpl<K, V> setHits(long hits) {
        this.hits = hits;
        return this;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public MultiMapMergingEntryImpl<K, V> setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public MultiMapMergingEntryImpl<K, V> setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, key);
        out.writeInt(value.size());
        for (Object aValue : value) {
            out.writeObject(aValue);
        }
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(hits);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readObject(in);
        int size = in.readInt();
        value = new ArrayList<Object>(size);
        for (int i = 0; i < size; i++) {
            value.add(in.readObject());
        }
        creationTime = in.readLong();
        expirationTime = in.readLong();
        hits = in.readLong();
        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return SplitBrainDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SplitBrainDataSerializerHook.MULTI_MAP_MERGING_ENTRY;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultiMapMergingEntryImpl that = (MultiMapMergingEntryImpl) o;
        if (creationTime != that.creationTime) {
            return false;
        }
        if (expirationTime != that.expirationTime) {
            return false;
        }
        if (hits != that.hits) {
            return false;
        }
        if (lastAccessTime != that.lastAccessTime) {
            return false;
        }
        if (lastUpdateTime != that.lastUpdateTime) {
            return false;
        }
        if (!Objects.equals(key, that.key)) {
            return false;
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        result = 31 * result + (int) (hits ^ (hits >>> 32));
        result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
        result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "MultiMapMergingEntry{"
                + "key=" + key
                + ", value=" + value
                + ", creationTime=" + creationTime
                + ", expirationTime=" + expirationTime
                + ", hits=" + hits
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + '}';
    }
}
