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
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation of {@link CacheMergeTypes}.
 *
 * @since 3.10
 */
@SuppressWarnings("WeakerAccess")
public class CacheMergingEntryImpl<K, V>
        implements CacheMergeTypes<K, V>, SerializationServiceAware, IdentifiedDataSerializable {

    private Data value;
    private Data key;
    private long creationTime = -1;
    private long expirationTime = -1;
    private long hits = -1;
    private long lastAccessTime = -1;

    private transient SerializationService serializationService;

    public CacheMergingEntryImpl() {
    }

    public CacheMergingEntryImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Data getRawValue() {
        return value;
    }

    @Override
    public V getValue() {
        return serializationService.toObject(value);
    }

    public CacheMergingEntryImpl<K, V> setValue(Data value) {
        this.value = value;
        return this;
    }

    @Override
    public Data getRawKey() {
        return key;
    }

    @Override
    public K getKey() {
        return serializationService.toObject(key);
    }

    public CacheMergingEntryImpl<K, V> setKey(Data key) {
        this.key = key;
        return this;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public CacheMergingEntryImpl<K, V> setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    public CacheMergingEntryImpl<K, V> setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    @Override
    public long getHits() {
        return hits;
    }

    public CacheMergingEntryImpl<K, V> setHits(long hits) {
        this.hits = hits;
        return this;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public CacheMergingEntryImpl<K, V> setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, key);
        IOUtil.writeObject(out, value);
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(hits);
        out.writeLong(lastAccessTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readObject(in);
        value = IOUtil.readObject(in);
        creationTime = in.readLong();
        expirationTime = in.readLong();
        hits = in.readLong();
        lastAccessTime = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return SplitBrainDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SplitBrainDataSerializerHook.CACHE_MERGING_ENTRY;
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

        CacheMergingEntryImpl that = (CacheMergingEntryImpl) o;
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
        return result;
    }

    @Override
    public String toString() {
        return "CacheMergingEntry{"
                + "key=" + key
                + ", value=" + value
                + ", creationTime=" + creationTime
                + ", expirationTime=" + expirationTime
                + ", hits=" + hits
                + ", lastAccessTime=" + lastAccessTime
                + '}';
    }
}
