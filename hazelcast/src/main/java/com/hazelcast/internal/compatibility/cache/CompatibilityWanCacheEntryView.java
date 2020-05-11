/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.cache;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.internal.compatibility.wan.CompatibilityOSWanDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.spi.serialization.SerializationServiceAware;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * WAN heap based implementation of {@link CacheEntryView} for processing
 * compatibility WAN replication events from 4.x clusters.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
public class CompatibilityWanCacheEntryView<K, V> implements CacheEntryView<K, V>,
        IdentifiedDataSerializable, SerializationServiceAware {

    private SerializationService serializationService;
    private K key;
    private V value;
    private Data dataKey;
    private Data dataValue;
    private long creationTime;
    private long expirationTime;
    private long lastAccessTime;
    private long hits;

    public CompatibilityWanCacheEntryView() {
    }

    public CompatibilityWanCacheEntryView(@Nonnull Data dataKey,
                                          @Nullable Data dataValue,
                                          long creationTime,
                                          long expirationTime,
                                          long lastAccessTime,
                                          long hits,
                                          @Nonnull SerializationService serializationService) {
        this.dataKey = dataKey;
        this.dataValue = dataValue;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.lastAccessTime = lastAccessTime;
        this.hits = hits;
        this.serializationService = serializationService;
    }

    @Override
    public K getKey() {
        if (key == null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
    }

    public Data getDataKey() {
        return dataKey;
    }

    @Override
    public V getValue() {
        if (value == null) {
            value = serializationService.toObject(dataValue);
        }
        return value;
    }

    public Data getDataValue() {
        return dataValue;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getAccessHit() {
        return hits;
    }

    @Override
    public Data getExpiryPolicy() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " should not be serialized!");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        expirationTime = in.readLong();
        lastAccessTime = in.readLong();
        hits = in.readLong();
        dataKey = in.readData();
        dataValue = in.readData();
    }

    @Override
    public int getFactoryId() {
        return CompatibilityOSWanDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CompatibilityOSWanDataSerializerHook.WAN_CACHE_ENTRY_VIEW;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompatibilityWanCacheEntryView<?, ?> that = (CompatibilityWanCacheEntryView<?, ?>) o;

        if (creationTime != that.creationTime) {
            return false;
        }
        if (expirationTime != that.expirationTime) {
            return false;
        }
        if (lastAccessTime != that.lastAccessTime) {
            return false;
        }
        if (hits != that.hits) {
            return false;
        }
        if (!dataKey.equals(that.dataKey)) {
            return false;
        }
        return dataValue.equals(that.dataValue);
    }

    @Override
    public int hashCode() {
        int result = dataKey.hashCode();
        result = 31 * result + dataValue.hashCode();
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
        result = 31 * result + (int) (hits ^ (hits >>> 32));
        return result;
    }
}
