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

package com.hazelcast.cache.impl.wan;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.impl.WanDataSerializerHook;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

/**
 * WAN heap based implementation of {@link CacheEntryView}.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
public class WanCacheEntryView<K, V> implements CacheEntryView<K, V>, IdentifiedDataSerializable, SerializationServiceAware {

    private SerializationService serializationService;
    private K key;
    private V value;
    private Data dataKey;
    private Data dataValue;
    private Data trimmedDataKey;
    private Data trimmedDataValue;
    private long creationTime;
    private long expirationTime;
    private long lastAccessTime;
    private long hits;

    public WanCacheEntryView() {
    }

    public WanCacheEntryView(@Nonnull Data dataKey,
                             @Nullable Data dataValue,
                             long creationTime,
                             long expirationTime,
                             long lastAccessTime,
                             long hits,
                             @Nonnull SerializationService serializationService) {
        this.dataKey = serializationService.toDataWithSchema(dataKey);
        this.dataValue = serializationService.toDataWithSchema(dataValue);
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
        if (trimmedDataKey == null) {
            trimmedDataKey = serializationService.trimSchema(dataKey);
        }
        return trimmedDataKey;
    }

    @Override
    public V getValue() {
        if (value == null) {
            value = serializationService.toObject(dataValue);
        }
        return value;
    }

    public Data getDataValue() {
        if (trimmedDataValue == null) {
            trimmedDataValue = serializationService.trimSchema(dataValue);
        }
        return trimmedDataValue;
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
    public long getHits() {
        return hits;
    }

    @Override
    public Data getExpiryPolicy() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(hits);
        IOUtil.writeData(out, dataKey);
        IOUtil.writeData(out, dataValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        expirationTime = in.readLong();
        lastAccessTime = in.readLong();
        hits = in.readLong();
        dataKey = IOUtil.readData(in);
        dataValue = IOUtil.readData(in);
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_CACHE_ENTRY_VIEW;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WanCacheEntryView<?, ?> that = (WanCacheEntryView<?, ?>) o;
        return creationTime == that.creationTime
                && expirationTime == that.expirationTime
                && lastAccessTime == that.lastAccessTime
                && hits == that.hits
                && dataKey.equals(that.dataKey)
                && dataValue.equals(that.dataValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataKey, dataValue, creationTime, expirationTime, lastAccessTime, hits);
    }
}
