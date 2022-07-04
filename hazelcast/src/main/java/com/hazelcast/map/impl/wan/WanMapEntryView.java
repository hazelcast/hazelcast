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

package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
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

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * WAN heap based implementation of {@link EntryView}.
 * <p>
 * It is lazy because you intialise it with serialized formats
 * of key and value and it will deserialise only if {@link #getKey()} or
 * {@link #getValue()} are invoked.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
@SuppressWarnings("checkstyle:methodcount")
public class WanMapEntryView<K, V> implements EntryView<K, V>, IdentifiedDataSerializable, SerializationServiceAware {
    private SerializationService serializationService;
    private K key;
    private V value;
    private Data dataKey;
    private Data dataValue;
    private Data trimmedDataKey;
    private Data trimmedDataValue;
    private long cost;
    private long creationTime;
    private long expirationTime;
    private long hits;
    private long lastAccessTime;
    private long lastStoredTime;
    private long lastUpdateTime;
    private long version;
    private long ttl;
    private long maxIdle;

    public WanMapEntryView() {
    }

    public WanMapEntryView(@Nonnull Data dataKey,
                           @Nullable Data dataValue,
                           @Nonnull SerializationService serializationService) {
        checkNotNull(dataKey);
        checkNotNull(serializationService);
        this.dataKey = serializationService.toDataWithSchema(dataKey);
        this.dataValue = serializationService.toDataWithSchema(dataValue);
        this.serializationService = serializationService;
    }

    @Override
    public K getKey() {
        if (key == null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
    }

    /**
     * Returns the serialised format of the entry key.
     */
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

    /**
     * Returns the serialised format of the entry value.
     */
    public Data getDataValue() {
        if (trimmedDataValue == null) {
            trimmedDataValue = serializationService.trimSchema(dataValue);
        }
        return trimmedDataValue;
    }

    @Override
    public long getCost() {
        return cost;
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
    public long getHits() {
        return hits;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    @Override
    public long getMaxIdle() {
        return maxIdle;
    }

    public WanMapEntryView<K, V> withCost(long cost) {
        this.cost = cost;
        return this;
    }

    public WanMapEntryView<K, V> withCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    public WanMapEntryView<K, V> withExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public WanMapEntryView<K, V> withHits(long hits) {
        this.hits = hits;
        return this;
    }

    public WanMapEntryView<K, V> withLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    public WanMapEntryView<K, V> withLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
        return this;
    }

    public WanMapEntryView<K, V> withLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    public WanMapEntryView<K, V> withVersion(long version) {
        this.version = version;
        return this;
    }

    public WanMapEntryView<K, V> withTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public WanMapEntryView<K, V> withMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, dataKey);
        IOUtil.writeData(out, dataValue);
        out.writeLong(cost);
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(hits);
        out.writeLong(lastAccessTime);
        out.writeLong(lastStoredTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(version);
        out.writeLong(ttl);
        out.writeLong(maxIdle);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataKey = IOUtil.readData(in);
        dataValue = IOUtil.readData(in);
        cost = in.readLong();
        creationTime = in.readLong();
        expirationTime = in.readLong();
        hits = in.readLong();
        lastAccessTime = in.readLong();
        lastStoredTime = in.readLong();
        lastUpdateTime = in.readLong();
        version = in.readLong();
        ttl = in.readLong();
        maxIdle = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_MAP_ENTRY_VIEW;
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WanMapEntryView<?, ?> that = (WanMapEntryView<?, ?>) o;
        return cost == that.cost
                && creationTime == that.creationTime
                && expirationTime == that.expirationTime
                && hits == that.hits
                && lastAccessTime == that.lastAccessTime
                && lastStoredTime == that.lastStoredTime
                && lastUpdateTime == that.lastUpdateTime
                && version == that.version
                && ttl == that.ttl
                && maxIdle == that.maxIdle
                && Objects.equals(key, that.key)
                && Objects.equals(value, that.value)
                && dataKey.equals(that.dataKey)
                && dataValue.equals(that.dataValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, dataKey, dataValue, cost,
                creationTime, expirationTime, hits, lastAccessTime, lastStoredTime,
                lastUpdateTime, version, ttl, maxIdle);
    }

    @Override
    public String toString() {
        return "WanMapEntryView{"
                + "dataKey=" + dataKey
                + ", key=" + key
                + ", dataValue=" + dataValue
                + ", value=" + value
                + ", cost=" + cost
                + ", creationTime=" + creationTime
                + ", expirationTime=" + expirationTime
                + ", hits=" + hits
                + ", lastAccessTime=" + lastAccessTime
                + ", lastStoredTime=" + lastStoredTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", version=" + version
                + ", ttl=" + ttl
                + ", maxIdle=" + maxIdle
                + '}';
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }
}
