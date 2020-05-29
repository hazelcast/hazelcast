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

package com.hazelcast.internal.compatibility.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.compatibility.wan.CompatibilityOSWanDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.spi.serialization.SerializationServiceAware;

import java.io.IOException;

/**
 * WAN heap based implementation of {@link EntryView} for processing
 * compatibility WAN replication events from 4.x clusters.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
@SuppressWarnings("checkstyle:methodcount")
public class CompatibilityWanMapEntryView<K, V> implements EntryView<K, V>,
        IdentifiedDataSerializable, SerializationServiceAware {
    private SerializationService serializationService;
    private K key;
    private V value;
    private Data dataKey;
    private Data dataValue;
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

    public CompatibilityWanMapEntryView() {
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
        return dataKey;
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
        return dataValue;
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
    public Long getMaxIdle() {
        return maxIdle;
    }

    public CompatibilityWanMapEntryView<K, V> withCost(long cost) {
        this.cost = cost;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withHits(long hits) {
        this.hits = hits;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withVersion(long version) {
        this.version = version;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public CompatibilityWanMapEntryView<K, V> withMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " should not be serialized!");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataKey = in.readData();
        dataValue = in.readData();
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
        return CompatibilityOSWanDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CompatibilityOSWanDataSerializerHook.WAN_MAP_ENTRY_VIEW;
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

        CompatibilityWanMapEntryView<?, ?> that = (CompatibilityWanMapEntryView<?, ?>) o;

        if (cost != that.cost) {
            return false;
        }
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
        if (lastStoredTime != that.lastStoredTime) {
            return false;
        }
        if (lastUpdateTime != that.lastUpdateTime) {
            return false;
        }
        if (version != that.version) {
            return false;
        }
        if (ttl != that.ttl) {
            return false;
        }
        if (maxIdle != that.maxIdle) {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        }
        if (!dataKey.equals(that.dataKey)) {
            return false;
        }
        return dataValue.equals(that.dataValue);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + dataKey.hashCode();
        result = 31 * result + dataValue.hashCode();
        result = 31 * result + (int) (cost ^ (cost >>> 32));
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        result = 31 * result + (int) (hits ^ (hits >>> 32));
        result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
        result = 31 * result + (int) (lastStoredTime ^ (lastStoredTime >>> 32));
        result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + (int) (ttl ^ (ttl >>> 32));
        result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CompatibilityWanMapEntryView{"
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
