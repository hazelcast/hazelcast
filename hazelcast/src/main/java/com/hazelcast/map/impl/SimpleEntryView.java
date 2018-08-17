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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V3_11;

/**
 * SimpleEntryView is an implementation of {@link com.hazelcast.core.EntryView} and also it is writable.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
@SuppressWarnings("checkstyle:methodcount")
public class SimpleEntryView<K, V>
        implements EntryView<K, V>, IdentifiedDataSerializable, Versioned {

    private K key;
    private V value;

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

    public SimpleEntryView(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public SimpleEntryView() {
    }

    @Override
    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public SimpleEntryView<K, V> withKey(K key) {
        this.key = key;
        return this;
    }

    @Override
    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public SimpleEntryView<K, V> withValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public SimpleEntryView<K, V> withCost(long cost) {
        this.cost = cost;
        return this;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public SimpleEntryView<K, V> withCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public SimpleEntryView<K, V> withExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    @Override
    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    public SimpleEntryView<K, V> withHits(long hits) {
        this.hits = hits;
        return this;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public SimpleEntryView<K, V> withLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    public SimpleEntryView<K, V> withLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
        return this;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public SimpleEntryView<K, V> withLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public SimpleEntryView<K, V> withVersion(long version) {
        this.version = version;
        return this;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public SimpleEntryView<K, V> withTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public long getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
    }

    public SimpleEntryView<K, V> withMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    /**
     * Needed for client protocol compatibility.
     */
    @SuppressWarnings("unused")
    public long getEvictionCriteriaNumber() {
        return 0;
    }

    /**
     * Needed for client protocol compatibility.
     */
    @SuppressWarnings("unused")
    public void setEvictionCriteriaNumber(long evictionCriteriaNumber) {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, key);
        IOUtil.writeObject(out, value);
        out.writeLong(cost);
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(hits);
        out.writeLong(lastAccessTime);
        out.writeLong(lastStoredTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(version);
        // writes the deprecated evictionCriteriaNumber to the data output (client protocol compatibility)
        out.writeLong(0);
        out.writeLong(ttl);
        //RU_COMPAT_3_10
        if (out.getVersion().isGreaterOrEqual(V3_11)) {
            out.writeLong(maxIdle);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readObject(in);
        value = IOUtil.readObject(in);
        cost = in.readLong();
        creationTime = in.readLong();
        expirationTime = in.readLong();
        hits = in.readLong();
        lastAccessTime = in.readLong();
        lastStoredTime = in.readLong();
        lastUpdateTime = in.readLong();
        version = in.readLong();
        // reads the deprecated evictionCriteriaNumber from the data input (client protocol compatibility)
        in.readLong();
        ttl = in.readLong();
        //RU_COMPAT_3_10
        if (in.getVersion().isGreaterOrEqual(V3_11)) {
            maxIdle = in.readLong();
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.ENTRY_VIEW;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleEntryView<?, ?> that = (SimpleEntryView<?, ?>) o;
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
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
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
        return "EntryView{"
                + "key=" + key
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
}
