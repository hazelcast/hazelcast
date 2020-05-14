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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.Objects;

/**
 * WAN heap based implementation of {@link EntryView} for processing
 * compatibility WAN replication events from 3.x clusters.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 */
public class CompatibilityWanMapEntryView<K, V> implements EntryView<K, V>, IdentifiedDataSerializable, Versioned {

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
    private long maxIdle = Long.MAX_VALUE;

    public CompatibilityWanMapEntryView() {
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " should not be serialized!");
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
        if (!in.getVersion().isUnknown()) {
            // this means we have serialized SimpleEntryView which
            // is both Versioned and contains an additional maxIdle field
            // as opposed to WanMapEntryView which is not Versioned
            // and does not have an additional field
            // SimpleEntryView is sent only for merkle tree sync
            maxIdle = in.readLong();
        }
    }

    @Override
    public int getFactoryId() {
        return CompatibilityMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CompatibilityMapDataSerializerHook.ENTRY_VIEW;
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
        CompatibilityWanMapEntryView<?, ?> that = (CompatibilityWanMapEntryView<?, ?>) o;
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
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, cost, creationTime, expirationTime,
                hits, lastAccessTime, lastStoredTime, lastUpdateTime, version, ttl, maxIdle);
    }

    @Override
    public String toString() {
        return "CompatibilityWanMapEntryView{"
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
                + '}';
    }
}
