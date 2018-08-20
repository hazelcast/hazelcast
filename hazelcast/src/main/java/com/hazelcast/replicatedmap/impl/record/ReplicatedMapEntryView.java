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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.core.EntryView;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;

import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V3_11;

public class ReplicatedMapEntryView<K, V>
        implements EntryView, IdentifiedDataSerializable, Versioned {

    private static final int NOT_AVAILABLE = -1;

    private K key;
    private V value;
    private long creationTime;
    private long hits;
    private long lastAccessTime;
    private long lastUpdateTime;
    private long ttl;
    private long maxIdle;

    public ReplicatedMapEntryView() {
    }

    @Override
    public K getKey() {
        return key;
    }

    public ReplicatedMapEntryView<K, V> setKey(K key) {
        this.key = key;
        return this;
    }

    @Override
    public V getValue() {
        return value;
    }

    public ReplicatedMapEntryView<K, V> setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public long getCost() {
        return NOT_AVAILABLE;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    public ReplicatedMapEntryView<K, V> setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    @Override
    public long getExpirationTime() {
        return NOT_AVAILABLE;
    }

    @Override
    public long getHits() {
        return hits;
    }

    public ReplicatedMapEntryView<K, V> setHits(long hits) {
        this.hits = hits;
        return this;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public ReplicatedMapEntryView<K, V> setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    @Override
    public long getLastStoredTime() {
        return NOT_AVAILABLE;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public ReplicatedMapEntryView<K, V> setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    @Override
    public long getVersion() {
        return NOT_AVAILABLE;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    @Override
    public long getMaxIdle() {
        return maxIdle;
    }

    public ReplicatedMapEntryView<K, V> setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeObject(out, key);
        IOUtil.writeObject(out, value);
        out.writeLong(creationTime);
        out.writeLong(hits);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
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
        creationTime = in.readLong();
        hits = in.readLong();
        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();
        ttl = in.readLong();
        //RU_COMPAT_3_10
        if (in.getVersion().isGreaterOrEqual(V3_11)) {
            maxIdle = in.readLong();
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.ENTRY_VIEW;
    }

    @Override
    public String toString() {
        return "ReplicatedMapEntryView{"
                + "key=" + key
                + ", value=" + value
                + ", creationTime=" + creationTime
                + ", hits=" + hits
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", ttl=" + ttl
                + '}';
    }
}
