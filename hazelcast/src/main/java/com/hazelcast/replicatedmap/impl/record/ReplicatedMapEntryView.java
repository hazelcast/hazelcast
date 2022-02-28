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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;

public class ReplicatedMapEntryView<K, V>
        implements EntryView, IdentifiedDataSerializable {

    private static final int NOT_AVAILABLE = -1;

    private Object key;
    private Object value;
    private long creationTime;
    private long hits;
    private long lastAccessTime;
    private long lastUpdateTime;
    private long ttl;
    private long maxIdle;

    private SerializationService serializationService;

    public ReplicatedMapEntryView() {
    }

    public ReplicatedMapEntryView(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public K getKey() {
        // Null serializationService means, use raw type without any conversion
        if (serializationService != null) {
            key = serializationService.toObject(key);
        }
        return (K) key;
    }

    public ReplicatedMapEntryView<K, V> setKey(K key) {
        this.key = key;
        return this;
    }

    @Override
    public V getValue() {
        // Null serializationService means, use raw type without any conversion
        if (serializationService != null) {
            value = serializationService.toObject(value);
        }
        return (V) value;
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
        IOUtil.writeObject(out, getKey());
        IOUtil.writeObject(out, getValue());
        out.writeLong(creationTime);
        out.writeLong(hits);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(ttl);
        out.writeLong(maxIdle);
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
        maxIdle = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.ENTRY_VIEW;
    }

    @Override
    public String toString() {
        return "ReplicatedMapEntryView{"
                + "key=" + getKey()
                + ", value=" + getValue()
                + ", creationTime=" + creationTime
                + ", hits=" + hits
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", ttl=" + ttl
                + ", maxIdle=" + maxIdle
                + '}';
    }
}
