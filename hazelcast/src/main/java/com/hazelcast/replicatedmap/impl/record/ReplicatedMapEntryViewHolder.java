/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class ReplicatedMapEntryViewHolder implements IdentifiedDataSerializable {

    private Data key;
    private Data value;
    private long creationTime;
    private long hits;
    private long lastAccessTime;
    private long lastUpdateTime;
    private long ttlMillis;

    public ReplicatedMapEntryViewHolder() {
    }

    public ReplicatedMapEntryViewHolder(Data key, Data value, long creationTime, long hits, long lastAccessTime,
                                        long lastUpdateTime, long ttlMillis) {
        this.key = key;
        this.value = value;
        this.creationTime = creationTime;
        this.hits = hits;
        this.lastAccessTime = lastAccessTime;
        this.lastUpdateTime = lastUpdateTime;
        this.ttlMillis = ttlMillis;
    }

    public Data getKey() {
        return key;
    }

    public Data getValue() {
        return value;
    }

    public ReplicatedMapEntryViewHolder setKey(Data key) {
        checkNotNull(key);
        this.key = key;
        return this;
    }

    public ReplicatedMapEntryViewHolder setValue(Data value) {
        checkNotNull(value);
        this.value = value;
        return this;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public ReplicatedMapEntryViewHolder setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    public long getHits() {
        return hits;
    }

    public ReplicatedMapEntryViewHolder setHits(long hits) {
        this.hits = hits;
        return this;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public ReplicatedMapEntryViewHolder setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public ReplicatedMapEntryViewHolder setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public ReplicatedMapEntryViewHolder setTtlMillis(long ttlMillis) {
        this.ttlMillis = ttlMillis;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.ENTRY_VIEW_HOLDER;
    }

    @Override
    public String toString() {
        return "ReplicatedMapEntryViewHolder{"
                + "key=" + getKey()
                + ", value=" + getValue()
                + ", creationTime=" + creationTime
                + ", hits=" + hits
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", ttlMillis=" + ttlMillis
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, key);
        IOUtil.writeData(out, value);
        out.writeLong(creationTime);
        out.writeLong(hits);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(ttlMillis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readData(in);
        value = IOUtil.readData(in);
        creationTime = in.readLong();
        hits = in.readLong();
        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();
        ttlMillis = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicatedMapEntryViewHolder that = (ReplicatedMapEntryViewHolder) o;
        return creationTime == that.creationTime && hits == that.hits && lastAccessTime == that.lastAccessTime
                && lastUpdateTime == that.lastUpdateTime && ttlMillis == that.ttlMillis && Objects.equals(key, that.key)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, creationTime, hits, lastAccessTime, lastUpdateTime, ttlMillis);
    }
}
