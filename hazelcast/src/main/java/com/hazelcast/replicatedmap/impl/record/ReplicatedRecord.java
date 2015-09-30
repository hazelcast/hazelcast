/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.util.Clock;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A ReplicatedRecord is the actual data holding entity. It also collects statistic metadata.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedRecord<K, V> implements IdentifiedDataSerializable {

    private static final AtomicLongFieldUpdater<ReplicatedRecord> HITS = AtomicLongFieldUpdater
            .newUpdater(ReplicatedRecord.class, "hits");

    // These fields are only accessed through the updaters
    @SuppressWarnings("unused")
    private volatile long hits;
    @SuppressWarnings("unused")
    private volatile long lastAccessTime = Clock.currentTimeMillis();

    private K key;
    private V value;
    private long ttlMillis;
    private volatile long updateTime = Clock.currentTimeMillis();
    private volatile long creationTime = Clock.currentTimeMillis();
    private int partitionId;

    public ReplicatedRecord() {
    }

    public ReplicatedRecord(K key, V value, long ttlMillis, int partitionId) {
        this.key = key;
        this.value = value;
        this.ttlMillis = ttlMillis;
        this.partitionId = partitionId;
    }

    public K getKey() {
        access();
        return getKeyInternal();
    }

    public K getKeyInternal() {
        return key;
    }

    public V getValue() {
        access();
        return getValueInternal();
    }

    public V getValueInternal() {
        return value;
    }

    public boolean isTombstone() {
        return value == null;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public V setValue(V value, long ttlMillis) {
        access();
        return setValueInternal(value, ttlMillis);
    }

    public V setValueInternal(V value, long ttlMillis) {
        V oldValue = this.value;
        this.value = value;
        this.updateTime = Clock.currentTimeMillis();
        this.ttlMillis = ttlMillis;
        return oldValue;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public long getHits() {
        return hits;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    private void access() {
        HITS.incrementAndGet(this);
        lastAccessTime = Clock.currentTimeMillis();
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.RECORD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(value);
        out.writeLong(ttlMillis);
        out.writeLong(updateTime);
        out.writeLong(creationTime);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        value = in.readObject();
        ttlMillis = in.readLong();
        updateTime = in.readLong();
        creationTime = in.readLong();
        partitionId = in.readInt();
    }

    //CHECKSTYLE:OFF
    // Deactivated due to complexity of the equals method
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReplicatedRecord that = (ReplicatedRecord) o;

        if (ttlMillis != that.ttlMillis) {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        }

        return true;
    }
    //CHECKSTYLE:ON

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (ttlMillis ^ (ttlMillis >>> 32));
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ReplicatedRecord{");
        sb.append("key=").append(key);
        sb.append(", value=").append(value);
        sb.append(", ttlMillis=").append(ttlMillis);
        sb.append('}');
        return sb.toString();
    }
}


