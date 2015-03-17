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

import com.hazelcast.core.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A ReplicatedRecord is the actual data holding entity. It also collects statistic metadata.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedRecord<K, V> implements IdentifiedDataSerializable {

    private static final AtomicLongFieldUpdater<ReplicatedRecord> HITS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ReplicatedRecord.class, "hits");
    private static final AtomicReferenceFieldUpdater<ReplicatedRecord, VectorClockTimestamp> VECTOR_CLOCK_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ReplicatedRecord.class, VectorClockTimestamp.class, "vectorClockTimestamp");

    // These fields are only accessed through the updaters
    @SuppressWarnings("unused")
    private volatile long hits;
    @SuppressWarnings("unused")
    private volatile long lastAccessTime;

    private K key;
    private V value;
    private volatile VectorClockTimestamp vectorClockTimestamp;
    private int latestUpdateHash;
    private long ttlMillis;
    private volatile long updateTime = Clock.currentTimeMillis();

    public ReplicatedRecord() {
    }

    public ReplicatedRecord(K key, V value, VectorClockTimestamp vectorClockTimestamp, int hash, long ttlMillis) {
        this.key = key;
        this.value = value;
        this.vectorClockTimestamp = vectorClockTimestamp;
        this.latestUpdateHash = hash;
        this.ttlMillis = ttlMillis;
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

    public VectorClockTimestamp getVectorClockTimestamp() {
        return vectorClockTimestamp;
    }

    public VectorClockTimestamp applyAndIncrementVectorClock(VectorClockTimestamp otherVectorClockTimestamp, Member member) {
        for (;;) {
            VectorClockTimestamp vectorClockTimestamp = this.vectorClockTimestamp;
            VectorClockTimestamp vectorClockTimestampCopy = VectorClockTimestamp.copyVector(vectorClockTimestamp);
            vectorClockTimestampCopy = vectorClockTimestampCopy.applyVector0(otherVectorClockTimestamp);
            vectorClockTimestampCopy = vectorClockTimestampCopy.incrementClock0(member);
            if (VECTOR_CLOCK_UPDATER.compareAndSet(this, vectorClockTimestamp, vectorClockTimestampCopy)) {
                return vectorClockTimestampCopy;
            }
        }
    }

    public VectorClockTimestamp applyVectorClock(VectorClockTimestamp otherVectorClockTimestamp) {
        for (;;) {
            VectorClockTimestamp vectorClockTimestamp = this.vectorClockTimestamp;
            VectorClockTimestamp vectorClockTimestampCopy = VectorClockTimestamp.copyVector(vectorClockTimestamp);
            vectorClockTimestampCopy = vectorClockTimestampCopy.applyVector0(otherVectorClockTimestamp);
            if (VECTOR_CLOCK_UPDATER.compareAndSet(this, vectorClockTimestamp, vectorClockTimestampCopy)) {
                return vectorClockTimestampCopy;
            }
        }
    }

    public VectorClockTimestamp incrementVectorClock(Member member) {
        for (;;) {
            VectorClockTimestamp vectorClockTimestamp = this.vectorClockTimestamp;
            VectorClockTimestamp vectorClockTimestampCopy = VectorClockTimestamp.copyVector(vectorClockTimestamp);
            vectorClockTimestampCopy = vectorClockTimestampCopy.incrementClock0(member);
            if (VECTOR_CLOCK_UPDATER.compareAndSet(this, vectorClockTimestamp, vectorClockTimestampCopy)) {
                return vectorClockTimestampCopy;
            }
        }
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public V setValue(V value, int hash, long ttlMillis) {
        access();
        return setValueInternal(value, hash, ttlMillis);
    }

    public V setValueInternal(V value, int hash, long ttlMillis) {
        V oldValue = this.value;
        this.value = value;
        this.latestUpdateHash = hash;
        this.updateTime = Clock.currentTimeMillis();
        this.ttlMillis = ttlMillis;
        return oldValue;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public int getLatestUpdateHash() {
        return latestUpdateHash;
    }

    public long getHits() {
        return hits;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    private void access() {
        HITS_UPDATER.incrementAndGet(this);
        lastAccessTime = Clock.currentTimeMillis();
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
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeObject(key);
        out.writeObject(value);
        vectorClockTimestamp.writeData(out);
        out.writeInt(latestUpdateHash);
        out.writeLong(ttlMillis);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        key = in.readObject();
        value = in.readObject();
        vectorClockTimestamp = new VectorClockTimestamp();
        vectorClockTimestamp.readData(in);
        latestUpdateHash = in.readInt();
        ttlMillis = in.readLong();
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

        if (latestUpdateHash != that.latestUpdateHash) {
            return false;
        }
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
        result = 31 * result + latestUpdateHash;
        result = 31 * result + (int) (ttlMillis ^ (ttlMillis >>> 32));
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ReplicatedRecord{");
        sb.append("key=").append(key);
        sb.append(", value=").append(value);
        sb.append(", vector=").append(vectorClockTimestamp);
        sb.append(", latestUpdateHash=").append(latestUpdateHash);
        sb.append(", ttlMillis=").append(ttlMillis);
        sb.append('}');
        return sb.toString();
    }
}


