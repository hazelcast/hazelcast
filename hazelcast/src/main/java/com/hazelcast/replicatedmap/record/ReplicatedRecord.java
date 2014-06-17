/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.record;

import com.hazelcast.core.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.operation.ReplicatedMapDataSerializerHook;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A ReplicatedRecord is the actual data holding entity. It also collects statistic metadata.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedRecord<K, V>
        implements IdentifiedDataSerializable {

    private static final AtomicLongFieldUpdater<ReplicatedRecord> HITS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ReplicatedRecord.class, "hits");
    private static final AtomicLongFieldUpdater<ReplicatedRecord> LAST_ACCESS_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ReplicatedRecord.class, "hits");
    private static final AtomicReferenceFieldUpdater<ReplicatedRecord, VectorClock> VECTOR_CLOCK_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ReplicatedRecord.class, VectorClock.class, "vectorClock");

    // These fields are only accessed through the updaters
    private volatile long hits;
    private volatile long lastAccessTime;

    private K key;
    private V value;
    private volatile VectorClock vectorClock;
    private int latestUpdateHash;
    private long ttlMillis;
    private volatile long updateTime = System.currentTimeMillis();

    public ReplicatedRecord() {
    }

    public ReplicatedRecord(K key, V value, VectorClock vectorClock, int hash, long ttlMillis) {
        this.key = key;
        this.value = value;
        this.vectorClock = vectorClock;
        this.latestUpdateHash = hash;
        this.ttlMillis = ttlMillis;
    }

    public K getKey() {
        access();
        return key;
    }

    public V getValue() {
        access();
        return value;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public VectorClock applyAndIncrementVectorClock(VectorClock otherVectorClock, Member member) {
        for (;;) {
            VectorClock vectorClock = this.vectorClock;
            VectorClock vectorClockCopy = VectorClock.copyVector(vectorClock);
            vectorClockCopy = vectorClockCopy.applyVector0(otherVectorClock);
            vectorClockCopy = vectorClockCopy.incrementClock0(member);
            if (VECTOR_CLOCK_UPDATER.compareAndSet(this, vectorClock, vectorClockCopy)) {
                return vectorClockCopy;
            }
        }
    }

    public VectorClock applyVectorClock(VectorClock otherVectorClock) {
        for (;;) {
            VectorClock vectorClock = this.vectorClock;
            VectorClock vectorClockCopy = VectorClock.copyVector(vectorClock);
            vectorClockCopy = vectorClockCopy.applyVector0(otherVectorClock);
            if (VECTOR_CLOCK_UPDATER.compareAndSet(this, vectorClock, vectorClockCopy)) {
                return vectorClockCopy;
            }
        }
    }

    public VectorClock incrementVectorClock(Member member) {
        for (;;) {
            VectorClock vectorClock = this.vectorClock;
            VectorClock vectorClockCopy = VectorClock.copyVector(vectorClock);
            vectorClockCopy = vectorClockCopy.incrementClock0(member);
            if (VECTOR_CLOCK_UPDATER.compareAndSet(this, vectorClock, vectorClockCopy)) {
                return vectorClockCopy;
            }
        }
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public V setValue(V value, int hash, long ttlMillis) {
        access();
        V oldValue = this.value;
        this.value = value;
        this.latestUpdateHash = hash;
        this.updateTime = System.currentTimeMillis();
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

    public void access() {
        HITS_UPDATER.incrementAndGet(this);
        LAST_ACCESS_TIME_UPDATER.set(this, System.currentTimeMillis());
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
        vectorClock.writeData(out);
        out.writeInt(latestUpdateHash);
        out.writeLong(ttlMillis);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        key = in.readObject();
        value = in.readObject();
        vectorClock = new VectorClock();
        vectorClock.readData(in);
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
        sb.append(", vector=").append(vectorClock);
        sb.append(", latestUpdateHash=").append(latestUpdateHash);
        sb.append(", ttlMillis=").append(ttlMillis);
        sb.append('}');
        return sb.toString();
    }

}


