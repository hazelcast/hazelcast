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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.operation.ReplicatedMapDataSerializerHook;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicatedRecord<K, V>
        implements IdentifiedDataSerializable {

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong lastAccessTime = new AtomicLong();

    private K key;
    private V value;
    private VectorClock vectorClock;
    private int latestUpdateHash = 0;
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
        return hits.get();
    }

    public long getLastAccessTime() {
        return lastAccessTime.get();
    }

    public void access() {
        hits.incrementAndGet();
        lastAccessTime.set(System.currentTimeMillis());
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


