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

import com.hazelcast.internal.util.Clock;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A ReplicatedRecord is the actual data holding entity. It also collects statistic metadata.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedRecord<K, V> {

    private static final AtomicLongFieldUpdater<ReplicatedRecord> HITS
            = AtomicLongFieldUpdater.newUpdater(ReplicatedRecord.class, "hits");

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

    public ReplicatedRecord(K key, V value, long ttlMillis) {
        assert key != null;
        assert value != null;
        this.key = key;
        this.value = value;
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

    public long getTtlMillis() {
        return ttlMillis;
    }

    public V setValue(V value, long ttlMillis) {
        access();
        return setValueInternal(value, ttlMillis);
    }

    public V setValueInternal(V value, long ttlMillis) {
        assert value != null;
        V oldValue = this.value;
        this.value = value;
        this.updateTime = Clock.currentTimeMillis();
        this.ttlMillis = ttlMillis;
        return oldValue;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    private void access() {
        HITS.incrementAndGet(this);
        lastAccessTime = Clock.currentTimeMillis();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
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
        if (!Objects.equals(key, that.key)) {
            return false;
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (ttlMillis ^ (ttlMillis >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ReplicatedRecord{"
                + "key=" + key
                + ", value=" + value
                + ", ttlMillis=" + ttlMillis
                + ", hits=" + HITS.get(this)
                + ", creationTime=" + creationTime
                + ", lastAccessTime=" + lastAccessTime
                + ", updateTime=" + updateTime
                + '}';
    }
}
