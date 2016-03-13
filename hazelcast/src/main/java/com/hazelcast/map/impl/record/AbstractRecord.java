/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.record;

import com.hazelcast.nio.serialization.Data;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.map.impl.record.RecordStatistics.EMPTY_STATS;

/**
 * @param <V> the type of the value of Record.
 */
@SuppressWarnings("VolatileLongOrDoubleField")
public abstract class AbstractRecord<V> implements Record<V> {

    protected Data key;
    protected long version;
    protected long ttl;
    protected long creationTime;

    protected volatile long lastAccessTime;
    protected volatile long lastUpdateTime;

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "Record can be accessed by only its own partition thread.")
    protected volatile long hits;

    AbstractRecord() {
    }

    @Override
    public final long getVersion() {
        return version;
    }

    @Override
    public final void setVersion(long version) {
        this.version = version;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    @Override
    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public long getHits() {
        return hits;
    }

    @Override
    public void setHits(long hits) {
        this.hits = hits;
    }

    @Override
    public long getCost() {
        final int objectReferenceInBytes = 4;
        final int numberOfLongs = 6;
        long size = numberOfLongs * (Long.SIZE / Byte.SIZE);
        // add key size.
        size += objectReferenceInBytes;
        return size;
    }

    @Override
    public void onUpdate(long now) {
        onAccess(now);

        version++;
        lastUpdateTime = now;
    }

    @Override
    public Object getCachedValueUnsafe() {
        return Record.NOT_CACHED;
    }

    @Override
    public void onAccess(long now) {
        hits++;
        lastAccessTime = now;
    }

    @Override
    public void onStore() {

    }

    @Override
    public RecordStatistics getStatistics() {
        return EMPTY_STATS;
    }

    @Override
    public void setStatistics(RecordStatistics stats) {

    }

    @Override
    public boolean casCachedValue(Object expectedValue, Object newValue) {
        return true;
    }

    @Override
    public Data getKey() {
        return key;
    }

    public void setKey(Data key) {
        this.key = key;
    }

    @Override
    public final long getSequence() {
        return -1L;
    }

    @Override
    public final void setSequence(long sequence) {
    }
}
