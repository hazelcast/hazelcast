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

package com.hazelcast.map.impl.record;

import com.hazelcast.nio.serialization.Data;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * @param <V> the type of the value of Record.
 */
@SuppressWarnings("VolatileLongOrDoubleField")
public abstract class AbstractRecord<V> implements Record<V> {

    private static final int NUMBER_OF_LONGS = 6;

    protected Data key;
    protected long version;
    protected long ttl;
    protected long creationTime;

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "Record can be accessed by only its own partition thread.")
    protected volatile long hits;
    protected volatile long lastAccessTime;
    protected volatile long lastUpdateTime;

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
        return REFERENCE_COST_IN_BYTES + NUMBER_OF_LONGS * LONG_SIZE_IN_BYTES;
    }

    @Override
    public void onUpdate(long now) {
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
        return NOT_AVAILABLE;
    }

    @Override
    public final void setSequence(long sequence) {
    }

    @Override
    public long getExpirationTime() {
        return NOT_AVAILABLE;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
    }

    @Override
    public long getLastStoredTime() {
        return NOT_AVAILABLE;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractRecord<?> that = (AbstractRecord<?>) o;
        if (version != that.version) {
            return false;
        }
        if (ttl != that.ttl) {
            return false;
        }
        if (creationTime != that.creationTime) {
            return false;
        }
        if (hits != that.hits) {
            return false;
        }
        if (lastAccessTime != that.lastAccessTime) {
            return false;
        }
        if (lastUpdateTime != that.lastUpdateTime) {
            return false;
        }
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + (int) (ttl ^ (ttl >>> 32));
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (hits ^ (hits >>> 32));
        result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
        result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
        return result;
    }
}
