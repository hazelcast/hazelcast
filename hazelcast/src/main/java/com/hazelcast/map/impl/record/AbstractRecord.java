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

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @param <V> the type of the value of Record.
 */
@SuppressWarnings({ "checkstyle:methodcount", "VolatileLongOrDoubleField" })
public abstract class AbstractRecord<V> implements Record<V> {

    /**
     * Base time to be used for storing time values as diffs (int) rather than full blown epoch based vals (long)
     * This allows for a space in seconds, of roughly 68 years.
     */
    public static final long EPOCH_TIME = zeroOutMillis(System.currentTimeMillis());

    private static final int NUMBER_OF_LONGS = 2;
    private static final int NUMBER_OF_INTS = 5;

    protected Data key;
    protected long version;
    protected int ttl;
    protected int maxIdle;

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "Record can be accessed by only its own partition thread.")
    protected volatile long hits;
    private volatile int lastAccessTime = NOT_AVAILABLE;
    private volatile int lastUpdateTime = NOT_AVAILABLE;
    private int creationTime = NOT_AVAILABLE;

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
        return ttl == Integer.MAX_VALUE ? Long.MAX_VALUE : SECONDS.toMillis(ttl);
    }

    @Override
    public void setTtl(long ttl) {
        long ttlSeconds = MILLISECONDS.toSeconds(ttl);
        if (ttlSeconds == 0 && ttl != 0) {
            ttlSeconds = 1;
        }

        this.ttl =  ttlSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSeconds;
    }

    @Override
    public long getMaxIdle() {
        return maxIdle == Integer.MAX_VALUE ? Long.MAX_VALUE : SECONDS.toMillis(maxIdle);
    }

    @Override
    public void setMaxIdle(long maxIdle) {
        long maxIdleSeconds = MILLISECONDS.toSeconds(maxIdle);
        if (maxIdleSeconds == 0 && maxIdle != 0) {
            maxIdleSeconds = 1;
        }
        this.maxIdle = maxIdleSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxIdleSeconds;
    }

    @Override
    public long getLastAccessTime() {
        return recomputeWithBaseTime(lastAccessTime);
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = stripBaseTime(lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return recomputeWithBaseTime(lastUpdateTime);
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = stripBaseTime(lastUpdateTime);
    }

    @Override
    public long getCreationTime() {
        return recomputeWithBaseTime(creationTime);
    }

    @Override
    public void setCreationTime(long creationTime) {
        this.creationTime = stripBaseTime(creationTime);
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
        return REFERENCE_COST_IN_BYTES + (NUMBER_OF_LONGS * LONG_SIZE_IN_BYTES) + (NUMBER_OF_INTS * INT_SIZE_IN_BYTES);
    }

    @Override
    public void onUpdate(long now) {
        version++;
        lastUpdateTime = stripBaseTime(now);
    }

    @Override
    public Object getCachedValueUnsafe() {
        return Record.NOT_CACHED;
    }

    @Override
    public void onAccess(long now) {
        hits++;
        lastAccessTime = stripBaseTime(now);
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
        if (maxIdle != that.maxIdle) {
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
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + ttl;
        result = 31 * result + maxIdle;
        result = 31 * result + creationTime;
        result = 31 * result + (int) (hits ^ (hits >>> 32));
        result = 31 * result + lastAccessTime;
        result = 31 * result + lastUpdateTime;
        return result;
    }

    protected long recomputeWithBaseTime(int value) {
        if (value == NOT_AVAILABLE) {
            return 0L;
        }

        long exploded = SECONDS.toMillis(value);
        return exploded + EPOCH_TIME;
    }

    protected int stripBaseTime(long value) {
        int diff = NOT_AVAILABLE;
        if (value > 0) {
            diff = (int) MILLISECONDS.toSeconds(value - EPOCH_TIME);
        }

        return diff;
    }

    private static long zeroOutMillis(long value) {
        return SECONDS.toMillis(MILLISECONDS.toSeconds(value));
    }
}
