/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.Clock;
import com.hazelcast.query.impl.Metadata;

import static com.hazelcast.internal.util.TimeUtil.zeroOutMs;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @param <V> the type of value which is in the Record
 */
@SuppressWarnings("checkstyle:methodcount")
public interface Record<V> {
    /**
     * Base time to be used for storing time values as diffs
     * (int) rather than full blown epoch based vals (long)
     * This allows for a space in seconds, of roughly 68 years.
     *
     * Reference value (1514764800000) -
     * Monday, January 1, 2018 12:00:00 AM
     *
     * The fixed time in the past (instead of {@link
     * System#currentTimeMillis()} prevents any time
     * discrepancies among nodes, mis-translated as
     * diffs of -1 ie. {@link Record#UNSET} values. (see.
     * https://github.com/hazelcast/hazelcast-enterprise/issues/2527)
     */
    long EPOCH_TIME = zeroOutMs(1514764800000L);

    /**
     * Represents an unset value. This is the default
     * value of ttl, max-idle or anything unavailable.
     */
    int UNSET = -1;

    /**
     * If not a {@link com.hazelcast.map.impl.record.CachedDataRecord}.
     */
    Object NOT_CACHED = new Object();

    V getValue();

    void setValue(V value);

    /**
     * Returns heap cost of this record in bytes.
     *
     * @return heap cost of this record in bytes.
     */
    long getCost();

    long getVersion();

    void setVersion(long version);

    /**
     * Get current cache value or null.
     * <p>
     * Warning: Do not use this method directly as it
     * might expose arbitrary objects acting as a lock.
     * Use {@link Records#getCachedValue(Record)} instead.
     *
     * @return current cached value or null or cached record mutex.
     */
    Object getCachedValueUnsafe();

    /**
     * Atomically sets the cached value to the given new value
     * if the current cached value {@code ==} the expected value.
     *
     * @param expectedValue the expected cached value
     * @param newValue      the new cached value
     * @return {@code true} if successful. False
     * return indicates that the actual cached value
     * was not equal to the expected cached value.
     */
    boolean casCachedValue(Object expectedValue, Object newValue);

    long getLastAccessTime();

    void setLastAccessTime(long lastAccessTime);

    long getLastUpdateTime();

    void setLastUpdateTime(long lastUpdatedTime);

    long getCreationTime();

    void setCreationTime(long creationTime);

    int getHits();

    void setHits(int hits);

    long getExpirationTime();

    void setExpirationTime(long expirationTime);

    long getLastStoredTime();

    void setLastStoredTime(long lastStoredTime);

    /**
     * Only used for Hot Restart, HDRecord
     *
     * @return current sequence number
     */
    long getSequence();

    /**
     * Only used for Hot Restart, HDRecord
     */
    void setSequence(long sequence);

    void setMetadata(Metadata metadata);

    Metadata getMetadata();

    default long recomputeWithBaseTime(int value) {
        if (value == UNSET) {
            return 0L;
        }

        long exploded = SECONDS.toMillis(value);
        return exploded + EPOCH_TIME;
    }

    default int stripBaseTime(long value) {
        int diff = UNSET;
        if (value > 0) {
            diff = (int) MILLISECONDS.toSeconds(value - EPOCH_TIME);
        }

        return diff;
    }

    default long getTtl() {
        int ttl = getRawTtl();
        return ttl == Integer.MAX_VALUE ? Long.MAX_VALUE : SECONDS.toMillis(ttl);
    }

    default void setTtl(long ttl) {
        long ttlSeconds = MILLISECONDS.toSeconds(ttl);
        if (ttlSeconds == 0 && ttl != 0) {
            ttlSeconds = 1;
        }

        setRawTtl(ttlSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSeconds);
    }

    default long getMaxIdle() {
        int maxIdle = getRawMaxIdle();
        return maxIdle == Integer.MAX_VALUE ? Long.MAX_VALUE : SECONDS.toMillis(maxIdle);
    }

    default void setMaxIdle(long maxIdle) {
        long maxIdleSeconds = MILLISECONDS.toSeconds(maxIdle);
        if (maxIdleSeconds == 0 && maxIdle != 0) {
            maxIdleSeconds = 1;
        }
        setRawMaxIdle(maxIdleSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxIdleSeconds);
    }

    default void onAccess(long now) {
        int hits = getHits();
        if (hits < Integer.MAX_VALUE) {
            // protect against potential overflow
            setHits(hits + 1);
        }

        onAccessSafe(now);
    }

    /**
     * An implementation must be thread safe if the
     * record might be accessed from multiple threads.
     */
    default void onAccessSafe(long now) {
        setLastAccessTime(now);
    }

    default void onUpdate(long now) {
        setVersion(getVersion() + 1);
        setLastUpdateTime(now);
    }

    default void onStore() {
        setLastStoredTime(Clock.currentTimeMillis());
    }

    /**
     * @return record reader writer to be used when
     * serializing/de-serializing this record instance.
     */
    RecordReaderWriter getMatchingRecordReaderWriter();

    /* Below `raw` methods are used during serialization of a record. */

    int getRawTtl();

    int getRawMaxIdle();

    int getRawCreationTime();

    int getRawLastAccessTime();

    int getRawLastUpdateTime();

    void setRawTtl(int readInt);

    void setRawMaxIdle(int readInt);

    void setRawCreationTime(int readInt);

    void setRawLastAccessTime(int readInt);

    void setRawLastUpdateTime(int readInt);

    int getRawLastStoredTime();

    void setRawLastStoredTime(int time);

    int getRawExpirationTime();

    void setRawExpirationTime(int time);
}
