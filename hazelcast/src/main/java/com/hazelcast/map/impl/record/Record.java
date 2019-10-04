/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
     * Reference value (1514764800000) - Monday, January 1, 2018 12:00:00 AM
     *
     * The fixed time in the past (instead of {@link
     * System#currentTimeMillis()} prevents any time discrepancies among nodes,
     * mis-translated as diffs of -1 ie. {@link Record#NOT_AVAILABLE} values.
     * (see. https://github.com/hazelcast/hazelcast-enterprise/issues/2527)
     */
    long EPOCH_TIME = zeroOutMs(1514764800000L);

    /**
     * Default TTL value of a record.
     */
    long DEFAULT_TTL = -1L;

    /**
     * Default Max Idle value of a record.
     */
    long DEFAULT_MAX_IDLE = -1L;

    /**
     * If not a {@link com.hazelcast.map.impl.record.CachedDataRecord}.
     */
    Object NOT_CACHED = new Object();

    int NOT_AVAILABLE = -1;

    Data getKey();

    void setKey(Data key);

    V getValue();

    void setValue(V value);

    void onAccess(long now);

    /**
     * An implementation must be thread safe if the record might be accessed from multiple threads.
     */
    void onAccessSafe(long now);

    void onUpdate(long now);

    void onStore();

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
     * Warning: Do not use this method directly as it might expose arbitrary objects acting as a lock.
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
     * @return {@code true} if successful. False return indicates that
     * the actual cached value was not equal to the expected cached value.
     */
    boolean casCachedValue(Object expectedValue, Object newValue);

    long getTtl();

    void setTtl(long ttl);

    long getMaxIdle();

    void setMaxIdle(long maxIdle);

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
     * @return
     */
    long getSequence();

    /**
     * Only used for Hot Restart, HDRecord
     */
    void setSequence(long sequence);

    void setMetadata(Metadata metadata);

    Metadata getMetadata();

    default long recomputeWithBaseTime(int value) {
        if (value == NOT_AVAILABLE) {
            return 0L;
        }

        long exploded = SECONDS.toMillis(value);
        return exploded + EPOCH_TIME;
    }

    default int stripBaseTime(long value) {
        int diff = NOT_AVAILABLE;
        if (value > 0) {
            diff = (int) MILLISECONDS.toSeconds(value - EPOCH_TIME);
        }

        return diff;
    }

    // Below raw methods are used during serialization of a record.
    /**
     * @return record reader writer id to be used
     * when serializing/de-serializing this record object.
     */
    RecordReaderWriter getMatchingRecordReaderWriter();

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
