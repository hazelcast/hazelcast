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

package com.hazelcast.internal.nearcache;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.Expirable;

import java.util.UUID;

/**
 * An expirable and evictable data object
 * which represents a Near Cache entry.
 *
 * Record of {@link NearCacheRecordStore}.
 *
 * @param <V> the type of the value
 *            stored by this {@link NearCacheRecord}
 * @see com.hazelcast.internal.eviction.Expirable
 * @see com.hazelcast.internal.eviction.Evictable
 */
public interface NearCacheRecord<V> extends Expirable, Evictable<V> {

    int TIME_NOT_SET = -1;

    long NOT_RESERVED = -1;

    long READ_PERMITTED = -2;

    /**
     * Sets the value of this {@link NearCacheRecord}.
     *
     * @param value the value for this {@link NearCacheRecord}
     */
    void setValue(V value);

    /**
     * Sets the creation time of this {@link Evictable} in milliseconds.
     *
     * @param time the creation time for this {@link Evictable} in milliseconds
     */
    void setCreationTime(long time);

    /**
     * Sets the access time of this {@link Evictable} in milliseconds.
     *
     * @param time the latest access time of this {@link Evictable} in milliseconds
     */
    void setLastAccessTime(long time);

    /**
     * Sets the access hit count of this {@link Evictable}.
     *
     * @param hit the access hit count for this {@link Evictable}
     */
    void setHits(int hit);

    /**
     * Increases the access hit count of this {@link Evictable} by {@code 1}.
     */
    void incrementHits();

    /**
     * It can have 2 different value:
     *
     * 1. {@link #READ_PERMITTED} if no
     * update is happening on this record.
     *
     * 2. A `long` reservation id to indicate
     * an update is happening on this record now.
     *
     * @return reservation that this record has.
     */
    long getReservationId();

    /**
     * @param reservationId net reservation id to set.
     */
    void setReservationId(long reservationId);

    /**
     * @return the partition ID of this record
     */
    int getPartitionId();

    /**
     * @param partitionId the partition ID of this record
     */
    void setPartitionId(int partitionId);

    /**
     * @return last known invalidation sequence
     * at time of this records' creation
     */
    long getInvalidationSequence();

    /**
     * @param sequence last known invalidation
     *                 sequence at time of this records' creation
     */
    void setInvalidationSequence(long sequence);

    /**
     * @param uuid last known UUID of invalidation
     *             source at time of this records' creation
     */
    void setUuid(UUID uuid);

    /**
     * @return {@code true} if supplied UUID equals
     * existing one, otherwise and when one of supplied
     * or existing is null returns {@code false}
     */
    boolean hasSameUuid(UUID uuid);

    boolean isCachedAsNull();

    void setCachedAsNull(boolean valueCachedAsNull);

    default boolean isExpiredAt(long now) {
        long expirationTime = getExpirationTime();
        return (expirationTime > 0L) && (expirationTime <= now);
    }

    /**
     * Checks whether the maximum idle time is passed with
     * respect to the provided time without any access
     * during this time period as {@code maxIdleSeconds}.
     *
     * @param maxIdleMilliSeconds maximum idle time in milliseconds
     * @param now                 current time in milliseconds @return
     *                            {@code true} if exceeds max idle seconds, otherwise {@code false}
     */
    default boolean isIdleAt(long maxIdleMilliSeconds, long now) {
        if (maxIdleMilliSeconds <= 0) {
            return false;
        }

        long lastAccessTime = getLastAccessTime();
        return lastAccessTime > 0L
                ? lastAccessTime + maxIdleMilliSeconds < now
                : getCreationTime() + maxIdleMilliSeconds < now;
    }
}
