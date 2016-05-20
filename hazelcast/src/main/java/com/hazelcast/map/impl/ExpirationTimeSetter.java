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

package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.record.Record;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNegative;

/**
 * Utility methods for setting TTL and max idle seconds.
 */
public final class ExpirationTimeSetter {

    private ExpirationTimeSetter() {
    }

    /**
     * Sets expiration time if statistics are enabled.
     */
    public static void setExpirationTime(Record record, long maxIdleMillis) {
        final long expirationTime = calculateExpirationTime(record, maxIdleMillis);
        record.setExpirationTime(expirationTime);
    }

    private static long calculateExpirationTime(Record record, long maxIdleMillis) {
        // 1. Calculate TTL expiration time.
        final long ttl = checkedTime(record.getTtl());
        final long ttlExpirationTime = sumForExpiration(ttl, getLifeStartTime(record));

        // 2. Calculate idle expiration time.
        maxIdleMillis = checkedTime(maxIdleMillis);
        final long idleExpirationTime = sumForExpiration(maxIdleMillis, getIdlenessStartTime(record));

        // 3. Select most nearest expiration time.
        return Math.min(ttlExpirationTime, idleExpirationTime);
    }

    /**
     * Returns last-access-time of an entry if it was accessed before, otherwise it returns creation-time of the entry.
     * This calculation is required for max-idle-seconds expiration, because after first creation of an entry via
     * {@link com.hazelcast.core.IMap#put}, the {@code lastAccessTime} is zero till the first access.
     * Any subsequent get or update operation after first put will increase the {@code lastAccessTime}.
     */
    public static long getIdlenessStartTime(Record record) {
        long lastAccessTime = record.getLastAccessTime();
        return lastAccessTime == 0L ? record.getCreationTime() : lastAccessTime;
    }

    /**
     * Returns last-update-time of an entry if it was updated before, otherwise it returns creation-time of the entry.
     * This calculation is required for time-to-live expiration, because after first creation of an entry via
     * {@link com.hazelcast.core.IMap#put}, the {@code lastUpdateTime} is zero till the first update.
     */
    public static long getLifeStartTime(Record record) {
        long lastUpdateTime = record.getLastUpdateTime();
        return lastUpdateTime == 0L ? record.getCreationTime() : lastUpdateTime;
    }

    private static long checkedTime(long time) {
        return time <= 0L ? Long.MAX_VALUE : time;
    }

    private static long sumForExpiration(long criteriaTime, long now) {
        if (criteriaTime < 0 || now < 0) {
            throw new IllegalArgumentException("Parameters can not have negative values");
        }
        if (criteriaTime == 0) {
            return Long.MAX_VALUE;
        }
        final long expirationTime = criteriaTime + now;
        // detect potential overflow.
        if (expirationTime < 0L) {
            return Long.MAX_VALUE;
        }
        return expirationTime;
    }

    /**
     * Picks right TTL value.
     * <p/>
     * Decides which TTL to set;
     * TTL from config or put operation.
     */
    public static long pickTTL(long ttlMillis, long ttlMillisFromConfig) {

        if (ttlMillis < 0L && ttlMillisFromConfig > 0L) {
            return ttlMillisFromConfig;
        }

        if (ttlMillis > 0L) {
            return ttlMillis;
        }

        return 0L;
    }

    public static long calculateMaxIdleMillis(MapConfig mapConfig) {
        final int maxIdleSeconds = mapConfig.getMaxIdleSeconds();
        if (maxIdleSeconds == 0) {
            return Long.MAX_VALUE;
        }
        return TimeUnit.SECONDS.toMillis(maxIdleSeconds);
    }

    public static long calculateTTLMillis(MapConfig mapConfig) {
        final int timeToLiveSeconds = mapConfig.getTimeToLiveSeconds();
        if (timeToLiveSeconds == 0) {
            return Long.MAX_VALUE;
        }
        return TimeUnit.SECONDS.toMillis(timeToLiveSeconds);
    }

    /**
     * Updates records TTL and expiration time.
     */
    public static void updateExpiryTime(Record record, long ttl, MapConfig mapConfig) {

        // Preserve previously set TTL, if TTL < 0.
        if (ttl < 0) {
            ttl = record.getTtl();
        }
        // If TTL == 0, convert it to Long.MAX_VALUE.
        ttl = checkedTime(ttl);

        record.setTtl(ttl);

        long maxIdleMillis = calculateMaxIdleMillis(mapConfig);
        setExpirationTime(record, maxIdleMillis);
    }

    /**
     * On backup partitions, this method delays key`s expiration.
     */
    public static long calculateExpirationWithDelay(long timeInMillis, long delayMillis, boolean backup) {
        checkNotNegative(timeInMillis, "timeInMillis can't be negative");

        if (backup) {
            final long delayedTime = timeInMillis + delayMillis;
            // check for a potential long overflow.
            if (delayedTime < 0L) {
                return Long.MAX_VALUE;
            } else {
                return delayedTime;
            }
        }
        return timeInMillis;
    }
}
