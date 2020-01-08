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

package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.record.Record;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility methods for setting TTL and max idle seconds.
 */
public final class ExpirationTimeSetter {

    private ExpirationTimeSetter() {
    }

    /**
     * Sets expiration time if statistics are enabled.
     */
    public static void setExpirationTime(Record record) {
        long expirationTime = calculateExpirationTime(record);
        record.setExpirationTime(expirationTime);
    }

    private static long calculateExpirationTime(Record record) {
        // calculate TTL expiration time
        long ttl = checkedTime(record.getTtl());
        long ttlExpirationTime = sumForExpiration(ttl, getLifeStartTime(record));

        // calculate MaxIdle expiration time
        long maxIdle = checkedTime(record.getMaxIdle());
        long maxIdleExpirationTime = sumForExpiration(maxIdle, getIdlenessStartTime(record));
        // select most nearest expiration time
        return Math.min(ttlExpirationTime, maxIdleExpirationTime);
    }

    /**
     * Returns last-access-time of an entry if it was accessed before, otherwise it returns creation-time of the entry.
     * This calculation is required for max-idle-seconds expiration, because after first creation of an entry via
     * {@link IMap#put}, the {@code lastAccessTime} is zero till the first access.
     * Any subsequent get or update operation after first put will increase the {@code lastAccessTime}.
     */
    public static long getIdlenessStartTime(Record record) {
        long lastAccessTime = record.getLastAccessTime();
        return lastAccessTime <= 0 ? record.getCreationTime() : lastAccessTime;
    }

    /**
     * Returns last-update-time of an entry if it was updated before, otherwise it returns creation-time of the entry.
     * This calculation is required for time-to-live expiration, because after first creation of an entry via
     * {@link IMap#put}, the {@code lastUpdateTime} is zero till the first update.
     */
    public static long getLifeStartTime(Record record) {
        long lastUpdateTime = record.getLastUpdateTime();
        return lastUpdateTime <= 0 ? record.getCreationTime() : lastUpdateTime;
    }

    private static long checkedTime(long time) {
        return time <= 0 ? Long.MAX_VALUE : time;
    }

    private static long sumForExpiration(long criteriaTime, long now) {
        if (criteriaTime < 0 || now < 0) {
            throw new IllegalArgumentException("Parameters can not have negative values");
        }
        if (criteriaTime == 0) {
            return Long.MAX_VALUE;
        }
        long expirationTime = criteriaTime + now;
        // detect potential overflow
        if (expirationTime < 0) {
            return Long.MAX_VALUE;
        }
        return expirationTime;
    }

    /**
     * Updates records TTL and expiration time.
     *
     * @param operationTTLMillis user provided TTL during operation call like put with TTL
     * @param record             record to be updated
     * @param mapConfig          map config object
     * @param consultMapConfig   give {@code true} if this update should consult map ttl configuration,
     *                           otherwise give {@code false} to indicate update
     */
    public static void setExpirationTimes(long operationTTLMillis, long operationMaxIdleMillis, Record record,
                                          MapConfig mapConfig, boolean consultMapConfig) {
        long ttlMillis = pickTTLMillis(operationTTLMillis, mapConfig, consultMapConfig);
        long maxIdleMillis = pickMaxIdleMillis(operationMaxIdleMillis, mapConfig, consultMapConfig);

        record.setTtl(ttlMillis);
        record.setMaxIdle(maxIdleMillis);
        setExpirationTime(record);
    }

    /**
     * Decides if TTL millis should to be set on record.
     *
     * @param operationTTLMillis user provided TTL during operation call like put with TTL
     * @param mapConfig          used to get configured TTL
     * @param consultMapConfig   give {@code true} if this update should consult map ttl configuration,
     *                           otherwise give {@code false}
     * @return TTL value in millis to set to record
     */
    private static long pickTTLMillis(long operationTTLMillis, MapConfig mapConfig,
                                      boolean consultMapConfig) {
        // if user set operationTTLMillis when calling operation, use it
        if (operationTTLMillis > 0) {
            return checkedTime(operationTTLMillis);
        }

        // if this is the first creation of entry, try to get TTL from mapConfig
        if (consultMapConfig && operationTTLMillis < 0 && mapConfig.getTimeToLiveSeconds() > 0) {
            return checkedTime(SECONDS.toMillis(mapConfig.getTimeToLiveSeconds()));
        }

        // if we are here, entry should live forever
        return Long.MAX_VALUE;
    }

    private static long pickMaxIdleMillis(long operationMaxIdleMillis, MapConfig mapConfig,
                                          boolean entryCreated) {
        // if user set operationMaxIdleMillis when calling operation, use it
        if (operationMaxIdleMillis > 0) {
            return checkedTime(operationMaxIdleMillis);
        }

        // if this is the first creation of entry, try to get MaxIdle from mapConfig
        if (entryCreated && operationMaxIdleMillis < 0 && mapConfig.getMaxIdleSeconds() > 0) {
            return checkedTime(SECONDS.toMillis(mapConfig.getMaxIdleSeconds()));
        }

        // if we are here, entry should live forever
        return Long.MAX_VALUE;
    }

    /**
     * On backup partitions, this method delays key's expiration.
     */
    public static long calculateExpirationWithDelay(long timeInMillis, long delayMillis, boolean backup) {
        checkNotNegative(timeInMillis, "timeInMillis can't be negative");

        if (backup) {
            long delayedTime = timeInMillis + delayMillis;
            // check for a potential long overflow
            if (delayedTime < 0) {
                return Long.MAX_VALUE;
            } else {
                return delayedTime;
            }
        }
        return timeInMillis;
    }
}
