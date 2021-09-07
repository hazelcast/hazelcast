/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility methods for setting TTL and max idle seconds.
 */
public final class ExpirationTimeSetter {

    private ExpirationTimeSetter() {
    }

    public static long getNextExpirationTime(Record record, long ttlMillis,
                                             long maxIdleMillis, long now,
                                             boolean perEntryStatsEnabled) {
        long lastUpdateTime = findLastUpdateTime(record, ttlMillis,
                maxIdleMillis, now, perEntryStatsEnabled);
        // select most nearest expiration time
        return Math.min(nextTtlExpirationTime(ttlMillis, lastUpdateTime),
                nextMaxIdleExpirationTime(maxIdleMillis, now));
    }

    private static long findLastUpdateTime(Record record, long ttlMillis,
                                           long maxIdleMillis, long now,
                                           boolean perEntryStatsEnabled) {
        if (canUseLastUpdateTimeAsVersion(ttlMillis, maxIdleMillis, perEntryStatsEnabled)) {
            return record.recomputeWithBaseTime(record.getVersion());
        }

        return perEntryStatsEnabled ? record.getLastUpdateTime() : now;
    }

    /**
     * Background:
     * This method is needed to work around some issues when
     * both ttl & maxIdle are set. With introduction of
     * {@link MapConfig#isPerEntryStatsEnabled()} we removed
     * {@code lastUpdateTime} field to have minimal records,
     * but later we saw that in some cases we still need it.
     *
     * To fix this, we use {@code lastUpdateTime} as {@code
     * version} when {@link MapConfig#isPerEntryStatsEnabled()}
     * is {@code false} and both maxIdle and ttl are set.
     */
    public static void trySetLastUpdateTimeAsVersion(Record record, long ttlMillis,
                                                     long maxIdleMillis, long now,
                                                     boolean perEntryStatsEnabled) {
        if (!canUseLastUpdateTimeAsVersion(ttlMillis, maxIdleMillis, perEntryStatsEnabled)) {
            return;
        }

        int prevVersion = record.getVersion();
        int newVersion = record.stripBaseTime(now);
        record.setVersion(prevVersion == newVersion ? newVersion + 1 : newVersion);
    }

    /**
     * Decides if TTL millis should to be set on record.
     *
     * @param mapConfig          used to get configured TTL
     * @param operationTTLMillis user provided TTL during operation call like put with TTL
     * @return TTL value in millis to set to record
     */
    public static long pickTTLMillis(MapConfig mapConfig, long operationTTLMillis) {
        if (operationTTLMillis < 0 && mapConfig.getTimeToLiveSeconds() == 0) {
            return Long.MAX_VALUE;
        }

        if (operationTTLMillis > 0) {
            // if user set operationTTLMillis when calling operation, use it
            return operationTTLMillis;
        } else if (operationTTLMillis == 0) {
            return Long.MAX_VALUE;
        } else if (mapConfig.getTimeToLiveSeconds() > 0) {
            // if this is the first creation of entry, try to get TTL from mapConfig
            return SECONDS.toMillis(mapConfig.getTimeToLiveSeconds());
        } else {
            // if we are here, entry should live forever
            return Long.MAX_VALUE;
        }
    }

    public static long pickMaxIdleMillis(MapConfig mapConfig, long operationMaxIdleMillis) {
        if (operationMaxIdleMillis < 0 && mapConfig.getMaxIdleSeconds() == 0) {
            return Long.MAX_VALUE;
        }

        if (operationMaxIdleMillis > 0) {
            // if user set operationMaxIdleMillis when calling operation, use it
            return operationMaxIdleMillis;
        } else if (operationMaxIdleMillis == 0) {
            return Long.MAX_VALUE;
        } else if (mapConfig.getMaxIdleSeconds() > 0) {
            // if this is the first creation of entry, try to get max-idle from mapConfig
            return SECONDS.toMillis(mapConfig.getMaxIdleSeconds());
        } else {
            // if we are here, entry should live forever
            return Long.MAX_VALUE;
        }
    }

    private static long nextTtlExpirationTime(long ttlMillis, long lastUpdateTime) {
        return handleOverflow(ttlMillis + lastUpdateTime);
    }

    private static long nextMaxIdleExpirationTime(long maxIdleMillis, long now) {
        return handleOverflow(maxIdleMillis + now);
    }

    private static long handleOverflow(long time) {
        return time <= 0 ? Long.MAX_VALUE : time;
    }

    /**
     * @return {@code true} if conditions to use lastUpdateTime
     * as version are satisfied, otherwise {@code false}
     */
    private static boolean canUseLastUpdateTimeAsVersion(long ttlMillis,
                                                         long maxIdleMillis,
                                                         boolean perEntryStatsEnabled) {
        return !perEntryStatsEnabled && isTtlSet(ttlMillis)
                && isMaxIdleSecondsSet(maxIdleMillis);
    }

    private static boolean isMaxIdleSecondsSet(long maxIdleMillis) {
        return maxIdleMillis > 0 && maxIdleMillis < Long.MAX_VALUE;
    }

    private static boolean isTtlSet(long ttlMillis) {
        return ttlMillis > 0 && ttlMillis < Long.MAX_VALUE;
    }
}
