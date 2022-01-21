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

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility methods for setting TTL and max idle seconds.
 */
public final class ExpirationTimeSetter {

    private ExpirationTimeSetter() {
    }

    public static long nextExpirationTime(long ttlMillis, long maxIdleMillis,
                                          long now, long lastUpdateTime) {
        long nextTtlExpirationTime = nextTtlExpirationTime(ttlMillis, lastUpdateTime);
        long nextMaxIdleExpirationTime = nextMaxIdleExpirationTime(maxIdleMillis, now);
        return Math.min(nextTtlExpirationTime, nextMaxIdleExpirationTime);
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
     * @param time value of ttl or maxIdle
     * @return {@code true} if time parameter is
     * between zero and long-max, this means ttl/maxIdle
     * value is configured otherwise {@code false}
     */
    public static boolean isTtlOrMaxIdleConfigured(long time) {
        return time > 0 && time < Long.MAX_VALUE;
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
}
