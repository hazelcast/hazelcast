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

package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
     * Pick the right value for TTL expiry.
     *
     * @param mapConfig           used to get configured TTL seconds
     * @param millisFromOperation user provided TTL during operation call
     * @return TTL value in millis
     */
    public static long pickTTLMillis(MapConfig mapConfig, long millisFromOperation) {
        return pickRightTimeInMillis(mapConfig.getTimeToLiveSeconds(), millisFromOperation);
    }

    /**
     * Pick the right value for idleness expiry.
     *
     * @param mapConfig           used to get configured maxIdleSeconds
     * @param millisFromOperation user provided maxIdle value during operation call
     * @return maxIdle value in millis
     */
    public static long pickMaxIdleMillis(MapConfig mapConfig, long millisFromOperation) {
        return pickRightTimeInMillis(mapConfig.getMaxIdleSeconds(), millisFromOperation);
    }

    /**
     * Have right expiry value by using mapConfig or operation provided values.
     *
     * @param secondsFromMapConfig expiry in seconds from map config
     * @param millisFromOperation  expiry in millis from operation
     * @return time in millis
     */
    private static long pickRightTimeInMillis(int secondsFromMapConfig, long millisFromOperation) {
        if (millisFromOperation > 0) {
            // if user set millisFromOperation when calling operation, use it
            return millisFromOperation;
        }

        if (millisFromOperation == 0) {
            return Long.MAX_VALUE;
        }

        if (secondsFromMapConfig > 0 && secondsFromMapConfig < Integer.MAX_VALUE) {
            // if this is the first creation of entry, try to get expiry value from mapConfig
            return SECONDS.toMillis(secondsFromMapConfig);
        }
        // if we are here, entry should live forever
        return Long.MAX_VALUE;
    }

    public static int toSeconds(long millis) {
        long seconds = MILLISECONDS.toSeconds(millis);
        if (seconds == 0 && millis != 0) {
            seconds = 1;
        }
        return seconds > Integer.MAX_VALUE
                ? Integer.MAX_VALUE : (int) seconds;
    }

    public static long toMillis(int seconds) {
        return seconds == Integer.MAX_VALUE
                ? Long.MAX_VALUE : SECONDS.toMillis(seconds);
    }
}
