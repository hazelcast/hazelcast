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

    public static long calculateExpirationTime(long ttlMillis, long maxIdleMillis, long now) {
        // calculate TTL expiration time
        long ttl = checkedTime(ttlMillis);
        long ttlExpirationTime = sumForExpiration(ttl, now);

        // calculate MaxIdle expiration time
        long maxIdle = checkedTime(maxIdleMillis);
        long maxIdleExpirationTime = sumForExpiration(maxIdle, now);
        // select most nearest expiration time
        return Math.min(ttlExpirationTime, maxIdleExpirationTime);
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
     * Decides if TTL millis should to be set on record.
     *
     * @param operationTTLMillis user provided TTL during operation call like put with TTL
     * @param mapConfig          used to get configured TTL
     * @return TTL value in millis to set to record
     */
    public static long pickTTLMillis(long operationTTLMillis, MapConfig mapConfig) {
        // if user set operationTTLMillis when calling operation, use it
        if (operationTTLMillis > 0) {
            return checkedTime(operationTTLMillis);
        }

        // if this is the first creation of entry, try to get TTL from mapConfig
        if (operationTTLMillis < 0 && mapConfig.getTimeToLiveSeconds() > 0) {
            return checkedTime(SECONDS.toMillis(mapConfig.getTimeToLiveSeconds()));
        }

        // if we are here, entry should live forever
        return Long.MAX_VALUE;
    }

    public static long pickMaxIdleMillis(long operationMaxIdleMillis, MapConfig mapConfig) {
        // if user set operationMaxIdleMillis when calling operation, use it
        if (operationMaxIdleMillis > 0) {
            return checkedTime(operationMaxIdleMillis);
        }

        // if this is the first creation of entry, try to get MaxIdle from mapConfig
        if (operationMaxIdleMillis < 0 && mapConfig.getMaxIdleSeconds() > 0) {
            return checkedTime(SECONDS.toMillis(mapConfig.getMaxIdleSeconds()));
        }

        // if we are here, entry should live forever
        return Long.MAX_VALUE;
    }
}
