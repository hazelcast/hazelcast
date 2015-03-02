/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hibernate;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper class to create timestamps and calculate timeouts based on either Hazelcast
 * configuration of by requesting values on the cluster.
 */
public final class HazelcastTimestamper {

    private static final int SEC_TO_MS = 1000;
    /**
     * Value for left shifting instance.getCluster().getClusterTime(), freeing some space for the counter.
     * Right now value is constant set to 12.
     */
    private static final int BASE_TIME_SHIFT = 12;
    private static final int ONE_MS = 1 << BASE_TIME_SHIFT;
    private static final AtomicLong VALUE  = new AtomicLong();
    private final ILogger logger = Logger.getLogger(getClass());

    private HazelcastTimestamper() {
    }

    /**
     * Returns an increasing unique value based on the cluster time. First loop works like a thread spin-waits and
     * inner for loop guarantees that unique value is generated.
     * @see com.hazelcast.hibernate.HazelcastTimestamper#BASE_TIME_SHIFT
     * @return uniquely & increasing value
     */
    public static long nextTimestamp(HazelcastInstance instance) {
        int runs = 0;
        while (true) {

            long base = instance.getCluster().getClusterTime() << BASE_TIME_SHIFT;
            long maxValue = base + ONE_MS - 1;
            long current = VALUE.get();
            long update = Math.max(base, current + 1);

            if (runs > 1) {
                Logger.getLogger(HazelcastTimestamper.class)
                        .finest(String.format("Waiting for time to pass. Looped %d times", runs));
            }
            //Make sure that base or max value is not negative, if one of them is negative return current + 1
            if (base < 0 || maxValue < 0) {
                VALUE.compareAndSet(current, update);
                return update;
            } else {
                //Make sure that update different than current value, otherwise while loop will run again.
                for (; update < maxValue; current = VALUE.get(), update = Math.max(base, current + 1)) {
                    if (VALUE.compareAndSet(current, update)) {
                        return update;
                    }
                }
            }
            ++runs;
        }
    }

    public static int getTimeout(HazelcastInstance instance, String regionName) {
        try {
            final MapConfig cfg = instance.getConfig().findMapConfig(regionName);
            if (cfg.getTimeToLiveSeconds() > 0) {
                // TTL in ms.
                return cfg.getTimeToLiveSeconds() * SEC_TO_MS;
            }
        } catch (UnsupportedOperationException e) {
            // HazelcastInstance is instance of HazelcastClient.
            Logger.getLogger(HazelcastTimestamper.class).finest(e);
        }
        return CacheEnvironment.getDefaultCacheTimeoutInMillis();
    }

    public static long getMaxOperationTimeout(HazelcastInstance instance) {
        String maxOpTimeoutProp = null;
        try {
            Config config = instance.getConfig();
            maxOpTimeoutProp = config.getProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS);
        } catch (UnsupportedOperationException e) {
            // HazelcastInstance is instance of HazelcastClient.
            Logger.getLogger(HazelcastTimestamper.class).finest(e);
        }
        if (maxOpTimeoutProp != null) {
            return Long.parseLong(maxOpTimeoutProp);
        }
        return Long.MAX_VALUE;
    }
}
