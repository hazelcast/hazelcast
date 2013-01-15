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
import com.hazelcast.impl.GroupProperties;

public final class HazelcastTimestamper {

    public static long nextTimestamp(HazelcastInstance instance) {
        return instance.getCluster().getClusterTime(); // System time in ms.
    }

    public static int getTimeout(HazelcastInstance instance, String regionName) {
        try {
            final MapConfig cfg = instance.getConfig().findMatchingMapConfig(regionName);
            if (cfg.getTimeToLiveSeconds() > 0) {
                return cfg.getTimeToLiveSeconds() * 1000; // TTL in ms.
            }
        } catch (UnsupportedOperationException ignored) {
            // HazelcastInstance is instance of HazelcastClient.
        }
        return CacheEnvironment.getDefaultCacheTimeoutInMillis();
    }

    public static long getMaxOperationTimeout(HazelcastInstance instance) {
        String maxOpTimeoutProp = null;
        try {
            Config config = instance.getConfig();
            maxOpTimeoutProp = config.getProperty(GroupProperties.PROP_MAX_OPERATION_TIMEOUT);
        } catch (UnsupportedOperationException ignored) {
            // HazelcastInstance is instance of HazelcastClient.
        }
        if (maxOpTimeoutProp != null) {
            return Long.parseLong(maxOpTimeoutProp);
        }
        return Long.MAX_VALUE;
    }
}
