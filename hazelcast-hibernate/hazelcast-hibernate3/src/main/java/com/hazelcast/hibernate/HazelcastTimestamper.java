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
import com.hazelcast.logging.Logger;

/**
 * Helper class to create timestamps and calculate timeouts based on either Hazelcast
 * configuration of by requesting values on the cluster.
 */
public final class HazelcastTimestamper {

    private static final int SEC_TO_MS = 1000;

    private HazelcastTimestamper() {
    }

    public static long nextTimestamp(HazelcastInstance instance) {
        // System time in ms.
        return instance.getCluster().getClusterTime();
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
