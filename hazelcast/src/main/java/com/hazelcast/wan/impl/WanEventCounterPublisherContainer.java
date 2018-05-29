/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.map.impl.MapService;

/**
 * Thread safe container for {@link WanEventCounterContainer}s grouped by
 * WAN replication name and target group name.
 */
class WanEventCounterPublisherContainer {
    private final WanEventCounter mapEventCounters = new WanEventCounter();
    private final WanEventCounter cacheEventCounters = new WanEventCounter();

    /**
     * Removes the counter for the given {@code serviceName} and {@code dataStructureName}.
     */
    void removeCounter(String serviceName, String dataStructureName) {
        getWanEventCounter(serviceName).removeCounter(dataStructureName);
    }

    /**
     * Returns the {@link WanEventCounter} for the given {@code serviceName}
     */
    WanEventCounter getWanEventCounter(String serviceName) {
        if (MapService.SERVICE_NAME.equals(serviceName)) {
            return mapEventCounters;
        } else if (CacheService.SERVICE_NAME.equals(serviceName)) {
            return cacheEventCounters;
        } else {
            throw new IllegalArgumentException("Unsupported service for counting WAN events " + serviceName);
        }
    }
}
