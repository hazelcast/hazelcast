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

package com.hazelcast.wan.impl;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.wan.WanEventCounters;

import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Thread safe container for {@link WanEventCounters}
 * grouped by WAN publisher, distributed service name and distributed object
 * name.
 * The publisher is defined by the WAN replication name and the target group
 * name.
 */
public class WanEventCounterRegistry {
    private static final ConstructorFunction<String, WanPublisherEventCounters> WAN_EVENT_COUNTER_CONSTRUCTOR_FN
            = ignored -> new WanPublisherEventCounters();

    private final ConcurrentHashMap<String, WanPublisherEventCounters> eventCounterMap = new ConcurrentHashMap<>();


    /**
     * Returns the {@link WanEventCounters} for the given {@code serviceName}
     */
    public WanEventCounters getWanEventCounter(String wanReplicationName,
                                               String wanPublisherId,
                                               String serviceName) {
        final String counterId = wanReplicationName + ":" + wanPublisherId;
        final WanPublisherEventCounters serviceWanEventCounters
                = getOrPutIfAbsent(eventCounterMap, counterId, WAN_EVENT_COUNTER_CONSTRUCTOR_FN);

        return serviceWanEventCounters.getWanEventCounter(serviceName);
    }

    /**
     * Removes the counter for the given {@code serviceName} and {@code dataStructureName}.
     */
    public void removeCounter(String serviceName, String dataStructureName) {
        for (WanPublisherEventCounters publisherWanCounterContainer : eventCounterMap.values()) {
            publisherWanCounterContainer.removeCounter(serviceName, dataStructureName);
        }
    }

    /**
     * Thread safe container for {@link WanEventCounters}s
     * for all distributed objects and a single WAN publisher.
     */
    private static final class WanPublisherEventCounters {
        private final WanEventCounters mapEventCounters = new WanEventCounters();
        private final WanEventCounters cacheEventCounters = new WanEventCounters();

        /**
         * Removes the counter for the given {@code serviceName} and {@code dataStructureName}.
         */
        void removeCounter(String serviceName, String dataStructureName) {
            getWanEventCounter(serviceName).removeCounter(dataStructureName);
        }

        /**
         * Returns the {@link WanEventCounters} for the given {@code serviceName}
         */
        WanEventCounters getWanEventCounter(String serviceName) {
            if (MapService.SERVICE_NAME.equals(serviceName)) {
                return mapEventCounters;
            } else if (CacheService.SERVICE_NAME.equals(serviceName)) {
                return cacheEventCounters;
            } else {
                throw new IllegalArgumentException("Unsupported service for counting WAN events " + serviceName);
            }
        }
    }

}
