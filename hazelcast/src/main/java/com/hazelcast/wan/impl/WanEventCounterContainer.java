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

import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Thread safe container for {@link WanEventCounter} grouped by WAN publisher
 * ID. The publisher ID is contained of the WAN replication name and the
 * target group name.
 */
public class WanEventCounterContainer {
    private static final ConstructorFunction<String, WanEventCounterPublisherContainer> WAN_EVENT_COUNTER_CONSTRUCTOR_FN
            = new ConstructorFunction<String, WanEventCounterPublisherContainer>() {
        @Override
        public WanEventCounterPublisherContainer createNew(String ignored) {
            return new WanEventCounterPublisherContainer();
        }
    };

    private final ConcurrentHashMap<String, WanEventCounterPublisherContainer> eventCounterMap =
            new ConcurrentHashMap<String, WanEventCounterPublisherContainer>();


    /**
     * Returns the {@link WanEventCounter} for the given {@code serviceName}
     */
    public WanEventCounter getWanEventCounter(String wanReplicationName,
                                              String targetGroupName,
                                              String serviceName) {
        final String wanPublisherId = wanReplicationName + ":" + targetGroupName;
        final WanEventCounterPublisherContainer serviceWanEventCounters
                = getOrPutIfAbsent(eventCounterMap, wanPublisherId, WAN_EVENT_COUNTER_CONSTRUCTOR_FN);

        return serviceWanEventCounters.getWanEventCounter(serviceName);
    }

    /**
     * Removes the counter for the given {@code serviceName} and {@code dataStructureName}.
     */
    public void removeCounter(String serviceName, String dataStructureName) {
        for (WanEventCounterPublisherContainer publisherWanCounterContainer : eventCounterMap.values()) {
            publisherWanCounterContainer.removeCounter(serviceName, dataStructureName);
        }
    }
}
