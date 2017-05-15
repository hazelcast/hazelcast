/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.internal.networking.nio.MigratableHandler;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.internal.networking.nio.SelectionHandler;
import com.hazelcast.util.ItemCounter;

import java.util.Map;
import java.util.Set;

/**
 * Describes a state of NioThread (im-)balance.
 *
 * It's used by {@link MigrationStrategy} to decide whether and what
 * {@link SelectionHandler} should be migrated.
 */
class LoadImbalance {
    //number of events recorded by the busiest NioThread
    long maximumEvents;
    //number of events recorded by the least busy NioThread
    long minimumEvents;
    //busiest NioThread
    NioThread sourceSelector;
    //least busy NioThread
    NioThread destinationSelector;

    private final Map<NioThread, Set<MigratableHandler>> selectorToHandlers;
    private final ItemCounter<MigratableHandler> handlerLoadCounter;

    LoadImbalance(Map<NioThread, Set<MigratableHandler>> selectorToHandlers,
                  ItemCounter<MigratableHandler> handlerLoadCounter) {
        this.selectorToHandlers = selectorToHandlers;
        this.handlerLoadCounter = handlerLoadCounter;
    }

    /**
     * @param selector
     * @return A set of Handlers owned by the selector
     */
    Set<MigratableHandler> getHandlersOwnerBy(NioThread selector) {
        return selectorToHandlers.get(selector);
    }

    /**
     * @param handler
     * @return number of events recorded by the handler
     */
    long getLoad(MigratableHandler handler) {
        return handlerLoadCounter.get(handler);
    }
}
