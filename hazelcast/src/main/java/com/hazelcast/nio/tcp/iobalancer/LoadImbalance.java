/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.iobalancer;

import com.hazelcast.nio.tcp.nonblocking.IOSelector;
import com.hazelcast.nio.tcp.nonblocking.MigratableHandler;
import com.hazelcast.nio.tcp.nonblocking.SelectionHandler;
import com.hazelcast.util.ItemCounter;

import java.util.Map;
import java.util.Set;

/**
 * Describes a state of IOSelector (im-)balance.
 *
 * It's used by {@link MigrationStrategy} to decide whether and what
 * {@link SelectionHandler} should be migrated.
 *
 *
 */
class LoadImbalance {
    //number of events recorded by the busiest IOSelector
    long maximumEvents;
    //number of events recorded by the least busy IOSelector
    long minimumEvents;
    //busiest IOSelector
    IOSelector sourceSelector;
    //least busy selector
    IOSelector destinationSelector;

    private final Map<IOSelector, Set<MigratableHandler>> selectorToHandlers;
    private final ItemCounter<MigratableHandler> handlerEventsCounter;

    LoadImbalance(Map<IOSelector, Set<MigratableHandler>> selectorToHandlers,
                  ItemCounter<MigratableHandler> handlerEventsCounter) {
        this.selectorToHandlers = selectorToHandlers;
        this.handlerEventsCounter = handlerEventsCounter;
    }

    /**
     * @param selector
     * @return A set of Handlers owned by the selector
     */
    Set<MigratableHandler> getHandlersOwnerBy(IOSelector selector) {
        return selectorToHandlers.get(selector);
    }

    /**
     * @param handler
     * @return number of events recorded by the handler
     */
    long getEventCount(MigratableHandler handler) {
        return handlerEventsCounter.get(handler);
    }
}
