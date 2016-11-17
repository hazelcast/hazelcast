/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nonblocking.iobalancer;

import com.hazelcast.internal.networking.nonblocking.MigratableHandler;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThread;
import com.hazelcast.internal.networking.nonblocking.SelectionHandler;
import com.hazelcast.util.ItemCounter;

import java.util.Map;
import java.util.Set;

/**
 * Describes a state of NonBlockingIOThread (im-)balance.
 *
 * It's used by {@link MigrationStrategy} to decide whether and what
 * {@link SelectionHandler} should be migrated.
 */
class LoadImbalance {
    //number of events recorded by the busiest NonBlockingIOThread
    long maximumEvents;
    //number of events recorded by the least busy NonBlockingIOThread
    long minimumEvents;
    //busiest NonBlockingIOThread
    NonBlockingIOThread sourceSelector;
    //least busy NonBlockingIOThread
    NonBlockingIOThread destinationSelector;

    private final Map<NonBlockingIOThread, Set<MigratableHandler>> selectorToHandlers;
    private final ItemCounter<MigratableHandler> handlerEventsCounter;

    LoadImbalance(Map<NonBlockingIOThread, Set<MigratableHandler>> selectorToHandlers,
                  ItemCounter<MigratableHandler> handlerEventsCounter) {
        this.selectorToHandlers = selectorToHandlers;
        this.handlerEventsCounter = handlerEventsCounter;
    }

    /**
     * @param selector
     * @return A set of Handlers owned by the selector
     */
    Set<MigratableHandler> getHandlersOwnerBy(NonBlockingIOThread selector) {
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
