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

package com.hazelcast.nio.tcp.handlermigration;

import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.MigratableHandler;
import com.hazelcast.nio.tcp.SelectionHandler;
import com.hazelcast.util.ItemCounter;

import java.util.Map;
import java.util.Set;

class BalancerState<T extends MigratableHandler & SelectionHandler> {
    long maximumEvents;
    long minimumEvents;
    IOSelector sourceSelector;
    IOSelector destinationSelector;

    private final Map<IOSelector, Set<T>> selectorToHandlers;
    private final ItemCounter<T> handlerEventsCounter;

    BalancerState(Map<IOSelector, Set<T>> selectorToHandlers,
                  ItemCounter<T> handlerEventsCounter) {
        this.selectorToHandlers = selectorToHandlers;
        this.handlerEventsCounter = handlerEventsCounter;
    }

    Set<T> getHandlersOwnerBy(IOSelector selector) {
        return selectorToHandlers.get(selector);
    }

    long getNoOfEvents(T handler) {
        return handlerEventsCounter.get(handler);
    }
}
