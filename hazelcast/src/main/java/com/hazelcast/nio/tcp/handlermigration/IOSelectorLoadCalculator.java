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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.AbstractIOSelector;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.MigratableHandler;
import com.hazelcast.nio.tcp.SelectionHandler;
import com.hazelcast.util.ItemCounter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.hazelcast.util.StringUtil.getLineSeperator;

/**
 * Calculates a state describing utilization of IOSelector(s) and creates a mapping between IOSelector -> Handler.
 *
 * This class is not thread-safe with the exception of
 * {@link #addHandler(T)}   and
 * {@link #removeHandler(T)}
 *
 */
class IOSelectorLoadCalculator<T extends MigratableHandler & SelectionHandler> {
    private final ILogger log;

    //all known IO selectors. we assume no. of selectors is constant during a lifespan of a member
    private final AbstractIOSelector[] allIOSelectors;
    private final Map<IOSelector, Set<T>> selectorToHandlers;

    //no. of events per handler since an instance started
    private final ItemCounter<T> lastEventCounter = new ItemCounter<T>();

    //no. of events per IOSelector since last calculation
    private final ItemCounter<IOSelector> selectorEvents = new ItemCounter<IOSelector>();
    //no. of events per handler since last calculation
    private final ItemCounter<T> handlerEventsCounter = new ItemCounter<T>();

    //contains all known handlers
    private final Set<T> handlers = new CopyOnWriteArraySet<T>();

    private final BalancerState state;

    IOSelectorLoadCalculator(AbstractIOSelector[] allIOSelectors, LoggingService loggingService) {
        this.log = loggingService.getLogger(IOSelectorLoadCalculator.class);

        this.allIOSelectors = new AbstractIOSelector[allIOSelectors.length];
        System.arraycopy(allIOSelectors, 0, this.allIOSelectors, 0, allIOSelectors.length);

        this.selectorToHandlers = new HashMap<IOSelector, Set<T>>();
        for (AbstractIOSelector selector : allIOSelectors) {
            selectorToHandlers.put(selector, new HashSet<T>());
        }
        this.state = new BalancerState(selectorToHandlers, handlerEventsCounter);
    }

    /**
     * Recalculates a new state. Returned instance of {@link com.hazelcast.nio.tcp.handlermigration.BalancerState} are recycled
     * between invocations therefore they are valid for the last invocation only.
     *
     * @return recalculated state
     */
    BalancerState<T> calculate() {
        clearWorkingState();
        calculateNewWorkingState();
        calculateNewFinalState();
        printDebugTable();
        return state;
    }

    private void calculateNewFinalState() {
        state.minimumEvents = Long.MAX_VALUE;
        state.maximumEvents = Long.MIN_VALUE;
        state.sourceSelector = null;
        state.destinationSelector = null;
        for (AbstractIOSelector selector : allIOSelectors) {
            long noOfEvents = selectorEvents.get(selector);
            if (noOfEvents > state.maximumEvents) {
                state.maximumEvents = noOfEvents;
                state.sourceSelector = selector;
            }
            if (noOfEvents < state.minimumEvents) {
                state.minimumEvents = noOfEvents;
                state.destinationSelector = selector;
            }
        }
    }


    private void calculateNewWorkingState() {
        for (T handler : handlers) {
            calculateHandlerState(handler);
        }
    }

    private void calculateHandlerState(T handler) {
        long noOfHandlerEvents = getNoOfEventsSinceLastCheck(handler);
        handlerEventsCounter.set(handler, noOfHandlerEvents);
        IOSelector owner = handler.getOwner();
        selectorEvents.add(owner, noOfHandlerEvents);
        Set<T> handlersOwnedBy = selectorToHandlers.get(owner);
        handlersOwnedBy.add(handler);
    }

    private long getNoOfEventsSinceLastCheck(T handler) {
        long totalNoOfEvents = handler.getNoOfEvents();
        Long lastNoOfEvents = lastEventCounter.getAndSet(handler, totalNoOfEvents);
        return totalNoOfEvents - lastNoOfEvents;
    }

    private void clearWorkingState() {
        handlerEventsCounter.reset();
        selectorEvents.reset();
        for (Set<T> handlerSet : selectorToHandlers.values()) {
            handlerSet.clear();
        }
    }


    void addHandler(T handler) {
        handlers.add(handler);
    }

    void removeHandler(T handler) {
        handlers.remove(handler);
    }



    private void printDebugTable() {
        if (!log.isFinestEnabled()) {
            return;
        }

        IOSelector minSelector = state.destinationSelector;
        IOSelector maxSelector = state.sourceSelector;
        if (minSelector == null || maxSelector == null) {
            return;
        }
        StringBuilder sb = new StringBuilder(getLineSeperator())
                .append("------------")
                .append(getLineSeperator());
        Long noOfEventsPerSelector = selectorEvents.get(minSelector);

        sb.append("Min Selector ")
                .append(minSelector)
                .append(" received ")
                .append(noOfEventsPerSelector)
                .append(" events. ");
        sb.append("It contains following handlers: ").
                append(getLineSeperator());
        appendSelectorInfo(minSelector, selectorToHandlers, sb);


        noOfEventsPerSelector = selectorEvents.get(maxSelector);
        sb.append("Max Selector ")
                .append(maxSelector)
                .append(" received ")
                .append(noOfEventsPerSelector)
                .append(" events. ");
        sb.append("It contains following handlers: ")
                .append(getLineSeperator());
        appendSelectorInfo(maxSelector, selectorToHandlers, sb);

        sb.append("Other Selectors: ")
                .append(getLineSeperator());

        for (AbstractIOSelector selector : allIOSelectors) {
            if (!selector.equals(minSelector) && !selector.equals(maxSelector)) {
                noOfEventsPerSelector = selectorEvents.get(selector);
                sb.append("Selector ")
                        .append(selector)
                        .append(" contains ")
                        .append(noOfEventsPerSelector)
                        .append(" and has these handlers: ")
                        .append(getLineSeperator());
                appendSelectorInfo(selector, selectorToHandlers, sb);
            }
        }
        sb.append("------------")
                .append(getLineSeperator());
        log.finest(sb.toString());

    }

    private void appendSelectorInfo(IOSelector minSelector, Map<IOSelector,
            Set<T>> selectorToHandlers, StringBuilder sb) {
        Set<T> handlerSet = selectorToHandlers.get(minSelector);
        for (T selectionHandler : handlerSet) {
            Long noOfEventsPerHandler = handlerEventsCounter.get(selectionHandler);
            sb.append(selectionHandler)
                    .append(":  ")
                    .append(noOfEventsPerHandler)
                    .append(getLineSeperator());
        }
        sb.append(getLineSeperator());
    }
}
