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

package com.hazelcast.nio.tcp.nonblocking.iobalancer;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.tcp.nonblocking.AbstractIOSelector;
import com.hazelcast.nio.tcp.nonblocking.IOSelector;
import com.hazelcast.nio.tcp.nonblocking.MigratableHandler;
import com.hazelcast.util.ItemCounter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.hazelcast.util.StringUtil.getLineSeperator;

/**
 * Tracks the load of of IOSelector(s) and creates a mapping between IOSelector -> Handler.
 *
 * This class is not thread-safe with the exception of
 * {@link #addHandler(MigratableHandler)}   and
 * {@link #removeHandler(MigratableHandler)}
 */
class LoadTracker {
    private final ILogger logger;

    //all known IO selectors. we assume no. of selectors is constant during a lifespan of a member
    private final AbstractIOSelector[] selectors;
    private final Map<IOSelector, Set<MigratableHandler>> selectorToHandlers;

    //no. of events per handler since an instance started
    private final ItemCounter<MigratableHandler> lastEventCounter = new ItemCounter<MigratableHandler>();

    //no. of events per IOSelector since last calculation
    private final ItemCounter<IOSelector> selectorEvents = new ItemCounter<IOSelector>();
    //no. of events per handler since last calculation
    private final ItemCounter<MigratableHandler> handlerEventsCounter = new ItemCounter<MigratableHandler>();

    //contains all known handlers
    private final Set<MigratableHandler> handlers = new CopyOnWriteArraySet<MigratableHandler>();

    private final LoadImbalance imbalance;

    LoadTracker(AbstractIOSelector[] selectors, ILogger logger) {
        this.logger = logger;

        this.selectors = new AbstractIOSelector[selectors.length];
        System.arraycopy(selectors, 0, this.selectors, 0, selectors.length);

        this.selectorToHandlers = new HashMap<IOSelector, Set<MigratableHandler>>();
        for (AbstractIOSelector selector : selectors) {
            selectorToHandlers.put(selector, new HashSet<MigratableHandler>());
        }
        this.imbalance = new LoadImbalance(selectorToHandlers, handlerEventsCounter);
    }

    /**
     * Recalculates a new LoadStatus. Returned instance of {@link LoadImbalance} are recycled
     * between invocations therefore they are valid for the last invocation only.
     *
     * @return recalculated imbalance
     */
    LoadImbalance updateImbalance() {
        clearWorkingImbalance();
        updateNewWorkingImbalance();
        updateNewFinalImbalance();
        printDebugTable();
        return imbalance;
    }

    private void updateNewFinalImbalance() {
        imbalance.minimumEvents = Long.MAX_VALUE;
        imbalance.maximumEvents = Long.MIN_VALUE;
        imbalance.sourceSelector = null;
        imbalance.destinationSelector = null;
        for (AbstractIOSelector selector : selectors) {
            long eventCount = selectorEvents.get(selector);
            int handlerCount = selectorToHandlers.get(selector).size();

            if (eventCount > imbalance.maximumEvents && handlerCount > 1) {
                // if a selector has only 1 handle, there is no point in making it a source selector since
                // there is no handler that can be migrated anyway. In that case it is better to move on to
                // the next selector.
                imbalance.maximumEvents = eventCount;
                imbalance.sourceSelector = selector;
            }

            if (eventCount < imbalance.minimumEvents) {
                imbalance.minimumEvents = eventCount;
                imbalance.destinationSelector = selector;
            }
        }
    }

    private void updateNewWorkingImbalance() {
        for (MigratableHandler handler : handlers) {
            updateHandlerState(handler);
        }
    }

    private void updateHandlerState(MigratableHandler handler) {
        long handlerEventCount = getEventCountSinceLastCheck(handler);
        handlerEventsCounter.set(handler, handlerEventCount);
        IOSelector owner = handler.getOwner();
        selectorEvents.add(owner, handlerEventCount);
        Set<MigratableHandler> handlersOwnedBy = selectorToHandlers.get(owner);
        handlersOwnedBy.add(handler);
    }

    private long getEventCountSinceLastCheck(MigratableHandler handler) {
        long eventCount = handler.getEventCount();
        Long lastEventCount = lastEventCounter.getAndSet(handler, eventCount);
        return eventCount - lastEventCount;
    }

    private void clearWorkingImbalance() {
        handlerEventsCounter.reset();
        selectorEvents.reset();
        for (Set<MigratableHandler> handlerSet : selectorToHandlers.values()) {
            handlerSet.clear();
        }
    }

    void addHandler(MigratableHandler handler) {
        handlers.add(handler);
    }

    void removeHandler(MigratableHandler handler) {
        handlers.remove(handler);
    }

    private void printDebugTable() {
        if (!logger.isFinestEnabled()) {
            return;
        }

        IOSelector minSelector = imbalance.destinationSelector;
        IOSelector maxSelector = imbalance.sourceSelector;
        if (minSelector == null || maxSelector == null) {
            return;
        }
        StringBuilder sb = new StringBuilder(getLineSeperator())
                .append("------------")
                .append(getLineSeperator());
        Long eventCountPerSelector = selectorEvents.get(minSelector);

        sb.append("Min Selector ")
                .append(minSelector)
                .append(" received ")
                .append(eventCountPerSelector)
                .append(" events. ");
        sb.append("It contains following handlers: ").
                append(getLineSeperator());
        appendSelectorInfo(minSelector, selectorToHandlers, sb);

        eventCountPerSelector = selectorEvents.get(maxSelector);
        sb.append("Max Selector ")
                .append(maxSelector)
                .append(" received ")
                .append(eventCountPerSelector)
                .append(" events. ");
        sb.append("It contains following handlers: ")
                .append(getLineSeperator());
        appendSelectorInfo(maxSelector, selectorToHandlers, sb);

        sb.append("Other Selectors: ")
                .append(getLineSeperator());

        for (AbstractIOSelector selector : selectors) {
            if (!selector.equals(minSelector) && !selector.equals(maxSelector)) {
                eventCountPerSelector = selectorEvents.get(selector);
                sb.append("Selector ")
                        .append(selector)
                        .append(" contains ")
                        .append(eventCountPerSelector)
                        .append(" and has these handlers: ")
                        .append(getLineSeperator());
                appendSelectorInfo(selector, selectorToHandlers, sb);
            }
        }
        sb.append("------------")
                .append(getLineSeperator());
        logger.finest(sb.toString());
    }

    private void appendSelectorInfo(IOSelector minSelector, Map<IOSelector,
            Set<MigratableHandler>> selectorToHandlers, StringBuilder sb) {
        Set<MigratableHandler> handlerSet = selectorToHandlers.get(minSelector);
        for (MigratableHandler selectionHandler : handlerSet) {
            Long eventCountPerHandler = handlerEventsCounter.get(selectionHandler);
            sb.append(selectionHandler)
                    .append(":  ")
                    .append(eventCountPerHandler)
                    .append(getLineSeperator());
        }
        sb.append(getLineSeperator());
    }
}
