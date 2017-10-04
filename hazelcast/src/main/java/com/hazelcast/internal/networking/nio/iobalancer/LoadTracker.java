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
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ItemCounter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * Tracks the load of of NioThread(s) and creates a mapping between NioThread -> Handler.
 * <p/>
 * This class is not thread-safe with the exception of
 * {@link #addHandler(MigratableHandler)}   and
 * {@link #removeHandler(MigratableHandler)}
 */
class LoadTracker {
    final Queue<Runnable> tasks = new LinkedBlockingQueue<Runnable>();

    private final ILogger logger;

    //all known IO ioThreads. we assume no. of ioThreads is constant during a lifespan of a member
    private final NioThread[] ioThreads;
    private final Map<NioThread, Set<MigratableHandler>> selectorToHandlers;

    //no. of events per handler since an instance started
    private final ItemCounter<MigratableHandler> lastEventCounter = new ItemCounter<MigratableHandler>();

    //no. of events per NioThread since last calculation
    private final ItemCounter<NioThread> selectorEvents = new ItemCounter<NioThread>();
    //no. of events per handler since last calculation
    private final ItemCounter<MigratableHandler> handlerEventsCounter = new ItemCounter<MigratableHandler>();

    //contains all known handlers
    private final Set<MigratableHandler> handlers = new HashSet<MigratableHandler>();

    private final LoadImbalance imbalance;

    LoadTracker(NioThread[] ioThreads, ILogger logger) {
        this.logger = logger;

        this.ioThreads = new NioThread[ioThreads.length];
        System.arraycopy(ioThreads, 0, this.ioThreads, 0, ioThreads.length);

        this.selectorToHandlers = new HashMap<NioThread, Set<MigratableHandler>>();
        for (NioThread selector : ioThreads) {
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
        handleAddedOrRemovedConnections();
        clearWorkingImbalance();
        updateNewWorkingImbalance();
        updateNewFinalImbalance();
        printDebugTable();
        return imbalance;
    }

    private void handleAddedOrRemovedConnections() {
        Iterator<Runnable> iterator = tasks.iterator();
        while (iterator.hasNext()) {
            Runnable task = iterator.next();
            task.run();
            iterator.remove();
        }
    }

    // just for testing
    Set<MigratableHandler> getHandlers() {
        return handlers;
    }

    // just for testing
    ItemCounter<MigratableHandler> getLastEventCounter() {
        return lastEventCounter;
    }

    // just for testing
    ItemCounter<MigratableHandler> getHandlerEventsCounter() {
        return handlerEventsCounter;
    }

    private void updateNewFinalImbalance() {
        imbalance.minimumEvents = Long.MAX_VALUE;
        imbalance.maximumEvents = Long.MIN_VALUE;
        imbalance.sourceSelector = null;
        imbalance.destinationSelector = null;
        for (NioThread selector : ioThreads) {
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

    public void notifyHandlerAdded(MigratableHandler handler) {
        AddHandlerTask addHandlerTask = new AddHandlerTask(handler);
        tasks.offer(addHandlerTask);
    }

    public void notifyHandlerRemoved(MigratableHandler handler) {
        RemoveHandlerTask removeHandlerTask = new RemoveHandlerTask(handler);
        tasks.offer(removeHandlerTask);
    }


    private void updateNewWorkingImbalance() {
        for (MigratableHandler handler : handlers) {
            updateHandlerState(handler);
        }
    }

    private void updateHandlerState(MigratableHandler handler) {
        long handlerEventCount = getEventCountSinceLastCheck(handler);
        handlerEventsCounter.set(handler, handlerEventCount);
        NioThread owner = handler.getOwner();
        selectorEvents.add(owner, handlerEventCount);
        Set<MigratableHandler> handlersOwnedBy = selectorToHandlers.get(owner);
        handlersOwnedBy.add(handler);
    }

    private long getEventCountSinceLastCheck(MigratableHandler handler) {
        long eventCount = handler.getLoad();
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
        handlerEventsCounter.remove(handler);
        lastEventCounter.remove(handler);
    }

    private void printDebugTable() {
        if (!logger.isFinestEnabled()) {
            return;
        }

        NioThread minThread = imbalance.destinationSelector;
        NioThread maxThread = imbalance.sourceSelector;
        if (minThread == null || maxThread == null) {
            return;
        }
        StringBuilder sb = new StringBuilder(LINE_SEPARATOR)
                .append("------------")
                .append(LINE_SEPARATOR);
        Long eventCountPerSelector = selectorEvents.get(minThread);

        sb.append("Min Selector ")
                .append(minThread)
                .append(" received ")
                .append(eventCountPerSelector)
                .append(" events. ");
        sb.append("It contains following handlers: ").
                append(LINE_SEPARATOR);
        appendSelectorInfo(minThread, selectorToHandlers, sb);

        eventCountPerSelector = selectorEvents.get(maxThread);
        sb.append("Max Selector ")
                .append(maxThread)
                .append(" received ")
                .append(eventCountPerSelector)
                .append(" events. ");
        sb.append("It contains following handlers: ")
                .append(LINE_SEPARATOR);
        appendSelectorInfo(maxThread, selectorToHandlers, sb);

        sb.append("Other Selectors: ")
                .append(LINE_SEPARATOR);

        for (NioThread selector : ioThreads) {
            if (!selector.equals(minThread) && !selector.equals(maxThread)) {
                eventCountPerSelector = selectorEvents.get(selector);
                sb.append("Selector ")
                        .append(selector)
                        .append(" contains ")
                        .append(eventCountPerSelector)
                        .append(" and has these handlers: ")
                        .append(LINE_SEPARATOR);
                appendSelectorInfo(selector, selectorToHandlers, sb);
            }
        }
        sb.append("------------")
                .append(LINE_SEPARATOR);
        logger.finest(sb.toString());
    }

    private void appendSelectorInfo(
            NioThread minThread,
            Map<NioThread, Set<MigratableHandler>> threadHandlers,
            StringBuilder sb) {
        Set<MigratableHandler> handlerSet = threadHandlers.get(minThread);
        for (MigratableHandler selectionHandler : handlerSet) {
            Long eventCountPerHandler = handlerEventsCounter.get(selectionHandler);
            sb.append(selectionHandler)
                    .append(":  ")
                    .append(eventCountPerHandler)
                    .append(LINE_SEPARATOR);
        }
        sb.append(LINE_SEPARATOR);
    }

    class RemoveHandlerTask implements Runnable {

        private final MigratableHandler handler;

        public RemoveHandlerTask(MigratableHandler handler) {
            this.handler = handler;
        }

        @Override
        public void run() {

            if (logger.isFinestEnabled()) {
                logger.finest("Removing handler: " + handler);
            }

            removeHandler(handler);
        }
    }

    class AddHandlerTask implements Runnable {

        private final MigratableHandler handler;

        public AddHandlerTask(MigratableHandler handler) {
            this.handler = handler;
        }

        @Override
        public void run() {

            if (logger.isFinestEnabled()) {
                logger.finest("Adding handler: " + handler);
            }

            addHandler(handler);
        }
    }

}
