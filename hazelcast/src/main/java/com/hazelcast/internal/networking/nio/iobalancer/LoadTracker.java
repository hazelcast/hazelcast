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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ItemCounter;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * Tracks the load of of NioThread(s) and creates a mapping between NioThread -> Handler.
 * <p/>
 * This class is not thread-safe with the exception of
 * {@link #addPipeline(MigratablePipeline)}   and
 * {@link #removePipeline(MigratablePipeline)}
 */
class LoadTracker {
    final Queue<Runnable> tasks = new LinkedBlockingQueue<Runnable>();

    private final ILogger logger;

    //all known IO ioThreads. we assume no. of ioThreads is constant during a lifespan of a member
    private final NioThread[] ioThreads;
    private final Map<NioThread, Set<MigratablePipeline>> selectorToHandlers;

    //no. of events per pipeline since an instance started
    private final ItemCounter<MigratablePipeline> lastEventCounter = new ItemCounter<MigratablePipeline>();

    //no. of events per NioThread since last calculation
    private final ItemCounter<NioThread> selectorEvents = new ItemCounter<NioThread>();
    //no. of events per pipeline since last calculation
    private final ItemCounter<MigratablePipeline> handlerEventsCounter = new ItemCounter<MigratablePipeline>();

    //contains all known pipelines
    private final Set<MigratablePipeline> pipelines = new HashSet<MigratablePipeline>();

    private final LoadImbalance imbalance;

    LoadTracker(NioThread[] ioThreads, ILogger logger) {
        this.logger = logger;

        this.ioThreads = new NioThread[ioThreads.length];
        System.arraycopy(ioThreads, 0, this.ioThreads, 0, ioThreads.length);

        this.selectorToHandlers = createHashMap(ioThreads.length);
        for (NioThread selector : ioThreads) {
            selectorToHandlers.put(selector, new HashSet<MigratablePipeline>());
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
    Set<MigratablePipeline> getPipelines() {
        return pipelines;
    }

    // just for testing
    ItemCounter<MigratablePipeline> getLastEventCounter() {
        return lastEventCounter;
    }

    // just for testing
    ItemCounter<MigratablePipeline> getHandlerEventsCounter() {
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
                // there is no pipeline that can be migrated anyway. In that case it is better to move on to
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

    public void notifyHandlerAdded(MigratablePipeline pipeline) {
        AddPipelineTask addPipelineTask = new AddPipelineTask(pipeline);
        tasks.offer(addPipelineTask);
    }

    public void notifyHandlerRemoved(MigratablePipeline pipeline) {
        RemovePipelineTask removePipelineTask = new RemovePipelineTask(pipeline);
        tasks.offer(removePipelineTask);
    }


    private void updateNewWorkingImbalance() {
        for (MigratablePipeline pipeline : pipelines) {
            updateHandlerState(pipeline);
        }
    }

    private void updateHandlerState(MigratablePipeline pipeline) {
        long pipelineEventCount = getEventCountSinceLastCheck(pipeline);
        handlerEventsCounter.set(pipeline, pipelineEventCount);
        NioThread owner = pipeline.owner();
        selectorEvents.add(owner, pipelineEventCount);
        Set<MigratablePipeline> handlersOwnedBy = selectorToHandlers.get(owner);
        handlersOwnedBy.add(pipeline);
    }

    private long getEventCountSinceLastCheck(MigratablePipeline pipeline) {
        long eventCount = pipeline.load();
        Long lastEventCount = lastEventCounter.getAndSet(pipeline, eventCount);
        return eventCount - lastEventCount;
    }

    private void clearWorkingImbalance() {
        handlerEventsCounter.reset();
        selectorEvents.reset();
        for (Set<MigratablePipeline> handlerSet : selectorToHandlers.values()) {
            handlerSet.clear();
        }
    }

    void addPipeline(MigratablePipeline pipeline) {
        pipelines.add(pipeline);
    }

    void removePipeline(MigratablePipeline pipeline) {
        pipelines.remove(pipeline);
        handlerEventsCounter.remove(pipeline);
        lastEventCounter.remove(pipeline);
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
        sb.append("It contains following pipelines: ").
                append(LINE_SEPARATOR);
        appendSelectorInfo(minThread, selectorToHandlers, sb);

        eventCountPerSelector = selectorEvents.get(maxThread);
        sb.append("Max Selector ")
                .append(maxThread)
                .append(" received ")
                .append(eventCountPerSelector)
                .append(" events. ");
        sb.append("It contains following pipelines: ")
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
                        .append(" and has these pipelines: ")
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
            Map<NioThread, Set<MigratablePipeline>> threadHandlers,
            StringBuilder sb) {
        Set<MigratablePipeline> handlerSet = threadHandlers.get(minThread);
        for (MigratablePipeline selectionHandler : handlerSet) {
            Long eventCountPerHandler = handlerEventsCounter.get(selectionHandler);
            sb.append(selectionHandler)
                    .append(":  ")
                    .append(eventCountPerHandler)
                    .append(LINE_SEPARATOR);
        }
        sb.append(LINE_SEPARATOR);
    }

    class RemovePipelineTask implements Runnable {

        private final MigratablePipeline pipeline;

        public RemovePipelineTask(MigratablePipeline pipeline) {
            this.pipeline = pipeline;
        }

        @Override
        public void run() {
            if (logger.isFinestEnabled()) {
                logger.finest("Removing pipeline: " + pipeline);
            }

            removePipeline(pipeline);
        }
    }

    class AddPipelineTask implements Runnable {

        private final MigratablePipeline pipeline;

        public AddPipelineTask(MigratablePipeline pipeline) {
            this.pipeline = pipeline;
        }

        @Override
        public void run() {
            if (logger.isFinestEnabled()) {
                logger.finest("Adding pipeline: " + pipeline);
            }

            addPipeline(pipeline);
        }
    }

}
