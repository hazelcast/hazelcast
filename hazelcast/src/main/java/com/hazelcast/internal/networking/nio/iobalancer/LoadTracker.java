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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.util.ItemCounter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;

/**
 * Tracks the load of of NioThread(s) and creates a mapping between NioThread -> NioPipeline.
 * <p>
 * This class is not thread-safe with the exception of
 * {@link #addPipeline(MigratablePipeline)}   and
 * {@link #removePipeline(MigratablePipeline)}
 */
class LoadTracker {
    private final ILogger logger;

    //all known IO ioThreads. we assume no. of ioThreads is constant during a lifespan of a member
    private final NioThread[] ioThreads;
    private final Map<NioThread, Set<MigratablePipeline>> ownerToPipelines;

    //load per pipeline since an instance started
    private final ItemCounter<MigratablePipeline> lastLoadCounter = new ItemCounter<MigratablePipeline>();

    //load per NioThread since last calculation
    private final ItemCounter<NioThread> ownerLoad = new ItemCounter<NioThread>();
    //load per pipeline since last calculation
    private final ItemCounter<MigratablePipeline> pipelineLoadCount = new ItemCounter<MigratablePipeline>();

    //contains all known pipelines
    private final Set<MigratablePipeline> pipelines = new HashSet<MigratablePipeline>();

    private final LoadImbalance imbalance;

    LoadTracker(NioThread[] ioThreads, ILogger logger) {
        this.logger = logger;

        this.ioThreads = new NioThread[ioThreads.length];
        System.arraycopy(ioThreads, 0, this.ioThreads, 0, ioThreads.length);

        this.ownerToPipelines = createHashMap(ioThreads.length);
        for (NioThread selector : ioThreads) {
            ownerToPipelines.put(selector, new HashSet<MigratablePipeline>());
        }
        this.imbalance = new LoadImbalance(ownerToPipelines, pipelineLoadCount);
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

    // just for testing
    Set<MigratablePipeline> getPipelines() {
        return pipelines;
    }

    // just for testing
    ItemCounter<MigratablePipeline> getLastLoadCounter() {
        return lastLoadCounter;
    }

    // just for testing
    ItemCounter<MigratablePipeline> getPipelineLoadCount() {
        return pipelineLoadCount;
    }

    private void updateNewFinalImbalance() {
        imbalance.minimumLoad = Long.MAX_VALUE;
        imbalance.maximumLoad = Long.MIN_VALUE;
        imbalance.srcOwner = null;
        imbalance.dstOwner = null;
        for (NioThread owner : ioThreads) {
            long load = ownerLoad.get(owner);
            int pipelineCount = ownerToPipelines.get(owner).size();

            if (load > imbalance.maximumLoad && pipelineCount > 1) {
                // if a nioThread has only 1 handle, there is no point in making it a source nioThread since
                // there is no pipeline that can be migrated anyway. In that case it is better to move on to
                // the next nioThread.
                imbalance.maximumLoad = load;
                imbalance.srcOwner = owner;
            }

            if (load < imbalance.minimumLoad) {
                imbalance.minimumLoad = load;
                imbalance.dstOwner = owner;
            }
        }
    }

    private void updateNewWorkingImbalance() {
        for (MigratablePipeline pipeline : pipelines) {
            updatePipelineState(pipeline);
        }
    }

    private void updatePipelineState(MigratablePipeline pipeline) {
        long pipelineLoad = getLoadSinceLastCheck(pipeline);
        pipelineLoadCount.set(pipeline, pipelineLoad);
        NioThread owner = pipeline.owner();
        if (owner == null) {
            // the pipeline is currently being migrated - owner is null
            return;
        }
        ownerLoad.add(owner, pipelineLoad);
        ownerToPipelines.get(owner).add(pipeline);
    }

    private long getLoadSinceLastCheck(MigratablePipeline pipeline) {
        long load = pipeline.load();
        long lastLoad = lastLoadCounter.getAndSet(pipeline, load);
        return load - lastLoad;
    }

    private void clearWorkingImbalance() {
        pipelineLoadCount.reset();
        ownerLoad.reset();
        for (Set<MigratablePipeline> pipelines : ownerToPipelines.values()) {
            pipelines.clear();
        }
    }

    void addPipeline(MigratablePipeline pipeline) {
        pipelines.add(pipeline);
    }

    void removePipeline(MigratablePipeline pipeline) {
        pipelines.remove(pipeline);
        pipelineLoadCount.remove(pipeline);
        lastLoadCounter.remove(pipeline);
    }

    private void printDebugTable() {
        if (!logger.isFinestEnabled()) {
            return;
        }

        NioThread minThread = imbalance.dstOwner;
        NioThread maxThread = imbalance.srcOwner;
        if (minThread == null || maxThread == null) {
            return;
        }
        StringBuilder sb = new StringBuilder(LINE_SEPARATOR)
                .append("------------")
                .append(LINE_SEPARATOR);
        Long loadPerOwner = ownerLoad.get(minThread);

        sb.append("Min NioThread ")
                .append(minThread)
                .append(" receive-load ")
                .append(loadPerOwner)
                .append(" load. ");
        sb.append("It contains following pipelines: ").
                append(LINE_SEPARATOR);
        appendSelectorInfo(minThread, ownerToPipelines, sb);

        loadPerOwner = ownerLoad.get(maxThread);
        sb.append("Max NioThread ")
                .append(maxThread)
                .append(" receive-load ")
                .append(loadPerOwner);
        sb.append("It contains following pipelines: ")
                .append(LINE_SEPARATOR);
        appendSelectorInfo(maxThread, ownerToPipelines, sb);

        sb.append("Other NioThread: ")
                .append(LINE_SEPARATOR);

        for (NioThread thread : ioThreads) {
            if (!thread.equals(minThread) && !thread.equals(maxThread)) {
                loadPerOwner = ownerLoad.get(thread);
                sb.append("NioThread ")
                        .append(thread)
                        .append(" contains ")
                        .append(loadPerOwner)
                        .append(" and has these pipelines: ")
                        .append(LINE_SEPARATOR);
                appendSelectorInfo(thread, ownerToPipelines, sb);
            }
        }
        sb.append("------------")
                .append(LINE_SEPARATOR);
        logger.finest(sb.toString());
    }

    private void appendSelectorInfo(
            NioThread minThread,
            Map<NioThread, Set<MigratablePipeline>> pipelinesPerOwner,
            StringBuilder sb) {
        Set<MigratablePipeline> pipelines = pipelinesPerOwner.get(minThread);
        for (MigratablePipeline pipeline : pipelines) {
            Long loadPerPipeline = pipelineLoadCount.get(pipeline);
            sb.append(pipeline)
                    .append(":  ")
                    .append(loadPerPipeline)
                    .append(LINE_SEPARATOR);
        }
        sb.append(LINE_SEPARATOR);
    }
}
