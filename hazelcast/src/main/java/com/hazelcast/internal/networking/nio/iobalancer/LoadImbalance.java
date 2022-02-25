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
import com.hazelcast.internal.networking.nio.NioPipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.internal.util.ItemCounter;

import java.util.Map;
import java.util.Set;

/**
 * Describes a state of NioThread (im-)balance.
 *
 * It's used by {@link MigrationStrategy} to decide whether and what
 * {@link NioPipeline} should be migrated.
 */
class LoadImbalance {
    //load recorded by the busiest NioThread
    long maximumLoad;
    //load recorded by the least busy NioThread
    long minimumLoad;
    //busiest NioThread
    NioThread srcOwner;
    //least busy NioThread
    NioThread dstOwner;

    private final Map<NioThread, Set<MigratablePipeline>> ownerToPipelines;
    private final ItemCounter<MigratablePipeline> pipelineLoadCounter;

    LoadImbalance(Map<NioThread, Set<MigratablePipeline>> ownerToPipelines,
                  ItemCounter<MigratablePipeline> pipelineLoadCounter) {
        this.ownerToPipelines = ownerToPipelines;
        this.pipelineLoadCounter = pipelineLoadCounter;
    }

    /**
     * @param owner
     * @return A set of Pipelines owned by the owner
     */
    Set<MigratablePipeline> getPipelinesOwnedBy(NioThread owner) {
        return ownerToPipelines.get(owner);
    }

    /**
     * @param pipeline
     * @return load recorded by the pipeline
     */
    long getLoad(MigratablePipeline pipeline) {
        return pipelineLoadCounter.get(pipeline);
    }
}
