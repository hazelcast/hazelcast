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
import com.hazelcast.internal.networking.nio.NioPipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.util.ItemCounter;

import java.util.Map;
import java.util.Set;

/**
 * Describes a state of NioThread (im-)balance.
 *
 * It's used by {@link MigrationStrategy} to decide whether and what
 * {@link NioPipeline} should be migrated.
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

    private final Map<NioThread, Set<MigratablePipeline>> selectorToHandlers;
    private final ItemCounter<MigratablePipeline> handlerLoadCounter;

    LoadImbalance(Map<NioThread, Set<MigratablePipeline>> selectorToHandlers,
                  ItemCounter<MigratablePipeline> handlerLoadCounter) {
        this.selectorToHandlers = selectorToHandlers;
        this.handlerLoadCounter = handlerLoadCounter;
    }

    /**
     * @param selector
     * @return A set of Handlers owned by the selector
     */
    Set<MigratablePipeline> getPipelinesOwnerBy(NioThread selector) {
        return selectorToHandlers.get(selector);
    }

    /**
     * @param pipeline
     * @return load recorded by the pipeline
     */
    long getLoad(MigratablePipeline pipeline) {
        return handlerLoadCounter.get(pipeline);
    }
}
