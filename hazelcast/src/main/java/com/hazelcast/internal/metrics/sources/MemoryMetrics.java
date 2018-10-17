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

package com.hazelcast.internal.metrics.sources;

import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.memory.MemoryStats;

public final class MemoryMetrics implements MetricsSource {

    private final NodeExtension nodeExtension;

    public MemoryMetrics(NodeExtension nodeExtension) {
        this.nodeExtension = nodeExtension;
    }

    @Override
    public void collectAll(CollectionCycle cycle) {
        collectMemoryAndGc(cycle, nodeExtension.getMemoryStats());
    }

    public static void collectMemoryAndGc(CollectionCycle cycle, MemoryStats memoryStats) {
        if (memoryStats != null) {
            cycle.switchContext().namespace("memory");
            cycle.collectAll(memoryStats);
            cycle.switchContext().namespace("gc");
            cycle.collectAll(memoryStats.getGCStats());
        }
    }

}
