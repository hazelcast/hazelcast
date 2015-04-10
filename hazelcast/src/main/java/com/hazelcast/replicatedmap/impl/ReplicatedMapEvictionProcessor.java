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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.replicatedmap.impl.record.ReplicatedMapContainer;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;

/**
 * Actual eviction processor implementation to remove values to evict values from the replicated map
 */
public class ReplicatedMapEvictionProcessor
        implements ScheduledEntryProcessor<Object, Object> {

    final NodeEngine nodeEngine;
    final ReplicatedMapService replicatedMapService;
    final String mapName;

    public ReplicatedMapEvictionProcessor(NodeEngine nodeEngine, ReplicatedMapService replicatedMapService, String mapName) {
        this.nodeEngine = nodeEngine;
        this.replicatedMapService = replicatedMapService;
        this.mapName = mapName;
    }

    public void process(EntryTaskScheduler<Object, Object> scheduler, Collection<ScheduledEntry<Object, Object>> entries) {
        final ReplicatedMapContainer replicatedMapContainer = replicatedMapService.getReplicatedMapContainer(mapName, false);

        if (replicatedMapContainer == null) {
            return;
        }

        for (ScheduledEntry<Object, Object> entry : entries) {
            Object key = entry.getKey();
            if (entry.getValue() == null) {
                replicatedMapContainer.removeTombstone(key);
            } else {
                replicatedMapContainer.evict(key);
            }
        }
    }
}
