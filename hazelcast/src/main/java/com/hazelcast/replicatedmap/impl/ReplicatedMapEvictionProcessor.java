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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.replicatedmap.impl.operation.EvictionOperation;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.util.scheduler.EntryTaskScheduler;
import com.hazelcast.internal.util.scheduler.ScheduledEntry;
import com.hazelcast.internal.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Actual eviction processor implementation to remove values to evict values from the replicated map
 */
public class ReplicatedMapEvictionProcessor implements ScheduledEntryProcessor<Object, Object> {

    private ReplicatedRecordStore store;
    private NodeEngine nodeEngine;
    private int partitionId;

    public ReplicatedMapEvictionProcessor(ReplicatedRecordStore store, NodeEngine nodeEngine, int partitionId) {
        this.store = store;
        this.nodeEngine = nodeEngine;
        this.partitionId = partitionId;
    }

    public void process(EntryTaskScheduler<Object, Object> scheduler, Collection<ScheduledEntry<Object, Object>> entries) {
        EvictionOperation evictionOperation = new EvictionOperation(store, entries);
        evictionOperation.setPartitionId(partitionId);
        nodeEngine.getOperationService().invokeOnTarget(SERVICE_NAME, evictionOperation, nodeEngine.getThisAddress());
    }
}
