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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.impl.queue.operations.CheckAndEvictOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.scheduler.EntryTaskScheduler;
import com.hazelcast.internal.util.scheduler.ScheduledEntry;
import com.hazelcast.internal.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;

/**
 * Eviction Processor for the Queue.
 */
public class QueueEvictionProcessor implements ScheduledEntryProcessor<String, Void> {

    private final NodeEngine nodeEngine;

    public QueueEvictionProcessor(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void process(EntryTaskScheduler<String, Void> scheduler, Collection<ScheduledEntry<String, Void>> entries) {
        if (entries.isEmpty()) {
            return;
        }

        IPartitionService partitionService = nodeEngine.getPartitionService();
        OperationService operationService = nodeEngine.getOperationService();

        for (ScheduledEntry<String, Void> entry : entries) {
            String name = entry.getKey();
            int partitionId = partitionService.getPartitionId(nodeEngine.toData(name));
            Operation op = new CheckAndEvictOperation(entry.getKey())
                    .setPartitionId(partitionId);

            operationService.invokeOnPartition(op).joinInternal();
        }
    }
}
