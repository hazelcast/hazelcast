/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;

/**
 * @ali 7/23/13
 */
public class QueueEvictionProcessor implements ScheduledEntryProcessor<String, Void> {

    final NodeEngine nodeEngine;

    final QueueService service;

    public QueueEvictionProcessor(NodeEngine nodeEngine, QueueService service) {
        this.nodeEngine = nodeEngine;
        this.service = service;
    }

    public void process(EntryTaskScheduler<String, Void> scheduler, Collection<ScheduledEntry<String, Void>> entries) {
        if (entries.isEmpty()) {
            return;
        }
        for (ScheduledEntry<String, Void> entry : entries) {
            String name = entry.getKey();
            int partitionId = nodeEngine.getPartitionService().getPartitionId(nodeEngine.toData(name));
            CheckAndEvictOperation op = new CheckAndEvictOperation(entry.getKey());
            nodeEngine.getOperationService().invokeOnPartition(QueueService.SERVICE_NAME, op, partitionId).getSafely();
        }

    }
}
