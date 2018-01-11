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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.spi.ReadonlyOperation;

/**
 * Returns the remaining capacity of the queue based on config max-size
 */
public class RemainingCapacityOperation extends QueueOperation implements ReadonlyOperation {

    public RemainingCapacityOperation() {
    }

    public RemainingCapacityOperation(String name) {
        super(name);
    }

    @Override
    public void run() {
        QueueContainer queueContainer = getContainer();
        response = queueContainer.getConfig().getMaxSize() - queueContainer.size();
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.REMAINING_CAPACITY;
    }
}
