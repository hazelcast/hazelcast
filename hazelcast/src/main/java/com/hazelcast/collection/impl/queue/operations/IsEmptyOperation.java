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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.internal.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

/**
 * check if queue is empty
 */
public class IsEmptyOperation extends QueueOperation implements ReadonlyOperation {

    public IsEmptyOperation() {
    }

    public IsEmptyOperation(final String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        response = queueContainer.size() == 0;
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.IS_EMPTY;
    }
}
