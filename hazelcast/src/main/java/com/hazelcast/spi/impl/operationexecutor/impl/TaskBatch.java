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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;

import java.util.BitSet;

import static com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl.getPartitionThreadId;

/**
 * A 'batch' of tasks to be executed on a partition thread.
 */
public class TaskBatch {

    private final PartitionTaskFactory taskFactory;
    private final BitSet partitions;
    private final int threadId;
    private final int partitionThreadCount;
    private int nextPartitionId;

    public TaskBatch(PartitionTaskFactory taskFactory, BitSet partitions, int threadId, int partitionThreadCount) {
        this.taskFactory = taskFactory;
        this.partitions = partitions;
        this.threadId = threadId;
        this.partitionThreadCount = partitionThreadCount;
    }

    public PartitionTaskFactory taskFactory() {
        return taskFactory;
    }

    /**
     * Gets the next task to execute.
     *
     * @return the task to execute, or null if the batch is complete.
     */
    public Object next() {
        int partitionId = nextPartitionId();
        return partitionId == -1 ? null : taskFactory.create(partitionId);
    }

    private int nextPartitionId() {
        for (; ; ) {
            int partitionId = partitions.nextSetBit(nextPartitionId);
            if (partitionId == -1) {
                return -1;
            }

            nextPartitionId = partitionId + 1;

            if (getPartitionThreadId(partitionId, partitionThreadCount) == threadId) {
                // only selected partitions that belong to the right partition thread.
                return partitionId;
            }
        }
    }
}
