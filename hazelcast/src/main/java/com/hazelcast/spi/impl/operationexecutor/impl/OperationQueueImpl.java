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

package com.hazelcast.spi.impl.operationexecutor.impl;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.util.Preconditions.checkNotNull;

public final class OperationQueueImpl implements OperationQueue {

    static final Object TRIGGER_TASK = new Object() {
        // purely for debugging purposes.
        @Override
        public String toString() {
            return "triggerTask";
        }
    };

    private final BlockingQueue<Object> normalQueue;
    private final Queue<Object> priorityQueue;

    // this queue is used for returned batches so these bathes can be interleaved
    // with other operations to prevent bubbles.
    private final Queue<TaskBatch> batchesInProgressQueue = new LinkedList<TaskBatch>();

    public OperationQueueImpl() {
        this(new LinkedBlockingQueue<Object>(), new ConcurrentLinkedQueue<Object>());
    }

    public OperationQueueImpl(BlockingQueue<Object> normalQueue, Queue<Object> priorityQueue) {
        this.normalQueue = checkNotNull(normalQueue, "normalQueue");
        this.priorityQueue = checkNotNull(priorityQueue, "priorityQueue");
    }

    @Override
    public int normalSize() {
        return normalQueue.size();
    }

    @Override
    public int prioritySize() {
        return priorityQueue.size();
    }

    @Override
    public int size() {
        return normalQueue.size() + priorityQueue.size();
    }

    @Override
    public void add(Object task, boolean priority) {
        checkNotNull(task, "task can't be null");

        if (priority) {
            priorityQueue.add(task);
            normalQueue.add(TRIGGER_TASK);
        } else {
            normalQueue.add(task);
        }
    }

    @Override
    public void returnBatch(TaskBatch batch) {
        batchesInProgressQueue.add(batch);
    }

    @Override
    public Object take(boolean priorityOnly) throws InterruptedException {
        if (priorityOnly) {
            return ((BlockingQueue) priorityQueue).take();
        }

        for (; ; ) {
            Object priorityItem = priorityQueue.poll();
            if (priorityItem != null) {
                return priorityItem;
            }

            Object normalItem;
            if (batchesInProgressQueue.isEmpty()) {
                normalItem = normalQueue.take();

                if (normalItem == TRIGGER_TASK) {
                    continue;
                }

                return normalItem;
            } else {
                normalItem = normalQueue.poll();

                if (normalItem == TRIGGER_TASK) {
                    continue;
                }

                if (normalItem != null) {
                    return normalItem;
                }

                return batchesInProgressQueue.poll();
            }
        }
    }
}
