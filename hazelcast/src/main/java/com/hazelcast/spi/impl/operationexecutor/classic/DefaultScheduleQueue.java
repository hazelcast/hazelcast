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

package com.hazelcast.spi.impl.operationexecutor.classic;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.util.Preconditions.checkNotNull;

public final class DefaultScheduleQueue implements ScheduleQueue {

    static final Object TRIGGER_TASK = new Object() {
        public String toString() {
            return "triggerTask";
        }
    };

    private final BlockingQueue normalQueue;
    private final ConcurrentLinkedQueue priorityQueue;
    private Object pendingNormalItem;

    public DefaultScheduleQueue() {
        this(new LinkedBlockingQueue(), new ConcurrentLinkedQueue());
    }

    public DefaultScheduleQueue(BlockingQueue normalQueue, ConcurrentLinkedQueue priorityQueue) {
        this.normalQueue = checkNotNull(normalQueue, "normalQueue");
        this.priorityQueue = checkNotNull(priorityQueue, "priorityQueue");
    }

    @Override
    public void add(Object task) {
        checkNotNull(task, "task can't be null");

        normalQueue.add(task);
    }

    @Override
    public void addUrgent(Object task) {
        checkNotNull(task, "task can't be null");

        priorityQueue.add(task);
        normalQueue.add(TRIGGER_TASK);
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
    public Object take() throws InterruptedException {
        ConcurrentLinkedQueue priorityQueue = this.priorityQueue;
        for (; ; ) {
            Object priorityItem = priorityQueue.poll();
            if (priorityItem != null) {
                return priorityItem;
            }

            if (pendingNormalItem != null) {
                Object tmp = pendingNormalItem;
                pendingNormalItem = null;
                return tmp;
            }

            Object normalItem = normalQueue.take();
            if (normalItem == TRIGGER_TASK) {
                continue;
            }

            priorityItem = priorityQueue.poll();
            if (priorityItem != null) {
                pendingNormalItem = normalItem;
                return priorityItem;
            }

            return normalItem;
        }
    }
}
