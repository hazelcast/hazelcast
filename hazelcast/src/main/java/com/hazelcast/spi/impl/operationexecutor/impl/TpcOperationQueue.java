/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.tpcengine.TaskQueue;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class TpcOperationQueue extends AbstractQueue implements OperationQueue {

    // There is no data-race on this queue because the field is set before the thread is started.
    private TaskQueue taskQueue;
    private final Queue<Object> normalQueue;
    private final Queue<Object> priorityQueue;

    public TpcOperationQueue(Queue<Object> normalQueue, Queue<Object> priorityQueue) {
        this.normalQueue = checkNotNull(normalQueue, "normalQueue");
        this.priorityQueue = checkNotNull(priorityQueue, "priorityQueue");
    }

    @Override
    public Iterator iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Object o) {
        return normalQueue.offer(o);
    }

    @Override
    public Object peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(Object task, boolean priority) {
        if (priority) {
            priorityQueue.offer(task);
        } else {
            normalQueue.offer(task);
        }
        // the problem is that adding the task should be done through the
        // task queue because the task queue isn't scheduled this way
        taskQueue.schedule();
    }

    @Override
    public Object take(boolean priorityOnly) throws InterruptedException {
        // We never want to block on the TpcPartitionOperationThread.
        throw new UnsupportedOperationException();
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
    public Object poll() {
        Object result = priorityQueue.poll();
        if (result == null) {
            result = normalQueue.poll();
        }
        return result;
    }

    @Override
    public boolean isEmpty() {
        return normalQueue.isEmpty() && priorityQueue.isEmpty();
    }

    public void setTaskQueue(TaskQueue taskQueue) {
        this.taskQueue = taskQueue;
    }
}
