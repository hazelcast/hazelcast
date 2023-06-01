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

package com.hazelcast.internal.tpcengine;

import java.util.PriorityQueue;

import static java.lang.Math.max;

/**
 * https://docs.kernel.org/scheduler/sched-design-CFS.html
 */
@SuppressWarnings({"checkstyle:MemberName"})
class CfsScheduler {

    private final PriorityQueue<TaskGroup> runQueue;
    private final int capacity;
    private long min_vruntimeNanos;
    private int size;

    CfsScheduler(int runQueueCapacity) {
        this.runQueue = new PriorityQueue<>(runQueueCapacity);
        this.capacity = runQueueCapacity;
    }

    /**
     * Returns the number of items in the runQueue.
     *
     * @return the size of the runQueue.
     */
    public int size() {
        return size;
    }

    public TaskGroup pickNext() {
        TaskGroup group = runQueue.poll();
        if (group == null) {
            return null;
        }

        TaskGroup peek = runQueue.peek();
        if (peek != null) {
            min_vruntimeNanos = peek.vruntimeNanos;
        }
        size--;
        return group;
    }

    public void enqueue(TaskGroup taskGroup) {
        if (size == capacity) {
            throw new IllegalStateException();
        }
        size++;
        taskGroup.state = TaskGroup.STATE_RUNNING;
        taskGroup.vruntimeNanos = max(taskGroup.vruntimeNanos, min_vruntimeNanos);
        runQueue.add(taskGroup);
    }
}
