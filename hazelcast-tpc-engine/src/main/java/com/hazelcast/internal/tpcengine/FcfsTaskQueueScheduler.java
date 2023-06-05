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

import com.hazelcast.internal.tpcengine.util.CircularQueue;

import static java.lang.Math.min;

/**
 * A first come first serve (FIFO) {@link TaskQueueScheduler}. So task
 * are added to a queue and the first task in the queue is picked. On yield
 * or new task queues are added to the end of the queue.
 * <p>
 * The time slice is calculated as by dividing the target latency by
 * the number of running taskQueues. To prevent very small time slices,
 * there is a lower bound using the {@code minGranularityNanos}.
 * <p>
 * The FcfsTaskQueueScheduler will be helpful to pinpoint to problems. Because
 * it can easily replace the FcsScheduler and can exclude problems in the
 * FcsScheduler. Also it will be useful to measure the performance advantages/overhead
 * of the CfsScheduler.
 */
class FcfsTaskQueueScheduler implements TaskQueueScheduler {

    private final CircularQueue<TaskQueue> runQueue;
    private final int capacity;
    private final long targetLatencyNanos;
    private final long minGranularityNanos;
    private int nrRunning;
    private TaskQueue current;

    FcfsTaskQueueScheduler(int runQueueCapacity,
                           long targetLatencyNanos,
                           long minGranularityNanos) {
        this.runQueue = new CircularQueue<>(runQueueCapacity);
        this.capacity = runQueueCapacity;
        this.targetLatencyNanos = targetLatencyNanos;
        this.minGranularityNanos = minGranularityNanos;
    }

    @Override
    public long timeSliceNanosCurrent() {
        assert current != null;

        return min(minGranularityNanos, targetLatencyNanos / nrRunning);
    }

    @Override
    public TaskQueue pickNext() {
        assert current == null;

        current = runQueue.peek();
        return current;
    }

    @Override
    public void updateCurrent(long deltaNanos) {
        current.sumExecRuntimeNanos += deltaNanos;
    }

    @Override
    public void dequeueCurrent() {
        assert current != null;

        runQueue.poll();
        nrRunning--;
        current = null;
    }

    @Override
    public void yieldCurrent() {
        assert current != null;

        if (nrRunning > 1) {
            // if there is only one taskQueue in the runQueue, then there
            // is no need to remove and then add the item
            runQueue.poll();
            runQueue.add(current);
        }

        current = null;
    }

    @Override
    public void enqueue(TaskQueue taskQueue) {
        // the eventloop should control the number of created taskQueues
        assert nrRunning <= capacity;

        nrRunning++;
        taskQueue.runState = TaskQueue.RUN_STATE_RUNNING;
        runQueue.add(taskQueue);
    }
}
