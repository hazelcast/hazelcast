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

import static java.lang.Math.max;

/**
 * A first come first serve (FIFO) {@link Scheduler}. So tasks
 * are added to a queue and the first task in the queue is picked. On yield
 * the task queue added to the end of the queue.
 * <p>
 * The time slice is calculated as by dividing the target latency by
 * the number of running taskQueues. To prevent very small time slices,
 * there is a lower bound using the {@code minGranularityNanos}.
 * <p>
 * The FifoScheduler will be helpful to pinpoint problems:
 * <ol>
 *     <li>we can exclude the {@link CompletelyFairScheduler} if there is some
 *     scheduling problem and we have difficulties which component is the
 *     cause.</li>
 *     <li>we can exclude the overhead of the O(log(n)) complexity of the
 *     {@link CompletelyFairScheduler}. This will be useful for benchmarking so
 *     we can have a baseline to compare against.</li>
 * </ol>
 */
public class FifoScheduler extends Scheduler {

    final CircularQueue<TaskQueue> runQueue;
    final int runQueueLimit;
    int runQueueSize;
    final long targetLatencyNanos;
    final long minGranularityNanos;
    TaskQueue active;

    public FifoScheduler(int runQueueLimit,
                         long targetLatencyNanos,
                         long minGranularityNanos) {
        this.runQueue = new CircularQueue<>(runQueueLimit);
        this.runQueueLimit = runQueueLimit;
        this.targetLatencyNanos = targetLatencyNanos;
        this.minGranularityNanos = minGranularityNanos;
    }

    @Override
    public int runQueueLimit() {
        return runQueueLimit;
    }

    @Override
    public int runQueueSize() {
        return runQueueSize;
    }

    @Override
    public long timeSliceNanosActive() {
        assert active != null;

        long timeslice = targetLatencyNanos / runQueueSize;
        return max(minGranularityNanos, timeslice);
    }

    @Override
    public TaskQueue pickNext() {
        assert active == null;

        active = runQueue.peek();
        return active;
    }

    @Override
    public void updateActive(long cpuTimeNanos) {
        active.actualRuntimeNanos += cpuTimeNanos;
    }

    @Override
    public void dequeueActive() {
        assert active != null;

        runQueue.poll();
        runQueueSize--;
        active = null;
    }

    @Override
    public void yieldActive() {
        assert active != null;

        if (runQueueSize > 1) {
            // if there is only one taskQueue in the runQueue, then there
            // is no need to remove and then add the item
            runQueue.poll();
            runQueue.add(active);
        }

        active = null;
    }

    @Override
    public void enqueue(TaskQueue taskQueue) {
        // the eventloop should control the number of created taskQueues
        assert runQueueSize <= runQueueLimit;
        runQueue.add(taskQueue);
        runQueueSize++;
    }
}
