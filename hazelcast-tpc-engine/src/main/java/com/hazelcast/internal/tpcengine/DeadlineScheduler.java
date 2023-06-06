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

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A scheduler that schedules {@link DeadlineTask} instances based on their
 * deadline. The scheduler is not thread-safe.
 * <p/>
 * The scheduler contains a run queue with tasks ordered by their deadline.
 * So the task with the earliest deadline, is at the beginning of the queue.
 */
public final class DeadlineScheduler {

    private int nrScheduled;
    private final int capacity;
    // -1 indicates that there is no task in the deadline scheduler.
    private long earliestDeadlineNanos = -1;

    // The DeadlineTasks are ordered by their deadline. So smallest deadline first.
    private final PriorityQueue<DeadlineTask> runQueue;

    /**
     * Creates a new scheduler with the given capacity.
     *
     * @param runQueueCapacity the capacity of the run queue
     * @throws IllegalArgumentException when the capacity is smaller than 1.
     */
    public DeadlineScheduler(int runQueueCapacity) {
        this.capacity = checkPositive(runQueueCapacity, "runQueueCapacity");
        this.runQueue = new PriorityQueue<>(runQueueCapacity);
    }

    /**
     * Returns the epoch time in nanos of the earliest deadline. If no task exist
     * with a deadline, -1 returned.
     *
     * @return the epoch time in nanos of the earliest deadline.
     */
    public long earliestDeadlineNanos() {
        return earliestDeadlineNanos;
    }

    /**
     * Offers a DeadlineTask to the scheduler. The task will be scheduled based
     * on its deadline.
     *
     * @param task the task to schedule
     * @return true if the task was added to the scheduler, false otherwise.
     */
    public boolean offer(DeadlineTask task) {
        assert task.deadlineNanos >= 0;

        if (nrScheduled == capacity) {
            return false;
        }

        nrScheduled++;
        runQueue.offer(task);

        if (task.deadlineNanos < earliestDeadlineNanos) {
            earliestDeadlineNanos = task.deadlineNanos;
        }

        return true;
    }

    /**
     * Gives the DeadlineScheduler a chance to schedule tasks. This method should be
     * called periodically.
     *
     * @param nowNanos the current epoch time in nanos.
     */
    public void tick(long nowNanos) {
        assert nowNanos >= 0;

        // We keep removing items from the runQueue until we find a task that is
        // not ready to be scheduled. All items remaining items are certainly
        // not ready to be scheduled.
        for (; ; ) {
            DeadlineTask task = runQueue.peek();

            if (task == null) {
                // Since the runQueue is empty, the earlierDeadlineNanos is reset to -1.
                earliestDeadlineNanos = -1;
                return;
            }

            if (task.deadlineNanos > nowNanos) {
                // the first item on the run queue should not be scheduled yet
                earliestDeadlineNanos = task.deadlineNanos;
                // we are done since all other tasks have even a larger deadline.
                return;
            }

            // the task first needs to be removed from the run queue since we peeked it.
            runQueue.poll();
            nrScheduled--;

            // offer the task to its task group.
            // this will trigger the taskQueue to schedule itself if needed.

            // todo: return value is ignored.
            task.taskQueue.offerLocal(task);

            // and go to the next task.
        }
    }
}
