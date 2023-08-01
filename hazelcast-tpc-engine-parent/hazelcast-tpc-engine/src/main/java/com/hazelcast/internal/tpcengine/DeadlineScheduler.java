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

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.Promise;

import java.util.PriorityQueue;

import static com.hazelcast.internal.tpcengine.util.EpochClock.epochNanos;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A scheduler that schedules {@link DeadlineTask} instances based on their
 * deadline (some point in time in the future).
 * <p/>
 * The scheduler is not thread-safe.
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
            task.taskQueue.offerInside(task);

            // and go to the next task.
        }
    }

    /**
     * A task that is going to be scheduled once or multiple times at some point
     * in the future.
     * <p>
     * todo: Should the DeadlineTask be a Task implementation? Or should the
     * DeadlineTask allow for executing
     * a Task?
     * <p>
     * todo: We need to deal with yielding of the task and we need to prevent the
     * task from being executed due to the deadline and then being executed again
     * due to the yield.
     */
    static final class DeadlineTask implements Runnable, Comparable<DeadlineTask> {

        private static final TpcLogger LOGGER = TpcLoggerLocator.getLogger(DeadlineTask.class);

        Promise promise;
        long deadlineNanos;
        Runnable cmd;
        long periodNanos = -1;
        long delayNanos = -1;
        TaskQueue taskQueue;
        private final DeadlineScheduler deadlineScheduler;

        DeadlineTask(DeadlineScheduler deadlineScheduler) {
            this.deadlineScheduler = deadlineScheduler;
        }

        @Override
        public void run() {
            if (cmd != null) {
                cmd.run();
            }

            if (periodNanos != -1 || delayNanos != -1) {
                if (periodNanos != -1) {
                    deadlineNanos += periodNanos;
                } else {
                    deadlineNanos = epochNanos() + delayNanos;
                }

                if (deadlineNanos < 0) {
                    deadlineNanos = Long.MAX_VALUE;
                }

                if (!deadlineScheduler.offer(this)) {
                    LOGGER.warning("Failed schedule task: " + this + " because there "
                            + "is no space in deadlineScheduler");
                }
            } else {
                if (promise != null) {
                    promise.complete(null);
                }
            }
        }

        @Override
        public int compareTo(DeadlineTask that) {
            return Long.compare(this.deadlineNanos, that.deadlineNanos);
        }

        @Override
        public String toString() {
            return "DeadlineTask{"
                    + "promise=" + promise
                    + ", deadlineNanos=" + deadlineNanos
                    + ", task=" + cmd
                    + ", periodNanos=" + periodNanos
                    + ", delayNanos=" + delayNanos
                    + '}';
        }
    }
}
