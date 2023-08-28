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

import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.tpcengine.util.EpochClock.epochNanos;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A scheduler that schedules {@link DeadlineTask} instances based on their
 * deadline (some point in time in the future).
 * <p/>
 * The DeadlineScheduler is not thread-safe.
 * <p/>
 * The DeadlineScheduler contains a run queue with tasks ordered by their deadline.
 * So the task with the earliest deadline, is at the beginning of the queue.
 * <p/>
 * The DeadlineScheduler pools and DeadlineTasks and creates them up front.
 * If the pool is empty, no further DeadlineTasks can be scheduled.
 */
public final class DeadlineScheduler {
    private static final TpcLogger LOGGER = TpcLoggerLocator.getLogger(DeadlineScheduler.class);

    TaskQueue defaultTaskQueue;

    // -1 indicates that there is no task in the deadline scheduler.
    private long earliestDeadlineNanos = -1;
    // The DeadlineTasks are ordered by their deadline. So smallest deadline first.
    private final PriorityQueue<DeadlineTask> runQueue;

    private final int poolCapacity;
    private final DeadlineTask[] pool;
    private int poolAllocIndex;

    /**
     * Creates a new scheduler with the given runQueue limit.
     *
     * @param runQueueLimit the limit on the number of items in the runQueue.
     * @throws IllegalArgumentException when the runQueueLimit is smaller than 1.
     */
    DeadlineScheduler(int runQueueLimit) {
        checkPositive(runQueueLimit, "runQueueLimit");
        this.runQueue = new PriorityQueue<>(runQueueLimit);

        this.poolCapacity = runQueueLimit;
        this.pool = new DeadlineTask[poolCapacity];
        for (int k = 0; k < poolCapacity; k++) {
            pool[k] = new DeadlineTask(this);
        }
    }

    private static long toDeadlineNanos(long delay, TimeUnit unit) {
        long deadlineNanos = epochNanos() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        return deadlineNanos;
    }

    private DeadlineTask allocate() {
        if (poolAllocIndex == poolCapacity) {
            return null;
        }

        DeadlineTask task = pool[poolAllocIndex];
        poolAllocIndex++;
        return task;
    }

    private void free(DeadlineTask task) {
        task.clear();
        poolAllocIndex--;
        pool[poolAllocIndex] = task;
        return;
    }

    /**
     * Returns the epoch time in nanos of the earliest deadline. If no task exist
     * with a deadline, -1 returned.
     *
     * @return the epoch time in nanos of the earliest deadline.
     */
    public long earliestDeadlineNs() {
        return earliestDeadlineNanos;
    }

    /**
     * Schedules a task to be performed with some delay.
     *
     * @param cmd   the task to perform.
     * @param delay the delay
     * @param unit  the unit of the delay
     * @return true if the task was scheduled, false if the task was rejected.
     * @throws NullPointerException     if cmd or unit is null.
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public boolean schedule(Runnable cmd, long delay, TimeUnit unit) {
        return schedule(cmd, delay, unit, defaultTaskQueue);
    }

    /**
     * Schedules a one shot action with the given delay.
     *
     * @param cmd       the cmd to execute.
     * @param delay     the delay
     * @param unit      the unit of the delay
     * @param taskQueue the handle of the TaskQueue the cmd belongs to.
     * @return true if the cmd was successfully scheduled.
     * @throws NullPointerException     if cmd or unit is null
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public boolean schedule(Runnable cmd, long delay, TimeUnit unit, TaskQueue taskQueue) {
        checkNotNull(cmd);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);
        checkNotNull(taskQueue);

        DeadlineTask task = allocate();
        if (task == null) {
            return false;
        }
        task.runnable = cmd;
        task.taskQueue = taskQueue;
        task.deadlineNanos = toDeadlineNanos(delay, unit);
        put(task);
        return true;
    }

    /**
     * Creates a periodically executing cmd with a fixed delay between the
     * completion and start of the cmd.
     *
     * @param cmd          the cmd to periodically execute. As long as true is
     *                     returned, the task will reschedule itself.
     * @param initialDelay the initial delay
     * @param delay        the delay between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the cmd was successfully executed.
     */
    public boolean scheduleWithFixedDelay(Callable<Boolean> cmd,
                                          long initialDelay,
                                          long delay,
                                          TimeUnit unit,
                                          TaskQueue taskQueue) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(delay, "delay");
        checkNotNull(unit);
        checkNotNull(taskQueue);

        DeadlineTask task = allocate();
        if (task == null) {
            return false;
        }
        task.callable = cmd;
        task.taskQueue = taskQueue;
        task.deadlineNanos = toDeadlineNanos(initialDelay, unit);
        task.delayNanos = unit.toNanos(delay);
        put(task);
        return true;
    }

    /**
     * Creates a periodically executing cmd with a fixed delay between the start
     * of the cmd.
     *
     * @param cmd          the cmd to periodically execute. As long as true is
     *                     returned, the task will reschedule itself.
     * @param initialDelay the initial delay
     * @param period       the period between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the cmd was successfully executed.
     */
    public boolean scheduleAtFixedRate(Callable<Boolean> cmd,
                                       long initialDelay,
                                       long period,
                                       TimeUnit unit,
                                       TaskQueue taskQueue) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(period, "period");
        checkNotNull(unit);
        checkNotNull(taskQueue);

        DeadlineTask task = allocate();
        if (task == null) {
            return false;
        }
        task.callable = cmd;
        task.taskQueue = taskQueue;
        task.deadlineNanos = toDeadlineNanos(initialDelay, unit);
        task.periodNanos = unit.toNanos(period);
        put(task);
        return true;
    }

    public boolean sleep(long delay, TimeUnit unit, BiConsumer<Void, Exception> consumer) {
        checkNotNegative(delay, "delay");
        checkNotNull(unit, "unit");
        checkNotNull(consumer, "consumer");

        DeadlineTask task = allocate();
        if (task == null) {
            return false;
        }
        task.consumer = consumer;
        task.deadlineNanos = toDeadlineNanos(delay, unit);
        task.taskQueue = defaultTaskQueue;
        put(task);
        return true;
    }

    /**
     * Offers a DeadlineTask to the scheduler. The task will be scheduled based
     * on its deadline.
     *
     * @param task the task to schedule
     * @return true if the task was added to the scheduler, false otherwise.
     */
    void put(DeadlineTask task) {
        assert task.deadlineNanos >= 0;

        runQueue.offer(task);

        if (task.deadlineNanos < earliestDeadlineNanos) {
            earliestDeadlineNanos = task.deadlineNanos;
        }
    }

    /**
     * Gives the DeadlineScheduler a chance to schedule tasks. This method should be
     * called periodically. Every deadline task that is ready to be scheduled, is
     * scheduled on its configured TaskQueue.
     *
     * @param nowNanos the current epoch time in nanos.
     */
    void tick(long nowNanos) {
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

        BiConsumer<?, Exception> consumer;
        long deadlineNanos;
        Runnable runnable;
        Callable<Boolean> callable;
        long periodNanos = -1;
        long delayNanos = -1;
        TaskQueue taskQueue;
        private final DeadlineScheduler deadlineScheduler;

        DeadlineTask(DeadlineScheduler deadlineScheduler) {
            this.deadlineScheduler = deadlineScheduler;
        }

        @Override
        public void run() {
            boolean again = false;
            Exception exception = null;

            if (callable != null) {
                try {
                    again = callable.call();
                } catch (Exception e) {
                    exception = e;
                }
            } else if (runnable != null) {
                try {
                    runnable.run();
                } catch (Exception e) {
                    exception = e;
                }
            }

            if (consumer != null) {
                try {
                    consumer.accept(null, exception);
                } catch (Exception e) {
                    LOGGER.warning(e);
                }
            }

            if (again && (periodNanos != -1 || delayNanos != -1)) {
                if (periodNanos != -1) {
                    deadlineNanos += periodNanos;
                } else {
                    deadlineNanos = epochNanos() + delayNanos;
                }

                if (deadlineNanos < 0) {
                    deadlineNanos = Long.MAX_VALUE;
                }

                deadlineScheduler.put(this);
            } else {
                deadlineScheduler.free(this);
            }
        }

        void clear() {
            consumer = null;
            deadlineNanos = 0;
            runnable = null;
            callable = null;
            periodNanos = -1;
            delayNanos = -1;
            taskQueue = null;
        }

        @Override
        public int compareTo(DeadlineTask that) {
            return Long.compare(this.deadlineNanos, that.deadlineNanos);
        }

        @Override
        public String toString() {
            return "DeadlineTask{"
                    + "consumer=" + consumer
                    + ", runnable=" + runnable
                    + ", callable=" + callable
                    + ", deadlineNanos=" + deadlineNanos
                    + ", periodNanos=" + periodNanos
                    + ", delayNanos=" + delayNanos
                    + '}';
        }
    }
}
