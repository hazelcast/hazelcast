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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.util.CachedNanoClock;
import com.hazelcast.internal.tpc.util.CircularQueue;
import com.hazelcast.internal.tpc.util.NanoClock;
import com.hazelcast.internal.tpc.util.StandardNanoClock;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * Exposes methods that should only be called from within the {@link Eventloop}.
 */
public abstract class Unsafe {

    protected final NanoClock nanoClock;
    private final Eventloop eventloop;
    private final FutAllocator futAllocator;
    private final CircularQueue<Runnable> localTaskQueue;
    private final PriorityQueue<ScheduledTask> scheduledTaskQueue;

    protected Unsafe(Eventloop eventloop) {
        this.eventloop = checkNotNull(eventloop, "eventloop");
        this.localTaskQueue = eventloop.localTaskQueue;
        this.scheduledTaskQueue = eventloop.scheduledTaskQueue;
        this.futAllocator = eventloop.futAllocator;
        this.nanoClock = eventloop.clockRefreshInterval == 0
                ? new StandardNanoClock()
                : new CachedNanoClock(eventloop.clockRefreshInterval);
    }

    /**
     * Returns the NanoClock.
     *
     * @return the NanoClock.
     */
    public final NanoClock nanoClock() {
        return nanoClock;
    }

    /**
     * Returns the {@link Eventloop} that belongs to this {@link Unsafe} instance.
     *
     * @return the Eventloop.
     */
    public final Eventloop eventloop() {
        return eventloop;
    }

    /**
     * Returns a new Fut.
     *
     * @return the future.
     * @param <E> future.
     */
    public final <E> Fut<E> newFut() {
        return futAllocator.allocate();
    }

    /**
     * Offers a task to be scheduled on the eventloop.
     *
     * @param task the task to schedule.
     * @return true if the task was successfully offered, false otherwise.
     */
    public final boolean offer(Runnable task) {
        return localTaskQueue.offer(task);
    }

    /**
     * Schedules a one shot action with the given delay.
     *
     * @param task  the task to execute.
     * @param delay the delay
     * @param unit  the unit of the delay
     * @return true if the task was successfully scheduled.
     * @throws NullPointerException     if task or unit is null
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public final boolean schedule(Runnable task, long delay, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(eventloop);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        return scheduledTaskQueue.offer(scheduledTask);
    }

    /**
     * Creates a periodically executing task with a fixed delay between the completion and start of
     * the task.
     *
     * @param task         the task to periodically execute.
     * @param initialDelay the initial delay
     * @param delay        the delay between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the task was successfully executed.
     */
    public final boolean scheduleWithFixedDelay(Runnable task, long initialDelay, long delay, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(eventloop);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTask.delayNanos = unit.toNanos(delay);
        return scheduledTaskQueue.offer(scheduledTask);
    }

    /**
     * Creates a periodically executing task with a fixed delay between the start of the task.
     *
     * @param task         the task to periodically execute.
     * @param initialDelay the initial delay
     * @param period       the period between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the task was successfully executed.
     */
    public final boolean scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(period, "period");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(eventloop);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTask.periodNanos = unit.toNanos(period);
        return scheduledTaskQueue.offer(scheduledTask);
    }

    public final Fut sleep(long delay, TimeUnit unit) {
        checkNotNegative(delay, "delay");
        checkNotNull(unit, "unit");

        Fut fut = newFut();
        ScheduledTask scheduledTask = new ScheduledTask(eventloop);
        scheduledTask.fut = fut;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTaskQueue.add(scheduledTask);
        return fut;
    }
}
