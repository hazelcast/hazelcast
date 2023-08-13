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

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.tpcengine.TaskQueue.RUN_STATE_BLOCKED;

/**
 * The {@link Scheduler} is a cooperative scheduler (unlike the
 * schedulers in Linux). So it is up to the task to yield the CPU. If a task
 * doesn't yield the CPU, it prevents the other tasks from running. It will
 * also prevent the deadline-scheduler and io-scheduler from running.
 * <p/>
 * The runqueue of a Scheduler only contains TaskQueue that are runnable.
 * So if a taskqueue is blocked, it is removed from the scheduler using
 * {@link Scheduler#dequeueActive()}. When the taskqueue becomes runnable
 * again, it is enqueued using {@link Scheduler#enqueue(TaskQueue)}
 * <p/>
 * A task-queue is tied to a particular {@link Eventloop} and doesn't need to be
 * thread-safe.
 * <p/>
 * Unlike Linux schedulers, a taskqueue-scheduler doesn't support migration of work
 * from one {@link Eventloop} to another. This is fine since we do not want to move
 * work between cores.
 */
public abstract class Scheduler {
    // todo: should be resources?
    // contains all the task-queues. The scheduler only contains the runnable ones.
    protected final Set<TaskQueue> taskQueues = new HashSet<>();

    // A double linked list of TaskQueues that have an outside queue and are
    // currently blocked (so waiting for some external event).
    private TaskQueue blockedOutsideFirst;
    private TaskQueue blockedOutsideLast;

    public final boolean scheduleOutsideBlocked() {
        boolean scheduled = false;
        TaskQueue queue = blockedOutsideFirst;

        while (queue != null) {
            assert queue.runState == RUN_STATE_BLOCKED : "taskQueue.state" + queue.runState;
            TaskQueue next = queue.next;

            if (!queue.outside.isEmpty()) {
                removeOutsideBlocked(queue);
                scheduled = true;
                enqueue(queue);
            }

            queue = next;
        }

        return scheduled;
    }

    /**
     * Checks if there are any outside taskQueues with pending work.
     *
     * @return true if there are outside task queue, false otherwise.
     */
    public final boolean hasOutsidePending() {
        TaskQueue queue = blockedOutsideFirst;

        while (queue != null) {
            if (!queue.outside.isEmpty()) {
                return true;
            }
            queue = queue.next;
        }

        return false;
    }

    public final void removeOutsideBlocked(TaskQueue taskQueue) {
        assert taskQueue.outside != null;
        assert taskQueue.runState == RUN_STATE_BLOCKED;

        TaskQueue next = taskQueue.next;
        TaskQueue prev = taskQueue.prev;

        if (prev == null) {
            blockedOutsideFirst = next;
        } else {
            prev.next = next;
            taskQueue.prev = null;
        }

        if (next == null) {
            blockedOutsideLast = prev;
        } else {
            next.prev = prev;
            taskQueue.next = null;
        }
    }

    public final void addOutsideBlocked(TaskQueue taskQueue) {
        assert taskQueue.outside != null;
        assert taskQueue.runState == RUN_STATE_BLOCKED;
        assert taskQueue.prev == null;
        assert taskQueue.next == null;

        TaskQueue l = blockedOutsideLast;
        taskQueue.prev = l;
        blockedOutsideLast = taskQueue;
        if (l == null) {
            blockedOutsideFirst = taskQueue;
        } else {
            l.next = taskQueue;
        }
    }

    /**
     * Returns the number of TaskQueues that are on the run-queue (including
     * the active taskQueue).
     * <p/>
     * This method is purely for testing/debugging purposes.
     *
     * @return the number of TaskQueues on the run-queue.
     */
    public abstract int runQueueSize();

    /**
     * Returns the capacity (so the number of TaskQueues) of this TaskQueueScheduler.
     *
     * @return the capacity.
     */
    public abstract int runQueueLimit();

    /**
     * Returns the length of the time slice of the active TaskQueue. This is
     * the total amount
     * of time that can be spend on the CPU before the task group should yield.
     * Individual tasks within a task group are bound by the the minimum granularity.
     * <p/>
     * So you could have e.g. a time slice for the task group of 1ms. If a single
     * task within this task group would run for that time slice, then it will
     * lead to stalls with respect to the io scheduler and the deadline scheduler.
     * So individual tasks within the task group should not run longer than the min
     * granularity.
     *
     * @return the length of the time slice of the active TaskQueue.
     */
    public abstract long timeSliceNanosActive();

    /**
     * Picks the next active taskQueue.
     *
     * @return the next taskQueue to run. If no task is available, then
     * <code>null</code> is returned.
     */
    public abstract TaskQueue pickNext();

    /**
     * Updates the active taskQueue with the given total time the taskQueue has
     * spend on the CPU.
     * <p/>
     * This method should be called after taskQueue that was selected by
     * {@link #pickNext()} has been running and is about to be context switched.
     * After this method is called then either {@link #yieldActive()} needs to be
     * called to schedule the taskQueue again or {@link #dequeueActive()} needs to
     * be called to remove the taskQueue from the run queue.
     *
     * @param cpuTimeNanos the number of nanoseconds the active taskQueue has been
     *                     running.
     */
    public abstract void updateActive(long cpuTimeNanos);

    /**
     * Remove the active task from the run queue. This happens when the task is
     * blocked or the task should be removed. So after a context switch (of
     * taskqueues), either this method or the {@link #yieldActive()} method is
     * called.
     */
    public abstract void dequeueActive();

    /**
     * Yields the active taskQueue which effectively removes it from the runQueue
     * and adds it back to the runQueue.
     * <p/>
     * This method is called when the active taskQueue needs to be context switched,
     * but has has more work to do.
     *
     * @see #dequeueActive()
     */
    public abstract void yieldActive();

    /**
     * Enqueues a taskQueue into the run queue. This could be a taskQueue that was
     * blocked or is new.
     *
     * @param taskQueue the taskQueue to enqueue.
     */
    public abstract void enqueue(TaskQueue taskQueue);
}
