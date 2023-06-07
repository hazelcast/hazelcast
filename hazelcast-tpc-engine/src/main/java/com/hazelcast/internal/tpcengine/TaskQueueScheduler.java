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

/**
 * The {@link TaskQueueScheduler} is a cooperative scheduler (unlike the schedulers in Linux).
 * So it is up to the task to yield the CPU. If a task doesn't yield the CPU, it prevents
 * the other tasks from running. It will also prevent the deadline-scheduler and io-scheduler
 * from running.
 * <p/>
 * A taskqueue-scheduler only contains taskqueues that are runnable. So if a taskqueue
 * is blocked, it is removed from the scheduler using {@link TaskQueueScheduler#dequeueActive()}.
 * When the taskqueue becomes runnable again, it is enqueued using
 * {@link TaskQueueScheduler#enqueue(TaskQueue)}
 * <p/>
 * A task-queue is tied to a particular {@link Eventloop} and doesn't need to be thread-safe.
 * <p/>
 * Unlike Linux schedulers, a taskqueue-scheduler doesn't support migration of work from one
 * {@link Eventloop} to another. This is fine since we do not want to move work between cores.
 */
public interface TaskQueueScheduler {

    /**
     * Returns the length of the time slice of the active TaskQueue. This is the total amount
     * of time that can be spend on the CPU before the task group should yield. Individual tasks
     * within a task group are bound by the the minimum granularity.
     * <p/>
     * So you could have e.g. a time slice for the task group of 1ms. If a single task within this
     * task group would run for that time slice, then it will lead to stalls with respect to the io
     * scheduler and the deadline scheduler.  So individual tasks within the task group should not
     * run longer than the min granularity.
     *
     * @return the length of the time slice of the active TaskQueue.
     */
    long timeSliceNanosActive();

    /**
     * Picks the next active taskQueue.
     *
     * @return the next taskQueue to run. If no task is available, then <code>null</code>
     * is returned.
     */
    TaskQueue pickNext();

    /**
     * Updates the active taskQueue with the given total time the taskQueue has spend
     * on the CPU.
     * <p/>
     * This method should be called after taskQueue that was selected by {@link #pickNext()}
     * has been running and is about to be context switched. After this method is called
     * then either {@link #yieldActive()} needs to be called to schedule the taskQueue again or
     * {@link #dequeueActive()} needs to be called to remove the taskQueue from the run queue.
     *
     * @param cpuTimeNanos the amount of time the active taskQueue has been running.
     */
    void updateActive(long cpuTimeNanos);

    /**
     * Remove the active task from the run queue. This happens when the task is blocked or the
     * task should be removed. So after a context switch (of task queues), either this method or the
     * {@link #yieldActive()} method is called.
     */
    void dequeueActive();

    /**
     * Yields the active taskQueue which effectively removes it from the runQueue and adds
     * it back to the runQueue.
     * <p/>
     * This method is called when the active taskQueue needs to be context switched, but has
     * has more work to do.
     *
     * @see #dequeueActive()
     */
    void yieldActive();

    /**
     * Enqueues a taskQueue into the run queue. This could be a taskQueue that was blocked or
     * is new.
     *
     * @param taskQueue the taskQueue to enqueue.
     */
    void enqueue(TaskQueue taskQueue);
}
