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
 * The taskqueue-scheduler is a cooperative scheduler (unlike the schedulers in Linux).
 * So it is up to the task to yield the CPU. If a task doesn't yield the CPU, it prevents
 * the other tasks from running. It will also prevent the deadline-scheduler and io-scheduler
 * from running.
 * <p/>
 * A taskqueue-scheduler only contains taskqueues that are runnable. So if a taskqueue
 * is blocked, it is removed from the scheduler. When the taskqueue becomes runnable again, it
 * is enqueud.
 * <p/>
 * A task-queue is tied to a particular {@link Eventloop} and doesn't need to be thread-safe.
 * <p/>
 * Unlike Linux schedulers, a taskqueue-scheduler doesn't support migration of work from one
 * {@link Eventloop} to another. This is fine since we do not want to move work between cores.
 */
//todo: javadoc needs to be fixed since it refers to the CFS scheduler
public interface TaskQueueScheduler {

    /**
     * Returns the length of the time slice of the current TaskQueue. The length is
     * proportional to the weight of the TaskQueue. There is a lower bound to the
     * time slice to prevent excessive context switching.
     *
     * @return the length of the time slice of the current TaskQueue.
     */
    long timeSliceNanosCurrent();

    /**
     * Picks the next taskQueue to run. The taskQueue with the lowest vruntime is picked.
     *
     * @return the next taskQueue to run. If no task is available, then <code>null</code> is returned.
     */
    TaskQueue pickNext();

    void updateCurrent(long deltaNanos);

    /**
     * Remove the current task from the run queue. This happens when the task is blocked or the
     * task should be removed. So after a context switch (of task queues), either this method or the
     * {@link #yieldCurrent()} method is called.
     */
    void dequeueCurrent();

    /**
     * Yields the current taskQueue which effectively removes it from the runQueue and adds
     * it back to the runQueue. This is needed so that the current taskQueue is properly inserted
     * into the runQueue based on its vruntime.
     * <p/>
     * This method is called when the current taskQueue is blocked.
     *
     * @see #dequeueCurrent()
     */
    void yieldCurrent();

    /**
     * Enqueues a taskQueue into the CfsScheduler. This could be a taskQueue that was blocked or
     * is completely new. The vruntime of the taskQueue is updated to the max of the min_vruntime
     * and its own vruntime. This is done to prevent that when a task had very little vruntime
     * compared to the other tasks, that it is going to own the CPU for a very long time.
     *
     * @param taskQueue the taskQueue to enqueue.
     */
    void enqueue(TaskQueue taskQueue);
}
