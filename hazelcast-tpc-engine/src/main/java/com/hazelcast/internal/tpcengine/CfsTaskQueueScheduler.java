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
 * A {@link TaskQueue} scheduler that always schedules the task group with the lowest vruntime first.
 * <p/>
 * The CFS scheduler is a fair scheduler. So if there are 2 tasks with equal weight, they will both
 * get half of the CPU time. If one of the tasks is blocked, the other task will get all the CPU time.
 * <p/>
 * The CFS scheduler is a cooperative scheduler (unlike the CFS scheduler in Linux). So it is up to the
 * task to yield the CPU. If a task doesn't yield the CPU, it prevents the other tasks from running.
 * <p/>
 * Currently a min-heap is used to store the tasks based on the vruntime. On the original CFS scheduler
 * a red-black tree is used. The complexity of picking the task with the lowest vruntime is O(1). The
 * complexity for reinserting is O(log(n)). The complexity of removing the task (when the tasks for example
 * blocks) is O(log(n)).
 * <p/>
 * The CfsScheduler only contains task queues that are runnable. So if a task is blocked, it is removed
 * from the CfsScheduler. When the task becomes runnable again, it is enqueud to the CfsScheduler.
 * <p/>
 * A CfsScheduler is tied to a particular {@link Eventloop} and doesn't need to be thread-safe.
 * <p/>
 * The CfsScheduler doesn't support migration of work from one {@link Eventloop} to another. This is fine
 * since we do not want to move work between cores automatically.
 * <p/>
 * The target latency is the total amount of latency proportionally divided over the different TaskQueues. If there
 * are e.g. 4 tasksQueues and the target latency is 1ms second, then each TaskQueue will get a time slice of
 * 250us.
 * <p/>
 * To prevent running a task for a very short period of time, the min granularity is used to set
 * the lower bound of the time slice. So if there are e.g 100 runnable task queues, then each task queue will
 * get a timeslice of 10us. But if the min granularity is 50, then the time slice will be 50us.
 * <p>
 * https://docs.kernel.org/scheduler/sched-design-CFS.html
 * <p>
 * https://mechpen.github.io/posts/2020-04-27-cfs-group/index.html
 */
@SuppressWarnings({"checkstyle:MemberName"})
class CfsTaskQueueScheduler {

    private final PriorityQueue<TaskQueue> runQueue;
    private final int capacity;
    private final long targetLatencyNanos;
    private final long minGranularityNanos;
    private long min_vruntimeNanos;
    private int nrRunning;
    // total weight of all the TaskGroups in this CfsScheduler
    private long loadWeight;

    private TaskQueue current;

    CfsTaskQueueScheduler(int runQueueCapacity, long targetLatencyNanos, long minGranularityNanos) {
        this.runQueue = new PriorityQueue<>(runQueueCapacity);
        this.capacity = runQueueCapacity;
        this.targetLatencyNanos = targetLatencyNanos;
        this.minGranularityNanos = minGranularityNanos;
    }

    /**
     * Returns the number of items in the runQueue.
     *
     * @return the size of the runQueue.
     */
    public int nrRunning() {
        return nrRunning;
    }

    /**
     * Returns the length of the time slice of the current TaskQueue. The length is
     * proportional to the weight of the TaskQueue. There is a lower bound to the
     * time slice to prevent excessive context switching.
     *
     * @return the length of the time slice of the current TaskQueue.
     */
    public long timeSliceNanosCurrent() {
        // Every task should get a quota proportional to its weight. But if the quota is very small
        // it will lead to excessive context switching. So we there is a minimum minGranularityNanos.
        return Math.min(minGranularityNanos, targetLatencyNanos * current.weight / loadWeight);
    }

    /**
     * Picks the next taskQueue to run. The taskQueue with the lowest vruntime is picked.
     *
     * @return the next taskQueue to run. If no task is available, then <code>null</code> is returned.
     */
    public TaskQueue pickNext() {
        assert current == null;
        current = runQueue.peek();
        return current;
    }

    public void updateCurrent(long deltaNanos) {
        // todo * include weight
        long deltaWeightedNanos = deltaNanos;
        current.sumExecRuntimeNanos += deltaNanos;
        current.vruntimeNanos += deltaWeightedNanos;

        //todo: do we need to update min_vruntimeNanos?
        //current.vruntimeNanos += durationNanos * current.weight / loadWeight;
    }

    /**
     * Remove the current task from the run queue. This happens when the task is blocked or the
     * task should be removed. So after a context switch (of task queues), either this method or the
     * {@link #yieldCurrent()} method is called.
     */
    public void dequeueCurrent() {
        assert current != null;
        runQueue.poll();
        nrRunning--;
        loadWeight -= current.weight;
        current = null;

        if (nrRunning > 0) {
            min_vruntimeNanos = runQueue.peek().vruntimeNanos;
        }
    }

    /**
     * Yields the current taskQueue which effectively removes it from the runQueue and adds
     * it back to the runQueue. This is needed so that the current taskQueue is properly inserted
     * into the runQueue based on its vruntime.
     * <p/>
     * This method is called when the current taskQueue is blocked.
     *
     * @see #dequeueCurrent()
     */
    public void yieldCurrent() {
        assert current != null;

        if (nrRunning > 1) {
            // if there is only one taskQueue in the runQueue, then there is no need to yield.
            runQueue.poll();
            runQueue.add(current);
        }

        current = null;
        min_vruntimeNanos = runQueue.peek().vruntimeNanos;
    }

    /**
     * Enqueues a taskQueue into the CfsScheduler. This could be a taskQueue that was blocked or
     * is completely new. The vruntime of the taskQueue is updated to the max of the min_vruntime
     * and its own vruntime. This is done to prevent that when a task had very little vruntime
     * compared to the other tasks, that it is going to own the CPU for a very long time.
     *
     * @param taskQueue the taskQueue to enqueue.
     */
    public void enqueue(TaskQueue taskQueue) {
        if (nrRunning == capacity) {
            // this should never happen because the eventloop should control the number of
            // created taskQueues.
            throw new IllegalStateException("Too many taskQueues.");
        }
        loadWeight += taskQueue.weight;
        nrRunning++;
        taskQueue.runState = TaskQueue.RUN_STATE_RUNNING;
        taskQueue.vruntimeNanos = max(taskQueue.vruntimeNanos, min_vruntimeNanos);
        runQueue.add(taskQueue);
    }
}
