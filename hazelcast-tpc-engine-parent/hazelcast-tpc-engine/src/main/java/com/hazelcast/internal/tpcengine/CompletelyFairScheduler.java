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

import static com.hazelcast.internal.tpcengine.TaskQueue.RUN_STATE_RUNNING;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;
import static java.lang.Math.max;
import static java.lang.Math.pow;
import static java.lang.Math.round;

/**
 * A {@link Scheduler} that always schedules the task group with the lowest
 * vruntime first and is modelled after the Linux CFS scheduler.
 * <p/>
 * The CompletelyFairScheduler is a fair scheduler. So if there are 2 tasks
 * with equal weight, they will both get half of the CPU time. If one of the
 * tasks is blocked, the other task will get all the CPU time.
 * <p/>
 * Currently a min-heap is used to store the tasks based on the vruntime. On the
 * original CFS scheduler a red-black tree is used. The complexity of picking the
 * task with the lowest vruntime is O(1). The complexity for reinserting is
 * O(log(n)). The complexity of removing the task (when the tasks for example
 * blocks) is O(log(n)).
 * <p/>
 * The target latency is the total amount of latency proportionally divided
 * over the different TaskQueues. If there are e.g. 4 tasksQueues and the
 * target latency is 1ms, then each TaskQueue will get a time slice of 250us.
 * <p/>
 * To prevent running a task for a very short period of time, the min
 * granularity is used to set the lower bound of the time slice. So if there are
 * e.g 100 runnable task queues, then each task queue will get a timeslice of
 * 10us. But if the min granularity is 50us, then the time slice will be 50us.
 * <p>
 * https://docs.kernel.org/scheduler/sched-design-CFS.html
 * <p>
 * https://mechpen.github.io/posts/2020-04-27-cfs-group/index.html
 */
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:MagicNumber"})
public class CompletelyFairScheduler extends Scheduler {
    public static final int NICE_0_LOAD = 1024;

    final PriorityQueue<TaskQueue> runQueue;
    final int runQueueLimit;
    final long targetLatencyNanos;
    final long minGranularityNanos;
    long min_virtualRuntimeNanos;
    int runQueueSize;
    // total weight of all the TaskGroups in this CfsScheduler (it is called
    // loadWeight in the kernel)
    long totalWeight;
    TaskQueue active;

    public CompletelyFairScheduler(int runQueueLimit,
                                   long targetLatencyNanos,
                                   long minGranularityNanos) {
        this.runQueueLimit = checkPositive(runQueueLimit, "runQueueLimit");
        this.runQueue = new PriorityQueue<>(runQueueLimit);
        this.targetLatencyNanos = checkPositive(targetLatencyNanos, "targetLatencyNanos");
        this.minGranularityNanos = checkPositive(minGranularityNanos, "minGranularityNanos");
    }

    @Override
    public int runQueueLimit() {
        return runQueueLimit;
    }

    @Override
    public int runQueueSize() {
        return runQueueSize;
    }

    public static int niceToWeight(int nice) {
        return (int) round(NICE_0_LOAD / pow(1.25, nice));
    }

    @Override
    public long timeSliceNanosActive() {
        assert active != null;

        // every task gets a timeslice proportional to its weight compared
        // the total weight.
        long timesliceNanos = targetLatencyNanos * active.weight / totalWeight;
        // If the timeslice is very small it will lead to excessive context
        // switching So we take the max value of the minGranularity and the
        // timeslice.
        return max(minGranularityNanos, timesliceNanos);
    }

    /**
     * @inheritDoc The taskQueue with the lowest vruntime is picked.
     */
    @Override
    public TaskQueue pickNext() {
        assert active == null;

        active = runQueue.poll();
        return active;
    }

    @Override
    public void updateActive(long cpuTimeNanos) {
        assert active != null;

        if (cpuTimeNanos == 0) {
            // due to low resolution (so smallest increment in time) of the clock
            // it can happen that the execDelta is 0ns. In that case we still need to
            // progress the time by just assuming 1ns of execution time.
            active.actualRuntimeNanos += 1;
            active.virtualRuntimeNanos += 1;
        } else {
            //virtual runtime = real runtime * NICE_0_LOAD / weight of the process

            active.actualRuntimeNanos += cpuTimeNanos;
            long vruntimeDelta = max(1, cpuTimeNanos * NICE_0_LOAD / active.weight);
            active.virtualRuntimeNanos += vruntimeDelta;
        }
    }

    @Override
    public void dequeueActive() {
        assert active != null;

        runQueueSize--;
        totalWeight -= active.weight;
        active = null;

        if (runQueueSize > 0) {
            min_virtualRuntimeNanos = runQueue.peek().virtualRuntimeNanos;
        }
    }

    /**
     * @inheritDoc yieldActive is needed so that the active taskQueue is
     * properly inserted into the runQueue based on its updated vruntime.
     */
    @Override
    public void yieldActive() {
        assert active != null;

        runQueue.offer(active);

        active = null;
        min_virtualRuntimeNanos = runQueue.peek().virtualRuntimeNanos;
    }

    /**
     * @inheritDoc The vruntime of the taskQueue is updated to the max of the
     * min_vruntime and its own vruntime. This is done to prevent that when a
     * task had very little vruntime compared to the other tasks, it is going
     * to own the CPU for a very long time.
     */
    @Override
    public void enqueue(TaskQueue taskQueue) {
        // the eventloop should control the number of created taskQueues
        assert runQueueSize <= runQueueLimit;

        totalWeight += taskQueue.weight;
        runQueueSize++;
        taskQueue.runState = RUN_STATE_RUNNING;
        taskQueue.virtualRuntimeNanos
                = max(taskQueue.virtualRuntimeNanos, min_virtualRuntimeNanos);
        runQueue.add(taskQueue);
    }
}
