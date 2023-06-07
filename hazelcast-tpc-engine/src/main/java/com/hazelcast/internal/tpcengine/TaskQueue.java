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

import java.util.Queue;

/**
 * A TaskQueue is the unit of scheduling within the eventloop. Each eventloop has a primordial
 * TaskQueue which can be used as the 'default' TaskQueue. But it is also possible to create your
 * own TaskQueues. For example when you have tasks from clients, but also long running tasks from
 * e.g. some compaction process, you could give the clients and the compaction process their
 * own taskQueues. If no clients are busy, the compaction process can get all resources. But when
 * clients need to CPU, they can get it.
 * <p>
 * The TaskQueue can be configured with either either (or both):
 * <ol>
 *     <li>local queue: for tasks submitted within the eventloop. This queue doesn't need to
 *     be thread safe.</li>
 *     <li>global queue: for tasks submitted outside of the eventloop. This queue needs to
 *     be thread safe.</li>
 * </ol>
 * <p/>
 * When there is only 1 queue, Tasks in the same TaskQueue will be processed in FIFO order.
 * When there are 2 queues, tasks will be picked in round robin fashion and tasks in the
 * same queue will be picked in FIFO order.
 * <p>
 * TaskGroups are relatively cheap. A task group that is blocked, will not be on the run queue
 * of the scheduler. But if the task group has a global queue, periodically a check will be done
 * to see if there are tasks on the global queue.
 * <p>
 * Every TaskQueue has a vruntime which stands for virtual runtime. THis is used by the
 * {@link CfsTaskQueueScheduler} to pick the TaskQueue with the lowest vruntime.
 * <p>
 * vruntime/pruntime
 * This number could be distorted when there are other threads running on the same CPU because
 * If a different task would be executed while a task is running on the CPU, the measured time
 * will include the time of that task as well.
 * <p>
 * In Linux terms the TaskQueue would be the sched_entity.
 * <p>
 * The TaskQueue is inspired by the <a href="https://github.com/DataDog/glommio">Glommio</> TaskQueue.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public final class TaskQueue implements Comparable<TaskQueue> {

    public static final int POLL_LOCAL_ONLY = 1;
    public static final int POLL_GLOBAL_ONLY = 2;
    public static final int POLL_LOCAL_FIRST = 3;
    public static final int POLL_GLOBAL_FIRST = 4;

    public static final int RUN_STATE_RUNNING = 1;
    public static final int RUN_STATE_BLOCKED = 2;


    public int pollState;

    // the interval in which the time on the CPU is measured. 1 means every interval.
    public int clockSampleInterval;
    public int runState = RUN_STATE_BLOCKED;
    public String name;
    public Queue<Object> local;
    public Queue<Object> global;

    // any runnable on the queue will be processed as is.
    // any Task on the queue will also be processed according to the contract of the task.
    // anything else is offered to the taskFactory to be wrapped inside a task.
    public TaskFactory taskFactory;
    public Eventloop eventloop;
    public TaskQueueScheduler scheduler;
    // The accumulated amount of time this task has spend on the CPU
    // If there are other threads running on the same processor, sumExecRuntimeNanos can be
    // distorted because these threads can contribute to the runtime of this taskQueue if such
    // a thread gets context switched while a task of the TaskQueue is running.
    public long actualRuntimeNanos;
    // Field is only used when the TaskQueue is scheduled by the CfsTaskQueueScheduler.
    // the virtual runtime. The vruntime is weighted + also when reinserted into the tree,
    // the vruntime is always updated to the min_vruntime. So the vruntime isn't the actual
    // amount of time spend on the CPU
    public long virtualRuntimeNanos;
    public long tasksProcessed;
    // the number of times this taskQueue has been blocked
    public long blockedCount;
    // the number of times this taskQueue has been context switched.
    public long contextSwitchCount;

    // the start time of this TaskQueue
    public long startNanos;

    // The TakGroup is an intrusive double-linked-list-node. This is used to keep track
    // of blocked shared tasksGroups.
    public TaskQueue prev;
    public TaskQueue next;

    public final TaskQueueMetrics metrics = new TaskQueueMetrics();
    public long weight = 1;

    public boolean isEmpty() {
        return (local != null && local.isEmpty()) && (global != null && global.isEmpty());
    }

    /**
     * Polls for a single Runnable. If only the local queue is set, a poll is done from the
     * local queue. If only a global queue is set, a poll is done from the global queue. If
     * both local and global queue are set, then a round robin poll is done over these 2 queues.
     *
     * @return the Runnable that is next or <code>null</code> if this TaskQueue has no more
     * tasks to execute.
     */
    public Runnable poll() {
        Object taskObj;
        switch (pollState) {
            case POLL_LOCAL_ONLY:
                taskObj = local.poll();
                break;
            case POLL_GLOBAL_ONLY:
                taskObj = global.poll();
                break;
            case POLL_GLOBAL_FIRST:
                taskObj = global.poll();
                if (taskObj != null) {
                    pollState = POLL_LOCAL_FIRST;
                } else {
                    taskObj = local.poll();
                    if (taskObj == null) {
                        pollState = POLL_LOCAL_FIRST;
                    }
                }
                break;
            case POLL_LOCAL_FIRST:
                taskObj = local.poll();
                if (taskObj != null) {
                    pollState = POLL_GLOBAL_FIRST;
                } else {
                    taskObj = global.poll();
                    if (taskObj == null) {
                        pollState = POLL_GLOBAL_FIRST;
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unknown pollState:" + pollState);
        }

        //return (Runnable) taskObj;

        if (taskObj == null) {
            return null;
        }

        if (taskObj instanceof Runnable) {
            return (Runnable) taskObj;
        } else {
            // todo: doesn't handle null
            Task task = taskFactory.toTask(taskObj);
            task.taskQueue = this;
            return task;
        }
    }

    public boolean offerLocal(Object task) {
        if (!local.offer(task)) {
            return false;
        }

        if (runState == RUN_STATE_RUNNING) {
            return true;
        }

        if (global != null) {
            eventloop.removeBlockedGlobal(this);
        }

        scheduler.enqueue(this);
        return true;
    }

    public boolean offerGlobal(Object task) {
        return global.offer(task);
    }

    @Override
    public int compareTo(TaskQueue that) {
        if (that.virtualRuntimeNanos == this.virtualRuntimeNanos) {
            return 0;
        }

        return this.virtualRuntimeNanos > that.virtualRuntimeNanos ? 1 : -1;
    }

    @Override
    public String toString() {
        return "TaskQueue{"
                + "name='" + name + '\''
                + ", pollState=" + pollState
                + ", runState=" + runState
                + ", weight=" + weight
                + ", sumExecRuntimeNanos=" + actualRuntimeNanos
                + ", vruntimeNanos=" + virtualRuntimeNanos
                + ", tasksProcessed=" + tasksProcessed
                + ", blockedCount=" + blockedCount
                + ", contextSwitchCount=" + contextSwitchCount
                + ", startNanos=" + startNanos
                + ", prev=" + prev
                + ", next=" + next
                + '}';
    }

}