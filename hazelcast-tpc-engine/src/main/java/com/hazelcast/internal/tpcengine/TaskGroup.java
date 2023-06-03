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
 * The TaskGroup can be configured with either either (or both):
 * <ol>
 *     <li>local queue: for tasks submitted within the eventloop. This queue doesn't need to
 *     be threadsafe.</li>
 *     <li>global queue: for tasks submitted outside of the eventloop. This queue needs to
 *     be threadsafe.</li>
 * </ol>
 *
 * When there is only 1 queue, Tasks in the same TaskGroup will be processed in FIFO order.
 * When there are 2 queues, tasks will be picked in round robin fashion and tasks in the
 * same queue will be picked in FIFO order.
 * <p>
 * Every TaskGroup has a vruntime which stands for virtual runtime. This is the amount of time
 * the TaskGroup has spend on the CPU.
 * <p>
 * vruntime/pruntime
 * This number could be distorted when there are other threads running on the same CPU because
 * If a different task would be executed while a task is running on the CPU, the measured time
 * will include the time of that task as well.
 * <p>
 * idea: TaskGroup without time tracking to prevent the overhead of System.nanotime.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public final class TaskGroup implements Comparable<TaskGroup> {

    public final static int POLL_LOCAL_ONLY = 1;
    public final static int POLL_GLOBAL_ONLY = 2;
    public final static int POLL_LOCAL_FIRST = 3;
    public final static int POLL_GLOBAL_FIRST = 4;

    public static final int STATE_RUNNING = 0;
    public static final int STATE_BLOCKED = 1;
    public int pollState;

    public int skid;
    public int state = STATE_BLOCKED;
    public String name;
    public int shares;
    public Queue<Object> localQueue;
    public Queue<Object> globalQueue;

    // any runnable on the queue will be processed as is.
    // any Task on the queue will also be processed according to the contract of the task.
    // anything else is offered to the taskFactory to be wrapped inside a task.
    public TaskFactory taskFactory;
    public int size;
    public Eventloop eventloop;
    public CfsScheduler scheduler;
    // the physical runtime
    // the actual amount of time this task has spend on the CPU
    // If there are other threads running on the same processor, pruntime can be destored because these tasks
    // can contribute to the pruntime of this taskGroup.
    public long pruntimeNanos;
    // the virtual runtime. The vruntime is weighted + also when reinserted into the tree, the vruntime
    // is always updated to the min_vruntime. So the vruntime isn't the actual amount of time spend on the CPU
    public long vruntimeNanos;
    public long tasksProcessed;
    // the number of times this taskGroup has been blocked
    public long blockedCount;
    // the number of times this taskGroup has been context switched.
    public boolean contextSwitchCount;

    // the start time of this TaskGroup
    public long startNanos;

    // The TakGroup is an intrusive double-linked-list-node. This is used to keep track
    // of blocked shared tasksGroups.
    public TaskGroup prev;
    public TaskGroup next;

    /**
     * The maximum amount of time the tasks in this group can run before the taskGroup is
     * context switched.
     */
    public long quotaNanos;

    public final TaskGroupMetrics metrics = new TaskGroupMetrics();

    @Override
    public int compareTo(TaskGroup that) {
        if (that.vruntimeNanos == this.vruntimeNanos) {
            return 0;
        }

        return this.vruntimeNanos > that.vruntimeNanos ? 1 : -1;
    }

    /**
     * Polls for a single TaskGroup.
     *
     * @return the TaskGroup that is next or <code>null</code> if this TaskGroup has no more
     * tasks to execute.
     */
    public Runnable poll() {
        Object taskObj;
        switch (pollState) {
            case POLL_LOCAL_ONLY:
                taskObj = localQueue.poll();
                break;
            case POLL_GLOBAL_ONLY:
                taskObj = globalQueue.poll();
                break;
            case POLL_GLOBAL_FIRST:
                taskObj = globalQueue.poll();
                if (taskObj != null) {
                    pollState = POLL_LOCAL_FIRST;
                } else {
                    taskObj = localQueue.poll();
                    if (taskObj == null) {
                        pollState = POLL_LOCAL_FIRST;
                    }
                }
                break;
            case POLL_LOCAL_FIRST:
                taskObj = localQueue.poll();
                if (taskObj != null) {
                    pollState = POLL_GLOBAL_FIRST;
                } else {
                    taskObj = globalQueue.poll();
                    if (taskObj == null) {
                        pollState = POLL_GLOBAL_FIRST;
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unknown pollState:" + pollState);
        }

        return (Runnable) taskObj;
//
//        if (taskObj == null) {
//            return null;
//        } else if (taskObj instanceof Runnable) {
//            return (Runnable) taskObj;
//        } else {
//            // todo: doesn't handle null
//            Task task = taskFactory.toTask(taskObj);
//            task.taskGroup = this;
//            return task;
//        }
    }

    public boolean offerLocal(Object task) {
        if (!localQueue.offer(task)) {
            return false;
        }

        if (state == STATE_RUNNING) {
            return true;
        }

        if (globalQueue != null) {
            eventloop.removeBlockedGlobal(this);
        }

        scheduler.enqueue(this);
        return true;
    }

    @Override
    public String toString() {
        return "TaskGroup{" +
                "name='" + name + '\'' +
                ", pollState=" + pollState +
                ", state=" + state +
                ", shares=" + shares +
                ", localQueue=" + localQueue +
                ", globalQueue=" + globalQueue +
                ", taskFactory=" + taskFactory +
                ", size=" + size +
                ", pruntimeNanos=" + pruntimeNanos +
                ", vruntimeNanos=" + vruntimeNanos +
                ", tasksProcessed=" + tasksProcessed +
                ", blockedCount=" + blockedCount +
                ", contextSwitchCount=" + contextSwitchCount +
                ", startNanos=" + startNanos +
                ", prev=" + prev +
                ", next=" + next +
                ", quotaNanos=" + quotaNanos +
                '}';
    }
}
