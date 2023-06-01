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
 * vruntime/pruntime
 * This number could be distorted when there are other threads running on the same CPU because
 * If a different task would be executed while a task is running on the CPU, the measured time
 * will include the time of that task as well.
 *
 * idea: TaskGroup without time tracking to prevent the overhead of System.nanotime.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public class TaskGroup implements Comparable<TaskGroup> {
    public static final int STATE_RUNNING = 0;
    public static final int STATE_BLOCKED = 1;

    public int state;
    public String name;
    public int shares;
    public Queue<Object> queue;

    // any runnable on the queue will be processed as is.
    // any Task on the queue will also be processed according to the contract of the task.
    // anything else is offered to the taskFactory to be wrapped inside a task.
    public TaskFactory taskFactory;
    public int size;
    public boolean shared;
    public Eventloop eventloop;
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

    @Override
    public int compareTo(TaskGroup that) {
        if (that.vruntimeNanos == this.vruntimeNanos) {
            return 0;
        }

        return this.vruntimeNanos > that.vruntimeNanos ? 1 : -1;
    }

    public boolean offer(Object task) {
        if (!queue.offer(task)) {
            return false;
        }

        if (!shared && state == STATE_BLOCKED) {
            eventloop.scheduler.enqueue(this);
        }

        return true;
    }
}
