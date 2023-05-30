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
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public class TaskGroup implements Comparable<TaskGroup> {
    public static final int STATE_RUNNING = 0;
    public static final int STATE_BLOCKED = 1;

    // the total number of nanoseconds this task has spend on the CPU
    public long vruntimeNanos;

    public int state;
    public String name;
    public int shares;
    public Queue<Object> queue;
    public Scheduler scheduler;
    public int size;
    public boolean concurrent;
    public Eventloop eventloop;
    // the actual amount of time this task has spend on the CPU
    public long pruntimeNanos;
    public long tasksProcessed;
    public long blockedCount;

    /**
     * The maximum amount of time the tasks in this group can run before they are
     * context switched.
     */
    public long taskQuotaNanos;

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

        if (!concurrent && state == STATE_BLOCKED) {
            eventloop.scheduler.enqueue(this);
        }

        return true;
    }

}
