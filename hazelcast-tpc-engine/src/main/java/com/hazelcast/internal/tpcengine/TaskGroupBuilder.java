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
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

public class TaskGroupBuilder {

    private final Eventloop eventloop;
    private long taskQuotaNanos;
    private String name;
    private int shares;
    private Queue<Object> queue;
    private boolean concurrent;

    public TaskGroupBuilder(Eventloop eventloop) {
        this.eventloop = eventloop;
        this.taskQuotaNanos = eventloop.taskQuotaNanos;
    }

    public TaskGroupBuilder setTaskQuota(long taskQuota, TimeUnit unit) {
        checkPositive(taskQuota, "taskQuota");
        checkNotNull(unit, "unit");
        this.taskQuotaNanos = unit.toNanos(taskQuota);
        return this;
    }

    public TaskGroupBuilder setName(String name) {
        this.name = checkNotNull(name, name);
        return this;
    }

    public TaskGroupBuilder setShares(int shares) {
        this.shares = checkPositive(shares, "shares");
        return this;
    }

    public TaskGroupBuilder setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
        return this;
    }

    public TaskGroupBuilder setQueue(Queue<Object> queue) {
        this.queue = checkNotNull(queue, "queue");
        return this;
    }

    public TaskGroupHandle build() {
        // todo: name check
        // todo: already build check
        // todo: loop active check

        TaskGroup taskQueue = eventloop.taskQueueAllocator.allocate();
        taskQueue.queue = queue;
        if (taskQueue.queue == null) {
            throw new RuntimeException();
        }
        taskQueue.shared = concurrent;
        taskQueue.shares = shares;
        taskQueue.name = name;
        taskQueue.eventloop = eventloop;
        taskQueue.taskQuotaNanos = taskQuotaNanos;
        taskQueue.state = TaskGroup.STATE_BLOCKED;

        if (concurrent) {
            eventloop.blockedSharedTaskGroups.add(taskQueue);
        }

        return new TaskGroupHandle(taskQueue);
    }
}
