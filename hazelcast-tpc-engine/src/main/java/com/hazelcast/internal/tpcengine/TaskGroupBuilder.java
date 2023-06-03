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
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

public class TaskGroupBuilder {

    private final static AtomicLong ID = new AtomicLong();

    private final Eventloop eventloop;
    private long taskQuotaNanos;
    private String name = "taskgroup-"+ ID.incrementAndGet();
    private int shares;
    private Queue<Object> localQueue;
    private Queue<Object> globalQueue;

     private TaskFactory taskFactory = NullTaskFactory.INSTANCE;

    public TaskGroupBuilder(Eventloop eventloop) {
        this.eventloop = eventloop;
        this.taskQuotaNanos = eventloop.taskGroupQuotaNanos;
    }

    public TaskGroupBuilder setTaskFactory(TaskFactory taskFactory) {
        this.taskFactory = checkNotNull(taskFactory, "taskFactory");
        return this;
    }

    public TaskGroupBuilder setTaskQuota(long taskQuota, TimeUnit unit) {
        checkPositive(taskQuota, "taskQuota");
        checkNotNull(unit, "unit");
        this.taskQuotaNanos = unit.toNanos(taskQuota);
        return this;
    }

    public TaskGroupBuilder setName(String name) {
        this.name = checkNotNull(name, "name");
        return this;
    }

    public TaskGroupBuilder setShares(int shares) {
        this.shares = checkPositive(shares, "shares");
        return this;
    }

    public TaskGroupBuilder setLocalQueue(Queue<Object> localQueue) {
        this.localQueue = checkNotNull(localQueue, "localQueue");
        return this;
    }

    public TaskGroupBuilder setGlobalQueue(Queue<Object> globalQueue) {
        this.globalQueue = checkNotNull(globalQueue, "globalQueue");
        return this;
    }

    public TaskGroupHandle build() {

        // todo: check thread
        // todo: name check
        // todo: already build check
        // todo: loop active check
        if (localQueue == null && globalQueue == null) {
            throw new IllegalStateException();
        }

        if (eventloop.taskGroups.size() == eventloop.taskGroupLimit) {
            throw new IllegalStateException("Too many taskgroups.");
        }

        TaskGroup taskGroup = eventloop.taskGroupAllocator.allocate();
        taskGroup.localQueue = localQueue;
        taskGroup.globalQueue = globalQueue;
        if (localQueue == null) {
            taskGroup.pollState = TaskGroup.POLL_GLOBAL_ONLY;
        } else if (globalQueue == null) {
            taskGroup.pollState = TaskGroup.POLL_LOCAL_ONLY;
        } else {
            taskGroup.pollState = TaskGroup.POLL_GLOBAL_FIRST;
        }
        taskGroup.taskFactory = taskFactory;
        taskGroup.shares = shares;
        taskGroup.name = name;
        taskGroup.eventloop = eventloop;
        taskGroup.scheduler = eventloop.scheduler;
        taskGroup.quotaNanos = taskQuotaNanos;
        //taskGroup.taskQuotaNanos =
        taskGroup.state = TaskGroup.STATE_BLOCKED;

        if (taskGroup.globalQueue!=null) {
            eventloop.addBlockedGlobal(taskGroup);
        }

        eventloop.taskGroups.add(taskGroup);
        return new TaskGroupHandle(taskGroup);
    }
}
