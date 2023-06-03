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

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A {@link TaskGroupBuilder} is used to configure and create a {@link TaskGroup}.
 * <p>
 * A {@link TaskGroup} can have both a local and global task queue.
 */
public class TaskGroupBuilder {

    private final static AtomicLong ID = new AtomicLong();

    private final Eventloop eventloop;
    private long quotaNanos;
    private String name = "taskgroup-" + ID.incrementAndGet();
    private int shares;
    private Queue<Object> localQueue;
    private Queue<Object> globalQueue;
    private int skid = 0;
    private boolean built;

    private TaskFactory taskFactory = NullTaskFactory.INSTANCE;

    TaskGroupBuilder(Eventloop eventloop) {
        this.eventloop = eventloop;
        this.quotaNanos = eventloop.taskGroupQuotaNanos;
    }

    /**
     * Sets the TaskFactory that will be used to create tasks for this TaskGroup.
     *
     * @param taskFactory the TaskFactory.
     * @return this.
     */
    public TaskGroupBuilder setTaskFactory(TaskFactory taskFactory) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.taskFactory = checkNotNull(taskFactory, "taskFactory");
        return this;
    }

    public TaskGroupBuilder setTaskQuota(long taskQuota, TimeUnit unit) {
        verifyNotBuilt();
        verifyEventloopThread();

        checkPositive(taskQuota, "taskQuota");
        checkNotNull(unit, "unit");
        this.quotaNanos = unit.toNanos(taskQuota);
        return this;
    }

    /**
     * Measuring the execution time of every task in a TaskGroup can be expensive.
     * To reduce the overhead, the skid option can be used. This will only measure
     * the execution time of every nth task within the TaskGroup. There are a few
     * drawback with setting skid>:
     * <ol>
     *      <li>due to skid you could wrongly identify a stalling task. If the skid
     *      is 10 and third task stalls, because time is measured at the the 10th task,
     *      that task will be seen as the stalling task</li>
     *      <li>task group could run longer than desired</li>
     *      <li>I/O scheduling could be delayed</li>
     *      <li>Deadline scheduling could be delayed</li>
     * </ol>
     * For the time being the skid option is mostly useful for benchmark and performance
     * tuning to reduce the overhead of calling System.nanotime.
     *
     * @param skid the skid value. If skid is 0, then no skid is disabled.
     * @throws IllegalArgumentException if skid smaller than 1.
     * @throws IllegalStateException if the TaskGroup is already built or when the call
     * isn't made from the eventloop thread.
     */
    public TaskGroupBuilder setSkid(int skid) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.skid = checkNotNegative(skid, "skid");
        return this;
    }

    /**
     * Sets the name of the TaskGroup. The name is used for logging and debugging purposes.
     *
     * @param name the name of the TaskGroup.
     * @return this
     * @throws NullPointerException if name is null.
     */
    public TaskGroupBuilder setName(String name) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.name = checkNotNull(name, "name");
        return this;
    }

    public TaskGroupBuilder setShares(int shares) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.shares = checkPositive(shares, "shares");
        return this;
    }

    /**
     * Sets the local queue of the TaskGroup. The local queue is should be used for tasks generated
     * within the eventloop. The local queue doesn't need to be thread-safe.
     *
     * @param localQueue the local queue.
     * @return this.
     * @throws NullPointerException if localQueue is null.
     */
    public TaskGroupBuilder setLocalQueue(Queue<Object> localQueue) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.localQueue = checkNotNull(localQueue, "localQueue");
        return this;
    }

    /**
     * Sets the global queue of the TaskGroup. The global queue is should be used for tasks generated
     * outside of the eventloop. The global queue must be thread-safe.
     *
     * @param globalQueue the global queue.
     * @return this.
     * @throws NullPointerException if globalQueue is null.
     */
    public TaskGroupBuilder setGlobalQueue(Queue<Object> globalQueue) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.globalQueue = checkNotNull(globalQueue, "globalQueue");
        return this;
    }


    private void verifyNotBuilt() {
        if (built) {
            throw new IllegalStateException("Can't call build twice on the same AsyncSocketBuilder");
        }
    }

    private void verifyEventloopThread() {
        if (Thread.currentThread() != eventloop.reactor.eventloopThread()) {
            throw new IllegalStateException("Can only call from eventloop thread");
        }
    }

    public TaskGroupHandle build() {
        verifyNotBuilt();
        verifyEventloopThread();

        built = true;

        if (localQueue == null && globalQueue == null) {
            throw new IllegalStateException("The local and global queue can't both be null. At least one of them must be set.");
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
        taskGroup.skid = skid;
        taskGroup.taskFactory = taskFactory;
        taskGroup.shares = shares;
        taskGroup.name = name;
        taskGroup.eventloop = eventloop;
        taskGroup.scheduler = eventloop.scheduler;
        taskGroup.quotaNanos = quotaNanos;
        taskGroup.state = TaskGroup.STATE_BLOCKED;

        if (taskGroup.globalQueue != null) {
            eventloop.addBlockedGlobal(taskGroup);
        }

        eventloop.taskGroups.add(taskGroup);
        return new TaskGroupHandle(taskGroup, taskGroup.metrics);
    }
}
