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
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.CfsTaskQueueScheduler.niceToWeight;
import static com.hazelcast.internal.tpcengine.TaskQueue.POLL_GLOBAL_FIRST;
import static com.hazelcast.internal.tpcengine.TaskQueue.POLL_GLOBAL_ONLY;
import static com.hazelcast.internal.tpcengine.TaskQueue.POLL_LOCAL_ONLY;
import static com.hazelcast.internal.tpcengine.TaskQueue.RUN_STATE_BLOCKED;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A {@link TaskQueueBuilder} is used to configure and create a {@link TaskQueue}.
 * <p>
 * A {@link TaskQueue} can have both a local and global task queue.
 */
public class TaskQueueBuilder {

    public static final int MIN_NICE = -20;
    public static final int MAX_NICE = 20;
    private static final AtomicLong ID = new AtomicLong();

    private final Eventloop eventloop;
    private String name;
    private int nice;
    private Queue<Object> local;
    private Queue<Object> global;
    private int clockSampleInterval = 1;
    private boolean built;
    private TaskFactory taskFactory = NullTaskFactory.INSTANCE;

    TaskQueueBuilder(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    /**
     * Sets the TaskFactory that will be used to create tasks for this TaskQueue.
     *
     * @param taskFactory the TaskFactory.
     * @return this.
     * @throws NullPointerException if taskFactory is null.
     * @throws IllegalStateException if the TaskQueue is already built or when the call
     *                               isn't made from the eventloop thread.
     */
    public TaskQueueBuilder setTaskFactory(TaskFactory taskFactory) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.taskFactory = checkNotNull(taskFactory, "taskFactory");
        return this;
    }

    /**
     * Measuring the execution time of every task in a TaskQueue can be expensive.
     * To reduce the overhead, the  clock sample interval option can be used. This will
     * only measure the execution time out of of every n tasks within the TaskQueue. There
     * are a few drawback with setting the interval to a value larger than 1:
     * <ol>
     *      <li>it can lead to skid where you wrongly identify a task as a stalling task.
     *      If interval is 10 and third task stalls, because time is measured at the the 10th task,
     *      task 10 will task will be seen as the stalling task even though the third task caused
     *      the problem.</li>
     *      <li>task group could run longer than desired.</li>
     *      <li>I/O scheduling could be delayed.</li>
     *      <li>Deadline scheduling could be delayed.</li>
     * </ol>
     * For the time being this option is mostly useful for benchmark and performance tuning
     * to reduce the overhead of calling System.nanotime.
     *
     * @param clockSampleInterval the clock sample interval. If the value is 1, then time
     *                            is measured for every task.
     * @throws IllegalArgumentException if clock sample interval is smaller than 1.
     * @throws IllegalStateException    if the TaskQueue is already built or when the call
     *                                  isn't made from the eventloop thread.
     */
    public TaskQueueBuilder setClockSampleInterval(int clockSampleInterval) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.clockSampleInterval = checkPositive(clockSampleInterval, "clockSampleInterval");
        return this;
    }

    /**
     * Sets the name of the TaskQueue. The name is used for logging and debugging purposes.
     *
     * @param name the name of the TaskQueue.
     * @return this
     * @throws NullPointerException  if name is null.
     * @throws IllegalStateException if the TaskQueue is already built or when the call
     *                               isn't made from the eventloop thread.
     */
    public TaskQueueBuilder setName(String name) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.name = checkNotNull(name, "name");
        return this;
    }

    /**
     * Sets the nice value. When the CfsScheduler is used, the nice value determines the size
     * of the time slice and the priority of the task queue. For the FcfsTaskQueueScheduler,
     * the value is ignored.
     * <p>
     * -20 is the lowest nice, which means the task isn't nice at all and wants to spend
     * as much time on the CPU as possible. 20 is the highest nice value, which means the
     * task is fine giving up its time on the CPU for any less nicer task queue.
     * <p>
     * A task that has a nice level of <code>n</code> will get 20 percent larger time slice
     * than a task with a priority of <code>n-1</code>.
     *
     * @param nice the nice level
     * @throws IllegalArgumentException if the nice value is smaller than MIN_NICE or
     *                                  larger than MAX_NICE.
     * @throws IllegalStateException    if the TaskQueue is already built or when the call
     *                                  isn't made from the eventloop thread.
     */
    public TaskQueueBuilder setNice(int nice) {
        verifyNotBuilt();
        verifyEventloopThread();

        if (nice < MIN_NICE) {
            throw new IllegalArgumentException();
        } else if (nice > MAX_NICE) {
            throw new IllegalArgumentException();
        }

        this.nice = nice;
        return this;
    }

    /**
     * Sets the local queue of the TaskQueue. The local queue is should be used for tasks generated
     * within the eventloop. The local queue doesn't need to be thread-safe.
     *
     * @param local the local queue.
     * @return this.
     * @throws NullPointerException  if localQueue is null.
     * @throws IllegalStateException if the TaskQueue is already built or when the call
     *                               isn't made from the eventloop thread.
     */
    public TaskQueueBuilder setLocal(Queue<Object> local) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.local = checkNotNull(local, "localQueue");
        return this;
    }

    /**
     * Sets the global queue of the TaskQueue. The global queue is should be used for tasks generated
     * outside of the eventloop and therefor must be thread-safe.
     *
     * @param global the global queue.
     * @return this.
     * @throws NullPointerException  if globalQueue is null.
     * @throws IllegalStateException if the TaskQueue is already built or when the call
     *                               isn't made from the eventloop thread.
     */
    public TaskQueueBuilder setGlobal(Queue<Object> global) {
        verifyNotBuilt();
        verifyEventloopThread();

        this.global = checkNotNull(global, "globalQueue");
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

    /**
     * Builds the TaskQueue.
     *
     * @return the handle to the TaskQueue.
     */
    public TaskQueueHandle build() {
        verifyNotBuilt();
        verifyEventloopThread();
        built = true;

        if (local == null && global == null) {
            throw new IllegalStateException("The local and global queue can't both be null.");
        }

        if (eventloop.taskQueues.size() == eventloop.runQueueCapacity) {
            throw new IllegalStateException("Too many taskgroups.");
        }

        TaskQueue taskQueue = eventloop.taskQueueAllocator.allocate();
        taskQueue.startNanos = eventloop.nanoClock.nanoTime();
        taskQueue.local = local;
        taskQueue.global = global;
        if (local == null) {
            taskQueue.pollState = POLL_GLOBAL_ONLY;
        } else if (global == null) {
            taskQueue.pollState = POLL_LOCAL_ONLY;
        } else {
            taskQueue.pollState = POLL_GLOBAL_FIRST;
        }
        taskQueue.clockSampleInterval = clockSampleInterval;
        taskQueue.taskFactory = taskFactory;
        if (name == null) {
            taskQueue.name = "taskqueue-" + ID.incrementAndGet();
        } else {
            taskQueue.name = name;
        }
        taskQueue.eventloop = eventloop;
        taskQueue.scheduler = eventloop.taskQueueScheduler;
        taskQueue.runState = RUN_STATE_BLOCKED;
        taskQueue.weight = niceToWeight(nice);

        if (taskQueue.global != null) {
            eventloop.addBlockedGlobal(taskQueue);
        }

        eventloop.taskQueues.add(taskQueue);
        return new TaskQueueHandle(taskQueue, taskQueue.metrics);
    }
}
