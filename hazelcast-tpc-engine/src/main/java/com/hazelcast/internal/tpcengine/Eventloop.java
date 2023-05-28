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

import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.BoundPriorityQueue;
import com.hazelcast.internal.tpcengine.util.CachedNanoClock;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;
import com.hazelcast.internal.tpcengine.util.NanoClock;
import com.hazelcast.internal.tpcengine.util.Promise;
import com.hazelcast.internal.tpcengine.util.PromiseAllocator;
import com.hazelcast.internal.tpcengine.util.StandardNanoClock;
import org.jctools.queues.MpscArrayQueue;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * Contains the actual eventloop run by a Reactor.
 * <p/>
 * The Eventloop should only be touched by the Reactor-thread.
 * <p/>
 * External code should not rely on a particular Eventloop-type. This way the same code
 * can be run on top of difference eventloops. So casting to a specific Eventloop type
 * is a no-go.
 */
@SuppressWarnings({"checkstyle:DeclarationOrder", "checkstyle:VisibilityModifier", "rawtypes"})
public abstract class Eventloop {
    private static final int INITIAL_PROMISE_ALLOCATOR_CAPACITY = 1024;

    protected final PriorityQueue<ScheduledTask> scheduledTaskQueue;
    protected final Reactor reactor;
    protected final boolean spin;
    protected final int batchSize;
    protected final ReactorBuilder builder;
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final NanoClock nanoClock;
    protected final Scheduler scheduler;
    protected final PromiseAllocator promiseAllocator;
    protected final IntPromiseAllocator intPromiseAllocator;
    public final TaskQueueHandle externalTaskQueueHandle;
    public final TaskQueueHandle localTaskQueueHandle;
    protected long earliestDeadlineNanos = -1;
    protected boolean stop;
    protected TaskQueue[] taskQueues = new TaskQueue[0];
    protected TaskQueue[] concurrentTaskQueues = new TaskQueue[0];

    protected Eventloop(Reactor reactor, ReactorBuilder builder) {
        this.reactor = reactor;
        this.builder = builder;
        this.scheduledTaskQueue = new BoundPriorityQueue<>(builder.scheduledTaskQueueCapacity);
        this.externalTaskQueueHandle = new TaskQueueBuilder(this)
                .setQueue(new MpscArrayQueue<>(builder.externalTaskQueueCapacity))
                .setConcurrent(true)
                .setShares(1)
                .build();
        this.localTaskQueueHandle = new TaskQueueBuilder(this)
                .setQueue(new CircularQueue<>(builder.localTaskQueueCapacity))
                .setConcurrent(false)
                .setShares(1)
                .build();
        this.spin = builder.spin;
        this.batchSize = builder.batchSize;
        this.promiseAllocator = new PromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.intPromiseAllocator = new IntPromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.nanoClock = builder.clockRefreshPeriod == 0
                ? new StandardNanoClock()
                : new CachedNanoClock(builder.clockRefreshPeriod);
        this.scheduler = builder.schedulerSupplier.get();
        scheduler.init(this);
    }

    protected final boolean hasConcurrentTask() {
        TaskQueue[] concurrentTaskQueues0 = concurrentTaskQueues;
        for (TaskQueue taskQueue : concurrentTaskQueues0) {
            if (!taskQueue.isEmpty()) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param handle
     * @return
     */
    public final TaskQueue getTaskQueue(TaskQueueHandle handle) {
        return taskQueues[handle.id];
    }

    /**
     * @return
     */
    public final TaskQueueBuilder newTaskQueueBuilder() {
        return new TaskQueueBuilder(this);
    }

    /**
     * Returns the TpcLogger for this Eventloop.
     *
     * @return the TpcLogger.
     */
    public final TpcLogger logger() {
        return logger;
    }

    /**
     * Returns the IntPromiseAllocator for this Eventloop.
     *
     * @return the IntPromiseAllocator for this Eventloop.
     */
    public final IntPromiseAllocator intPromiseAllocator() {
        return intPromiseAllocator;
    }

    /**
     * Returns the PromiseAllocator for this Eventloop.
     *
     * @return the PromiseAllocator for this Eventloop.
     */
    public final PromiseAllocator promiseAllocator() {
        return promiseAllocator;
    }

    /**
     * Returns the IOBufferAllocator for block device access. The eventloop will ensure
     * that a compatible IOBuffer is returned that can be used to deal with the {@link AsyncFile}
     * instances created by this Eventloop.
     *
     * @return the block IOBufferAllocator.
     */
    public abstract IOBufferAllocator blockIOBufferAllocator();

    /**
     * Creates a new AsyncFile instance for the given path.
     * <p>
     * todo: path validity
     *
     * @param path the path of the AsyncFile.
     * @return the created AsyncFile.
     * @throws NullPointerException          if path is null.
     * @throws UnsupportedOperationException if the operation eventloop doesn't support creating AsyncFile instances.
     */
    public abstract AsyncFile newAsyncFile(String path);

    /**
     * Runs the actual eventloop.
     * <p/>
     * Is called from the reactor thread.
     *
     * @throws Exception if something fails while running the eventloop. The reactor
     *                   terminates when this happens.
     */
    @SuppressWarnings("java:S112")
    protected abstract void run() throws Exception;

    /**
     * Destroys the resources of this Eventloop. Is called after the {@link #run()}.
     * <p>
     * Is called from the reactor thread.
     */
    @SuppressWarnings("java:S112")
    protected void destroy() throws Exception {
    }

    protected final boolean runScheduledTasks() {
        final PriorityQueue<ScheduledTask> scheduledTaskQueue0 = scheduledTaskQueue;
        final NanoClock nanoClock0 = nanoClock;
        final int batchSize0 = batchSize;
        for (int k = 0; k < batchSize0; k++) {
            ScheduledTask scheduledTask = scheduledTaskQueue0.peek();
            if (scheduledTask == null) {
                return false;
            }

            if (scheduledTask.deadlineNanos > nanoClock0.nanoTime()) {
                // Task should not yet be executed.
                earliestDeadlineNanos = scheduledTask.deadlineNanos;
                // we are done since all other tasks have a larger deadline.
                return false;
            }

            // the task first needs to be removed from the task queue.
            scheduledTaskQueue0.poll();
            earliestDeadlineNanos = -1;
            try {
                scheduledTask.run();
            } catch (Exception e) {
                logger.warning(e);
            }
        }

        return !scheduledTaskQueue0.isEmpty();
    }

    protected final boolean runTasks() {
        final TaskQueue[] taskQueues0 = this.taskQueues;
        boolean moreWork = false;

        // todo: what if there are too many tasks
        // todo: should the io be scheduled through a task queue
        for (TaskQueue taskQueue : taskQueues0) {
            moreWork |= taskQueue.process();
        }

        return moreWork;
    }

    /**
     * Schedules a one shot action with the given delay.
     *
     * @param task  the task to execute.
     * @param delay the delay
     * @param unit  the unit of the delay
     * @return true if the task was successfully scheduled.
     * @throws NullPointerException     if task or unit is null
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public final boolean schedule(Runnable task, long delay, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(this);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        return scheduledTaskQueue.offer(scheduledTask);
    }

    /**
     * Creates a periodically executing task with a fixed delay between the completion and start of
     * the task.
     *
     * @param task         the task to periodically execute.
     * @param initialDelay the initial delay
     * @param delay        the delay between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the task was successfully executed.
     */
    public final boolean scheduleWithFixedDelay(Runnable task, long initialDelay, long delay, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(this);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTask.delayNanos = unit.toNanos(delay);
        return scheduledTaskQueue.offer(scheduledTask);
    }

    /**
     * Creates a periodically executing task with a fixed delay between the start of the task.
     *
     * @param task         the task to periodically execute.
     * @param initialDelay the initial delay
     * @param period       the period between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the task was successfully executed.
     */
    public final boolean scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit) {
        checkNotNull(task);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(period, "period");
        checkNotNull(unit);

        ScheduledTask scheduledTask = new ScheduledTask(this);
        scheduledTask.task = task;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTask.periodNanos = unit.toNanos(period);
        return scheduledTaskQueue.offer(scheduledTask);
    }

    public final Promise sleep(long delay, TimeUnit unit) {
        checkNotNegative(delay, "delay");
        checkNotNull(unit, "unit");

        Promise promise = promiseAllocator.allocate();
        ScheduledTask scheduledTask = new ScheduledTask(this);
        scheduledTask.promise = promise;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTaskQueue.add(scheduledTask);
        return promise;
    }
}
