/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpc.util.BoundPriorityQueue;
import com.hazelcast.internal.tpc.util.CachedNanoClock;
import com.hazelcast.internal.tpc.util.CircularQueue;
import com.hazelcast.internal.tpc.util.NanoClock;
import com.hazelcast.internal.tpc.util.StandardNanoClock;
import org.jctools.queues.MpmcArrayQueue;

import java.util.BitSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

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
    private static final int INITIAL_ALLOCATOR_CAPACITY = 1024;

    protected final MpmcArrayQueue externalTaskQueue;
    protected final PriorityQueue<ScheduledTask> scheduledTaskQueue;
    public final CircularQueue localTaskQueue;
    protected final Reactor reactor;
    protected final boolean spin;
    protected final int batchSize;
    protected final ReactorBuilder builder;
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final NanoClock nanoClock;
    public final PromiseAllocator promiseAllocator;
    protected final Scheduler scheduler;

    protected long earliestDeadlineNanos = -1;
    protected boolean stop;

    public Eventloop(Reactor reactor, ReactorBuilder builder) {
        this.reactor = reactor;
        this.builder = builder;
        this.scheduledTaskQueue = new BoundPriorityQueue<>(builder.scheduledTaskQueueCapacity);
        this.localTaskQueue = new CircularQueue<>(builder.localTaskQueueCapacity);
        this.externalTaskQueue = new MpmcArrayQueue(builder.externalTaskQueueCapacity);
        this.spin = builder.spin;
        this.batchSize = builder.batchSize;
        this.promiseAllocator = new PromiseAllocator(this, INITIAL_ALLOCATOR_CAPACITY);
        this.nanoClock = builder.clockRefreshPeriod == 0
                ? new StandardNanoClock()
                : new CachedNanoClock(builder.clockRefreshPeriod);
        this.scheduler = builder.schedulerSupplier.get();
        scheduler.init(this);
    }

    public abstract IOBufferAllocator fileIOBufferAllocator();

    public abstract AsyncFile newAsyncFile(String path);

    /**
     * Runs the actual eventloop.
     * <p/>
     * Is called from the reactor thread.
     *
     * @throws Exception if something fails while running the eventloop. The reactor
     *                   terminates when this happens.
     */
    protected abstract void run() throws Exception;

    /**
     * Destroys the resources of this Eventloop. Is called after the {@link #run()}.
     * <p>
     * Is called from the reactor thread.
     */
    protected void destroy() throws Exception {
    }

    protected final boolean runScheduledTasks() {
        final PriorityQueue<ScheduledTask> scheduledTaskQueue = this.scheduledTaskQueue;
        final NanoClock nanoClock = this.nanoClock;
        final int batchSize = this.batchSize;
        for (int k = 0; k < batchSize; k++) {
            ScheduledTask scheduledTask = scheduledTaskQueue.peek();
            if (scheduledTask == null) {
                return false;
            }

            if (scheduledTask.deadlineNanos > nanoClock.nanoTime()) {
                // Task should not yet be executed.
                earliestDeadlineNanos = scheduledTask.deadlineNanos;
                // we are done since all other tasks have a larger deadline.
                return false;
            }

            scheduledTaskQueue.poll();
            earliestDeadlineNanos = -1;
            try {
                scheduledTask.run();
            } catch (Exception e) {
                logger.warning(e);
            }
        }

        return !scheduledTaskQueue.isEmpty();
    }

    protected final boolean runLocalTasks() {
        final int batchSize = this.batchSize;
        final CircularQueue localTaskQueue = this.localTaskQueue;
        for (int k = 0; k < batchSize; k++) {
            Object task = localTaskQueue.poll();
            if (task == null) {
                // there are no more tasks.
                return false;
            } else if (task instanceof Runnable) {
                try {
                    ((Runnable) task).run();
                } catch (Exception e) {
                    logger.warning(e);
                }
            } else {
                try {
                    scheduler.schedule(task);
                } catch (Exception e) {
                    logger.warning(e);
                }
            }
        }

        return !localTaskQueue.isEmpty();
    }

    protected final boolean runExternalTasks() {
        final int batchSize = this.batchSize;
        final MpmcArrayQueue concurrentTaskQueue = this.externalTaskQueue;
        final Scheduler scheduler = this.scheduler;
        for (int k = 0; k < batchSize; k++) {
            Object task = concurrentTaskQueue.poll();
            if (task == null) {
                // there are no more tasks
                return false;
            } else if (task instanceof Runnable) {
                try {
                    ((Runnable) task).run();
                } catch (Exception e) {
                    logger.warning(e);
                }
            } else {
                try {
                    scheduler.schedule(task);
                } catch (Exception e) {
                    logger.warning(e);
                }
            }
        }

        return !concurrentTaskQueue.isEmpty();
    }

    public void offerAll(List<IOBuffer> bufferedRequests) {
        externalTaskQueue.addAll(bufferedRequests);
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
