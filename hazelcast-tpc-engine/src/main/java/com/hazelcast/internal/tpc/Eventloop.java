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
import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpc.util.BoundPriorityQueue;
import com.hazelcast.internal.tpc.util.CachedNanoClock;
import com.hazelcast.internal.tpc.util.CircularQueue;
import com.hazelcast.internal.tpc.util.NanoClock;
import com.hazelcast.internal.tpc.util.StandardNanoClock;
import com.hazelcast.internal.util.ThreadAffinityHelper;
import org.jctools.queues.MpmcArrayQueue;

import java.util.BitSet;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.Reactor.State.TERMINATED;
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
public abstract class Eventloop implements Runnable {
    private static final int INITIAL_ALLOCATOR_CAPACITY = 1024;

    public final Reactor reactor;
    protected final boolean spin;
    protected final int batchSize;
    protected final MpmcArrayQueue externalTaskQueue;
    private final ReactorBuilder builder;
    protected long earliestDeadlineNanos = -1;
    protected final PriorityQueue<ScheduledTask> scheduledTaskQueue;
    protected Scheduler scheduler;
    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    public final NanoClock nanoClock;
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected boolean stop;
    public final CircularQueue<Runnable> localTaskQueue;
    public final PromiseAllocator promiseAllocator;

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
    }

    /**
     * Is called before the {@link #eventloop()} is called.
     * <p>
     * This method can be used to initialize resources.
     * <p>
     * Is called from the reactor thread.
     */
    protected void beforeEventloop() throws Exception {
    }

    /**
     * Executes the actual eventloop.
     * <p/>
     * Is called from the reactor thread.
     *
     * @throws Exception
     */
    protected abstract void eventloop() throws Exception;

    /**
     * Is called after the {@link #eventloop()} is called.
     * <p>
     * This method can be used to cleanup resources.
     * <p>
     * Is called from the reactor thread.
     */
    protected void afterEventloop() throws Exception {
    }

    @Override
    public void run() {
        try {
            try {
                configureAffinity();
                this.scheduler = builder.schedulerSupplier.get();
                scheduler.init(this);
                try {
                    beforeEventloop();
                    eventloop();
                } finally {
                    afterEventloop();
                }
            } catch (Throwable e) {
                logger.severe(e);
            } finally {
                reactor.state = TERMINATED;

                reactor.terminationLatch.countDown();

                if (reactor.engine != null) {
                    reactor.engine.notifyReactorTerminated();
                }

                if (logger.isInfoEnabled()) {
                    logger.info(Thread.currentThread().getName() + " terminated");
                }
            }
        } catch (Throwable e) {
            // log whatever wasn't caught so that we don't swallow throwables.
            logger.severe(e);
        }
    }

    private void configureAffinity() {
        BitSet allowedCpus = builder.threadAffinity == null ? null : builder.threadAffinity.nextAllowedCpus();
        if (allowedCpus != null) {
            ThreadAffinityHelper.setAffinity(allowedCpus);
            BitSet actualCpus = ThreadAffinityHelper.getAffinity();
            if (!actualCpus.equals(allowedCpus)) {
                logger.warning(Thread.currentThread().getName() + " affinity was not applied successfully. "
                        + "Expected CPUs:" + allowedCpus + ". Actual CPUs:" + actualCpus);
            } else {
                if (logger.isFineEnabled()) {
                    logger.fine(Thread.currentThread().getName() + " has affinity for CPUs:" + allowedCpus);
                }
            }
        }
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
        final CircularQueue<Runnable> localTaskQueue = this.localTaskQueue;
        for (int k = 0; k < batchSize; k++) {
            Runnable task = localTaskQueue.poll();
            if (task == null) {
                // there are no more tasks.
                return false;
            } else {
                // there is a task, so lets execute it.
                try {
                    task.run();
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
                // there is a task, so lets execute it.
                try {
                    ((Runnable) task).run();
                } catch (Exception e) {
                    logger.warning(e);
                }
            } else if (task instanceof IOBuffer) {
                scheduler.schedule((IOBuffer) task);
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }

        return !concurrentTaskQueue.isEmpty();
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
