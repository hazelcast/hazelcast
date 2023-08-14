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
import com.hazelcast.internal.tpcengine.file.StorageScheduler;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.net.NetworkScheduler;
import com.hazelcast.internal.tpcengine.util.AbstractBuilder;
import com.hazelcast.internal.tpcengine.util.EpochClock;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;
import com.hazelcast.internal.tpcengine.util.Promise;
import com.hazelcast.internal.tpcengine.util.PromiseAllocator;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpcengine.TaskQueue.RUN_STATE_BLOCKED;
import static com.hazelcast.internal.tpcengine.util.EpochClock.epochNanos;
import static com.hazelcast.internal.tpcengine.util.OS.pageSize;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.lang.Math.max;

/**
 * Contains the actual eventloop run by a Reactor. The effentloop is a responsible for scheduling
 * tasks that are waiting to be processed and deal with I/O. So both submitting and receiving I/O
 * requests.
 * <p/>
 * The Eventloop should only be touched by the Reactor-thread.
 * <p/>
 * External code should not rely on a particular Eventloop-type. This way the same code
 * can be run on top of difference eventloops. So casting to a specific Eventloop type
 * is a no-go.
 */
public abstract class Eventloop {

    static final ThreadLocal<Eventloop> EVENTLOOP_THREAD_LOCAL = new ThreadLocal<>();

    private static final int INITIAL_PROMISE_ALLOCATOR_CAPACITY = 1024;

    protected final Reactor reactor;
    protected final boolean spin;
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final PromiseAllocator promiseAllocator;
    protected final IntPromiseAllocator intPromiseAllocator;
    protected final TaskQueue defaultTaskQueue;
    protected final Reactor.Metrics metrics;
    protected final long minGranularityNanos;
    protected final NetworkScheduler networkScheduler;
    protected final IOBufferAllocator storageAllocator;
    protected final StorageScheduler storageScheduler;
    protected final Thread eventloopThread;
    protected long taskStartNanos;
    protected final long ioIntervalNanos;
    protected final DeadlineScheduler deadlineScheduler;
    protected final long stallThresholdNanos;
    protected final Scheduler scheduler;
    protected final StallHandler stallHandler;
    protected long taskDeadlineNanos;
    protected boolean stop;

    protected Eventloop(Builder builder) {
        this.reactor = builder.reactor;
        this.eventloopThread = builder.reactor.eventloopThread;
        this.metrics = reactor.metrics;
        this.minGranularityNanos = builder.reactorBuilder.minGranularityNanos;
        this.spin = builder.reactorBuilder.spin;
        this.deadlineScheduler = builder.deadlineScheduler;
        this.networkScheduler = builder.networkScheduler;
        this.storageScheduler = builder.storageScheduler;
        this.scheduler = builder.scheduler;
        this.storageAllocator = builder.storageAllocator;
        this.promiseAllocator = new PromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.intPromiseAllocator = new IntPromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.stallThresholdNanos = builder.reactorBuilder.stallThresholdNanos;
        this.ioIntervalNanos = builder.reactorBuilder.ioIntervalNanos;
        this.stallHandler = builder.reactorBuilder.stallHandler;

        TaskQueue.Builder defaultTaskQueueBuilder = builder.reactorBuilder.defaultTaskQueueBuilder;
        defaultTaskQueueBuilder.eventloop = this;
        this.defaultTaskQueue = defaultTaskQueueBuilder.build();
    }

    /**
     * Gets the Eventloop from a threadlocal. This method is useful if you are
     * running on the eventloop thread, but you don't have a reference to the
     * Eventloop instance.
     *
     * @return the Eventloop or null if the current thread isn't the
     *          eventloop thread.
     */
    public static Eventloop getThreadLocalEventloop() {
        return EVENTLOOP_THREAD_LOCAL.get();
    }

    /**
     * Returns the Reactor this Eventloop belongs to.
     *
     * @return the reactor.
     */
    public final Reactor reactor() {
        return reactor;
    }

    /**
     * Returns the current epoch time in nanos of when the active task started. Outside of
     * the execution active task, this value is undefined.
     *
     * @return the current epoch time in nanos of when the active task started.
     */
    public final long taskStartNanos() {
        return taskStartNanos;
    }

    /**
     * Gets the default {@link TaskQueue}.
     *
     * @return the handle for the default {@link TaskQueue}.
     */
    public final TaskQueue defaultTaskQueue() {
        return defaultTaskQueue;
    }

    /**
     * This method should be called by the current task to check if it should yield.
     * <p/>
     * So if there is some long running tasks, it periodically checks this method.
     * As long as it returns false, it can keep running. When true is returned, the
     * task should yield (see {@link Task#process()} for more details) and the task
     * will be scheduled again at some point in the future.
     * <p/>
     * This method is pretty expensive due to the overhead of {@link System#nanoTime()} which
     * is roughly between 15/30 nanoseconds. So you want to prevent calling this method too
     * often because you will loose a lot of performance. But if you don't call it often enough,
     * you can into problems because you could end up stalling the reactor. So it is a tradeoff.
     *
     * @return true if the caller should yield, false otherwise.
     */
    public final boolean shouldYield() {
        return epochNanos() > taskDeadlineNanos;
    }

    public final boolean offer(Object task) {
        return defaultTaskQueue.offer(task);
    }

    /**
     * Checks if the current thread is the eventloop thread of this Eventloop.
     *
     * @throws IllegalThreadStateException if the current thread not equals the
     *                                     eventloop thread.
     */
    public final void checkOnEventloopThread() {
        Thread currentThread = Thread.currentThread();

        if (currentThread != reactor.eventloopThread) {
            throw new IllegalThreadStateException("Can only be called from the eventloop thread "
                    + "[" + eventloopThread + "], found [" + currentThread + "].");
        }
    }

    /**
     * Creates an new {@link TaskQueue.Builder} for this Eventloop.
     *
     * @return the created builder.
     * @throws IllegalStateException if current thread is not the Eventloop thread.
     */
    public final TaskQueue.Builder newTaskQueueBuilder() {
        checkOnEventloopThread();
        TaskQueue.Builder taskQueueBuilder = new TaskQueue.Builder();
        taskQueueBuilder.eventloop = this;
        return taskQueueBuilder;
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
     * Returns the IOBufferAllocator for {@link AsyncFile}. The eventloop will
     * ensure that a compatible IOBuffer is returned that can be used to deal
     * with the {@link AsyncFile} instances created by this Eventloop.
     *
     * @return the storage IOBufferAllocator.
     */
    public final IOBufferAllocator storageAllocator() {
        return storageAllocator;
    }

    /**
     * Creates a new AsyncFile instance for the given path.
     * <p>
     * todo: path validity
     *
     * @param path the path of the AsyncFile.
     * @return the created AsyncFile.
     * @throws NullPointerException          if path is null.
     * @throws UnsupportedOperationException if the eventloop doesn't support
     *                                       creating AsyncFile instances.
     */
    public abstract AsyncFile newAsyncFile(String path);

    /**
     * Destroys the resources of this Eventloop. Is called after the {@link #run()}.
     * <p>
     * Is called from the reactor thread.
     */
    protected void destroy() throws Exception {
        reactor.files().foreach(AsyncFile::close);
        reactor.sockets().foreach(socket -> socket.close("Reactor is shutting down", null));
        reactor.serverSockets().foreach(serverSocket -> serverSocket.close("Reactor is shutting down", null));
    }

    /**
     * Override this method to execute some logic before the {@link #run()} method is called.
     * When you override it, make sure you call {@code super.beforeRun()}.
     */
    protected void beforeRun() {
        this.taskStartNanos = epochNanos();
    }

    /**
     * Runs the actual eventloop.
     * <p/>
     * Is called from the reactor thread.
     * <p>
     * {@link EpochClock#epochNanos()} is pretty expensive (+/-25ns) due to {@link System#nanoTime()}. For
     * every task processed we do not want to call the {@link EpochClock#epochNanos()} more than once because
     * the clock already dominates the context switch time.
     *
     * @throws Exception if something fails while running the eventloop. The reactor
     *                   terminates when this happens.
     */
    @SuppressWarnings({"checkstyle:NPathComplexity",
            "checkstyle:MethodLength",
            "checkstyle:CyclomaticComplexity",
            "checkstyle:InnerAssignment",
            "checkstyle:MagicNumber"})
    public final void run() throws Exception {
        //System.out.println("eventloop.run");
        long nowNanos = epochNanos();
        long ioDeadlineNanos = nowNanos + ioIntervalNanos;

        while (!stop) {
            deadlineScheduler.tick(nowNanos);

            scheduler.scheduleOutsideBlocked();

            TaskQueue taskQueue = scheduler.pickNext();
            if (taskQueue == null) {
                // There is no work and therefor we need to park.

                long earliestDeadlineNanos = deadlineScheduler.earliestDeadlineNanos();
                long timeoutNanos = earliestDeadlineNanos == -1
                        ? Long.MAX_VALUE
                        : max(0, earliestDeadlineNanos - nowNanos);

                park(timeoutNanos);

                // todo: we should only need to update the clock if real parking happened
                // and not when work was detected
                nowNanos = epochNanos();
                ioDeadlineNanos = nowNanos + ioIntervalNanos;
                continue;
            }

            final long taskQueueDeadlineNanos = nowNanos + scheduler.timeSliceNanosActive();
            // The time the taskGroup has spend on the CPU.
            long taskGroupCpuTimeNanos = 0;
            int taskCount = 0;
            boolean taskQueueEmpty = false;
            // This forces immediate time measurement of the first task.
            int clockSampleRound = 1;
            // Process the tasks in a queue as long as the deadline is not exceeded.
            while (nowNanos <= taskQueueDeadlineNanos) {
                if (!taskQueue.next()) {
                    taskQueueEmpty = true;
                    // queue is empty, we are done.
                    break;
                }

                taskStartNanos = nowNanos;
                taskDeadlineNanos = nowNanos + minGranularityNanos;

                taskQueue.run();
                taskCount++;

                if (clockSampleRound == 1) {
                    nowNanos = epochNanos();
                    clockSampleRound = taskQueue.clockSampleInterval;
                } else {
                    clockSampleRound--;
                }

                long taskEndNanos = nowNanos;
                // make sure that a task always progresses the time.
                long taskCpuTimeNanos = Math.max(taskStartNanos - taskEndNanos, 1);
                taskGroupCpuTimeNanos += taskCpuTimeNanos;

                if (taskCpuTimeNanos > stallThresholdNanos) {
                    stallHandler.onStall(reactor, taskQueue, taskQueue.task, taskStartNanos, taskCpuTimeNanos);
                }

                // todo:
                // what if this is the last task of the last task queue; then
                // first we are going to do an ioSchedulerTick and then we are
                // going to do a park.
                if (nowNanos >= ioDeadlineNanos) {
                    ioSchedulerTick();
                    nowNanos = epochNanos();
                    ioDeadlineNanos = nowNanos + ioIntervalNanos;
                }

                taskQueue.task = null;
            }

            scheduler.updateActive(taskGroupCpuTimeNanos);
            metrics.incTasksProcessedCount(taskCount);
            metrics.incCpuTimeNanos(taskGroupCpuTimeNanos);
            metrics.incContextSwitchCount();

            if (taskQueueEmpty || taskQueue.isEmpty()) {
                // the taskQueue has been fully drained.
                scheduler.dequeueActive();

                taskQueue.runState = RUN_STATE_BLOCKED;
                taskQueue.blockedCount++;

                if (taskQueue.outside != null) {
                    // we also need to add it to the shared taskQueues so the eventloop will
                    // see any items that are written to outside queues.
                    scheduler.addOutsideBlocked(taskQueue);
                }
            } else {
                // Task queue wasn't fully drained.
                scheduler.yieldActive();
            }
        }
    }

    /**
     * @return true if work was triggered that requires attention of the eventloop.
     * @throws IOException
     */
    protected abstract boolean ioSchedulerTick() throws IOException;

    /**
     * Parks the eventloop thread until there is work. So either there are
     * I/O events or some tasks that require processing.
     * <p>
     * todo: rename park to 'select' and ioScheduler tick to 'selectNow'?
     *
     * @param timeoutNanos the timeout in nanos. 0 means no timeout.
     *                     Long.MAX_VALUE means wait forever. a timeout
     *                     smaller than 0 should not be used.
     * @throws IOException
     */
    protected abstract void park(long timeoutNanos) throws IOException;

    /**
     * Schedules a task to be performed with some delay.
     *
     * @param cmd   the task to perform.
     * @param delay the delay
     * @param unit  the unit of the delay
     * @return true if the task was scheduled, false if the task was rejected.
     * @throws NullPointerException     if cmd or unit is null.
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit) {
        return schedule(cmd, delay, unit, defaultTaskQueue);
    }

    /**
     * Schedules a one shot action with the given delay.
     *
     * @param cmd    the cmd to execute.
     * @param delay  the delay
     * @param unit   the unit of the delay
     * @param taskQueue the handle of the TaskQueue the cmd belongs to.
     * @return true if the cmd was successfully scheduled.
     * @throws NullPointerException     if cmd or unit is null
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit,
                                  TaskQueue taskQueue) {
        checkNotNull(cmd);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);
        checkNotNull(taskQueue);

        DeadlineScheduler.DeadlineTask task = new DeadlineScheduler.DeadlineTask(deadlineScheduler);
        task.cmd = cmd;
        task.taskQueue = taskQueue;
        task.deadlineNanos = toDeadlineNanos(delay, unit);
        return deadlineScheduler.offer(task);
    }

    /**
     * Creates a periodically executing cmd with a fixed delay between the completion and start of
     * the cmd.
     *
     * @param cmd          the cmd to periodically execute.
     * @param initialDelay the initial delay
     * @param delay        the delay between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the cmd was successfully executed.
     */
    public final boolean scheduleWithFixedDelay(Runnable cmd,
                                                long initialDelay,
                                                long delay,
                                                TimeUnit unit,
                                                TaskQueue taskQueue) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(delay, "delay");
        checkNotNull(unit);
        checkNotNull(taskQueue);

        DeadlineScheduler.DeadlineTask task = new DeadlineScheduler.DeadlineTask(deadlineScheduler);
        task.cmd = cmd;
        task.taskQueue = taskQueue;
        task.deadlineNanos = toDeadlineNanos(initialDelay, unit);
        task.delayNanos = unit.toNanos(delay);
        return deadlineScheduler.offer(task);
    }

    /**
     * Creates a periodically executing cmd with a fixed delay between the start of the cmd.
     *
     * @param cmd          the cmd to periodically execute.
     * @param initialDelay the initial delay
     * @param period       the period between executions.
     * @param unit         the unit of the initial delay and delay
     * @return true if the cmd was successfully executed.
     */
    public final boolean scheduleAtFixedRate(Runnable cmd,
                                             long initialDelay,
                                             long period,
                                             TimeUnit unit,
                                             TaskQueue taskQueue) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(period, "period");
        checkNotNull(unit);
        checkNotNull(taskQueue);

        DeadlineScheduler.DeadlineTask task = new DeadlineScheduler.DeadlineTask(deadlineScheduler);
        task.cmd = cmd;
        task.taskQueue = taskQueue;
        task.deadlineNanos = toDeadlineNanos(initialDelay, unit);
        task.periodNanos = unit.toNanos(period);
        return deadlineScheduler.offer(task);
    }

    public final Promise sleep(long delay, TimeUnit unit) {
        checkNotNegative(delay, "delay");
        checkNotNull(unit, "unit");

        Promise promise = promiseAllocator.allocate();
        DeadlineScheduler.DeadlineTask task = new DeadlineScheduler.DeadlineTask(deadlineScheduler);
        task.promise = promise;
        task.deadlineNanos = toDeadlineNanos(delay, unit);
        task.taskQueue = defaultTaskQueue;
        deadlineScheduler.offer(task);
        return promise;
    }

    private long toDeadlineNanos(long delay, TimeUnit unit) {
        long deadlineNanos = epochNanos() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        return deadlineNanos;
    }

    @SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:MagicNumber"})
    public abstract static class Builder extends AbstractBuilder<Eventloop> {

        public Reactor reactor;
        public Reactor.Builder reactorBuilder;
        public NetworkScheduler networkScheduler;
        public StorageScheduler storageScheduler;
        public Scheduler scheduler;
        public DeadlineScheduler deadlineScheduler;
        public IOBufferAllocator storageAllocator;

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(reactor, "reactor");
            checkNotNull(reactorBuilder, "reactorBuilder");

            if (deadlineScheduler == null) {
                this.deadlineScheduler = new DeadlineScheduler(reactorBuilder.deadlineRunQueueLimit);
            }

            if (storageAllocator == null) {
                this.storageAllocator = new NonConcurrentIOBufferAllocator(4096, true, pageSize());
            }

            if (scheduler == null) {
                if (reactorBuilder.cfs) {
                    this.scheduler = new CompletelyFairScheduler(
                            reactorBuilder.runQueueLimit,
                            reactorBuilder.targetLatencyNanos,
                            reactorBuilder.minGranularityNanos);
                } else {
                    this.scheduler = new FifoScheduler(
                            reactorBuilder.runQueueLimit,
                            reactorBuilder.targetLatencyNanos,
                            reactorBuilder.minGranularityNanos);
                }
            }
        }
    }
}
