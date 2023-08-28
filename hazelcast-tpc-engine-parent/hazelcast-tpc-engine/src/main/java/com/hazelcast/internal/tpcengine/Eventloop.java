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
import com.hazelcast.internal.tpcengine.util.IntPromise;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;
import com.hazelcast.internal.tpcengine.util.PromiseAllocator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpcengine.util.EpochClock.epochNanos;
import static com.hazelcast.internal.tpcengine.util.OS.pageSize;
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
    protected final NetworkScheduler networkScheduler;
    protected final IOBufferAllocator storageAllocator;
    protected final StorageScheduler storageScheduler;
    protected final Thread eventloopThread;
    protected final TaskQueue.RunContext runContext;
    protected final DeadlineScheduler deadlineScheduler;
    protected final Scheduler scheduler;
    protected final StallHandler stallHandler;
    protected boolean stop;

    @SuppressWarnings({"checkstyle:ExecutableStatementCount"})
    protected Eventloop(Builder builder) {
        this.reactor = builder.reactor;
        this.eventloopThread = builder.reactor.eventloopThread;
        this.metrics = reactor.metrics;
        this.spin = builder.reactorBuilder.spin;
        this.deadlineScheduler = builder.deadlineScheduler;
        this.networkScheduler = builder.networkScheduler;
        this.storageScheduler = builder.storageScheduler;
        this.scheduler = builder.scheduler;
        this.storageAllocator = builder.storageAllocator;
        this.promiseAllocator = new PromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.intPromiseAllocator = new IntPromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.stallHandler = builder.reactorBuilder.stallHandler;

        TaskQueue.Builder defaultTaskQueueBuilder = builder.reactorBuilder.defaultTaskQueueBuilder;
        defaultTaskQueueBuilder.eventloop = this;
        this.defaultTaskQueue = defaultTaskQueueBuilder.build();
        this.deadlineScheduler.defaultTaskQueue = defaultTaskQueue;

        this.runContext = new TaskQueue.RunContext();
        runContext.stallHandler = stallHandler;
        runContext.scheduler = scheduler;
        runContext.stallThresholdNanos = builder.reactorBuilder.stallThresholdNanos;
        runContext.minGranularityNanos = builder.reactorBuilder.minGranularityNanos;
        runContext.ioIntervalNanos = builder.reactorBuilder.ioIntervalNanos;
        runContext.reactorMetrics = reactor.metrics;
        runContext.scheduler = scheduler;
        runContext.eventloop = this;
    }

    /**
     * Gets the Eventloop from a threadlocal. This method is useful if you are
     * running on the eventloop thread, but you don't have a reference to the
     * Eventloop instance.
     *
     * @return the Eventloop or null if the current thread isn't the
     * eventloop thread.
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
     * Returns the DeadlineScheduler that belongs to this eventloop.
     *
     * @return the DeadlineScheduler.
     */
    public final DeadlineScheduler deadlineScheduler() {
        return deadlineScheduler;
    }

    /**
     * Returns the current epoch time in nanos of when the active task started. Outside of
     * the execution active task, this value is undefined.
     *
     * @return the current epoch time in nanos of when the active task started.
     */
    public final long taskStartNanos() {
        return runContext.taskStartNanos;
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
     * Checks if the current task should yield. The value is undefined if this is
     * called outside running a task.
     * <p/>
     * So if there is some long running tasks, it periodically checks this method.
     * As long as it returns false, it can keep running. When true is returned, the
     * task should yield (see {@link Task#run()} for more details) and the task
     * will be scheduled again at some point in the future.
     * <p/>
     * This method is pretty expensive due to the overhead of
     * {@link System#nanoTime()} which is roughly between 15/30 nanoseconds. So
     * you want to prevent calling this method too often because you will loose
     * a lot of performance. But if you don't call it often enough, you can into
     * problems because you could end up stalling the reactor and this can lead to
     * I/O performance problems and starvation/latency problems for other tasks.
     * So it is a tradeoff.
     *
     * @return true if the caller should yield, false otherwise.
     */
    public final boolean shouldYield() {
        long nowNanos = epochNanos();
        // since we paid for getting the time, lets update the time int the
        // runContext.
        runContext.nowNanos = nowNanos;
        return nowNanos > runContext.taskDeadlineNanos;
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
     * Override this method if you want to add behavior before the {@link #run()}
     * is called.
     */
    protected void beforeRun() {
    }

    /**
     * Runs the actual eventloop.
     * <p/>
     * Is called from the eventloop thread.
     * <p>
     *
     * @throws Exception if something fails while running the eventloop.
     *                   The reactor will terminate when this happens.
     */
    public final void run() throws Exception {
        final TaskQueue.RunContext runContext = this.runContext;
        final DeadlineScheduler deadlineScheduler = this.deadlineScheduler;
        final Scheduler scheduler = this.scheduler;
        final Reactor.Metrics metrics = this.metrics;
        boolean timeUpdateNeeded = true;

        runContext.nowNanos = epochNanos();
        runContext.ioDeadlineNanos = runContext.nowNanos + runContext.ioIntervalNanos;
        while (!stop) {
            deadlineScheduler.tick(runContext.nowNanos);

            scheduler.scheduleOutsideBlocked();

            TaskQueue taskQueue = scheduler.pickNext();
            if (taskQueue == null) {
                long epochNanosBeforePark = epochNanos();
                runContext.nowNanos = epochNanosBeforePark;

                // There is no work and therefor we need to park.
                long earliestDeadlineNs = deadlineScheduler.earliestDeadlineNs();
                long timeoutNs = earliestDeadlineNs == -1
                        ? Long.MAX_VALUE
                        : max(0, earliestDeadlineNs - runContext.nowNanos);
                park(timeoutNs);

                // after a park we have to guarantee that the nowNanos is up to data.
                runContext.nowNanos = epochNanos();
                // and we need to update the ioDeadline
                runContext.ioDeadlineNanos = runContext.nowNanos + runContext.ioIntervalNanos;
                // since we just update the time, no need to obtain
                // it again whe the next taskQueue runs.
                timeUpdateNeeded = false;

                metrics.incParkCount();
                metrics.incParkTimeNanos(runContext.nowNanos - epochNanosBeforePark);
            } else {
                // before a task group is run, its nowNanos needs to be up to date.
                // this is either done here or after the park.
                if (timeUpdateNeeded) {
                    runContext.nowNanos = epochNanos();
                }

                taskQueue.run(runContext);
                // after a task group completes we need to ensure that the time gets
                // updated unless a park is going to happen, because that will also
                // update the time.
                timeUpdateNeeded = true;
            }
        }
    }

    /**
     * Destroys the resources of this Eventloop. Is called after the {@link #run()}.
     * <p>
     * Is called from the reactor thread.
     */
    protected void destroy() throws Exception {
        reactor.files().foreach(file -> {
            file.close(new IntPromise(Eventloop.this));
        });
        reactor.sockets().foreach(socket -> socket.close("Reactor is shutting down", null));
        reactor.serverSockets().foreach(serverSocket -> serverSocket.close("Reactor is shutting down", null));
    }

    /**
     * @return true if work was triggered that requires attention of the eventloop.
     * @throws IOException
     */
    protected abstract boolean ioSchedulerTick() throws IOException;

    /**
     * Parks the eventloop thread until there is work. So either there are
     * I/O events or some tasks that require processing.
     *
     * @param timeoutNanos the timeout in nanos. 0 means no timeout.
     *                     Long.MAX_VALUE means wait forever. a timeout
     *                     smaller than 0 should not be used.
     * @throws IOException
     */
    protected abstract void park(long timeoutNanos) throws IOException;

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
