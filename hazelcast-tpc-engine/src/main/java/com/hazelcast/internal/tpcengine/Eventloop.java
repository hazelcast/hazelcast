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
import com.hazelcast.internal.tpcengine.file.BlockDevice;
import com.hazelcast.internal.tpcengine.file.BlockDeviceRegistry;
import com.hazelcast.internal.tpcengine.file.BlockRequestScheduler;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.Clock;
import com.hazelcast.internal.tpcengine.util.EpochClock;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;
import com.hazelcast.internal.tpcengine.util.Promise;
import com.hazelcast.internal.tpcengine.util.PromiseAllocator;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpcengine.TaskQueue.RUN_STATE_BLOCKED;
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
@SuppressWarnings({"checkstyle:DeclarationOrder", "checkstyle:VisibilityModifier", "rawtypes"})
public abstract class Eventloop {
    private static final int INITIAL_PROMISE_ALLOCATOR_CAPACITY = 1024;

    protected final Reactor reactor;
    protected final boolean spin;
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    // todo:padding to prevent false sharing
    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final Clock nanoClock;
    protected final PromiseAllocator promiseAllocator;
    protected final IntPromiseAllocator intPromiseAllocator;
    protected final TaskQueueHandle primordialTaskQueueHandle;
    private final ReactorMetrics metrics;
    private final long minGranularityNanos;
    protected boolean stop;
    protected long taskStartNanos;
    protected final SlabAllocator<TaskQueue> taskQueueAllocator = new SlabAllocator<>(1024, TaskQueue::new);
    private final long ioIntervalNanos;
    protected final DeadlineScheduler deadlineScheduler;
    protected TaskQueue sharedFirst;
    protected TaskQueue sharedLast;
    // contains all the task-queues. The scheduler only contains the runnable ones.
    protected final Set<TaskQueue> taskQueues = new HashSet<>();
    protected final int runQueueCapacity;
    private final long stallThresholdNanos;
    final TaskQueueScheduler taskQueueScheduler;
    private final ReactorStallHandler stallHandler;
    private long taskQueueDeadlineNanos;
    protected final BlockDeviceRegistry blockDeviceRegistry;
    protected final Map<BlockDevice, BlockRequestScheduler> deviceSchedulers = new HashMap<>();

    protected Eventloop(Reactor reactor, ReactorBuilder builder) {
        this.nanoClock = new EpochClock();
        this.reactor = reactor;
        this.metrics = reactor.metrics;
        this.deadlineScheduler = new DeadlineScheduler(builder.deadlineRunQueueCapacity);
        this.runQueueCapacity = builder.runQueueCapacity;
        this.minGranularityNanos = builder.minGranularityNanos;
        if (builder.cfs) {
            this.taskQueueScheduler = new CfsTaskQueueScheduler(
                    builder.runQueueCapacity,
                    builder.targetLatencyNanos,
                    builder.minGranularityNanos);
        } else {
            this.taskQueueScheduler = new FcfsTaskQueueScheduler(
                    builder.runQueueCapacity,
                    builder.targetLatencyNanos,
                    builder.minGranularityNanos);
        }
        this.primordialTaskQueueHandle = new TaskQueueBuilder(this)
                .setName(reactor.name + "-primordial")
                .setGlobal(new MpscArrayQueue<>(builder.globalTaskQueueCapacity))
                .setLocal(new CircularQueue<>(builder.localTaskQueueCapacity))
                .setNice(1)
                .setTaskFactory(builder.taskFactorySupplier.get())
                .build();
        this.spin = builder.spin;
        this.promiseAllocator = new PromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.intPromiseAllocator = new IntPromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.blockDeviceRegistry = builder.blockDeviceRegistry;
        this.stallThresholdNanos = builder.stallThresholdNanos;
        this.ioIntervalNanos = builder.ioIntervalNanos;
        this.stallHandler = builder.stallHandler;
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

    public final TaskQueueHandle primordialTaskQueueHandle() {
        return primordialTaskQueueHandle;
    }

    /**
     * Checks if the current task that is performed should yield. So if there is some long running
     * tasks, it periodically checks this method. As long as it returns false, it can keep running.
     * When true is returned, the task should yield (see {@link Task#process()} for more details) and
     * the task will be scheduled again at some point in the future.
     * <p/>
     * This method is pretty expensive due to the overhead of {@link System#nanoTime()} which is roughly
     * between 15/30 nanoseconds. So you want to prevent calling this method too often because you will
     * loose a lot of performance. But if you don't call it often enough, you can into problems because
     * you could end up stalling the reactor. So it is a tradeoff.
     *
     * @return
     */
    public final boolean shouldYield() {
        return nanoClock.nanoTime() > taskDeadlineNanos;
    }

    public final boolean offer(Object task) {
        return offer(task, primordialTaskQueueHandle);
    }

    public final boolean offer(Object task, TaskQueueHandle handle) {
        //checkNotNull(task, "task");
        //checkNotNull(handle,"handle");

        // TaskQueue taskGroup = handle.queue;
        //if (taskGroup.eventloop != this) {
        //    throw new IllegalArgumentException();
        //}
        //checkEventloopThread();
        return handle.queue.offerLocal(task);
    }

    protected final void checkEventloopThread() {
        if (Thread.currentThread() != reactor.eventloopThread) {
            throw new IllegalStateException();
        }
    }

    /**
     * Gets the {@link TaskQueue} for the given {@link TaskQueueHandle}.
     *
     * @param handle the handle
     * @return the TaskQueue that belongs to this handle.
     */
    public final TaskQueue getTaskQueue(TaskQueueHandle handle) {
        checkEventloopThread();
        return handle.queue;
    }

    /**
     * Creates an new {@link TaskQueueBuilder} for this Eventloop.
     *
     * @return the TaskQueueBuilder.
     * @throws IllegalStateException if current thread is not the Eventloop thread.
     */
    public final TaskQueueBuilder newTaskQueueBuilder() {
        checkEventloopThread();
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
     * Destroys the resources of this Eventloop. Is called after the {@link #run()}.
     * <p>
     * Is called from the reactor thread.
     */
    @SuppressWarnings("java:S112")
    protected void destroy() throws Exception {
    }

    protected final boolean scheduleBlockedGlobal() {
        boolean scheduled = false;
        TaskQueue queue = sharedFirst;

        while (queue != null) {
            assert queue.runState == RUN_STATE_BLOCKED : "taskQueue.state" + queue.runState;
            TaskQueue next = queue.next;

            if (!queue.global.isEmpty()) {
                removeBlockedGlobal(queue);
                scheduled = true;
                taskQueueScheduler.enqueue(queue);
            }

            queue = next;
        }

        return scheduled;
    }

    final void removeBlockedGlobal(TaskQueue taskQueue) {
        assert taskQueue.global != null;
        assert taskQueue.runState == RUN_STATE_BLOCKED;

        TaskQueue next = taskQueue.next;
        TaskQueue prev = taskQueue.prev;

        if (prev == null) {
            sharedFirst = next;
        } else {
            prev.next = next;
            taskQueue.prev = null;
        }

        if (next == null) {
            sharedLast = prev;
        } else {
            next.prev = prev;
            taskQueue.next = null;
        }
    }

    final void addBlockedGlobal(TaskQueue taskQueue) {
        assert taskQueue.global != null;
        assert taskQueue.runState == RUN_STATE_BLOCKED;
        assert taskQueue.prev == null;
        assert taskQueue.next == null;

        TaskQueue l = sharedLast;
        taskQueue.prev = l;
        sharedLast = taskQueue;
        if (l == null) {
            sharedFirst = taskQueue;
        } else {
            l.next = taskQueue;
        }
    }

    /**
     * Override this method to execute some logic before the {@link #run()} method is called.
     * When you override it, make sure you call {@code super.beforeRun()}.
     */
    public void beforeRun() {
        this.taskStartNanos = nanoClock.nanoTime();
    }

    /**
     * Runs the actual eventloop.
     * <p/>
     * Is called from the reactor thread.
     * <p>
     * {@link EpochClock#nanoTime()} is pretty expensive (+/-25ns) due to {@link System#nanoTime()}. For
     * every task processed we do not want to call the {@link EpochClock#nanoTime()} more than once because
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
        long nowNanos = nanoClock.nanoTime();
        long ioDeadlineNanos = nowNanos + ioIntervalNanos;

        long x = 0;

        //int spinStep = 1;
        while (!stop) {
            // Thread.sleep(100);

            // There is no point in doing the deadlineScheduler tick in the queue processing loop
            // because the queue is going to be processed till it is empty (or the quotaDeadlineNanos
            // is exceeded).
            deadlineScheduler.tick(nowNanos);

            scheduleBlockedGlobal();

            TaskQueue queue = taskQueueScheduler.pickNext();
            if (queue == null) {
                // There is no work, we need to park.

                long earliestDeadlineNanos = deadlineScheduler.earliestDeadlineNanos();
                long timeoutNanos = earliestDeadlineNanos == -1
                        ? Long.MAX_VALUE
                        : max(0, earliestDeadlineNanos - nowNanos);

                long start = nanoClock.nanoTime();
                // experimental code to do some spinning instead of parking.
                //if (spinStep == 1) {
                //    spinStep = 15;
                //} else {
                //   Thread.onSpinWait();
                //   spinStep--;
                //   if (earliestDeadlineNanos == -1) {
                //       timeoutNanos = 0;
                //    }
                //}

                //System.out.println("park");
                park(timeoutNanos);

                //System.out.println("park done");
                // todo: we should only need to update the clock if real parking happened and not when work was detected
                nowNanos = nanoClock.nanoTime();
                ioDeadlineNanos = nowNanos + ioIntervalNanos;
                long duration = nowNanos - start;
//                if (x % 10000 == 0) {
//                    System.out.println("park: " +duration + " ns");
//                }
                continue;
            }

            //System.out.println("processing");

            long taskQueueDeadlineNanos = nowNanos + taskQueueScheduler.timeSliceNanosActive();
            long taskGroupExecNanos = 0;
            int taskCount = 0;
            boolean queueEmpty = false;
            // This forces immediate time measurement of the first task.
            int clockSampleRound = 1;
            // Process the tasks in a queue as long as the deadline is not exceeded.
            while (nowNanos <= taskQueueDeadlineNanos) {
                x++;

                Runnable task = queue.poll();
                if (task == null) {
                    queueEmpty = true;
                    // queue is empty, we are done.
                    break;
                }
                //System.out.println("task"+task);
                taskStartNanos = nowNanos;
                taskDeadlineNanos = nowNanos + minGranularityNanos;
                try {
                    task.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                queue.tasksProcessed++;
                taskCount++;

                if (clockSampleRound == 1) {
                    nowNanos = nanoClock.nanoTime();
                    clockSampleRound = queue.clockSampleInterval;
                } else {
                    clockSampleRound--;
                }

                long taskEndNanos = nowNanos;
                // make sure that a task always progresses the time.
                long taskExecNanos = Math.max(taskStartNanos - taskEndNanos, 1);
                taskGroupExecNanos += taskExecNanos;

                if (taskExecNanos > stallThresholdNanos) {
                    stallHandler.onStall(reactor, queue, task, taskStartNanos, taskExecNanos);
                }

                if (nowNanos >= ioDeadlineNanos) {
                    long start = nanoClock.nanoTime();
                    ioSchedulerTick();
                    nowNanos = nanoClock.nanoTime();
                    ioDeadlineNanos = nowNanos + ioIntervalNanos;

//                    if (x % 10000 == 0) {
//                        System.out.println("ioSchedulerTick: " + (nowNanos - start) + " ns");
//                    }
                    // todo: should read out time again? Think shuld be fine since this call isn't made frequently
                    // we could even measure latency of the ioscheduler tick.
                }
            }

            taskQueueScheduler.updateActive(taskGroupExecNanos);
            metrics.incTasksProcessedCount(taskCount);
            metrics.incCpuTimeNanos(taskGroupExecNanos);
            metrics.incContextSwitchCount();

            if (queueEmpty || queue.isEmpty()) {
                //System.out.println("park");
                // either the empty queue was detected in the loop or
                // the taskQueueDeadlineNanos and the queue is empty.

                // remove task queue from scheduler
                taskQueueScheduler.dequeueActive();

                queue.runState = RUN_STATE_BLOCKED;
                queue.blockedCount++;

                if (queue.global != null) {
                    // we also need to add it to the shared taskQueues so the eventloop will
                    // see any items that are written to its global queues.
                    addBlockedGlobal(queue);
                }
            } else {
                //System.out.println("yield");
                // the queue exceeded its quota and therefor wasn't drained completely.
                taskQueueScheduler.yieldActive();
            }
        }
    }

    protected abstract boolean ioSchedulerTick() throws IOException;

    /**
     * Parks the eventloop thread.
     *
     * @param timeoutNanos the timeout in nanos. 0 means no timeout.
     *                     Long.MAX_VALUE means wait forever. a timeout
     *                     smaller than 0 will not be used.
     * @throws IOException
     */
    protected abstract void park(long timeoutNanos) throws IOException;

    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit) {
        return schedule(cmd, delay, unit, primordialTaskQueueHandle);
    }

    /**
     * Schedules a one shot action with the given delay.
     *
     * @param cmd    the cmd to execute.
     * @param delay  the delay
     * @param unit   the unit of the delay
     * @param handle the handle of the TaskQueue the cmd belongs to.
     * @return true if the cmd was successfully scheduled.
     * @throws NullPointerException     if cmd or unit is null
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit,
                                  TaskQueueHandle handle) {
        checkNotNull(cmd);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);
        checkNotNull(handle);

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.cmd = cmd;
        task.taskQueue = handle.queue;
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
                                                TaskQueueHandle handle) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(delay, "delay");
        checkNotNull(unit);
        checkNotNull(handle);

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.cmd = cmd;
        task.taskQueue = handle.queue;
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
                                             TaskQueueHandle handle) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(period, "period");
        checkNotNull(unit);
        checkNotNull(handle);

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.cmd = cmd;
        task.taskQueue = handle.queue;
        task.deadlineNanos = toDeadlineNanos(initialDelay, unit);
        task.periodNanos = unit.toNanos(period);
        return deadlineScheduler.offer(task);
    }

    public final Promise sleep(long delay, TimeUnit unit) {
        checkNotNegative(delay, "delay");
        checkNotNull(unit, "unit");

        Promise promise = promiseAllocator.allocate();
        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.promise = promise;
        task.deadlineNanos = toDeadlineNanos(delay, unit);
        task.taskQueue = primordialTaskQueueHandle.queue;
        deadlineScheduler.offer(task);
        return promise;
    }

    private long toDeadlineNanos(long delay, TimeUnit unit) {
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        return deadlineNanos;
    }

    public Reactor getReactor() {
        return reactor;
    }
}
