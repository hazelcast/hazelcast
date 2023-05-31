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
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.Clock;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;
import com.hazelcast.internal.tpcengine.util.Promise;
import com.hazelcast.internal.tpcengine.util.PromiseAllocator;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;
import com.hazelcast.internal.tpcengine.util.StandardNanoClock;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpcengine.TaskGroup.STATE_BLOCKED;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

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
    protected final int batchSize;
    protected final ReactorBuilder builder;
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    // todo:padding to prevent false sharing
    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final Clock nanoClock;
    protected final PromiseAllocator promiseAllocator;
    protected final IntPromiseAllocator intPromiseAllocator;
    public final TaskGroupHandle externalTaskQueueHandle;
    public final TaskGroupHandle localTaskQueueHandle;
    final long taskQuotaNanos;
    private final ReactorMetrics metrics;
    protected boolean stop;
    protected long taskStartNanos;
    protected long ioDeadlineNanos;
    protected final SlabAllocator<TaskGroup> taskGroupAllocator = new SlabAllocator<>(1024, TaskGroup::new);
    private final long ioIntervalNanos;
    protected final DeadlineScheduler deadlineScheduler;
    protected TaskGroup sharedHead;
    protected TaskGroup sharedTail;
    //protected final ArrayList<TaskGroup> blockedSharedTaskGroups = new ArrayList<>();
    private final long hogThresholdNanos;
    CfsScheduler scheduler = new CfsScheduler();
    private long cycleStartNanos;

    protected Eventloop(Reactor reactor, ReactorBuilder builder) {
        this.reactor = reactor;
        this.builder = builder;
        this.metrics = reactor.metrics;
        this.deadlineScheduler = new DeadlineScheduler(builder.deadlineTaskQueueCapacity);

        TaskFactory taskFactory = builder.taskFactorySupplier.get();
        this.externalTaskQueueHandle = new TaskGroupBuilder(this)
                .setQueue(new MpscArrayQueue<>(builder.externalTaskQueueCapacity))
                .setShared(true)
                .setShares(1)
                .setTaskFactory(taskFactory)
                .build();
        this.localTaskQueueHandle = new TaskGroupBuilder(this)
                .setQueue(new CircularQueue<>(builder.localTaskQueueCapacity))
                .setShared(false)
                .setShares(1)
                .setTaskFactory(taskFactory)
                .build();
        this.spin = builder.spin;
        this.batchSize = builder.batchSize;
        this.promiseAllocator = new PromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.intPromiseAllocator = new IntPromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.nanoClock = new StandardNanoClock();
        this.taskStartNanos = nanoClock.nanoTime();
        this.taskQuotaNanos = builder.taskQuotaNanos;
        this.hogThresholdNanos = builder.hogThresholdNanos;
        this.ioIntervalNanos = builder.ioIntervalNanos;
        this.ioDeadlineNanos = taskStartNanos + ioIntervalNanos;
    }

    public final long cycleStartNanos() {
        return cycleStartNanos;
    }

    public boolean offer(Object task) {
        return localTaskQueueHandle.taskGroup.offer(task);
    }

    /**
     * @param handle
     * @return
     */
    public final TaskGroup getTaskGroup(TaskGroupHandle handle) {
        return handle.taskGroup;
    }

    /**
     * @return
     */
    public final TaskGroupBuilder newTaskGroupBuilder() {
        return new TaskGroupBuilder(this);
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

    protected final boolean scheduleBlockedSharedTaskGroups() {
        boolean scheduled = false;
        TaskGroup taskGroup = sharedHead;

        while (taskGroup != null) {
            assert taskGroup.state == STATE_BLOCKED;

            if (!taskGroup.queue.isEmpty()) {
                TaskGroup next = taskGroup.next;
                TaskGroup prev = taskGroup.prev;

                if (prev == null) {
                    sharedHead = next;
                } else {
                    prev.next = next;
                    taskGroup.prev = null;
                }

                if (next == null) {
                    sharedTail = prev;
                } else {
                    next.prev = prev;
                    taskGroup.next = null;
                }

                scheduled = true;
                scheduler.enqueue(taskGroup);
            }

            taskGroup = taskGroup.next;
        }

        return scheduled;
    }

    void addLastBlockedShared(TaskGroup taskGroup) {
        assert taskGroup.shared;
        assert taskGroup.state == STATE_BLOCKED;
        assert taskGroup.prev == null;
        assert taskGroup.next == null;

        TaskGroup l = sharedTail;
        taskGroup.prev = l;
        sharedTail = taskGroup;
        if (l == null) {
            sharedHead = taskGroup;
        } else {
            l.next = taskGroup;
        }
    }

    /**
     * Runs the actual eventloop.
     * <p/>
     * Is called from the reactor thread.
     *
     * @throws Exception if something fails while running the eventloop. The reactor
     *                   terminates when this happens.
     */
    @SuppressWarnings({"checkstyle:NPathComplexity",
            "checkstyle:MethodLength",
            "checkstyle:CyclomaticComplexity",
            "checkstyle:InnerAssignment"})
    public void run() throws Exception {
        this.cycleStartNanos = nanoClock.nanoTime();

        while (!stop) {
            // a single iteration of processing a task is called a cycle.

            // There is no point in doing the deadlineScheduler tick in the taskQueue processing loop
            // because the taskQueue is going to be processed till it is empty (or the deadline is reached).
            deadlineScheduler.tick(cycleStartNanos);

            scheduleBlockedSharedTaskGroups();

            TaskGroup taskGroup = scheduler.pickNext();
            if (taskGroup == null) {
                park(cycleStartNanos);
                // todo: we should only need to update the clock if real parking happened and not when work was detected
                cycleStartNanos = nanoClock.nanoTime();
                continue;
            }

            long quotaDeadlineNanos = cycleStartNanos + taskGroup.taskQuotaNanos;
            long cycleEndNanos = cycleStartNanos;
            long taskStartNanos = cycleStartNanos;
            boolean taskQueueEmpty = false;
            long deltaNanos = 0;
            int taskCount = 0;

            // Process the tasks in a taskQueue as long as the taskQuota is not exceeded.
            for (; ; ) {
                Object task = taskGroup.queue.poll();
                if (task == null) {
                    // taskQueue is empty, we are done.
                    taskQueueEmpty = true;
                    break;
                }

                processTask(taskGroup, task);
                taskGroup.tasksProcessed++;

                taskCount++;
                long taskEndNanos = nanoClock.nanoTime();
                long taskDurationNanos = taskStartNanos - taskEndNanos;
                deltaNanos += taskDurationNanos;

                if (taskDurationNanos > hogThresholdNanos) {
                    onHoggingTask(task, taskDurationNanos);
                }

                cycleEndNanos = taskEndNanos;
                if (cycleEndNanos >= ioDeadlineNanos) {
                    ioSchedulerTick();
                    ioDeadlineNanos = cycleEndNanos += ioIntervalNanos;
                }

                if (cycleEndNanos >= quotaDeadlineNanos) {
                    // quota exceeded, we are done
                    break;
                }
            }

            // todo * include weight
            long deltaWeightedNanos = deltaNanos;

            metrics.incTasksProcessedCount(taskCount);
            metrics.incCpuTimeNanos(deltaNanos);

            taskGroup.pruntimeNanos += deltaNanos;
            taskGroup.vruntimeNanos += deltaWeightedNanos;

            if (taskQueueEmpty) {
                // the taskQueue is empty, so we mark this taskQueue as blocked.
                taskGroup.state = STATE_BLOCKED;
                taskGroup.blockedCount++;

                // we also need to add it to the shared taskGroups so the eventloop will
                // see any items that are written to its queue.
                if (taskGroup.shared) {
                    addLastBlockedShared(taskGroup);
                }
            } else {
                // the taskQueue wasn't drained, so we need to insert it back into the scheduler because
                // there is more work to be done.
                scheduler.enqueue(taskGroup);
            }

            cycleStartNanos = cycleEndNanos;
        }
    }

    private void onHoggingTask(Object task, long taskDurationNanos) {
        // todo: we need to come up with a better approach
        if (logger.isSevereEnabled()) {
            logger.severe(reactor + " detected hogging task: " + task.getClass().getName()
                    + " task-duration " + taskDurationNanos + "ns.");
        }
    }

    private void processTask(TaskGroup taskGroup, Object task) {
        // process the task.
        if (task instanceof Runnable) {
            try {
                ((Runnable) task).run();
            } catch (Exception e) {
                logger.warning(e);
            }
        } else {
            Task taskWrapper = taskGroup.taskFactory.toTask(task);
            if (taskWrapper == null) {
                //todo:
                logger.severe("Unhandled command type: " + task.getClass().getName());
                return;
            }

            taskWrapper.taskGroup = taskGroup;
            taskWrapper.run();
        }
    }

    protected abstract boolean ioSchedulerTick() throws IOException;

    protected abstract void park(long nowNanos) throws IOException;

    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit) {
        return schedule(cmd, delay, unit, localTaskQueueHandle);
    }

    /**
     * Schedules a one shot action with the given delay.
     *
     * @param cmd             the cmd to execute.
     * @param delay           the delay
     * @param unit            the unit of the delay
     * @param taskGroupHandle the handle of this TaskGroup the cmd belongs to.
     * @return true if the cmd was successfully scheduled.
     * @throws NullPointerException     if cmd or unit is null
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit,
                                  TaskGroupHandle taskGroupHandle) {
        checkNotNull(cmd);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);
        checkNotNull(taskGroupHandle);

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.cmd = cmd;
        task.taskGroup = taskGroupHandle.taskGroup;
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
                                                TaskGroupHandle taskGroupHandle) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(delay, "delay");
        checkNotNull(unit);
        checkNotNull(taskGroupHandle);

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.cmd = cmd;
        task.taskGroup = taskGroupHandle.taskGroup;
        task.deadlineNanos = toDeadlineNanos(initialDelay, unit);
        task.delayNanos = unit.toNanos(delay);
        return deadlineScheduler.offer(task);
        //throw new UnsupportedOperationException();
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
                                             TaskGroupHandle taskGroupHandle) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(period, "period");
        checkNotNull(unit);
        checkNotNull(taskGroupHandle);

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.cmd = cmd;
        task.taskGroup = taskGroupHandle.taskGroup;
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
        task.taskGroup = localTaskQueueHandle.taskGroup;
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
}
