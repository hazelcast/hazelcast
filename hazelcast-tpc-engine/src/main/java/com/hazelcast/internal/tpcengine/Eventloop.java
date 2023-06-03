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
import java.util.HashSet;
import java.util.Set;
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
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    // todo:padding to prevent false sharing
    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final Clock nanoClock;
    protected final PromiseAllocator promiseAllocator;
    protected final IntPromiseAllocator intPromiseAllocator;
    public final TaskGroupHandle rootTaskGroupHandle;
    final long taskGroupQuotaNanos;
    private final ReactorMetrics metrics;
    private final long targetLatencyNanos;
    private final long minGranularityNanos;
    protected boolean stop;
    protected long taskStartNanos;
    protected final SlabAllocator<TaskGroup> taskGroupAllocator = new SlabAllocator<>(1024, TaskGroup::new);
    private final long ioIntervalNanos;
    protected final DeadlineScheduler deadlineScheduler;
    protected TaskGroup sharedFirst;
    protected TaskGroup sharedLast;
    public final Set<TaskGroup> taskGroups = new HashSet<>();
    public final int taskGroupLimit;
    private final long stallThresholdNanos;
    final CfsScheduler scheduler;
    private long taskGroupStartNanos;
    private final StallDetector stallDetector;
    private long contextSwitches;

    protected Eventloop(Reactor reactor, ReactorBuilder builder) {
        this.reactor = reactor;
        this.metrics = reactor.metrics;
        this.deadlineScheduler = new DeadlineScheduler(builder.deadlineRunQueueCapacity);
        this.taskGroupLimit = builder.taskGroupLimit;
        this.scheduler = new CfsScheduler(builder.taskGroupLimit);
        this.rootTaskGroupHandle = new TaskGroupBuilder(this)
                .setName(reactor.name + "-taskgroup-root")
                .setGlobalQueue(new MpscArrayQueue<>(builder.globalTaskQueueCapacity))
                .setLocalQueue(new CircularQueue<>(builder.localTaskQueueCapacity))
                .setShares(1)
                .setTaskFactory(builder.taskFactorySupplier.get())
                .build();
        this.spin = builder.spin;
        this.promiseAllocator = new PromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.intPromiseAllocator = new IntPromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.nanoClock = new StandardNanoClock();
        this.taskStartNanos = nanoClock.nanoTime();
        this.taskGroupQuotaNanos = builder.taskGroupQuotaNanos;
        this.stallThresholdNanos = builder.stallThresholdNanos;
        this.ioIntervalNanos = builder.ioIntervalNanos;
        this.stallDetector = builder.stallDetector;
        this.targetLatencyNanos = builder.targetLatencyNanos;
        this.minGranularityNanos = builder.minGranularityNanos;
    }

    public final long cycleStartNanos() {
        return taskGroupStartNanos;
    }

    public boolean offer(Object task) {
        return offer(task, rootTaskGroupHandle);
    }

    public boolean offer(Object task, TaskGroupHandle taskGroupHandle) {
        //checkNotNull(task, "task");
        //checkNotNull(taskGroupHandle,"taskGroupHandle");

        TaskGroup taskGroup = taskGroupHandle.taskGroup;
        //if (taskGroup.eventloop != this) {
        //    throw new IllegalArgumentException();
        //}
        //checkEventloopThread();
        return taskGroupHandle.taskGroup.offerLocal(task);
    }

    protected void checkEventloopThread() {
        if (Thread.currentThread() != reactor.eventloopThread) {
            throw new IllegalStateException();
        }
    }

    /**
     * @param handle
     * @return
     */
    public final TaskGroup getTaskGroup(TaskGroupHandle handle) {
        checkEventloopThread();
        return handle.taskGroup;
    }

    /**
     * @return
     */
    public final TaskGroupBuilder newTaskGroupBuilder() {
        checkEventloopThread();
        if (stop) {
            throw new IllegalStateException();
        }
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

    protected final boolean scheduleBlockedGlobal() {
        boolean scheduled = false;
        TaskGroup taskGroup = sharedFirst;

        while (taskGroup != null) {
            assert taskGroup.state == STATE_BLOCKED : "taskGroup.state" + taskGroup.state;
            TaskGroup next = taskGroup.next;

            if (!taskGroup.globalQueue.isEmpty()) {
                removeBlockedGlobal(taskGroup);
                scheduled = true;
                scheduler.enqueue(taskGroup);
            }

            taskGroup = next;
        }

        return scheduled;
    }

    protected final void removeBlockedGlobal(TaskGroup taskGroup) {
        TaskGroup next = taskGroup.next;
        TaskGroup prev = taskGroup.prev;

        if (prev == null) {
            sharedFirst = next;
        } else {
            prev.next = next;
            taskGroup.prev = null;
        }

        if (next == null) {
            sharedLast = prev;
        } else {
            next.prev = prev;
            taskGroup.next = null;
        }
    }

    void addBlockedGlobal(TaskGroup taskGroup) {
//        assert taskGroup.globalQueue !=null;
//        assert taskGroup.state == STATE_BLOCKED;
//        assert taskGroup.prev == null;
//        assert taskGroup.next == null;

        TaskGroup l = sharedLast;
        taskGroup.prev = l;
        sharedLast = taskGroup;
        if (l == null) {
            sharedFirst = taskGroup;
        } else {
            l.next = taskGroup;
        }
    }

    /**
     * Runs the actual eventloop.
     * <p/>
     * Is called from the reactor thread.
     * <p>
     * {@link StandardNanoClock#nanoTime()} is pretty expensive (+/-25ns) due to {@link System#nanoTime()}. For
     * every task processed we do not want to call the {@link StandardNanoClock#nanoTime()} more than once because
     * the clock already dominates the context switch time.
     *
     * @throws Exception if something fails while running the eventloop. The reactor
     *                   terminates when this happens.
     */
    @SuppressWarnings({"checkstyle:NPathComplexity",
            "checkstyle:MethodLength",
            "checkstyle:CyclomaticComplexity",
            "checkstyle:InnerAssignment"})
    public void run() throws Exception {
        long now = nanoClock.nanoTime();
        long ioDeadlineNanos = now + ioIntervalNanos;
        this.taskGroupStartNanos = now;

        while (!stop) {
          // Thread.sleep(100);

            // There is no point in doing the deadlineScheduler tick in the taskQueue processing loop
            // because the taskQueue is going to be processed till it is empty (or the quotaDeadlineNanos
            // is exceeded).
            deadlineScheduler.tick(now);

            scheduleBlockedGlobal();

            TaskGroup taskGroup = scheduler.pickNext();
            if (taskGroup == null) {
                park(now);
                // todo: we should only need to update the clock if real parking happened and not when work was detected
                now = nanoClock.nanoTime();
                taskGroupStartNanos = now;
                continue;
            }

            // I think the deadline nanos should be determined based on the weight of the taskGroup
            // So there should be a 'target' granularity and devide that by the taskgroups based on
            // their weight.
            long taskGroupDeadlineNanos = now + taskGroup.quotaNanos;
            long taskStartNanos = now;
            boolean blockTaskGroup = false;
            long deltaNanos = 0;
            int taskCount = 0;

            contextSwitches++;
            int x = taskGroup.skid;
            // Process the tasks in a taskQueue as long as the taskQuota is not exceeded.
            while (now <= taskGroupDeadlineNanos) {
                Runnable task = taskGroup.poll();
                if (task == null) {
                    // taskQueue is empty, we are done.
                    blockTaskGroup = true;
                    break;
                }

                try {
                    task.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                taskGroup.tasksProcessed++;
                taskCount++;

                if (x == 0) {
                    now = nanoClock.nanoTime();
                    x = taskGroup.skid;
                } else {
                    x--;
                }

                long taskEndNanos = now;
                long taskExecNanos = taskStartNanos - taskEndNanos;
                deltaNanos += taskExecNanos;

                if (taskExecNanos > stallThresholdNanos) {
                    stallDetector.onStall(reactor, task, taskStartNanos, taskExecNanos);
                }

                if (now >= ioDeadlineNanos) {
                    ioDeadlineNanos = now + ioIntervalNanos;
                    ioSchedulerTick();
                }
            }
//
//            if(contextSwitches%1000==0){
//                System.out.println("taskCount:"+taskCount);
//            }

            // todo * include weight
            long deltaWeightedNanos = deltaNanos;

            metrics.incTasksProcessedCount(taskCount);
            metrics.incCpuTimeNanos(deltaNanos);

            taskGroup.pruntimeNanos += deltaNanos;
            taskGroup.vruntimeNanos += deltaWeightedNanos;

            if (blockTaskGroup) {
                // the taskGroup is empty, so we mark this taskGroup as blocked.
                taskGroup.state = STATE_BLOCKED;
                taskGroup.blockedCount++;

                // we also need to add it to the shared taskGroups so the eventloop will
                // see any items that are written to its queue.
                if (taskGroup.globalQueue != null) {
                    addBlockedGlobal(taskGroup);
                }
            } else {
                // todo: it could be that the task group was empty and the quota was exceeded

                // the taskGroup exceeded its quota and therefor wasn't drained, so we
                // need to insert it back into the scheduler because there is more work
                // to be done.
                scheduler.enqueue(taskGroup);
            }
        }
    }

    protected abstract boolean ioSchedulerTick() throws IOException;

    protected abstract void park(long nowNanos) throws IOException;

    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit) {
        return schedule(cmd, delay, unit, rootTaskGroupHandle);
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
        task.taskGroup = rootTaskGroupHandle.taskGroup;
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
