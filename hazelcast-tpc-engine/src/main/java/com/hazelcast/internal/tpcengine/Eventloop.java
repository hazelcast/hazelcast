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
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;
import com.hazelcast.internal.tpcengine.util.NanoClock;
import com.hazelcast.internal.tpcengine.util.Promise;
import com.hazelcast.internal.tpcengine.util.PromiseAllocator;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;
import com.hazelcast.internal.tpcengine.util.StandardNanoClock;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpcengine.SchedulingGroup.STATE_BLOCKED;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.lang.Math.max;

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

    protected final PriorityQueue<DeadlineTask> deadlineTaskQueue;
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
    public final SchedulingGroupHandle externalTaskQueueHandle;
    public final SchedulingGroupHandle localTaskQueueHandle;
    protected long earliestDeadlineNanos = -1;
    protected boolean stop;


    protected long taskStartNanos;
    protected long ioDeadlineNanos;
    protected final SlabAllocator<SchedulingGroup> taskQueueAllocator = new SlabAllocator<>(1024, SchedulingGroup::new);
    private final long ioIntervalNanos = TimeUnit.MICROSECONDS.toNanos(10);
    CfsScheduler runQueue = new CfsScheduler();

    protected Eventloop(Reactor reactor, ReactorBuilder builder) {
        this.reactor = reactor;
        this.builder = builder;
        this.deadlineTaskQueue = new BoundPriorityQueue<>(builder.scheduledTaskQueueCapacity);
        this.externalTaskQueueHandle = new SchedulingGroupBuilder(this)
                .setQueue(new MpscArrayQueue<>(builder.externalTaskQueueCapacity))
                .setConcurrent(true)
                .setShares(1)
                .build();
        this.localTaskQueueHandle = new SchedulingGroupBuilder(this)
                .setQueue(new CircularQueue<>(builder.localTaskQueueCapacity))
                .setConcurrent(false)
                .setShares(1)
                .build();
        this.spin = builder.spin;
        this.batchSize = builder.batchSize;
        this.promiseAllocator = new PromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.intPromiseAllocator = new IntPromiseAllocator(this, INITIAL_PROMISE_ALLOCATOR_CAPACITY);
        this.nanoClock = new StandardNanoClock();
        this.taskStartNanos = nanoClock.nanoTime();
        this.ioDeadlineNanos = taskStartNanos + ioIntervalNanos;
        this.scheduler = builder.schedulerSupplier.get();
        scheduler.init(this);
    }

    /**
     * @param handle
     * @return
     */
    public final SchedulingGroup getTaskQueue(SchedulingGroupHandle handle) {
        return handle.schedulingGroup;
    }

    /**
     * @return
     */
    public final SchedulingGroupBuilder newTaskQueueBuilder() {
        return new SchedulingGroupBuilder(this);
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

//    /**
//     * Runs the actual eventloop.
//     * <p/>
//     * Is called from the reactor thread.
//     *
//     * @throws Exception if something fails while running the eventloop. The reactor
//     *                   terminates when this happens.
//     */
//    @SuppressWarnings("java:S112")
//    protected abstract void run() throws Exception;

    /**
     * Destroys the resources of this Eventloop. Is called after the {@link #run()}.
     * <p>
     * Is called from the reactor thread.
     */
    @SuppressWarnings("java:S112")
    protected void destroy() throws Exception {
    }

    // todo: when a concurrent run queue gets blocked, it should be added to this list.
    protected ArrayList<SchedulingGroup> concurrentSchedGroups = new ArrayList<>();

    protected final boolean scheduleConcurrent() {
        boolean scheduled = false;
        // make use of hasConcurrentTasks
        for (int k = 0; k < concurrentSchedGroups.size(); k++) {
            SchedulingGroup schedGroup = concurrentSchedGroups.get(k);

            if (!schedGroup.queue.isEmpty()) {
                scheduled = true;
                insertRunQueue(schedGroup);

                concurrentSchedGroups.remove(k);
                k--;
                // todo: the concurrent schedGroup needs to be removed from the concurrentTaskQueue
            }
        }
        return scheduled;

    }

    void insertRunQueue(SchedulingGroup schedGroup) {
        schedGroup.state = SchedulingGroup.STATE_RUNNING;
        schedGroup.vruntimeNanos = max(schedGroup.vruntimeNanos, runQueue.min_vruntime());
        runQueue.insert(schedGroup);
    }

    public void run() throws Exception {
        long cycleStartNanos = nanoClock.nanoTime();

        while (!stop) {
            // Thread.sleep(100);

            scheduleConcurrent();

            SchedulingGroup schedGroup = runQueue.next();
            if (schedGroup == null) {
                park();
                cycleStartNanos = nanoClock.nanoTime();
            } else {
                Object cmd = schedGroup.queue.poll();
                if (cmd instanceof Runnable) {
                    ((Runnable) cmd).run();
                } else {
                    throw new RuntimeException();
                }

                long cycleEndNanos = nanoClock.nanoTime();
                long runtimeDelta = cycleEndNanos - cycleStartNanos;

                // todo * include weight
                long vruntimeNanosDelta = runtimeDelta;

                schedGroup.vruntimeNanos += vruntimeNanosDelta;

                if (schedGroup.queue.isEmpty()) {
                    schedGroup.state = STATE_BLOCKED;

                    if (schedGroup.concurrent) {
                        concurrentSchedGroups.add(schedGroup);
                    }
                } else {
                    insertRunQueue(schedGroup);
                }

                // todo: report duration violations.
                if (cycleEndNanos >= ioDeadlineNanos) {
                    if (ioSchedulerTick()) {
                        ioDeadlineNanos = cycleEndNanos += ioIntervalNanos;
                    }
                }

                deadlineSchedulerTick(cycleEndNanos);
                cycleStartNanos = cycleEndNanos;
            }
        }
    }

    protected void deadlineSchedulerTick(long nowNanos) {
        while (true) {
            DeadlineTask deadlineTask = deadlineTaskQueue.peek();

            if (deadlineTask == null) {
                return;
            }

            if (deadlineTask.deadlineNanos > nowNanos) {
                // Task should not yet be executed.
                earliestDeadlineNanos = deadlineTask.deadlineNanos;
                // we are done since all other tasks have a larger deadline.
                return;
            }

            // the deadlineTask first needs to be removed from the deadlineTask queue.
            deadlineTaskQueue.poll();
            earliestDeadlineNanos = -1;

            // offer the ScheduledTask to the task queue.
            deadlineTask.schedGroup.offer(deadlineTask);
        }
    }

    protected abstract boolean ioSchedulerTick();

    protected abstract void park() throws IOException;

    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit) {
        return schedule(cmd, delay, unit, localTaskQueueHandle);
    }

    /**
     * Schedules a one shot action with the given delay.
     *
     * @param cmd   the cmd to execute.
     * @param delay the delay
     * @param unit  the unit of the delay
     * @return true if the cmd was successfully scheduled.
     * @throws NullPointerException     if cmd or unit is null
     * @throws IllegalArgumentException when delay smaller than 0.
     */
    public final boolean schedule(Runnable cmd,
                                  long delay,
                                  TimeUnit unit,
                                  SchedulingGroupHandle taskQueueHandle) {
        checkNotNull(cmd);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        DeadlineTask scheduledTask = new DeadlineTask(this);
        scheduledTask.cmd = cmd;
        scheduledTask.schedGroup = taskQueueHandle.schedulingGroup;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        return deadlineTaskQueue.offer(scheduledTask);
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
                                                SchedulingGroupHandle taskQueueHandle) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        DeadlineTask scheduledTask = new DeadlineTask(this);
        scheduledTask.schedGroup = taskQueueHandle.schedulingGroup;
        scheduledTask.cmd = cmd;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTask.delayNanos = unit.toNanos(delay);
        return deadlineTaskQueue.offer(scheduledTask);
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
                                             SchedulingGroupHandle taskQueueHandle) {
        checkNotNull(cmd);
        checkNotNegative(initialDelay, "initialDelay");
        checkNotNegative(period, "period");
        checkNotNull(unit);

        DeadlineTask scheduledTask = new DeadlineTask(this);
        scheduledTask.schedGroup = taskQueueHandle.schedulingGroup;
        scheduledTask.cmd = cmd;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        scheduledTask.periodNanos = unit.toNanos(period);
        return deadlineTaskQueue.offer(scheduledTask);
    }

    public final Promise sleep(long delay, TimeUnit unit) {
        checkNotNegative(delay, "delay");
        checkNotNull(unit, "unit");

        Promise promise = promiseAllocator.allocate();
        DeadlineTask scheduledTask = new DeadlineTask(this);
        scheduledTask.promise = promise;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        scheduledTask.deadlineNanos = deadlineNanos;
        deadlineTaskQueue.add(scheduledTask);
        return promise;
    }
}
