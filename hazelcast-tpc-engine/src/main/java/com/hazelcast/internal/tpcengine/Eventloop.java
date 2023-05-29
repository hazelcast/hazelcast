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
import com.hazelcast.internal.tpcengine.util.EpochClock;
import com.hazelcast.internal.tpcengine.util.IntPromiseAllocator;
import com.hazelcast.internal.tpcengine.util.Promise;
import com.hazelcast.internal.tpcengine.util.PromiseAllocator;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;
import com.hazelcast.internal.tpcengine.util.StandardNanoClock;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpcengine.TaskGroup.STATE_BLOCKED;
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

    protected final Reactor reactor;
    protected final boolean spin;
    protected final int batchSize;
    protected final ReactorBuilder builder;
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final EpochClock nanoClock;
    protected final Scheduler crappyScheduler;
    protected final PromiseAllocator promiseAllocator;
    protected final IntPromiseAllocator intPromiseAllocator;
    public final TaskGroupHandle externalTaskQueueHandle;
    public final TaskGroupHandle localTaskQueueHandle;
    protected boolean stop;


    protected long taskStartNanos;
    protected long ioDeadlineNanos;
    protected final SlabAllocator<TaskGroup> taskQueueAllocator = new SlabAllocator<>(1024, TaskGroup::new);
    private final long ioIntervalNanos = TimeUnit.MICROSECONDS.toNanos(10);
    protected final DeadlineScheduler deadlineScheduler;
    protected final ArrayList<TaskGroup> concurrentBlockedTaskGroups = new ArrayList<>();
    protected final

    CfsScheduler scheduler = new CfsScheduler();
    private long cycleStartNanos;
    private long tasksProcess;

    protected Eventloop(Reactor reactor, ReactorBuilder builder) {
        this.reactor = reactor;
        this.builder = builder;
        this.deadlineScheduler = new DeadlineScheduler(builder.scheduledTaskQueueCapacity);
        this.externalTaskQueueHandle = new TaskGroupBuilder(this)
                .setQueue(new MpscArrayQueue<>(builder.externalTaskQueueCapacity))
                .setConcurrent(true)
                .setShares(1)
                .build();
        this.localTaskQueueHandle = new TaskGroupBuilder(this)
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
        this.crappyScheduler = builder.schedulerSupplier.get();
        crappyScheduler.init(this);
    }

    public final long cycleStartNanos(){
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

    protected final boolean scheduleConcurrent() {
        boolean scheduled = false;
        for (int k = 0; k < concurrentBlockedTaskGroups.size(); k++) {
            TaskGroup taskGroups = concurrentBlockedTaskGroups.get(k);

            if (!taskGroups.queue.isEmpty()) {
                scheduled = true;
                scheduler.enqueue(taskGroups);

                concurrentBlockedTaskGroups.remove(k);
                k--;
            }
        }
        return scheduled;
    }


    /**
     * Runs the actual eventloop.
     * <p/>
     * Is called from the reactor thread.
     *
     * @throws Exception if something fails while running the eventloop. The reactor
     *                   terminates when this happens.
     */
     public void run() throws Exception {
        this.cycleStartNanos = nanoClock.nanoTime();

        int clockSkip = 0;

        while (!stop) {
            // Thread.sleep(100);

            scheduleConcurrent();

            TaskGroup taskGroup = scheduler.pickNext();
            if (taskGroup == null) {
                park();
                cycleStartNanos = nanoClock.nanoTime();
            } else {
                tasksProcess++;
                taskGroup.tasksProcessed++;
                Object cmd = taskGroup.queue.poll();
                if (cmd instanceof Runnable) {
                    try {
                        ((Runnable) cmd).run();
                    } catch (Exception e) {
                        logger.warning(e);
                    }
                } else {
                    throw new RuntimeException();
                }

                long cycleEndNanos = nanoClock.nanoTime();

                long deltaNanos = cycleEndNanos - cycleStartNanos;

                // todo * include weight
                long deltaWeightedNanos = deltaNanos;// / taskGroup.weight;

                taskGroup.pruntimeNanos += deltaNanos;
                taskGroup.vruntimeNanos += deltaWeightedNanos;

                if (taskGroup.queue.isEmpty()) {
                    taskGroup.state = STATE_BLOCKED;
                    taskGroup.blockedCount++;

                    if (taskGroup.concurrent) {
                        concurrentBlockedTaskGroups.add(taskGroup);
                    }
                } else {
                    scheduler.enqueue(taskGroup);
                }

                // todo: report duration violations.
                if (cycleEndNanos >= ioDeadlineNanos) {
                    ioSchedulerTick();
                    ioDeadlineNanos = cycleEndNanos += ioIntervalNanos;
                }

                deadlineScheduler.tick(cycleEndNanos);
                cycleStartNanos = cycleEndNanos;
            }
        }
    }

    protected abstract boolean ioSchedulerTick() throws IOException;

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
                                  TaskGroupHandle taskGroupHandle) {
        checkNotNull(cmd);
        checkNotNegative(delay, "delay");
        checkNotNull(unit);

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.cmd = cmd;
        task.taskGroup = taskGroupHandle.taskGroup;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        task.deadlineNanos = deadlineNanos;
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

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.taskGroup = taskGroupHandle.taskGroup;
        task.cmd = cmd;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        task.deadlineNanos = deadlineNanos;
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

        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.taskGroup = taskGroupHandle.taskGroup;
        task.cmd = cmd;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(initialDelay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        task.deadlineNanos = deadlineNanos;
        task.periodNanos = unit.toNanos(period);
        return deadlineScheduler.offer(task);
    }

    public final Promise sleep(long delay, TimeUnit unit) {
        checkNotNegative(delay, "delay");
        checkNotNull(unit, "unit");

        Promise promise = promiseAllocator.allocate();
        DeadlineTask task = new DeadlineTask(nanoClock, deadlineScheduler);
        task.promise = promise;
        long deadlineNanos = nanoClock.nanoTime() + unit.toNanos(delay);
        if (deadlineNanos < 0) {
            // protection against overflow
            deadlineNanos = Long.MAX_VALUE;
        }
        task.deadlineNanos = deadlineNanos;
        deadlineScheduler.offer(task);
        return promise;
    }
}
