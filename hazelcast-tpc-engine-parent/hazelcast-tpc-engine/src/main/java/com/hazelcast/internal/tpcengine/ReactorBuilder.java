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

import com.hazelcast.internal.tpcengine.file.BlockDeviceRegistry;
import com.hazelcast.internal.tpcengine.nio.NioReactorBuilder;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.util.ThreadAffinity;
import org.jctools.queues.MpscArrayQueue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A builder for {@link Reactor} instances.
 * <p/>
 * Only a single Reactor can be build per builder instance.
 */
public abstract class ReactorBuilder {

    public static final String NAME_SCHEDULED_RUN_QUEUE_CAPACITY = "hazelcast.tpc.reactor.deadlineRunQueue.capacity";
    public static final String NAME_REACTOR_SPIN = "hazelcast.tpc.reactor.spin";
    public static final String NAME_REACTOR_AFFINITY = "hazelcast.tpc.reactor.affinity";
    public static final String NAME_RUN_QUEUE_CAPACITY = "hazelcast.tpc.reactor.runQueue.capacity";
    public static final String NAME_TARGET_LATENCY_NANOS = "hazelcast.tpc.reactor.targetLatency.ns";
    public static final String NAME_MIN_GRANULARITY_NANOS = "hazelcast.tpc.reactor.minGranularity.ns";
    public static final String NAME_STALL_THRESHOLD_NANOS = "hazelcast.tpc.reactor.stallThreshold.ns";
    public static final String NAME_IO_INTERVAL_NANOS = "hazelcast.tpc.reactor.ioInterval.ns";
    public static final String NAME_CFS = "hazelcast.tpc.reactor.cfs";

    private static final int DEFAULT_LOCAL_TASK_QUEUE_CAPACITY = 65536;
    private static final int DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY = 4096;
    private static final int DEFAULT_RUN_QUEUE_CAPACITY = 1024;
    private static final long DEFAULT_STALL_THRESHOLD_NANOS = MICROSECONDS.toNanos(500);
    private static final long DEFAULT_IO_INTERVAL_NANOS = MICROSECONDS.toNanos(50);
    private static final long DEFAULT_TARGET_LATENCY_NANOS = MILLISECONDS.toNanos(1);
    private static final long DEFAULT_MIN_GRANULARITY_NANOS = MICROSECONDS.toNanos(100);
    private static final boolean DEFAULT_CFS = true;
    private static final boolean DEFAULT_SPIN = false;

    private static final Constructor<ReactorBuilder> IO_URING_REACTOR_BUILDER_CONSTRUCTOR;

    private static final String IOURING_IOURING_REACTOR_BUILDER_CLASS_NAME
            = "com.hazelcast.internal.tpcengine.iouring.IOUringReactorBuilder";

    static {
        Constructor<ReactorBuilder> constructor = null;
        try {
            Class clazz = ReactorBuilder.class.getClassLoader().loadClass(
                    IOURING_IOURING_REACTOR_BUILDER_CLASS_NAME);
            constructor = clazz.getConstructor();
        } catch (ClassNotFoundException e) {
            constructor = null;
        } catch (NoSuchMethodException e) {
            throw new Error(e);
        } finally {
            IO_URING_REACTOR_BUILDER_CONSTRUCTOR = constructor;
        }
    }

    protected BlockDeviceRegistry blockDeviceRegistry = new BlockDeviceRegistry();
    protected final ReactorType type;

    ThreadAffinity threadAffinity = ThreadAffinity.newSystemThreadAffinity(NAME_REACTOR_AFFINITY);
    ThreadFactory threadFactory = Thread::new;

    boolean spin;
    int deadlineRunQueueCapacity;
    TpcEngine engine;
    long stallThresholdNanos;
    long ioIntervalNanos;
    int runQueueCapacity;
    StallHandler stallHandler = LoggingStallHandler.INSTANCE;
    long targetLatencyNanos;
    long minGranularityNanos;
    boolean cfs;
    TaskQueueBuilder defaultTaskQueueBuilder;
    Consumer<Reactor> initCommand;
    String reactorName;
    String threadName;
    private boolean built;

    protected ReactorBuilder(ReactorType type) {
        this.type = checkNotNull(type);

        setDeadlineRunQueueCapacity(Integer.getInteger(NAME_SCHEDULED_RUN_QUEUE_CAPACITY, DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY));
        setRunQueueCapacity(Integer.getInteger(NAME_RUN_QUEUE_CAPACITY, DEFAULT_RUN_QUEUE_CAPACITY));
        setTargetLatency(Long.getLong(NAME_TARGET_LATENCY_NANOS, DEFAULT_TARGET_LATENCY_NANOS), NANOSECONDS);
        setMinGranularity(Long.getLong(NAME_MIN_GRANULARITY_NANOS, DEFAULT_MIN_GRANULARITY_NANOS), NANOSECONDS);
        setStallThreshold(Long.getLong(NAME_STALL_THRESHOLD_NANOS, DEFAULT_STALL_THRESHOLD_NANOS), NANOSECONDS);
        setIoInterval(Long.getLong(NAME_IO_INTERVAL_NANOS, DEFAULT_IO_INTERVAL_NANOS), NANOSECONDS);
        setSpin(Boolean.parseBoolean(getProperty(NAME_REACTOR_SPIN, Boolean.toString(DEFAULT_SPIN))));
        setCfs(Boolean.parseBoolean(getProperty(NAME_CFS, Boolean.toString(DEFAULT_CFS))));
    }

    /**
     * Creates a new {@link ReactorBuilder} based on the {@link ReactorType}.
     *
     * @param type the reactor type.
     * @return the created ReactorBuilder.
     * @throws NullPointerException if type is null.
     * @throws RuntimeException     if the IO_URING reactor is requested but the class is
     *                              not found or there are other problems.
     */
    public static ReactorBuilder newReactorBuilder(ReactorType type) {
        checkNotNull(type, "type");

        switch (type) {
            case NIO:
                return new NioReactorBuilder();
            case IOURING:
                if (IO_URING_REACTOR_BUILDER_CONSTRUCTOR == null) {
                    throw new IllegalStateException("class " + IOURING_IOURING_REACTOR_BUILDER_CLASS_NAME + " is not found");
                }

                try {
                    return IO_URING_REACTOR_BUILDER_CONSTRUCTOR.newInstance();
                } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new IllegalStateException("Unhandled reactorType: " + type);
        }
    }

    /**
     * A command that is executed on the eventloop as soon as the eventloop is starting.
     *
     * @param initCommand the command to execute.
     * @throws NullPointerException  if <code>initCommand</code> is null.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setInitCommand(Consumer<Reactor> initCommand) {
        verifyNotBuilt();
        checkNotNull(initCommand, "initCommand");
        this.initCommand = initCommand;
    }

    /**
     * Sets the default {@link TaskQueueBuilder}.
     *
     * @param defaultTaskQueueBuilder the default {@link TaskQueueBuilder}.
     * @throws NullPointerException  if <code>defaultTaskQueueBuilder</code> is null.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setDefaultTaskQueueBuilder(TaskQueueBuilder defaultTaskQueueBuilder) {
        verifyNotBuilt();
        checkNotNull(defaultTaskQueueBuilder, "defaultTaskQueueBuilder");
        this.defaultTaskQueueBuilder = defaultTaskQueueBuilder;
    }

    /**
     * Sets the capacity for the run queue of the {@link TaskQueueScheduler}.This defines
     * the maximum number of TaskQueues that can be created within an {@link Eventloop}.
     *
     * @param runQueueCapacity the capacity of the run queue.
     * @throws IllegalArgumentException if the capacity is not a positive number.
     * @throws IllegalStateException    if the Reactor has already been built.
     */
    public void setRunQueueCapacity(int runQueueCapacity) {
        verifyNotBuilt();
        this.runQueueCapacity = checkPositive(runQueueCapacity, "runQueueCapacity");
    }

    /**
     * Sets the total amount of time that can be divided over the taskqueues in the
     * {@link TaskQueueScheduler}. It depends on the scheduler implementation how
     * this is interpreted.
     *
     * @param targetLatency the target latency.
     * @param unit          the unit of the target latency.
     * @throws IllegalArgumentException if the targetLatency is not a positive number.
     * @throws NullPointerException     if unit is null.
     * @throws IllegalStateException    if the Reactor has already been built.
     */
    public void setTargetLatency(long targetLatency, TimeUnit unit) {
        verifyNotBuilt();
        checkPositive(targetLatency, "targetLatency");
        checkNotNull(unit, "unit");
        this.targetLatencyNanos = unit.toNanos(targetLatency);
    }

    /**
     * Sets the minimum amount of time a taskqueue is guaranteed to run (unless the
     * taskgroup decided to stop/yield).
     * <p>
     * Setting this value too low could lead to excessive context switching. Setting
     * this value too high could lead to unresponsiveness (increased latency).
     *
     * @param minGranularity the minimum granularity.
     * @param unit           the unit of the minGranularity.
     * @throws IllegalArgumentException if the targetLatency is not a positive number.
     * @throws NullPointerException     if unit is <code>null</code>.
     * @throws IllegalStateException    if the Reactor has already been built.
     */
    public void setMinGranularity(long minGranularity, TimeUnit unit) {
        verifyNotBuilt();
        checkPositive(minGranularity, "minGranularity");
        checkNotNull(unit, "unit");
        this.minGranularityNanos = unit.toNanos(minGranularity);
    }

    /**
     * The maximum amount of time a task is allowed to run before being considered stalling
     * the reactor.
     * <p/>
     * Setting this value too low will lead to a lot of noise (false positives). Setting
     * this value too high will lead to not detecting the stalls on the reactor (false
     * negatives).
     *
     * @param stallThreshold the stall threshold.
     * @param unit           the unit of the stall threshold.
     * @throws IllegalArgumentException if the targetLatency is not a positive number.
     * @throws NullPointerException     if unit is <code>null</code>>.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setStallThreshold(long stallThreshold, TimeUnit unit) {
        verifyNotBuilt();
        checkPositive(stallThreshold, "stallThreshold");
        checkNotNull(unit, "unit");
        this.stallThresholdNanos = unit.toNanos(stallThreshold);
    }

    /**
     * Sets the {@link StallHandler}.
     *
     * @param stallHandler the new StallHandler.
     * @throws NullPointerException if stallHandler is <code>null</code>.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setStallHandler(StallHandler stallHandler) {
        verifyNotBuilt();
        this.stallHandler = checkNotNull(stallHandler, "stallHandler");
    }

    /**
     * The interval the I/O scheduler is be checked if there if any I/O activity (either
     * submitting work or there are any completed events.
     * <p/>
     * There is no guarantee that the I/O scheduler is going to be called at the exact interval
     * when there are other threads/processes contending for the core and when there are stalls
     * on the reactor.
     * <p/>
     * Setting the value too low will cause a lot of overhead. It can even lead to the eventloop
     * spinning on ticks to the io-scheduler instead of able to park. Setting it too high will
     * suboptimal performance in the I/O system because I/O requests will be delayed.
     *
     * @param ioInterval the io interval
     * @param unit       the unit for the io interval.
     * @throws NullPointerException if unit is <code>null</code>.
     * @throws IllegalArgumentException if ioInterval is not a positive number.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setIoInterval(long ioInterval, TimeUnit unit) {
        verifyNotBuilt();
        checkPositive(ioInterval, "ioInterval");
        checkNotNull(unit, "unit");
        this.ioIntervalNanos = unit.toNanos(ioInterval);
    }

    /**
     * Sets the reactor name.
     *
     * @param reactorName the reactor name.
     * @throws NullPointerException if reactorName is <code>null</code>.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setReactorName(String reactorName) {
        verifyNotBuilt();
        this.reactorName = checkNotNull(reactorName, "reactorName");
    }

    /**
     * Sets the ThreadFactory used to create the Thread that runs the {@link Reactor}.
     *
     * @param threadFactory the ThreadFactory
     * @throws NullPointerException if threadFactory is set to <code>null</code>>.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        verifyNotBuilt();
        this.threadFactory = checkNotNull(threadFactory, "threadFactory");
    }

    /**
     * Sets the name of the thread. If configured, the thread name is set
     * after the thread is created. If not configured, the thread name provided
     * by the ThreadFactory is used.
     *
     * @param threadName the name of the thread.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setThreadName(String threadName) {
        verifyNotBuilt();
        this.threadName = threadName;
    }

    /**
     * Sets the {@link ThreadAffinity}. If the threadAffinity is <code>null</code>, no thread affinity
     * is applied.
     *
     * @param threadAffinity the ThreadAffinity.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setThreadAffinity(ThreadAffinity threadAffinity) {
        verifyNotBuilt();
        this.threadAffinity = threadAffinity;
    }

    /**
     * Sets the capacity of the run queue for the deadline scheduler.
     *
     * @param deadlineRunQueueCapacity the capacity
     * @throws IllegalArgumentException if scheduledTaskQueueCapacity not positive.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setDeadlineRunQueueCapacity(int deadlineRunQueueCapacity) {
        verifyNotBuilt();
        this.deadlineRunQueueCapacity = checkPositive(deadlineRunQueueCapacity, "deadlineRunQueueCapacity");
    }

    /**
     * Sets the spin policy. If spin is true, the reactor will spin on the run queue if there are no
     * tasks to run. If spin is false, the reactor will park the thread if there are no tasks to run.
     * <p/>
     * In the future we want to have better policies than only spinning. For example, see
     * BackoffIdleStrategy
     *
     * @param spin true is spin is enabled, false otherwise.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public final void setSpin(boolean spin) {
        verifyNotBuilt();
        this.spin = spin;
    }

    /**
     * Sets the scheduler to use. If cfs is true, the {@link CfsTaskQueueScheduler} it used. Otherwise the
     * {@link FcfsTaskQueueScheduler} is used. Primary reason to set cfs=false is for performance testing
     * and debugging purposes.
     *
     * @param cfs if true, the {@link CfsTaskQueueScheduler} is used. Otherwise the {@link FcfsTaskQueueScheduler}
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public final void setCfs(boolean cfs) {
        verifyNotBuilt();
        this.cfs = cfs;
    }

    /**
     * Sets the BlockDeviceRegistry
     *
     * @param blockDeviceRegistry the BlockDeviceRegistry
     * @throws NullPointerException if <code>blockDeviceRegistry</code> is <code>null</code>.
     * @throws IllegalStateException if the Reactor has already been built.
     */
    public void setBlockDeviceRegistry(BlockDeviceRegistry blockDeviceRegistry) {
        verifyNotBuilt();
        this.blockDeviceRegistry = checkNotNull(blockDeviceRegistry, "blockDeviceRegistry");
    }

    /**
     * Builds a Reactor based on the configuration of this {@link ReactorBuilder}.
     * <p/>
     * This method can be called multiple times. So a single ReactorBuilder instance can
     * create a family of similar {@link Reactor} instances.
     *
     * @return the created Reactor.
     * @throws IllegalStateException if the ReactorBuilder is already built.
     */
    public final Reactor build() {
        verifyNotBuilt();
        built = true;
        return build0();
    }

    protected abstract Reactor build0();

    protected void verifyNotBuilt() {
        if (built) {
            throw new IllegalStateException("Can't call build twice on the same ReactorBuilder");
        }
    }

    TaskQueueBuilder newDefaultTaskQueueBuilder() {
        if (defaultTaskQueueBuilder == null) {
            defaultTaskQueueBuilder = new TaskQueueBuilder()
                    .setName("default")
                    .setGlobal(new MpscArrayQueue<>(DEFAULT_LOCAL_TASK_QUEUE_CAPACITY))
                    .setLocal(new CircularQueue<>(DEFAULT_LOCAL_TASK_QUEUE_CAPACITY));
        }

        return defaultTaskQueueBuilder;
    }
}
