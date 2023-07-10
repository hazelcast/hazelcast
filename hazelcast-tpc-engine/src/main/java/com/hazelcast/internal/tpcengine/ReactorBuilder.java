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
import com.hazelcast.internal.tpcengine.util.Preconditions;
import com.hazelcast.internal.util.ThreadAffinity;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A builder for {@link Reactor} instances.
 */
public abstract class ReactorBuilder {

    public static final String NAME_LOCAL_TASK_QUEUE_CAPACITY = "hazelcast.tpc.reactor.localTaskQueue.capacity";
    public static final String NAME_GLOBAL_TASK_QUEUE_CAPACITY = "hazelcast.tpc.reactor.globalTaskQueue.capacity";
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
    private static final int DEFAULT_GLOBAL_TASK_QUEUE_CAPACITY = 65536;
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
    Supplier<TaskFactory> taskFactorySupplier = () -> NullTaskFactory.INSTANCE;

    Supplier<String> threadNameSupplier;
    Supplier<String> reactorNameSupplier = new Supplier<>() {
        private final AtomicInteger idGenerator = new AtomicInteger();

        @Override
        public String get() {
            return "Reactor-" + idGenerator.incrementAndGet();
        }
    };

    ThreadAffinity threadAffinity = ThreadAffinity.newSystemThreadAffinity(NAME_REACTOR_AFFINITY);
    ThreadFactory threadFactory = Thread::new;
    boolean spin;
    int localTaskQueueCapacity;
    int globalTaskQueueCapacity;
    int deadlineRunQueueCapacity;
    TpcEngine engine;
    long stallThresholdNanos;
    long ioIntervalNanos;
    int runQueueCapacity;
    ReactorStallHandler stallHandler = LoggingStallHandler.INSTANCE;
    long targetLatencyNanos;
    long minGranularityNanos;
    boolean cfs;

    protected ReactorBuilder(ReactorType type) {
        this.type = checkNotNull(type);

        setLocalTaskQueueCapacity(Integer.getInteger(NAME_LOCAL_TASK_QUEUE_CAPACITY, DEFAULT_LOCAL_TASK_QUEUE_CAPACITY));
        setGlobalTaskQueueCapacity(Integer.getInteger(NAME_GLOBAL_TASK_QUEUE_CAPACITY, DEFAULT_GLOBAL_TASK_QUEUE_CAPACITY));
        setDeadlineRunQueueCapacity(Integer.getInteger(NAME_SCHEDULED_RUN_QUEUE_CAPACITY, DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY));
        setRunQueueCapacity(Integer.getInteger(NAME_RUN_QUEUE_CAPACITY, DEFAULT_RUN_QUEUE_CAPACITY));
        setTargetLatency(Long.getLong(NAME_TARGET_LATENCY_NANOS, DEFAULT_TARGET_LATENCY_NANOS), NANOSECONDS);
        setMinGranularity(Long.getLong(NAME_MIN_GRANULARITY_NANOS, DEFAULT_MIN_GRANULARITY_NANOS), NANOSECONDS);
        setStallThreshold(Long.getLong(NAME_STALL_THRESHOLD_NANOS, DEFAULT_STALL_THRESHOLD_NANOS), NANOSECONDS);
        setIoInterval(Long.getLong(NAME_IO_INTERVAL_NANOS, DEFAULT_IO_INTERVAL_NANOS), NANOSECONDS);
        setSpin(Boolean.parseBoolean(getProperty(NAME_REACTOR_SPIN, Boolean.toString(DEFAULT_SPIN))));
        setCfs(Boolean.parseBoolean(getProperty(NAME_CFS, Boolean.toString(DEFAULT_CFS))));
    }

    public static ReactorBuilder newReactorBuilder(ReactorType type) {
        Preconditions.checkNotNull(type, "type");
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
     * Sets the capacity for the run queue of the {@link TaskQueueScheduler}.This defines
     * the maximum number of TaskQueues that can be created within an {@link EventLoop}.
     *
     * @param runQueueCapacity the capacity of the run queue.
     * @throws IllegalArgumentException if the capacity is not a positive number.
     */
    public void setRunQueueCapacity(int runQueueCapacity) {
        this.runQueueCapacity = checkPositive(runQueueCapacity, "runQueueCapacity");
    }

    /**
     * Sets the total amount of time that can be divided over the taskqueues in the
     * {@link TaskQueueScheduler}. It depends on the scheduler implementation how
     * this is interpreted.
     *
     * @param targetLatency the target latency.
     * @param unit the unit of the target latency.
     * @throws IllegalArgumentException if the targetLatency is not a positive number.
     * @throws NullPointerException if unit is null.
     */
    public void setTargetLatency(long targetLatency, TimeUnit unit) {
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
     * @param unit the unit of the minGranularity.
     * @throws IllegalArgumentException if the targetLatency is not a positive number.
     * @throws NullPointerException if unit is null.
     */
    public void setMinGranularity(long minGranularity, TimeUnit unit) {
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
     * @param unit the unit of the stall threshold.
     * @throws IllegalArgumentException if the targetLatency is not a positive number.
     * @throws NullPointerException if unit is null.
     */
    public void setStallThreshold(long stallThreshold, TimeUnit unit) {
        checkPositive(stallThreshold, "stallThreshold");
        checkNotNull(unit, "unit");
        this.stallThresholdNanos = unit.toNanos(stallThreshold);
    }

    /**
     * Sets the {@link ReactorStallHandler}.
     *
     * @param stallHandler the new StallHandler.
     * @throws NullPointerException if stallHandler is <code>null</code>.
     */
    public void setStallHandler(ReactorStallHandler stallHandler) {
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
     */
    public void setIoInterval(long ioInterval, TimeUnit unit) {
        checkPositive(ioInterval, "ioInterval");
        checkNotNull(unit, "unit");
        this.ioIntervalNanos = unit.toNanos(ioInterval);
    }

    /**
     * Sets the reactor name supplier.
     *
     * @param reactorNameSupplier the reactor name supplier.
     * @throws NullPointerException if reactorNameSupplier is <code>null</code>.
     */
    public void setReactorNameSupplier(Supplier<String> reactorNameSupplier) {
        this.reactorNameSupplier = checkNotNull(reactorNameSupplier, "reactorNameSupplier");
    }

    /**
     * Sets the ThreadFactory used to create the Thread that runs the {@link Reactor}.
     *
     * @param threadFactory the ThreadFactory
     * @throws NullPointerException if threadFactory is set to <code>null</code>>.
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = checkNotNull(threadFactory, "threadFactory");
    }

    /**
     * Sets the supplier for the thread name. If configured, the thread name is set
     * after the thread is created.
     * <p/>
     * If <code>null</code>, there is no thread name supplier and the thread name
     * will not be modified.
     *
     * @param threadNameSupplier the supplier for the thread name.
     */
    public void setThreadNameSupplier(Supplier<String> threadNameSupplier) {
        this.threadNameSupplier = threadNameSupplier;
    }

    /**
     * Sets the {@link ThreadAffinity}. If the threadAffinity is <code>null</code>, no thread affinity
     * is applied.
     *
     * @param threadAffinity the ThreadAffinity.
     */
    public void setThreadAffinity(ThreadAffinity threadAffinity) {
        this.threadAffinity = threadAffinity;
    }

    /**
     * Sets the capacity of the local task queue. The local task queue is used only from the
     * eventloop thread.
     *
     * @param localTaskQueueCapacity the capacity
     * @throws IllegalArgumentException if localTaskQueueCapacity not positive.
     */
    public void setLocalTaskQueueCapacity(int localTaskQueueCapacity) {
        this.localTaskQueueCapacity = checkPositive(localTaskQueueCapacity, "localTaskQueueCapacity");
    }

    /**
     * Sets the capacity of the global task queue. The global task queue is the task queue used
     * for other threads to communicate with the reactor.
     *
     * @param globalTaskQueueCapacity the capacity
     * @throws IllegalArgumentException if externalTaskQueueCapacity not positive.
     */
    public void setGlobalTaskQueueCapacity(int globalTaskQueueCapacity) {
        this.globalTaskQueueCapacity = checkPositive(globalTaskQueueCapacity, "externalTaskQueueCapacity");
    }

    /**
     * Sets the capacity of the run queue for the deadline scheduler.
     *
     * @param deadlineRunQueueCapacity the capacity
     * @throws IllegalArgumentException if scheduledTaskQueueCapacity not positive.
     */
    public void setDeadlineRunQueueCapacity(int deadlineRunQueueCapacity) {
        this.deadlineRunQueueCapacity = checkPositive(deadlineRunQueueCapacity, "deadlineRunQueueCapacity");
    }

    // In the future we want to have better policies than only spinning.
    // See BackoffIdleStrategy
    public final void setSpin(boolean spin) {
        this.spin = spin;
    }

    /**
     * Sets the scheduler to use. If cfs is true, the {@link CfsTaskQueueScheduler} it used. Otherwise the
     * {@link FcfsTaskQueueScheduler} is used. Primary reason to set cfs=false is for performance testing
     * and debugging purposes.
     *
     * @param cfs if true, the {@link CfsTaskQueueScheduler} is used. Otherwise the {@link FcfsTaskQueueScheduler}
     */
    public final void setCfs(boolean cfs) {
        this.cfs = cfs;
    }

    /**
     * Sets the supplier function for {@link TaskFactory} instances.
     *
     * @param taskFactorySupplier the supplier
     * @throws NullPointerException if taskFactorySupplier is <code>null</code>.
     */
    public final void setTaskFactorySupplier(Supplier<TaskFactory> taskFactorySupplier) {
        this.taskFactorySupplier = checkNotNull(taskFactorySupplier, "taskFactorySupplier");
    }

    public void setBlockDeviceRegistry(BlockDeviceRegistry blockDeviceRegistry) {
        this.blockDeviceRegistry = checkNotNull(blockDeviceRegistry, "blockDeviceRegistry");
    }

    /**
     * Builds a Reactor based on the configuration of this {@link ReactorBuilder}.
     * <p/>
     * This method can be called multiple times. So a single ReactorBuilder instance can
     * create a family of similar {@link Reactor} instances.
     *
     * @return the created Reactor.
     */
    public abstract Reactor build();

}
