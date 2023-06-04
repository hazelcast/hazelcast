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

/**
 * A builder for {@link Reactor} instances.
 */
public abstract class ReactorBuilder {

    public static final String NAME_LOCAL_TASK_QUEUE_CAPACITY = "hazelcast.tpc.localTaskQueue.capacity";
    public static final String NAME_GLOBAL_TASK_QUEUE_CAPACITY = "hazelcast.tpc.globalTaskQueue.capacity";
    public static final String NAME_SCHEDULED_TASK_QUEUE_CAPACITY = "hazelcast.tpc.deadlineTaskQueue.capacity";
    public static final String NAME_REACTOR_SPIN = "hazelcast.tpc.reactor.spin";
    public static final String NAME_REACTOR_AFFINITY = "hazelcast.tpc.reactor.affinity";
    public static final String NAME_RUN_QUEUE_CAPACITY = "hazelcast.tpc.runqueue.capacity";
    public static final String NAME_TARGET_LATENCY_NANOS = "hazelcast.tpc.targetLatency.ns";
    public static final String NAME_MIN_GRANULARITY_NANOS = "hazelcast.tpc.minGranularity.ns";

    private static final int DEFAULT_LOCAL_TASK_QUEUE_CAPACITY = 65536;
    private static final int DEFAULT_GLOBAL_TASK_QUEUE_CAPACITY = 65536;
    private static final int DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY = 4096;
    private static final int DEFAULT_TASK_QUOTA_NANOS = 500;
    private static final int DEFAULT_STALL_THRESHOLD_NANOS = 500;
    private static final int DEFAULT_IO_INTERVAL_NANOS = 10;
    private static final int DEFAULT_RUN_QUEUE_CAPACITY = 1024;
    private static final long DEFAULT_TARGET_LATENCY_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long DEFAULT_MIN_GRANULARITY_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

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
    // Are not set through system property.
    long stallThresholdNanos = TimeUnit.MICROSECONDS.toNanos(DEFAULT_STALL_THRESHOLD_NANOS);
    long ioIntervalNanos = TimeUnit.MICROSECONDS.toNanos(DEFAULT_IO_INTERVAL_NANOS);
    int runQueueCapacity;
    StallHandler stallHandler = LoggingStallHandler.INSTANCE;
    long targetLatencyNanos;
    long minGranularityNanos;

    protected ReactorBuilder(ReactorType type) {
        this.type = checkNotNull(type);

        // todo: these properties should be set through a setter. Now they are unvalidated.
        this.localTaskQueueCapacity = Integer.getInteger(
                NAME_LOCAL_TASK_QUEUE_CAPACITY, DEFAULT_LOCAL_TASK_QUEUE_CAPACITY);
        this.globalTaskQueueCapacity = Integer.getInteger(
                NAME_GLOBAL_TASK_QUEUE_CAPACITY, DEFAULT_GLOBAL_TASK_QUEUE_CAPACITY);
        this.deadlineRunQueueCapacity = Integer.getInteger(
                NAME_SCHEDULED_TASK_QUEUE_CAPACITY, DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY);
        this.runQueueCapacity = Integer.getInteger(NAME_RUN_QUEUE_CAPACITY, DEFAULT_RUN_QUEUE_CAPACITY);
        this.targetLatencyNanos = Long.getLong(NAME_TARGET_LATENCY_NANOS, DEFAULT_TARGET_LATENCY_NANOS);
        this.minGranularityNanos = Long.getLong(NAME_MIN_GRANULARITY_NANOS, DEFAULT_MIN_GRANULARITY_NANOS);
        this.spin = Boolean.parseBoolean(getProperty(NAME_REACTOR_SPIN, Boolean.toString(DEFAULT_SPIN)));
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
     * Builds a Reactor based on the configuration of this {@link ReactorBuilder}.
     * <p/>
     * This method can be called multiple times. So a single ReactorBuilder instance can
     * create a family of similar {@link Reactor} instances.
     *
     * @return the created Reactor.
     */
    public abstract Reactor build();

    public ReactorBuilder setRunQueueCapacity(int runQueueCapacity) {
        this.runQueueCapacity = checkPositive(runQueueCapacity, "runQueueCapacity");
        return this;
    }

    public ReactorBuilder setTargetLatencyNanos(long targetLatencyNanos) {
        this.targetLatencyNanos = checkPositive(targetLatencyNanos, "targetLatencyNanos");
        return this;
    }

    public ReactorBuilder setMinGranularityNanos(long minGranularityNanos) {
        this.minGranularityNanos = checkPositive(minGranularityNanos, "minGranularityNanos");
        return this;
    }

    /**
     * The maximum amount of time a task is allowed to run before being considered stalling
     * the reactor.
     *
     * @param stallThreshold
     * @param unit
     */
    public void setStallThreshold(long stallThreshold, TimeUnit unit) {
        checkPositive(stallThreshold, "stallThreshold");
        checkNotNull(unit, "unit");
        this.stallThresholdNanos = unit.toNanos(stallThreshold);
    }

    /**
     * Configures the stallHandler.
     *
     * @param stallHandler
     * @throws NullPointerException if stallHandler is <code>null</code>.
     */
    public void setStallHandler(StallHandler stallHandler) {
        this.stallHandler = checkNotNull(stallHandler, "stallHandler");
    }

    /**
     * The interval the I/O scheduler should be checked if there if any I/O activity (either
     * submitting work or there are any completed events.
     * <p/>
     * There is no guarantee that the I/O scheduler is going to be called at the exact interval
     * when there are other threads/processes contending for the core and when there are stalls
     * on the reactor.
     *
     * @param ioInterval
     * @param unit
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
     * Sets the capacity of the runqueue for the deadline scheduler.
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
}
