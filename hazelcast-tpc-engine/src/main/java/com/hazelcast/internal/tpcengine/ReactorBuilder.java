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

import com.hazelcast.internal.util.ThreadAffinity;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;
import static java.lang.System.getProperty;

/**
 * A builder for {@link Reactor} instances.
 */
public abstract class ReactorBuilder {
    public static final String NAME_LOCAL_TASK_QUEUE_CAPACITY = "hazelcast.tpc.localTaskQueue.capacity";
    public static final String NAME_EXTERNAL_TASK_QUEUE_CAPACITY = "hazelcast.tpc.externalTaskQueue.capacity";
    public static final String NAME_SCHEDULED_TASK_QUEUE_CAPACITY = "hazelcast.tpc.scheduledTaskQueue.capacity";
    public static final String NAME_BATCH_SIZE = "hazelcast.tpc.batch.size";
    public static final String NAME_CLOCK_REFRESH_PERIOD = "hazelcast.tpc.clock.refreshPeriod";
    public static final String NAME_REACTOR_SPIN = "hazelcast.tpc.reactor.spin";
    public static final String NAME_REACTOR_AFFINITY = "hazelcast.tpc.reactor.affinity";

    private static final int DEFAULT_LOCAL_TASK_QUEUE_CAPACITY = 65536;
    private static final int DEFAULT_EXTERNAL_TASK_QUEUE_CAPACITY = 65536;
    private static final int DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY = 4096;
    private static final int DEFAULT_BATCH_SIZE = 64;
    private static final int DEFAULT_CLOCK_REFRESH_INTERVAL = 16;
    private static final boolean DEFAULT_SPIN = false;

    protected final ReactorType type;
    Supplier<Scheduler> schedulerSupplier = NopScheduler::new;
    Supplier<String> threadNameSupplier;
    Supplier<String> reactorNameSupplier = new Supplier<String>() {
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
    int externalTaskQueueCapacity;
    int scheduledTaskQueueCapacity;
    int batchSize;
    int clockRefreshPeriod;
    TpcEngine engine;

    protected ReactorBuilder(ReactorType type) {
        this.type = checkNotNull(type);
        this.localTaskQueueCapacity = Integer.getInteger(
                NAME_LOCAL_TASK_QUEUE_CAPACITY, DEFAULT_LOCAL_TASK_QUEUE_CAPACITY);
        this.externalTaskQueueCapacity = Integer.getInteger(
                NAME_EXTERNAL_TASK_QUEUE_CAPACITY, DEFAULT_EXTERNAL_TASK_QUEUE_CAPACITY);
        this.scheduledTaskQueueCapacity = Integer.getInteger(
                NAME_SCHEDULED_TASK_QUEUE_CAPACITY, DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY);
        this.batchSize = Integer.getInteger(NAME_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.clockRefreshPeriod = Integer.getInteger(NAME_CLOCK_REFRESH_PERIOD, DEFAULT_CLOCK_REFRESH_INTERVAL);
        this.spin = Boolean.parseBoolean(getProperty(NAME_REACTOR_SPIN, Boolean.toString(DEFAULT_SPIN)));
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
     * Sets the clock refresh period.
     *
     * @param clockRefreshPeriod the period to refresh the time. A clockRefreshPeriod of 0 means
     *                           that always the newest time is obtained. There will be more overhead,
     *                           but you get better granularity.
     * @throws IllegalArgumentException when clockRefreshPeriod smaller than 0.
     */
    public void setClockRefreshPeriod(int clockRefreshPeriod) {
        this.clockRefreshPeriod = checkNotNegative(clockRefreshPeriod, "clockRefreshPeriod");
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
     * An eventloop has multiple queues to process. This setting controls the number of items
     * that are processed from a single queue in batch, before moving to the next queue.
     * <p>
     * Setting it to a lower value will improve fairness but can reduce throughput. Setting
     * it to a very high value could in theory lead to certain queues or event sources not
     * being processed at all. So imagine some local task that rescheduled itself, then it
     * could happen that with a very high batch size this tasks is processed in a loop while
     * none of the other queues/event-sources is checked and hence they are being starved
     * from CPU time.
     *
     * @param batchSize the size of the batch
     * @throws IllegalArgumentException if batchSize smaller than 1.
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = checkPositive(batchSize, "batchSize");
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
     * Sets the capacity of the local task queue.
     *
     * @param localTaskQueueCapacity the capacity
     * @throws IllegalArgumentException if localTaskQueueCapacity not positive.
     */
    public void setLocalTaskQueueCapacity(int localTaskQueueCapacity) {
        this.localTaskQueueCapacity = checkPositive(localTaskQueueCapacity, "localTaskQueueCapacity");
    }

    /**
     * Sets the capacity of the external task queue. The external task queue is the task queue used
     * for other threads to communicate with the reactor.
     *
     * @param externalTaskQueueCapacity the capacity
     * @throws IllegalArgumentException if externalTaskQueueCapacity not positive.
     */
    public void setExternalTaskQueueCapacity(int externalTaskQueueCapacity) {
        this.externalTaskQueueCapacity = checkPositive(externalTaskQueueCapacity, "externalTaskQueueCapacity");
    }

    /**
     * Sets the capacity of the scheduled task queue.
     *
     * @param scheduledTaskQueueCapacity the capacity
     * @throws IllegalArgumentException if scheduledTaskQueueCapacity not positive.
     */
    public void setScheduledTaskQueueCapacity(int scheduledTaskQueueCapacity) {
        this.scheduledTaskQueueCapacity = checkPositive(scheduledTaskQueueCapacity, "scheduledTaskQueueCapacity");
    }

    // In the future we want to have better policies than only spinning.
    // See BackoffIdleStrategy
    public final void setSpin(boolean spin) {
        this.spin = spin;
    }

    /**
     * Sets the supplier function for {@link Scheduler} instances.
     *
     * @param schedulerSupplier the supplier
     * @throws NullPointerException if schedulerSupplier is <code>null</code>.
     */
    public final void setSchedulerSupplier(Supplier<Scheduler> schedulerSupplier) {
        this.schedulerSupplier = checkNotNull(schedulerSupplier);
    }
}
