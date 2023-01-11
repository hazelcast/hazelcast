/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.util.ThreadAffinity;

import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;
import static java.lang.System.getProperty;

/**
 * A factory for {@link Eventloop} instances.
 */
public abstract class EventloopBuilder {
    public static final String NAME_LOCAL_TASK_QUEUE_CAPACITY = "hazelcast.tpc.localTaskQueue.capacity";
    public static final String NAME_CONCURRENT_TASK_QUEUE_CAPACITY = "hazelcast.tpc.concurrentTaskQueue.capacity";
    public static final String NAME_SCHEDULED_TASK_QUEUE_CAPACITY = "hazelcast.tpc.scheduledTaskQueue.capacity";
    public static final String NAME_BATCH_SIZE = "hazelcast.tpc.batch.size";
    public static final String NAME_CLOCK_REFRESH_PERIOD = "hazelcast.tpc.clock.refreshPeriod";
    public static final String NAME_EVENTLOOP_SPIN = "hazelcast.tpc.eventloop.spin";

    private static final int DEFAULT_LOCAL_QUEUE_CAPACITY = 65536;
    private static final int DEFAULT_CONCURRENT_QUEUE_CAPACITY = 65536;
    private static final int DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY = 4096;
    private static final int DEFAULT_BATCH_SIZE = 64;
    private static final int DEFAULT_CLOCK_REFRESH_INTERVAL = 16;
    private static final boolean DEFAULT_SPIN = false;

    protected final EventloopType type;
    Supplier<Scheduler> schedulerSupplier = NopScheduler::new;
    Supplier<String> threadNameSupplier;
    ThreadAffinity threadAffinity = ThreadAffinity.newSystemThreadAffinity("hazelcast.tpc.eventloop.affinity");

    ThreadFactory threadFactory = Thread::new;
    boolean spin;
    int localTaskQueueCapacity;
    int concurrentTaskQueueCapacity;
    int scheduledTaskQueueCapacity;
    int batchSize;
    int clockRefreshPeriod;

    protected EventloopBuilder(EventloopType type) {
        this.type = type;
        this.localTaskQueueCapacity = Integer.getInteger(NAME_LOCAL_TASK_QUEUE_CAPACITY, DEFAULT_LOCAL_QUEUE_CAPACITY);
        this.concurrentTaskQueueCapacity = Integer.getInteger(NAME_CONCURRENT_TASK_QUEUE_CAPACITY, DEFAULT_CONCURRENT_QUEUE_CAPACITY);
        this.scheduledTaskQueueCapacity = Integer.getInteger(NAME_SCHEDULED_TASK_QUEUE_CAPACITY, DEFAULT_SCHEDULED_TASK_QUEUE_CAPACITY);
        this.batchSize = Integer.getInteger(NAME_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.clockRefreshPeriod = Integer.getInteger(NAME_CLOCK_REFRESH_PERIOD, DEFAULT_CLOCK_REFRESH_INTERVAL);
        this.spin = Boolean.parseBoolean(getProperty(NAME_EVENTLOOP_SPIN, "" + DEFAULT_SPIN));
    }

    /**
     * Creates an Eventloop based on the configuration of this {@link EventloopBuilder}.
     *
     * @return the created Eventloop.
     */
    public abstract Eventloop create();

    /**
     * Sets the clock refresh period.
     *
     * @param clockRefreshPeriod the period to refresh the time. A clockRefreshPeriod of 0 means that always the newest
     *                           time is obtained. There will be more overhead, but you get better granularity.
     * @throws IllegalArgumentException when clockRefreshPeriod smaller than 0.
     */
    public void setClockRefreshPeriod(int clockRefreshPeriod) {
        this.clockRefreshPeriod = checkNotNegative(clockRefreshPeriod, "clockRefreshInterval");
    }

    /**
     * Sets the ThreadFactory used to create the Thread that runs the {@link Eventloop}.
     *
     * @param threadFactory the ThreadFactory
     * @throws NullPointerException if threadFactory is null.
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = checkNotNull(threadFactory, "threadFactory");
    }

    /**
     * An eventloop has multiple queues to process. This setting controls the number of items that are
     * processed from a single queue in batch, before moving to the next queue.
     * <p>
     * Setting it to a lower value will improve fairness but can reduce throughput. Setting it to a very high
     * value could in theory lead to certain queues or event sources not being processed at all. So imagine some
     * local task that rescheduled itself, then it could happen that with a very high batch size this tasks is
     * processed in a loop while none of the other queues/event-sources is checked and hence they are being starved
     * from CPU time.
     *
     * @param batchSize
     * @throws IllegalArgumentException if batchSize smaller than 1.
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = checkPositive(batchSize, "batchSize");
    }

    /**
     * Sets the supplier for the thread name. If configured, the thread name is set
     * after the thread is created.
     * <p/>
     * If null, there is no thread name supplier and the thread name will not be modified.
     *
     * @param threadNameSupplier the supplier for the thread name.
     */
    public void setThreadNameSupplier(Supplier<String> threadNameSupplier) {
        this.threadNameSupplier = threadNameSupplier;
    }

    /**
     * Sets the {@link ThreadAffinity}. If the threadAffinity is null, no thread affinity
     * is applied.
     *
     * @param threadAffinity the ThreadAffinity.
     */
    public void setThreadAffinity(ThreadAffinity threadAffinity) {
        this.threadAffinity = threadAffinity;
    }

    /**
     * Sets the capacity of the local run queue.
     *
     * @param localTaskQueueCapacity the capacity
     * @throws IllegalArgumentException if localRunQueueCapacity not positive.
     */
    public void setLocalTaskQueueCapacity(int localTaskQueueCapacity) {
        this.localTaskQueueCapacity = checkPositive(localTaskQueueCapacity, "localRunQueueCapacity");
    }

    /**
     * Sets the capacity of the scheduled task queue.
     *
     * @param concurrentTaskQueueCapacity the capacity
     * @throws IllegalArgumentException if scheduledTaskQueueCapacity not positive.
     */
    public void setConcurrentTaskQueueCapacity(int concurrentTaskQueueCapacity) {
        this.concurrentTaskQueueCapacity = checkPositive(concurrentTaskQueueCapacity, "concurrentRunQueueCapacity");
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

    public final void setSpin(boolean spin) {
        this.spin = spin;
    }

    public final void setSchedulerSupplier(Supplier<Scheduler> schedulerSupplier) {
        this.schedulerSupplier = checkNotNull(schedulerSupplier);
    }
}
