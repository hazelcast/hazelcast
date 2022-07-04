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

package com.hazelcast.internal.eviction;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class is responsible for gradual cleanup of expired entries from
 * IMap and ICache. For this purpose it uses a background task. Gradual
 * cleanup is in place for IMap since {@code 3.3} and ICache since
 * {@code 3.11}
 */
@SuppressWarnings("checkstyle:linelength")
public final class ExpirationManager implements LifecycleListener, PartitionLostListener {

    private final int taskPeriodSeconds;
    private final UUID lifecycleListenerId;
    private final UUID partitionLostListenerId;
    private final NodeEngine nodeEngine;
    private final ClearExpiredRecordsTask task;
    private final TaskScheduler globalTaskScheduler;
    private final LifecycleService lifecycleService;
    private final PartitionService partitionService;
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    /**
     * @see #rescheduleIfScheduledBefore()
     */
    private final AtomicBoolean scheduledOneTime = new AtomicBoolean(false);

    private volatile ScheduledFuture<?> scheduledExpirationTask;

    @SuppressWarnings("checkstyle:magicnumber")
    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public ExpirationManager(ClearExpiredRecordsTask task, NodeEngine nodeEngine) {
        this.task = task;
        this.nodeEngine = nodeEngine;
        this.globalTaskScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        this.taskPeriodSeconds = checkPositive("taskPeriodSeconds", task.getTaskPeriodSeconds());

        this.lifecycleService = getHazelcastInstance().getLifecycleService();
        this.lifecycleListenerId = lifecycleService.addLifecycleListener(this);
        this.partitionService = getHazelcastInstance().getPartitionService();
        this.partitionLostListenerId = partitionService.addPartitionLostListener(this);
    }

    protected HazelcastInstance getHazelcastInstance() {
        return this.nodeEngine.getHazelcastInstance();
    }

    /**
     * Starts scheduling of the task that clears expired entries.
     * Calling this method multiple times has same effect.
     */
    public void scheduleExpirationTask() {
        if (!task.isCleanupEnabled() || nodeEngine.getLocalMember().isLiteMember() || scheduled.get()
                || !scheduled.compareAndSet(false, true)) {
            return;
        }

        scheduledExpirationTask =
                globalTaskScheduler.scheduleWithRepetition(task, taskPeriodSeconds,
                        taskPeriodSeconds, SECONDS);

        scheduledOneTime.set(true);
    }

    /**
     * Ends scheduling of the task that clears expired entries.
     * Calling this method multiple times has same effect.
     */
    void unscheduleExpirationTask() {
        scheduled.set(false);
        ScheduledFuture<?> scheduledFuture = this.scheduledExpirationTask;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        switch (event.getState()) {
            case SHUTTING_DOWN:
            case MERGING:
                unscheduleExpirationTask();
                sendQueuedExpiredKeys();
                break;
            case MERGED:
                rescheduleIfScheduledBefore();
                break;
            default:
                return;
        }
    }

    private void sendQueuedExpiredKeys() {
        for (Object container : task.containers) {
            task.sendQueuedExpiredKeys(container);
        }
    }

    @Override
    public void partitionLost(PartitionLostEvent event) {
        task.partitionLost(event);
    }

    public void onClusterStateChange(ClusterState newState) {
        if (newState == ClusterState.PASSIVE) {
            unscheduleExpirationTask();
        } else {
            rescheduleIfScheduledBefore();
        }
    }

    /**
     * Called upon shutdown of {@link com.hazelcast.map.impl.MapService}
     */
    public void onShutdown() {
        lifecycleService.removeLifecycleListener(lifecycleListenerId);
        partitionService.removePartitionLostListener(partitionLostListenerId);
    }

    public ClearExpiredRecordsTask getTask() {
        return task;
    }

    /**
     * Re-schedules {@link ClearExpiredRecordsTask}, if it has been scheduled at least one time before.
     * This info is important for the methods: {@link #stateChanged(LifecycleEvent)}
     * and {@link #onClusterStateChange(ClusterState)}. Because even if we call these methods, it is still
     * possible that the {@link ClearExpiredRecordsTask} has not been scheduled before and in this method we
     * prevent unnecessary scheduling of it.
     */
    private void rescheduleIfScheduledBefore() {
        if (!scheduledOneTime.get()) {
            return;
        }

        scheduleExpirationTask();
    }

    // only used for testing purposes
    int getTaskPeriodSeconds() {
        return taskPeriodSeconds;
    }

    // only used for testing purposes
    int getCleanupOperationCount() {
        return this.task.getCleanupOperationCount();
    }

    // only used for testing purposes
    int getCleanupPercentage() {
        return this.task.getCleanupPercentage();
    }

    // only used for testing purposes
    boolean isScheduled() {
        return scheduled.get();
    }
}
