/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.operation.ClearExpiredOperation;
import com.hazelcast.map.impl.operation.EvictBatchBackupOperation;
import com.hazelcast.map.impl.recordstore.ExpiredKey;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Integer.parseInt;
import static java.lang.Math.min;
import static java.util.Collections.sort;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class is responsible for gradual cleanup of expired entries. For this purpose it uses a background task.
 * <p>
 * This background task can be accelerated or can be slowed down by using the system properties below:
 * <p>
 * <ul>
 * <li>
 * {@value com.hazelcast.map.impl.eviction.ExpirationManager#PROP_TASK_PERIOD_SECONDS}: A new round is
 * started after this period of seconds.
 * Default value is {@value com.hazelcast.map.impl.eviction.ExpirationManager#DEFAULT_TASK_PERIOD_SECONDS}
 * seconds. So every {@value com.hazelcast.map.impl.eviction.ExpirationManager#DEFAULT_TASK_PERIOD_SECONDS}
 * seconds there will be a new round.
 * </li>
 * <li>
 * {@value com.hazelcast.map.impl.eviction.ExpirationManager#PROP_CLEANUP_PERCENTAGE}: Scannable percentage
 * of entries in a maps' partition in each round.
 * Default percentage is {@value com.hazelcast.map.impl.eviction.ExpirationManager#DEFAULT_CLEANUP_PERCENTAGE}%.
 * </li>
 * <li>
 * {@value com.hazelcast.map.impl.eviction.ExpirationManager#PROP_CLEANUP_OPERATION_COUNT}: Number of
 * scannable partitions in each round. No default value exists. Dynamically calculated against partition-count or
 * partition-thread-count.
 * </li>
 * <li>
 * {@value com.hazelcast.map.impl.eviction.ExpirationManager#PROP_PRIMARY_DRIVES_BACKUP}: Used to enable/disable
 * management of backup expiration from primary. This can only be used with max idle seconds expiration.
 * </li>
 * </ul>
 * <p>
 * These parameters can be set node-wide or system-wide
 * <p>
 * Node-wide setting example:
 * <pre>
 *          Config config = new Config();
 *          config.setProperty(
 *          {@value com.hazelcast.map.impl.eviction.ExpirationManager#PROP_CLEANUP_OPERATION_COUNT}, "3");
 *          Hazelcast.newHazelcastInstance(config);
 *      </pre>
 * </p>
 * <p>
 * System-wide setting example:
 * <pre>
 *       System.setProperty(
 *       {@value com.hazelcast.map.impl.eviction.ExpirationManager#PROP_CLEANUP_OPERATION_COUNT}, "3");
 *   </pre>
 * </p>
 *
 * @since 3.3
 */
@SuppressWarnings("checkstyle:linelength")
public final class ExpirationManager implements OperationResponseHandler, LifecycleListener {

    public static final String PROP_PRIMARY_DRIVES_BACKUP = "hazelcast.internal.map.expiration.primary.drives_backup";
    public static final String PROP_TASK_PERIOD_SECONDS = "hazelcast.internal.map.expiration.task.period.seconds";
    public static final String PROP_CLEANUP_PERCENTAGE = "hazelcast.internal.map.expiration.cleanup.percentage";
    public static final String PROP_CLEANUP_OPERATION_COUNT = "hazelcast.internal.map.expiration.cleanup.operation.count";

    public static final boolean DEFAULT_PRIMARY_DRIVES_BACKUP = true;
    public static final int DEFAULT_TASK_PERIOD_SECONDS = 5;
    public static final int DEFAULT_CLEANUP_PERCENTAGE = 10;
    public static final int MAX_EXPIRED_KEY_COUNT_IN_BATCH = 100;
    public static final int DIFFERENCE_BETWEEN_TWO_SUBSEQUENT_PARTITION_CLEANUP_MILLIS = 1000;

    public static final HazelcastProperty PRIMARY_DRIVES_BACKUP
            = new HazelcastProperty(PROP_PRIMARY_DRIVES_BACKUP, DEFAULT_PRIMARY_DRIVES_BACKUP);
    public static final HazelcastProperty TASK_PERIOD_SECONDS
            = new HazelcastProperty(PROP_TASK_PERIOD_SECONDS, DEFAULT_TASK_PERIOD_SECONDS, SECONDS);
    public static final HazelcastProperty CLEANUP_PERCENTAGE
            = new HazelcastProperty(PROP_CLEANUP_PERCENTAGE, DEFAULT_CLEANUP_PERCENTAGE);
    public static final HazelcastProperty CLEANUP_OPERATION_COUNT
            = new HazelcastProperty(PROP_CLEANUP_OPERATION_COUNT);

    private final boolean primaryDrivesEviction;
    private final int taskPeriodSeconds;
    private final int partitionCount;
    private final int cleanupPercentage;
    private final int cleanupOperationCount;
    private final Address thisAddress;
    private final NodeEngine nodeEngine;
    private final HazelcastProperties properties;
    private final TaskScheduler globalTaskScheduler;
    private final IPartitionService partitionService;
    private final PartitionContainer[] partitionContainers;
    private final InternalOperationService operationService;
    /**
     * @see #rescheduleIfScheduledBefore()
     */
    private final AtomicBoolean scheduledOneTime = new AtomicBoolean(false);
    /**
     * Used to ensure no concurrent run of {@link ClearExpiredRecordsTask#run()} exists
     */
    private final AtomicBoolean singleRunPermit = new AtomicBoolean(false);
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private final ClearExpiredRecordsTask task = new ClearExpiredRecordsTask();

    private volatile ScheduledFuture<?> expirationTask;

    @SuppressWarnings("checkstyle:magicnumber")
    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public ExpirationManager(PartitionContainer[] partitionContainers, NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.partitionContainers = partitionContainers;
        this.thisAddress = nodeEngine.getThisAddress();
        this.partitionService = nodeEngine.getPartitionService();
        this.globalTaskScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        this.operationService = (InternalOperationService) nodeEngine.getOperationService();
        this.partitionCount = partitionService.getPartitionCount();
        this.properties = nodeEngine.getProperties();
        this.taskPeriodSeconds = properties.getSeconds(TASK_PERIOD_SECONDS);
        checkPositive(taskPeriodSeconds, "taskPeriodSeconds should be a positive number");
        this.cleanupPercentage = properties.getInteger(CLEANUP_PERCENTAGE);
        checkTrue(cleanupPercentage > 0 && cleanupPercentage <= 100,
                "cleanupPercentage should be in range (0,100]");
        this.cleanupOperationCount
                = calculateCleanupOperationCount(properties, partitionCount, operationService.getPartitionThreadCount());
        checkPositive(cleanupOperationCount, "cleanupOperationCount should be a positive number");
        this.primaryDrivesEviction = properties.getBoolean(PRIMARY_DRIVES_BACKUP);
        this.nodeEngine.getHazelcastInstance().getLifecycleService().addLifecycleListener(this);
    }

    /**
     * Starts scheduling of the task that clears expired entries.
     * Calling this method multiple times has same effect.
     */
    public void scheduleExpirationTask() {
        if (scheduled.get() || !scheduled.compareAndSet(false, true)) {
            return;
        }

        expirationTask = globalTaskScheduler.scheduleWithRepetition(task, taskPeriodSeconds,
                taskPeriodSeconds, SECONDS);
        scheduledOneTime.set(true);
    }

    /**
     * Ends scheduling of the task that clears expired entries.
     * Calling this method multiple times has same effect.
     */
    void unscheduleExpirationTask() {
        scheduled.set(false);
        ScheduledFuture<?> scheduledFuture = this.expirationTask;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        switch (event.getState()) {
            case SHUTTING_DOWN:
            case MERGING:
                unscheduleExpirationTask();
                break;
            case MERGED:
                rescheduleIfScheduledBefore();
                break;
            default:
                return;
        }
    }

    public void onClusterStateChange(ClusterState newState) {
        if (newState == ClusterState.PASSIVE) {
            unscheduleExpirationTask();
        } else {
            rescheduleIfScheduledBefore();
        }
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

    private static int calculateCleanupOperationCount(HazelcastProperties properties, int partitionCount, int partitionThreadCount) {
        String stringValue = properties.getString(CLEANUP_OPERATION_COUNT);
        if (stringValue != null) {
            return parseInt(stringValue);
        }

        // calculate operation count to be sent by using partition-count.
        final double scanPercentage = 0.1D;
        final int opCountFromPartitionCount = (int) (partitionCount * scanPercentage);

        // calculate operation count to be sent by using partition-thread-count.
        final int inflationFactor = 3;
        int opCountFromThreadCount = partitionThreadCount * inflationFactor;

        if (opCountFromPartitionCount == 0) {
            return opCountFromThreadCount;
        }

        return min(opCountFromPartitionCount, opCountFromThreadCount);
    }

    /**
     * Periodically clears expired entries (TTL & idle).
     * This task provides per partition expiration operation logic. (not per map, not per record store).
     * Fires cleanup operations at most partition operation thread count or some factor of it in one round.
     */
    private class ClearExpiredRecordsTask implements Runnable {

        private final Comparator<PartitionContainer> partitionContainerComparator = new Comparator<PartitionContainer>() {
            @Override
            public int compare(PartitionContainer o1, PartitionContainer o2) {
                final long s1 = o1.getLastCleanupTimeCopy();
                final long s2 = o2.getLastCleanupTimeCopy();
                return (s1 < s2) ? -1 : ((s1 == s2) ? 0 : 1);
            }
        };

        @Override
        public void run() {
            try {
                if (!singleRunPermit.compareAndSet(false, true)) {
                    return;
                }

                runInternal();

            } finally {
                singleRunPermit.set(false);
            }
        }

        private void runInternal() {
            final long now = Clock.currentTimeMillis();
            int inFlightCleanupOperationsCount = 0;

            List<PartitionContainer> containersToProcess = null;
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                IPartition partition = partitionService.getPartition(partitionId, false);
                PartitionContainer container = ExpirationManager.this.partitionContainers[partitionId];

                if (partition.isOwnerOrBackup(thisAddress)) {
                    // ttl expiration on backups is still done by this loop. There is no separate operation
                    // as in idle expiration case.

                    if (isContainerEmpty(container)
                            && !hasExpiredKeyToSendBackup(container)) {
                        continue;
                    }

                    if (container.hasRunningCleanup()) {
                        inFlightCleanupOperationsCount++;
                        continue;
                    }

                    if (inFlightCleanupOperationsCount > cleanupOperationCount
                            || notInProcessableTimeWindow(container, now)
                            || notHaveAnyExpirableRecord(container)) {
                        continue;
                    }

                    containersToProcess = addContainerTo(container, containersToProcess);

                    if (!partition.isLocal()) {
                        clearLeftoverExpiredKeyQueues(container);
                    }
                }
            }

            if (isEmpty(containersToProcess)) {
                return;
            }

            sortPartitionContainers(containersToProcess);
            sendCleanupOperations(containersToProcess);
        }

        private List<PartitionContainer> addContainerTo(PartitionContainer container,
                                                        List<PartitionContainer> containersToProcess) {
            if (containersToProcess == null) {
                containersToProcess = new ArrayList<PartitionContainer>();
            }

            containersToProcess.add(container);

            return containersToProcess;
        }

        private void sortPartitionContainers(List<PartitionContainer> partitionContainers) {
            updateLastCleanupTimesBeforeSorting(partitionContainers);
            sort(partitionContainers, partitionContainerComparator);
        }

        private void sendCleanupOperations(List<PartitionContainer> partitionContainers) {
            final int start = 0;
            int end = cleanupOperationCount;
            if (end > partitionContainers.size()) {
                end = partitionContainers.size();
            }
            List<PartitionContainer> partitionIds = partitionContainers.subList(start, end);
            for (PartitionContainer container : partitionIds) {
                // mark partition container as has on going expiration operation.
                container.setHasRunningCleanup(true);
                Operation operation = createExpirationOperation(cleanupPercentage, container.getPartitionId());
                operationService.execute(operation);
            }
        }

        private boolean notInProcessableTimeWindow(PartitionContainer partitionContainer, long now) {
            return now - partitionContainer.getLastCleanupTime() < DIFFERENCE_BETWEEN_TWO_SUBSEQUENT_PARTITION_CLEANUP_MILLIS;
        }

        private boolean isContainerEmpty(PartitionContainer container) {
            long size = 0L;
            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            for (RecordStore store : maps.values()) {
                size += store.size();
                if (size > 0L) {
                    return false;
                }
            }
            return true;
        }

        private boolean hasExpiredKeyToSendBackup(PartitionContainer container) {
            long size = 0L;
            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            for (RecordStore store : maps.values()) {
                size += store.getExpiredKeys().size();
                if (size > 0L) {
                    return true;
                }
            }
            return false;
        }

        /**
         * This can happen due to the partition ownership changes.
         */
        private void clearLeftoverExpiredKeyQueues(PartitionContainer container) {
            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            for (RecordStore store : maps.values()) {
                InvalidationQueue expiredKeys = store.getExpiredKeys();
                for (; ; ) {
                    if (expiredKeys.poll() == null) {
                        break;
                    }
                }
            }
        }

        /**
         * Here we check if that partition has any expirable record or not,
         * if no expirable record exists in that partition no need to fire an expiration operation.
         *
         * @param partitionContainer corresponding partition container.
         * @return <code>true</code> if no expirable record in that partition <code>false</code> otherwise.
         */
        private boolean notHaveAnyExpirableRecord(PartitionContainer partitionContainer) {
            boolean notExist = true;
            final ConcurrentMap<String, RecordStore> maps = partitionContainer.getMaps();
            for (RecordStore store : maps.values()) {
                if (store.isExpirable()) {
                    notExist = false;
                    break;
                }
            }
            return notExist;
        }

        @Override
        public String toString() {
            return ClearExpiredRecordsTask.class.getName();
        }

    }

    /**
     * Sets last clean-up time before sorting.
     *
     * @param partitionContainers partition containers.
     */
    private void updateLastCleanupTimesBeforeSorting(List<PartitionContainer> partitionContainers) {
        for (PartitionContainer partitionContainer : partitionContainers) {
            partitionContainer.setLastCleanupTimeCopy(partitionContainer.getLastCleanupTime());
        }
    }

    private Operation createExpirationOperation(int expirationPercentage, int partitionId) {
        return new ClearExpiredOperation(expirationPercentage)
                .setNodeEngine(nodeEngine)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setPartitionId(partitionId)
                .setValidateTarget(false)
                .setServiceName(SERVICE_NAME)
                .setOperationResponseHandler(this);
    }

    @Override
    public void sendResponse(Operation op, Object response) {
        if (canPrimaryDriveExpiration()) {
            PartitionContainer partitionContainer = partitionContainers[op.getPartitionId()];
            doBackupExpiration(partitionContainer);
        }
    }

    public boolean canPrimaryDriveExpiration() {
        return primaryDrivesEviction;
    }

    private void doBackupExpiration(PartitionContainer container) {
        ConcurrentMap<String, RecordStore> maps = container.getMaps();
        for (RecordStore recordStore : maps.values()) {
            sendExpiredKeysToBackups(recordStore, false);
        }
    }

    public void sendExpiredKeysToBackups(RecordStore recordStore, boolean checkIfReachedBatch) {
        InvalidationQueue<ExpiredKey> invalidationQueue = recordStore.getExpiredKeys();

        int size = invalidationQueue.size();
        if (size == 0 || checkIfReachedBatch && size < MAX_EXPIRED_KEY_COUNT_IN_BATCH) {
            return;
        }

        if (!invalidationQueue.tryAcquire()) {
            return;
        }

        Collection<ExpiredKey> expiredKeys;
        try {
            expiredKeys = pollExpiredKeys(invalidationQueue);
        } finally {
            invalidationQueue.release();
        }

        if (expiredKeys.size() == 0) {
            return;
        }

        // send expired keys to all backups
        OperationService operationService = nodeEngine.getOperationService();
        int backupReplicaCount = recordStore.getMapContainer().getTotalBackupCount();
        for (int replicaIndex = 1; replicaIndex < backupReplicaCount + 1; replicaIndex++) {
            if (hasReplicaAddress(recordStore.getPartitionId(), replicaIndex)) {
                Operation operation = new EvictBatchBackupOperation(recordStore.getName(), expiredKeys, recordStore.size());
                operationService.createInvocationBuilder(MapService.SERVICE_NAME, operation, recordStore.getPartitionId())
                        .setReplicaIndex(replicaIndex).invoke();
            }
        }
    }

    private boolean hasReplicaAddress(int partitionId, int replicaIndex) {
        return partitionService.getPartition(partitionId).getReplicaAddress(replicaIndex) != null;
    }

    private static Collection<ExpiredKey> pollExpiredKeys(Queue<ExpiredKey> expiredKeys) {
        Collection<ExpiredKey> polledKeys = new ArrayList<ExpiredKey>(expiredKeys.size());

        do {
            ExpiredKey expiredKey = expiredKeys.poll();
            if (expiredKey == null) {
                break;
            }
            polledKeys.add(expiredKey);
        } while (true);

        return polledKeys;
    }

    // only used for testing purposes
    int getTaskPeriodSeconds() {
        return taskPeriodSeconds;
    }

    // only used for testing purposes
    boolean getPrimaryDrivesEviction() {
        return primaryDrivesEviction;
    }

    // only used for testing purposes
    int getCleanupPercentage() {
        return cleanupPercentage;
    }

    // only used for testing purposes
    int getCleanupOperationCount() {
        return cleanupOperationCount;
    }

    // only used for testing purposes
    boolean isScheduled() {
        return scheduled.get();
    }

}
