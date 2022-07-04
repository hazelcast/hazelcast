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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static com.hazelcast.internal.eviction.ToBackupSender.newToBackupSender;
import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.lang.Integer.parseInt;
import static java.lang.Math.min;

@SuppressWarnings({"checkstyle:magicnumber", "checkstyle:methodcount"})
@SuppressFBWarnings({"URF_UNREAD_FIELD"})
public abstract class ClearExpiredRecordsTask<T, S> implements Runnable {

    private static final int DIFFERENCE_BETWEEN_TWO_SUBSEQUENT_PARTITION_CLEANUP_MILLIS = 1000;

    protected final T[] containers;
    protected final NodeEngine nodeEngine;
    protected final ToBackupSender<S> toBackupSender;
    protected final IPartitionService partitionService;

    private final boolean cleanupEnabled;
    private final int partitionCount;
    private final int taskPeriodSeconds;
    private final int cleanupPercentage;
    private final int cleanupOperationCount;

    private final Address thisAddress;
    private final OperationServiceImpl operationService;
    private final AtomicBoolean singleRunPermit = new AtomicBoolean(false);
    private final AtomicInteger lostPartitionCounter = new AtomicInteger();
    private final AtomicInteger nextExpiryQueueToScanIndex = new AtomicInteger();

    private volatile int lastKnownLostPartitionCount;

    private int runningCleanupOperationsCount;

    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    protected ClearExpiredRecordsTask(String serviceName,
                                      T[] containers,
                                      HazelcastProperty cleanupEnabled,
                                      HazelcastProperty cleanupOpProperty,
                                      HazelcastProperty cleanupPercentageProperty,
                                      HazelcastProperty taskPeriodProperty,
                                      NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.containers = containers;
        this.operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.thisAddress = nodeEngine.getThisAddress();
        HazelcastProperties properties = nodeEngine.getProperties();
        this.cleanupOperationCount = calculateCleanupOperationCount(properties, cleanupOpProperty, partitionCount,
                operationService.getPartitionThreadCount());
        checkPositive(cleanupOperationCount, "cleanupOperationCount should be a positive number");
        this.cleanupPercentage = properties.getInteger(cleanupPercentageProperty);

        checkTrue(cleanupPercentage > 0 && cleanupPercentage <= 100,
                "cleanupPercentage should be in range (0,100]");
        this.taskPeriodSeconds = properties.getSeconds(taskPeriodProperty);
        this.cleanupEnabled = properties.getBoolean(cleanupEnabled);
        this.toBackupSender = newToBackupSender(serviceName, newBackupExpiryOpSupplier(),
                newBackupExpiryOpFilter(), nodeEngine);
    }

    protected BiFunction<Integer, Integer, Boolean> newBackupExpiryOpFilter() {
        return (partitionId, replicaIndex) -> {
            IPartition partition = partitionService.getPartition(partitionId);
            return partition.getReplicaAddress(replicaIndex) != null;
        };
    }

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
        runningCleanupOperationsCount = 0;

        long nowInMillis = nowInMillis();
        boolean lostPartitionDetected = lostPartitionDetected();

        List<T> containersToProcess = null;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            T container = this.containers[partitionId];

            IPartition partition = partitionService.getPartition(partitionId, false);
            if (partition.isMigrating()) {
                continue;
            }

            if (partition.isLocal()) {
                if (lostPartitionDetected) {
                    equalizeBackupSizeWithPrimary(container);
                }
            }

            if (canProcessContainer(container, partition, nowInMillis)) {
                containersToProcess = addContainerTo(containersToProcess, container);
            }
        }

        if (!isEmpty(containersToProcess)) {
            sortPartitionContainers(containersToProcess);
            sendCleanupOperations(containersToProcess);
        }

        sendExpiryQueuesToBackupIncrementally();
    }

    private void sendExpiryQueuesToBackupIncrementally() {
        int scanned = 0;
        for (int partitionId = nextExpiryQueueToScanIndex.get(); partitionId < partitionCount; partitionId++) {
            sendQueuedExpiredKeys(containers[partitionId]);

            nextExpiryQueueToScanIndex.incrementAndGet();
            if (++scanned % cleanupOperationCount == 0) {
                break;
            }
        }

        if (nextExpiryQueueToScanIndex.get() == partitionCount) {
            nextExpiryQueueToScanIndex.set(0);
        }
    }

    private boolean canProcessContainer(T container, IPartition partition, long nowInMillis) {
        if (!getProcessablePartitionType().isProcessable(partition, thisAddress)) {
            return false;
        }

        if (isContainerEmpty(container) && !hasExpiredKeyToSendBackup(container)) {
            return false;
        }

        if (hasRunningCleanup(container)) {
            runningCleanupOperationsCount++;
            return false;
        }

        return runningCleanupOperationsCount <= cleanupOperationCount
                && !notInProcessableTimeWindow(container, nowInMillis)
                && !notHaveAnyExpirableRecord(container);
    }

    /**
     * This method increments a counter to count partition lost events.
     *
     * After an ungraceful shutdown, backups can have expired entries.
     * And these entries can remain forever on them. Reason for this is,
     * the lost invalidations on a primary partition. During ungraceful
     * shutdown, these invalidations can be lost before sending them to
     * backups.
     *
     * Here, the counter in this method, is used to detect the lost
     * invalidations case. If it is detected, we send expiry operations to
     * remove leftover backup entries. Otherwise leftover entries can remain on
     * backups forever.
     */
    public final void partitionLost(PartitionLostEvent ignored) {
        lostPartitionCounter.incrementAndGet();
    }

    private static long nowInMillis() {
        return Clock.currentTimeMillis();
    }

    /**
     * see {@link #partitionLost}
     */
    private boolean lostPartitionDetected() {
        int currentLostPartitionCount = lostPartitionCounter.get();
        if (currentLostPartitionCount == lastKnownLostPartitionCount) {
            return false;
        }

        lastKnownLostPartitionCount = currentLostPartitionCount;
        return true;
    }

    private static int calculateCleanupOperationCount(HazelcastProperties properties,
                                                      final HazelcastProperty cleanupOpCountProperty,
                                                      int partitionCount, int partitionThreadCount) {
        String stringValue = properties.getString(cleanupOpCountProperty);
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

    private boolean notInProcessableTimeWindow(T container, long now) {
        return now - getLastCleanupTime(container) < DIFFERENCE_BETWEEN_TWO_SUBSEQUENT_PARTITION_CLEANUP_MILLIS;
    }

    private List<T> addContainerTo(List<T> containersToProcess, T container) {
        if (containersToProcess == null) {
            containersToProcess = new LinkedList<>();
        }

        containersToProcess.add(container);

        return containersToProcess;
    }

    private void sendCleanupOperations(List<T> partitionContainers) {
        final int start = 0;
        int end = cleanupOperationCount;
        if (end > partitionContainers.size()) {
            end = partitionContainers.size();
        }
        List<T> partitionIds = partitionContainers.subList(start, end);
        for (T container : partitionIds) {
            // mark partition container as has on going expiration operation.
            setHasRunningCleanup(container);
            Operation operation = newPrimaryExpiryOp(cleanupPercentage, container);
            operationService.execute(operation);
        }
    }

    private BiFunction<S, Collection<ExpiredKey>, Operation> newBackupExpiryOpSupplier() {
        return (recordStore, expiredKeys) -> newBackupExpiryOp(recordStore, expiredKeys);
    }

    public final void sendQueuedExpiredKeys(T container) {
        Iterator<S> storeIterator = storeIterator(container);
        while (storeIterator.hasNext()) {
            tryToSendBackupExpiryOp(storeIterator.next(), false);
        }
    }

    // only used for testing purposes
    int getCleanupPercentage() {
        return cleanupPercentage;
    }

    int getTaskPeriodSeconds() {
        return taskPeriodSeconds;
    }

    int getCleanupOperationCount() {
        return cleanupOperationCount;
    }

    protected abstract boolean isContainerEmpty(T container);

    protected abstract boolean hasRunningCleanup(T container);

    protected abstract long getLastCleanupTime(T container);

    protected abstract void equalizeBackupSizeWithPrimary(T container);

    protected abstract boolean hasExpiredKeyToSendBackup(T container);

    protected abstract boolean notHaveAnyExpirableRecord(T container);

    protected abstract void sortPartitionContainers(List<T> containers);

    protected abstract void setHasRunningCleanup(T container);

    protected abstract ProcessablePartitionType getProcessablePartitionType();

    protected abstract Operation newPrimaryExpiryOp(int cleanupPercentage, T container);

    protected abstract Operation newBackupExpiryOp(S store, Collection<ExpiredKey> expiredKeys);

    public abstract void tryToSendBackupExpiryOp(S store, boolean sendIfAtBatchSize);

    public abstract Iterator<S> storeIterator(T container);

    public boolean isCleanupEnabled() {
        return cleanupEnabled;
    }

    /**
     * Used when traversing partitions. Map needs to traverse both
     * backup and primary partitions due to catch ttl expired entries
     * but Cache only needs primary ones.
     */
    protected enum ProcessablePartitionType {
        PRIMARY_PARTITION {
            @Override
            boolean isProcessable(IPartition partition, Address address) {
                return partition.isLocal();
            }
        },

        PRIMARY_OR_BACKUP_PARTITION {
            @Override
            boolean isProcessable(IPartition partition, Address address) {
                return partition.isOwnerOrBackup(address);
            }
        };

        abstract boolean isProcessable(IPartition partition, Address address);
    }
}
