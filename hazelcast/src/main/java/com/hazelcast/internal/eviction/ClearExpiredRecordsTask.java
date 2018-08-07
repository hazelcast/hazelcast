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

package com.hazelcast.internal.eviction;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Integer.parseInt;
import static java.lang.Math.min;

@SuppressWarnings("checkstyle:magicnumber")
@SuppressFBWarnings({"URF_UNREAD_FIELD"})
public abstract class ClearExpiredRecordsTask<T> implements Runnable {

    public static final int DIFFERENCE_BETWEEN_TWO_SUBSEQUENT_PARTITION_CLEANUP_MILLIS = 1000;
    protected final int cleanupOperationCount;
    protected final int cleanupPercentage;
    protected final int taskPeriodSeconds;
    protected final T[] containers;
    protected NodeEngine nodeEngine;
    protected InternalOperationService operationService;

    volatile long lastStartMillis;
    volatile long lastEndMillis;

    private AtomicBoolean singleRunPermit = new AtomicBoolean(false);

    private Address thisAddress;
    private int partitionCount;
    private IPartitionService partitionService;
    private HazelcastProperties properties;

    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public ClearExpiredRecordsTask(NodeEngine nodeEngine, T[] containers, HazelcastProperty cleanupOpProperty,
                                   HazelcastProperty cleanupPercentageProperty, HazelcastProperty taskPeriodProperty) {
        this.properties = nodeEngine.getProperties();
        this.containers = containers;
        this.nodeEngine = nodeEngine;
        this.operationService = (InternalOperationService) nodeEngine.getOperationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.thisAddress = nodeEngine.getThisAddress();
        this.cleanupOperationCount = calculateCleanupOperationCount(properties, cleanupOpProperty, partitionCount,
                operationService.getPartitionThreadCount());
        checkPositive(cleanupOperationCount, "cleanupOperationCount should be a positive number");
        this.cleanupPercentage = properties.getInteger(cleanupPercentageProperty);

        checkTrue(cleanupPercentage > 0 && cleanupPercentage <= 100,
                "cleanupPercentage should be in range (0,100]");
        this.taskPeriodSeconds = nodeEngine.getProperties().getSeconds(taskPeriodProperty);
    }

    @Override
    public void run() {
        try {
            if (!singleRunPermit.compareAndSet(false, true)) {
                return;
            }

            lastStartMillis = System.currentTimeMillis();

            runInternal();

            lastEndMillis = System.currentTimeMillis();

        } finally {
            singleRunPermit.set(false);
        }
    }

    private void runInternal() {
        final long now = Clock.currentTimeMillis();
        int inFlightCleanupOperationsCount = 0;

        List<T> containersToProcess = null;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            IPartition partition = partitionService.getPartition(partitionId, false);
            T container = this.containers[partitionId];

            if (partition.isOwnerOrBackup(thisAddress)) {

                if (isContainerEmpty(container) && !hasExpiredKeyToSendBackup(container)) {
                    continue;
                }

                if (hasRunningCleanup(container)) {
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

    private List<T> addContainerTo(T container, List<T> containersToProcess) {
        if (containersToProcess == null) {
            containersToProcess = new ArrayList<T>();
        }

        containersToProcess.add(container);

        return containersToProcess;
    }

    protected void sendCleanupOperations(List<T> partitionContainers) {
        final int start = 0;
        int end = cleanupOperationCount;
        if (end > partitionContainers.size()) {
            end = partitionContainers.size();
        }
        List<T> partitionIds = partitionContainers.subList(start, end);
        for (T container : partitionIds) {
            // mark partition container as has on going expiration operation.
            setHasRunningCleanup(container, true);
            Operation operation = createExpirationOperation(cleanupPercentage, container);
            operationService.execute(operation);
        }
    }

    // only used for testing purposes
    int getCleanupPercentage() {
        return cleanupPercentage;
    }

    int getTaskPeriodSeconds() {
        return taskPeriodSeconds;
    }

    public int getCleanupOperationCount() {
        return cleanupOperationCount;
    }

    protected abstract boolean hasExpiredKeyToSendBackup(T container);

    protected abstract boolean isContainerEmpty(T container);

    protected abstract boolean hasRunningCleanup(T container);

    protected abstract void setHasRunningCleanup(T container, boolean status);

    protected abstract boolean notHaveAnyExpirableRecord(T container);

    protected abstract long getLastCleanupTime(T container);

    protected abstract void clearLeftoverExpiredKeyQueues(T container);

    protected abstract void sortPartitionContainers(List<T> containers);

    protected abstract Operation createExpirationOperation(int cleanupPercentage, T container);
}
