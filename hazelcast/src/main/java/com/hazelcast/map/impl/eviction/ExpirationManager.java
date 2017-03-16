/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.operation.ClearExpiredOperation;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Integer.parseInt;
import static java.lang.Math.min;
import static java.util.Collections.sort;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Responsible for gradual cleanup of expired entries due to the time-to-live and max-idle-seconds.
 * By using these system properties, one can accelerate or slow down background expiration process.
 * <li>
 * <ul>
 *     {@value SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS}: Background task runs in every this period seconds.
 *      Default is {@value DEFAULT_EXPIRATION_TASK_PERIOD_SECONDS}
 * </ul>
 * <ul>
 *     {@value SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE}: Scannable percentage of entries of a map partition in every run round.
 *      Default is {@value DEFAULT_EXPIRATION_CLEANUP_PERCENTAGE}%
 * </ul>
 * <ul>
 *     {@value SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT}: Number of scannable partitions in every run round.
 *      No default value exists. Dynamically calculated from partition and partition-thread counts
 * </ul>
 * </li>
 *
 * These parameters can be set node-wide via Config object or system-wide via system property
 * <p>
 *     Node-wide setting example:
 *      <pre>
 *          Config config = new Config();
 *          config.setProperty("{@value SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT}", "3");
 *          Hazelcast.newHazelcastInstance(config);
 *      </pre>
 * </p>
 * <p>
 *     System-wide setting example:
 *   <pre>
 *       System.setProperty("{@value SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT}", "3");
 *   </pre>
 * </p>
 *
 * @since 3.3
 */
public final class ExpirationManager {

    // These are `default` for testing purposes.
    static final String SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS = "hazelcast.internal.map.expiration.task.period.seconds";
    static final String SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE = "hazelcast.internal.map.expiration.cleanup.percentage";
    @SuppressWarnings("checkstyle:linelength")
    static final String SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT = "hazelcast.internal.map.expiration.cleanup.operation.count";

    private static final int DEFAULT_EXPIRATION_TASK_PERIOD_SECONDS = 5;
    private static final int DEFAULT_EXPIRATION_CLEANUP_PERCENTAGE = 10;
    private static final int DIFFERENCE_BETWEEN_TWO_SUBSEQUENT_PARTITION_CLEANUP_MILLIS = 1000;

    private final NodeEngine nodeEngine;
    private final PartitionContainer[] partitionContainers;
    private final Address thisAddress;
    private final IPartitionService partitionService;
    private final ExecutionService executionService;
    private final InternalOperationService operationService;
    private final int partitionCount;
    private final int taskPeriodSeconds;
    private final int cleanupPercentage;
    private final int cleanupOperationCount;

    private ScheduledFuture<?> expirationTask;

    @SuppressWarnings("checkstyle:magicnumber")
    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public ExpirationManager(PartitionContainer[] partitionContainers, NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.partitionContainers = partitionContainers;
        this.thisAddress = nodeEngine.getThisAddress();
        this.partitionService = nodeEngine.getPartitionService();
        this.executionService = nodeEngine.getExecutionService();
        this.operationService = (InternalOperationService) nodeEngine.getOperationService();
        this.partitionCount = partitionService.getPartitionCount();

        this.taskPeriodSeconds = getInteger(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, DEFAULT_EXPIRATION_TASK_PERIOD_SECONDS);
        checkPositive(taskPeriodSeconds, "taskPeriodSeconds should be a positive number");

        this.cleanupPercentage = getInteger(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, DEFAULT_EXPIRATION_CLEANUP_PERCENTAGE);
        checkTrue(cleanupPercentage > 0 && cleanupPercentage <= 100, "cleanupPercentage should be in range (0,100]");

        int defaultCleanupOpCount = calculateCleanupOperationCount(partitionCount, operationService.getPartitionThreadCount());
        this.cleanupOperationCount = getInteger(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT, defaultCleanupOpCount);
        checkPositive(cleanupOperationCount, "cleanupOperationCount should be a positive number");
    }

    public synchronized void start() {
        if (expirationTask != null) {
            return;
        }

        ClearExpiredRecordsTask task = new ClearExpiredRecordsTask();
        expirationTask = executionService.getGlobalTaskScheduler().
                scheduleWithRepetition(task, taskPeriodSeconds, taskPeriodSeconds, SECONDS);
    }

    public synchronized void stop() {
        if (expirationTask == null) {
            return;
        }

        expirationTask.cancel(true);
        expirationTask = null;
    }


    private int getInteger(String propertyName, int defaultValue) {
        Config config = nodeEngine.getConfig();
        String property = config.getProperty(propertyName);
        return property == null ? defaultValue : parseInt(property);
    }

    private static int calculateCleanupOperationCount(int partitionCount, int partitionThreadCount) {
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
     * Periodically clears expired entries.(ttl & idle)
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
            final long now = Clock.currentTimeMillis();
            int currentlyRunningCleanupOperationsCount = 0;

            List<PartitionContainer> partitionContainers = null;
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                IPartition partition = partitionService.getPartition(partitionId, false);

                if (partition.isOwnerOrBackup(thisAddress)) {
                    PartitionContainer partitionContainer = ExpirationManager.this.partitionContainers[partitionId];

                    if (isContainerEmpty(partitionContainer)) {
                        continue;
                    }

                    if (partitionContainer.hasRunningCleanup()) {
                        currentlyRunningCleanupOperationsCount++;
                        continue;
                    }

                    if (currentlyRunningCleanupOperationsCount > cleanupOperationCount
                            || notInProcessableTimeWindow(partitionContainer, now)
                            || notHaveAnyExpirableRecord(partitionContainer)) {
                        continue;
                    }

                    if (partitionContainers == null) {
                        partitionContainers = new ArrayList<PartitionContainer>();
                    }

                    partitionContainers.add(partitionContainer);
                }
            }

            if (isEmpty(partitionContainers)) {
                return;
            }

            sortPartitionContainers(partitionContainers);
            sendCleanupOperations(partitionContainers);
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
    }

    private Operation createExpirationOperation(int expirationPercentage, int partitionId) {
        return new ClearExpiredOperation(expirationPercentage)
                .setNodeEngine(nodeEngine)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setPartitionId(partitionId)
                .setValidateTarget(false)
                .setServiceName(SERVICE_NAME);
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

    // used for testing purposes
    int getTaskPeriodSeconds() {
        return taskPeriodSeconds;
    }

    // used for testing purposes
    int getCleanupPercentage() {
        return cleanupPercentage;
    }

    // used for testing purposes
    int getCleanupOperationCount() {
        return cleanupOperationCount;
    }
}
