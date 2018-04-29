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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.operation.HasOngoingMigration;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.util.Clock;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.internal.partition.impl.PartitionServiceState.FETCHING_PARTITION_TABLE;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.MIGRATION_LOCAL;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.MIGRATION_ON_MASTER;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.REPLICA_NOT_OWNED;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.REPLICA_NOT_SYNC;
import static com.hazelcast.internal.partition.impl.PartitionServiceState.SAFE;
import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;
import static java.lang.Thread.currentThread;

/**
 * Verifies up-to-dateness of each of partition replicas owned by this member.
 * Triggers replica sync process for out-of-date replicas.
 */
public class PartitionReplicaStateChecker {

    private static final int DEFAULT_PAUSE_MILLIS = 1000;
    private static final int REPLICA_SYNC_CHECK_TIMEOUT_SECONDS = 10;
    private static final int INVOCATION_TRY_COUNT = 10;
    private static final int INVOCATION_TRY_PAUSE_MILLIS = 100;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;

    private final PartitionStateManager partitionStateManager;
    private final MigrationManager migrationManager;

    PartitionReplicaStateChecker(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.nodeEngine = node.getNodeEngine();
        this.partitionService = partitionService;
        this.logger = node.getLogger(getClass());

        this.partitionStateManager = partitionService.getPartitionStateManager();
        this.migrationManager = partitionService.getMigrationManager();
    }

    public PartitionServiceState getPartitionServiceState() {
        if (partitionService.isFetchMostRecentPartitionTableTaskRequired()) {
            return FETCHING_PARTITION_TABLE;
        }

        if (hasMissingReplicaOwners()) {
            return REPLICA_NOT_OWNED;
        }

        if (migrationManager.hasOnGoingMigration()) {
            return MIGRATION_LOCAL;
        }

        if (!node.isMaster() && hasOnGoingMigrationMaster(Level.OFF)) {
            return MIGRATION_ON_MASTER;
        }

        if (!checkAndTriggerReplicaSync()) {
            return REPLICA_NOT_SYNC;
        }

        return SAFE;
    }

    public boolean triggerAndWaitForReplicaSync(long timeout, TimeUnit unit) {
        return triggerAndWaitForReplicaSync(timeout, unit, DEFAULT_PAUSE_MILLIS);
    }

    boolean triggerAndWaitForReplicaSync(long timeout, TimeUnit unit, long sleepMillis) {
        long timeoutInMillis = unit.toMillis(timeout);
        while (timeoutInMillis > 0) {
            timeoutInMillis = waitForMissingReplicaOwners(Level.FINE, timeoutInMillis, sleepMillis);
            if (timeoutInMillis <= 0) {
                break;
            }

            timeoutInMillis = waitForOngoingMigrations(Level.FINE, timeoutInMillis, sleepMillis);
            if (timeoutInMillis <= 0) {
                break;
            }

            long start = Clock.currentTimeMillis();
            boolean syncResult = checkAndTriggerReplicaSync();
            timeoutInMillis -= (Clock.currentTimeMillis() - start);
            if (syncResult) {
                logger.finest("Replica sync state is OK");
                return true;
            }

            if (timeoutInMillis <= 0) {
                break;
            }
            logger.info("Some backup replicas are inconsistent with primary, waiting for synchronization. Timeout: "
                    + timeoutInMillis + "ms");
            timeoutInMillis = sleepWithBusyWait(timeoutInMillis, sleepMillis);
        }
        return false;
    }

    private long waitForMissingReplicaOwners(Level level, long timeoutInMillis, long sleep) {
        long timeout = timeoutInMillis;
        while (timeout > 0 && hasMissingReplicaOwners()) {
            // ignore elapsed time during master inv.
            if (logger.isLoggable(level)) {
                logger.log(level, "Waiting for ownership assignments of missing replica owners...");
            }
            timeout = sleepWithBusyWait(timeout, sleep);
        }
        return timeout;
    }

    private boolean hasMissingReplicaOwners() {
        if (!needsReplicaStateCheck()) {
            return false;
        }

        int memberGroupsSize = partitionStateManager.getMemberGroupsSize();
        int replicaCount = Math.min(InternalPartition.MAX_REPLICA_COUNT, memberGroupsSize);

        ClusterServiceImpl clusterService = node.getClusterService();
        ClusterState clusterState = clusterService.getClusterState();

        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            for (int index = 0; index < replicaCount; index++) {
                Address address = partition.getReplicaAddress(index);
                if (address == null) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Missing replica=" + index + " for partitionId=" + partition.getPartitionId());
                    }
                    return true;
                }

                // Checking IN_TRANSITION state is not needed,
                // because to be able to change cluster state, we ensure that there are no ongoing/pending migrations
                if (clusterService.getMember(address) == null
                        && (clusterState.isJoinAllowed() || !clusterService.isMemberRemovedInNotJoinableState(address))) {

                    if (logger.isFinestEnabled()) {
                        logger.finest("Unknown replica owner= " + address + ", partitionId="
                                + partition.getPartitionId() + ", replica=" + index);
                    }
                    return true;
                }
            }
        }
        return false;
    }

    private long waitForOngoingMigrations(Level level, long timeoutInMillis, long sleep) {
        long timeout = timeoutInMillis;
        while (timeout > 0 && (migrationManager.hasOnGoingMigration() || hasOnGoingMigrationMaster(level))) {
            // ignore elapsed time during master inv.
            if (logger.isLoggable(level)) {
                logger.log(level, "Waiting for the master node to complete remaining migrations...");
            }
            timeout = sleepWithBusyWait(timeout, sleep);
        }
        return timeout;
    }

    private long sleepWithBusyWait(long timeoutInMillis, long sleep) {
        try {
            //noinspection BusyWait
            Thread.sleep(sleep);
        } catch (InterruptedException ie) {
            currentThread().interrupt();
            logger.finest("Busy wait interrupted", ie);
        }
        return timeoutInMillis - sleep;
    }

    private boolean checkAndTriggerReplicaSync() {
        if (!needsReplicaStateCheck()) {
            return true;
        }

        final Semaphore semaphore = new Semaphore(0);
        final AtomicBoolean ok = new AtomicBoolean(true);

        int maxBackupCount = partitionService.getMaxAllowedBackupCount();
        int ownedPartitionCount = invokeReplicaSyncOperations(maxBackupCount, semaphore, ok);

        try {
            if (!ok.get()) {
                return false;
            }

            int permits = ownedPartitionCount * maxBackupCount;
            boolean receivedAllResponses = semaphore.tryAcquire(permits, REPLICA_SYNC_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return receivedAllResponses && ok.get();
        } catch (InterruptedException ignored) {
            currentThread().interrupt();
            return false;
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private int invokeReplicaSyncOperations(int maxBackupCount, Semaphore semaphore, AtomicBoolean result) {
        Address thisAddress = node.getThisAddress();
        ExecutionCallback<Object> callback = new ReplicaSyncResponseCallback(result, semaphore);

        ClusterServiceImpl clusterService = node.getClusterService();
        ClusterState clusterState = clusterService.getClusterState();

        int ownedCount = 0;
        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                result.set(false);
                continue;
            }

            if (!thisAddress.equals(owner)) {
                continue;
            }
            ownedCount++;

            if (maxBackupCount == 0) {
                if (partition.isMigrating()) {
                    result.set(false);
                }
                continue;
            }

            for (int index = 1; index <= maxBackupCount; index++) {
                Address replicaAddress = partition.getReplicaAddress(index);

                if (replicaAddress == null) {
                    result.set(false);
                    semaphore.release();
                    continue;
                }

                // Checking IN_TRANSITION state is not needed,
                // because to be able to change cluster state, we ensure that there are no ongoing/pending migrations
                if (!clusterState.isJoinAllowed() && clusterService.isMemberRemovedInNotJoinableState(replicaAddress)) {
                    semaphore.release();
                    continue;
                }

                int partitionId = partition.getPartitionId();
                PartitionSpecificRunnable task = new CheckPartitionReplicaVersionTask(nodeEngine, partitionId, index, callback);
                nodeEngine.getOperationService().execute(task);
            }
        }
        return ownedCount;
    }

    private boolean needsReplicaStateCheck() {
        return partitionStateManager.isInitialized() && partitionStateManager.getMemberGroupsSize() > 0;
    }

    boolean hasOnGoingMigrationMaster(Level level) {
        ClusterService clusterService = node.getClusterService();
        Address masterAddress = clusterService.getMasterAddress();
        if (masterAddress == null) {
            return clusterService.isJoined();
        }

        Operation operation = new HasOngoingMigration();
        OperationService operationService = nodeEngine.getOperationService();
        InternalCompletableFuture<Boolean> future = operationService
                .createInvocationBuilder(SERVICE_NAME, operation, masterAddress)
                .setTryCount(INVOCATION_TRY_COUNT)
                .setTryPauseMillis(INVOCATION_TRY_PAUSE_MILLIS)
                .invoke();

        try {
            return future.join();
        } catch (Exception e) {
            logger.log(level, "Could not get a response from master about migrations! -> " + e.toString());
        }
        return false;
    }

    private static class ReplicaSyncResponseCallback implements ExecutionCallback<Object> {

        private final AtomicBoolean result;
        private final Semaphore semaphore;

        ReplicaSyncResponseCallback(AtomicBoolean result, Semaphore semaphore) {
            this.result = result;
            this.semaphore = semaphore;
        }

        @Override
        public void onResponse(Object response) {
            if (Boolean.FALSE.equals(response)) {
                result.set(false);
            }
            semaphore.release();
        }

        @Override
        public void onFailure(Throwable t) {
            result.set(false);
        }
    }
}
