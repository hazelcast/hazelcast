/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.operation.HasOngoingMigration;
import com.hazelcast.internal.partition.operation.IsReplicaVersionSync;
import com.hazelcast.internal.partition.operation.SyncReplicaVersion;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.internal.partition.impl.InternalPartitionServiceState.MIGRATION_LOCAL;
import static com.hazelcast.internal.partition.impl.InternalPartitionServiceState.MIGRATION_ON_MASTER;
import static com.hazelcast.internal.partition.impl.InternalPartitionServiceState.REPLICA_NOT_SYNC;
import static com.hazelcast.internal.partition.impl.InternalPartitionServiceState.SAFE;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;
import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 *
 */
// TODO: find a better name!
public class PartitionReplicaChecker {

    private static final int DEFAULT_PAUSE_MILLIS = 1000;
    private static final int REPLICA_SYNC_CHECK_TIMEOUT_SECONDS = 10;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;

    private final PartitionStateManager partitionStateManager;
    private final MigrationManager migrationManager;
    private final PartitionReplicaManager replicaManager;

    public PartitionReplicaChecker(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.partitionService = partitionService;
        nodeEngine = node.nodeEngine;
        logger = node.getLogger(getClass());

        partitionStateManager = partitionService.getPartitionStateManager();
        migrationManager = partitionService.getMigrationManager();
        replicaManager = partitionService.getReplicaManager();
    }

    public boolean prepareToSafeShutdown(long timeout, TimeUnit unit) {
        long timeoutInMillis = unit.toMillis(timeout);
        long sleep = DEFAULT_PAUSE_MILLIS;
        while (timeoutInMillis > 0) {
            while (timeoutInMillis > 0 && shouldWaitMigrationOrBackups(Level.INFO)) {
                timeoutInMillis = sleepWithBusyWait(timeoutInMillis, sleep);
            }
            if (timeoutInMillis <= 0) {
                break;
            }

            if (node.isMaster()) {
                final List<MemberImpl> members = partitionService.getCurrentMembersAndMembersRemovedWhileNotClusterNotActive();
                partitionService.syncPartitionRuntimeState(members);
            } else {
                timeoutInMillis = waitForOngoingMigrations(timeoutInMillis, sleep);
                if (timeoutInMillis <= 0) {
                    break;
                }
            }

            long start = Clock.currentTimeMillis();
            boolean ok = checkReplicaSyncState();
            timeoutInMillis -= (Clock.currentTimeMillis() - start);
            if (ok) {
                logger.finest("Replica sync state before shutdown is OK");
                return true;
            } else {
                if (timeoutInMillis <= 0) {
                    break;
                }
                logger.info("Some backup replicas are inconsistent with primary, waiting for synchronization. Timeout: "
                        + timeoutInMillis + "ms");
                timeoutInMillis = sleepWithBusyWait(timeoutInMillis, sleep);
            }
        }
        return false;
    }

    private long waitForOngoingMigrations(long timeoutInMillis, long sleep) {
        long timeout = timeoutInMillis;
        while (timeout > 0 && hasOnGoingMigrationMaster(Level.WARNING)) {
            // ignore elapsed time during master inv.
            logger.info("Waiting for the master node to complete remaining migrations!");
            timeout = sleepWithBusyWait(timeout, sleep);
        }
        return timeout;
    }

    private long sleepWithBusyWait(long timeoutInMillis, long sleep) {
        try {
            //noinspection BusyWait
            Thread.sleep(sleep);
        } catch (InterruptedException ie) {
            logger.finest("Busy wait interrupted", ie);
        }
        return timeoutInMillis - sleep;
    }

    public InternalPartitionServiceState getMemberState() {
        if (migrationManager.hasOnGoingMigration()) {
            return MIGRATION_LOCAL;
        }

        if (!node.isMaster()) {
            if (hasOnGoingMigrationMaster(Level.OFF)) {
                return MIGRATION_ON_MASTER;
            }
        }

        return isReplicaInSyncState() ? SAFE : REPLICA_NOT_SYNC;
    }

    private boolean hasOnGoingMigrationMaster(Level level) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            return node.joined();
        }
        Operation operation = new HasOngoingMigration();
        OperationService operationService = nodeEngine.getOperationService();
        InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation,
                masterAddress);
        Future future = invocationBuilder.setTryCount(100).setTryPauseMillis(100).invoke();
        try {
            return (Boolean) future.get(1, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            Logger.getLogger(InternalPartitionServiceImpl.class).finest("Future wait interrupted", ie);
        } catch (Exception e) {
            logger.log(level, "Could not get a response from master about migrations! -> " + e.toString());
        }
        return false;
    }

    private boolean isReplicaInSyncState() {
        if (!partitionStateManager.isInitialized() || !hasMultipleMemberGroups()) {
            return true;
        }
        final int replicaIndex = 1;
        final List<Future> futures = new ArrayList<Future>();
        final Address thisAddress = node.getThisAddress();
        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            final Address owner = partition.getOwnerOrNull();
            if (thisAddress.equals(owner)) {
                if (partition.getReplicaAddress(replicaIndex) != null) {
                    final int partitionId = partition.getPartitionId();
                    final long replicaVersion = getCurrentReplicaVersion(replicaIndex, partitionId);
                    final Operation operation = createReplicaSyncStateOperation(replicaVersion, partitionId);
                    final Future future = invoke(operation, replicaIndex, partitionId);
                    futures.add(future);
                }
            }
        }
        if (futures.isEmpty()) {
            return true;
        }
        for (Future future : futures) {
            boolean isSync = getFutureResult(future, REPLICA_SYNC_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!isSync) {
                return false;
            }
        }
        return true;
    }

    // TODO: VISIBILITY PROBLEM! Replica versions are updated & read only by partition threads!
    private long getCurrentReplicaVersion(int replicaIndex, int partitionId) {
        final long[] versions = replicaManager.getPartitionReplicaVersions(partitionId);
        return versions[replicaIndex - 1];
    }

    private boolean getFutureResult(Future future, long seconds, TimeUnit unit) {
        boolean sync;
        try {
            sync = (Boolean) future.get(seconds, unit);
        } catch (Throwable t) {
            sync = false;
            logger.warning("Exception while getting future", t);
        }
        return sync;
    }

    private Future invoke(Operation operation, int replicaIndex, int partitionId) {
        final OperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                .setTryCount(3)
                .setTryPauseMillis(250)
                .setReplicaIndex(replicaIndex)
                .invoke();
    }

    private Operation createReplicaSyncStateOperation(long replicaVersion, int partitionId) {
        final Operation op = new IsReplicaVersionSync(replicaVersion);
        op.setService(this);
        op.setNodeEngine(nodeEngine);
        op.setOperationResponseHandler(createErrorLoggingResponseHandler(node.getLogger(IsReplicaVersionSync.class)));
        op.setPartitionId(partitionId);

        return op;
    }

    private boolean checkReplicaSyncState() {
        if (!partitionStateManager.isInitialized()) {
            return true;
        }

        if (!hasMultipleMemberGroups()) {
            return true;
        }

        final Address thisAddress = node.getThisAddress();
        final Semaphore s = new Semaphore(0);
        final AtomicBoolean ok = new AtomicBoolean(true);
        final ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                if (Boolean.FALSE.equals(response)) {
                    ok.compareAndSet(true, false);
                }
                s.release();
            }

            @Override
            public void onFailure(Throwable t) {
                ok.compareAndSet(true, false);
            }
        };
        int ownedCount = submitSyncReplicaOperations(thisAddress, s, ok, callback);
        try {
            if (ok.get()) {
                int permits = ownedCount * partitionService.getMaxAllowedBackupCount();
                return s.tryAcquire(permits, REPLICA_SYNC_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS) && ok.get();
            } else {
                return false;
            }
        } catch (InterruptedException ignored) {
            return false;
        }
    }

    private int submitSyncReplicaOperations(Address thisAddress, Semaphore s, AtomicBoolean ok,
            ExecutionCallback callback) {

        int ownedCount = 0;
        ILogger responseLogger = node.getLogger(SyncReplicaVersion.class);
        OperationResponseHandler responseHandler =
                createErrorLoggingResponseHandler(responseLogger);

        int maxBackupCount = partitionService.getMaxAllowedBackupCount();

        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            Address owner = partition.getOwnerOrNull();
            if (thisAddress.equals(owner)) {
                for (int i = 1; i <= maxBackupCount; i++) {
                    final Address replicaAddress = partition.getReplicaAddress(i);
                    if (replicaAddress != null) {
                        if (checkClusterStateForReplicaSync(replicaAddress)) {
                            SyncReplicaVersion op = new SyncReplicaVersion(i, callback);
                            op.setService(this);
                            op.setNodeEngine(nodeEngine);
                            op.setOperationResponseHandler(responseHandler);
                            op.setPartitionId(partition.getPartitionId());
                            nodeEngine.getOperationService().executeOperation(op);
                        } else {
                            s.release();
                        }
                    } else {
                        ok.set(false);
                        s.release();
                    }
                }
                ownedCount++;
            } else if (owner == null) {
                ok.set(false);
            }
        }
        return ownedCount;
    }

    private boolean checkClusterStateForReplicaSync(final Address address) {
        final ClusterServiceImpl clusterService = node.clusterService;
        final ClusterState clusterState = clusterService.getClusterState();

        if (clusterState == ClusterState.ACTIVE || clusterState == ClusterState.IN_TRANSITION) {
            return true;
        }

        return !clusterService.isMemberRemovedWhileClusterIsNotActive(address);
    }

    private boolean shouldWaitMigrationOrBackups(Level level) {
        if (!preCheckShouldWaitMigrationOrBackups()) {
            return false;
        }

        if (checkForActiveMigrations(level)) {
            return true;
        }

        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            if (partition.getReplicaAddress(1) == null) {
                final boolean canTakeBackup = !partitionService.isClusterFormedByOnlyLiteMembers();

                if (canTakeBackup && logger.isLoggable(level)) {
                    logger.log(level, "Should take backup of partitionId=" + partition.getPartitionId());
                }

                return canTakeBackup;
            }
        }
        int replicaSyncProcesses = replicaManager.onGoingReplicationProcesses();
        if (replicaSyncProcesses > 0) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Processing replica sync requests: " + replicaSyncProcesses);
            }
            return true;
        }
        return false;
    }

    private boolean preCheckShouldWaitMigrationOrBackups() {
        return partitionStateManager.isInitialized() && hasMultipleMemberGroups();
    }

    private boolean hasMultipleMemberGroups() {
        return partitionStateManager.getMemberGroupsSize() >= 2;
    }

    private boolean checkForActiveMigrations(Level level) {
        final MigrationInfo activeMigrationInfo = migrationManager.getActiveMigration();
        if (activeMigrationInfo != null) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Waiting for active migration: " + activeMigrationInfo);
            }
            return true;
        }

        int queueSize = migrationManager.getMigrationQueueSize();
        if (queueSize != 0) {
            if (logger.isLoggable(level)) {
                logger.log(level, "Waiting for cluster migration tasks: " + queueSize);
            }
            return true;
        }
        return false;
    }
}
