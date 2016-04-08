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

import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.operation.HasOngoingMigration;
import com.hazelcast.internal.partition.operation.IsReplicaVersionSync;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.internal.partition.impl.InternalPartitionServiceState.MIGRATION_LOCAL;
import static com.hazelcast.internal.partition.impl.InternalPartitionServiceState.MIGRATION_ON_MASTER;
import static com.hazelcast.internal.partition.impl.InternalPartitionServiceState.REPLICA_NOT_SYNC;
import static com.hazelcast.internal.partition.impl.InternalPartitionServiceState.SAFE;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;
import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;

/**
 *  Verifies up-to-dateness of each of partition replicas owned by this member.
 *  Triggers replica sync process for out-of-date replicas.
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
    private final PartitionReplicaManager replicaManager;

    public PartitionReplicaStateChecker(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.partitionService = partitionService;
        nodeEngine = node.nodeEngine;
        logger = node.getLogger(getClass());

        partitionStateManager = partitionService.getPartitionStateManager();
        migrationManager = partitionService.getMigrationManager();
        replicaManager = partitionService.getReplicaManager();
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

    boolean hasOnGoingMigrationMaster(Level level) {
        Address masterAddress = node.getMasterAddress();
        if (masterAddress == null) {
            return node.joined();
        }
        Operation operation = new HasOngoingMigration();
        Future future = invokeOnTarget(operation, masterAddress);
        try {
            return (Boolean) future.get(1, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            Logger.getLogger(InternalPartitionServiceImpl.class).finest("Future wait interrupted", ie);
        } catch (Exception e) {
            logger.log(level, "Could not get a response from master about migrations! -> " + e.toString());
        }
        return false;
    }

    private Future invokeOnTarget(Operation operation, Address target) {
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(SERVICE_NAME, operation, target)
                .setTryCount(INVOCATION_TRY_COUNT)
                .setTryPauseMillis(INVOCATION_TRY_PAUSE_MILLIS)
                .invoke();
    }

    private boolean isReplicaInSyncState() {
        if (!partitionStateManager.isInitialized() || partitionStateManager.getMemberGroupsSize() == 0) {
            return true;
        }

        final int replicaIndex = 1;
        final List<Future> futures = new ArrayList<Future>();
        final Address thisAddress = node.getThisAddress();
        for (InternalPartition partition : partitionStateManager.getPartitions()) {
            final Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                return false;
            } else if (thisAddress.equals(owner)) {
                if (partition.getReplicaAddress(replicaIndex) != null) {
                    int partitionId = partition.getPartitionId();
                    long replicaVersion = getCurrentReplicaVersion(replicaIndex, partitionId);
                    Operation operation = createReplicaSyncStateOperation(replicaVersion, partitionId);
                    Future future = invoke(operation, replicaIndex, partitionId);
                    futures.add(future);
                }
            }
        }

        for (Future future : futures) {
            boolean isSync = getFutureResult(future, REPLICA_SYNC_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!isSync) {
                return false;
            }
        }
        return true;
    }

    private Future invoke(Operation operation, int replicaIndex, int partitionId) {
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId)
                .setTryCount(INVOCATION_TRY_COUNT)
                .setTryPauseMillis(INVOCATION_TRY_PAUSE_MILLIS)
                .setReplicaIndex(replicaIndex)
                .invoke();
    }

    // TODO: VISIBILITY PROBLEM! Replica versions are updated & read only by partition threads!
    // This problem will be solved alongside graceful shutdown improvements.
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

    private Operation createReplicaSyncStateOperation(long replicaVersion, int partitionId) {
        final Operation op = new IsReplicaVersionSync(replicaVersion);
        op.setService(partitionService);
        op.setNodeEngine(nodeEngine);
        op.setOperationResponseHandler(createErrorLoggingResponseHandler(node.getLogger(IsReplicaVersionSync.class)));
        op.setPartitionId(partitionId);

        return op;
    }
}
