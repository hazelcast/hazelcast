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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.TargetAware;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;

import static com.hazelcast.internal.partition.InternalPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.hasActiveInvocation;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallId;
import static java.lang.Math.min;

/**
 * Responsible for creating a backups of an operation.
 */
final class OperationBackupHandler {

    private static final boolean ASSERTION_ENABLED = OperationBackupHandler.class.desiredAssertionStatus();

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final BackpressureRegulator backpressureRegulator;
    private final OutboundOperationHandler outboundOperationHandler;
    private final ILogger logger;

    OperationBackupHandler(OperationServiceImpl operationService, OutboundOperationHandler outboundOperationHandler) {
        this.outboundOperationHandler = outboundOperationHandler;
        this.node = operationService.node;
        this.nodeEngine = operationService.nodeEngine;
        this.backpressureRegulator = operationService.backpressureRegulator;
        this.logger = node.getLogger(getClass());
    }

    /**
     * Sends the appropriate backups. This call will not wait till the backups have ACK'ed.
     *
     * If this call is made with a none BackupAwareOperation, then 0 is returned.
     *
     * @param op the Operation to backup.
     * @return the number of ACKS required to complete the invocation.
     */
    int sendBackups(Operation op) {
        if (!(op instanceof BackupAwareOperation)) {
            return 0;
        }

        int backupAcks = 0;
        BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
        if (backupAwareOp.shouldBackup()) {
            backupAcks = sendBackups0(backupAwareOp);
        }
        return backupAcks;
    }

    int sendBackups0(BackupAwareOperation backupAwareOp) {
        int requestedSyncBackups = requestedSyncBackups(backupAwareOp);
        int requestedAsyncBackups = requestedAsyncBackups(backupAwareOp);
        int requestedTotalBackups = requestedTotalBackups(backupAwareOp);
        if (requestedTotalBackups == 0) {
            return 0;
        }

        Operation op = (Operation) backupAwareOp;
        PartitionReplicaVersionManager versionManager = node.getPartitionService().getPartitionReplicaVersionManager();
        ServiceNamespace namespace = versionManager.getServiceNamespace(op);
        long[] replicaVersions = versionManager.incrementPartitionReplicaVersions(op.getPartitionId(), namespace,
                requestedTotalBackups);

        boolean syncForced = backpressureRegulator.isSyncForced(backupAwareOp);

        int syncBackups = syncBackups(requestedSyncBackups, requestedAsyncBackups, syncForced);
        int asyncBackups = asyncBackups(requestedSyncBackups, requestedAsyncBackups, syncForced);

        // TODO: This could cause a problem with back pressure
        if (!op.returnsResponse()) {
            asyncBackups += syncBackups;
            syncBackups = 0;
        }

        if (syncBackups + asyncBackups == 0) {
            return 0;
        }

        return makeBackups(backupAwareOp, op.getPartitionId(), replicaVersions, syncBackups, asyncBackups);
    }

    int syncBackups(int requestedSyncBackups, int requestedAsyncBackups, boolean syncForced) {
        if (syncForced) {
            // if force sync enabled, then the sum of the backups
            requestedSyncBackups += requestedAsyncBackups;
        }

        InternalPartitionService partitionService = node.getPartitionService();
        int maxBackupCount = partitionService.getMaxAllowedBackupCount();
        return min(maxBackupCount, requestedSyncBackups);
    }

    int asyncBackups(int requestedSyncBackups, int requestedAsyncBackups, boolean syncForced) {
        if (syncForced || requestedAsyncBackups == 0) {
            // if syncForced, then there will never be any async backups (they are forced to become sync)
            // if there are no asyncBackups then we are also done.
            return 0;
        }

        InternalPartitionService partitionService = node.getPartitionService();
        int maxBackupCount = partitionService.getMaxAllowedBackupCount();
        return min(maxBackupCount - requestedSyncBackups, requestedAsyncBackups);
    }

    private int requestedSyncBackups(BackupAwareOperation op) {
        int backups = op.getSyncBackupCount();

        if (backups < 0) {
            throw new IllegalArgumentException("Can't create backup for " + op
                    + ", sync backup count can't be smaller than 0, but found: " + backups);
        }

        if (backups > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Can't create backup for " + op
                    + ", sync backup count can't be larger than " + MAX_BACKUP_COUNT
                    + ", but found: " + backups);
        }
        return backups;
    }

    private int requestedAsyncBackups(BackupAwareOperation op) {
        int backups = op.getAsyncBackupCount();

        if (backups < 0) {
            throw new IllegalArgumentException("Can't create backup for " + op
                    + ", async backup count can't be smaller than 0, but found: " + backups);
        }

        if (backups > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Can't create backup for " + op
                    + ", async backup count can't be larger than " + MAX_BACKUP_COUNT
                    + ", but found: " + backups);
        }

        return backups;
    }

    private int requestedTotalBackups(BackupAwareOperation op) {
        int backups = op.getSyncBackupCount() + op.getAsyncBackupCount();

        if (backups > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Can't create backup for " + op
                    + ", the sum of async and sync backups is larger than " + MAX_BACKUP_COUNT
                    + ", sync backup count is " + op.getSyncBackupCount()
                    + ", async backup count is " + op.getAsyncBackupCount());
        }

        return backups;
    }

    private int makeBackups(BackupAwareOperation backupAwareOp, int partitionId, long[] replicaVersions,
                            int syncBackups, int asyncBackups) {
        int sendSyncBackups;
        int totalBackups = syncBackups + asyncBackups;

        InternalPartitionService partitionService = node.getPartitionService();
        InternalPartition partition = partitionService.getPartition(partitionId);

        if (totalBackups == 1) {
            sendSyncBackups = sendSingleBackup(backupAwareOp, partition, replicaVersions, syncBackups);
        } else {
            sendSyncBackups = sendMultipleBackups(backupAwareOp, partition, replicaVersions, syncBackups, totalBackups);
        }
        return sendSyncBackups;
    }

    private int sendSingleBackup(BackupAwareOperation backupAwareOp, InternalPartition partition,
                                 long[] replicaVersions, int syncBackups) {
        // Since there is only one replica, replica index is `1`
        return sendSingleBackup(backupAwareOp, partition, replicaVersions, syncBackups, 1);
    }

    private int sendMultipleBackups(BackupAwareOperation backupAwareOp, InternalPartition partition,
                                    long[] replicaVersions, int syncBackups, int totalBackups) {
        int sendSyncBackups = 0;
        Operation backupOp = getBackupOperation(backupAwareOp);
        if (!(backupOp instanceof TargetAware)) {
            // optimize common case: serialize operation once and send to multiple targets
            Data backupOpData = nodeEngine.getSerializationService().toData(backupOp);

            for (int replicaIndex = 1; replicaIndex <= totalBackups; replicaIndex++) {
                PartitionReplica target = partition.getReplica(replicaIndex);

                if (target == null) {
                    continue;
                }

                if (skipSendingBackupToTarget(partition, target)) {
                    continue;
                }

                boolean isSyncBackup = replicaIndex <= syncBackups;

                Backup backup = newBackup(backupAwareOp, backupOpData, replicaVersions, replicaIndex, isSyncBackup);
                outboundOperationHandler.send(backup, target.address());

                if (isSyncBackup) {
                    sendSyncBackups++;
                }
            }
        } else {
            for (int replicaIndex = 1; replicaIndex <= totalBackups; replicaIndex++) {
                int syncBackupSent = sendSingleBackup(backupAwareOp, partition, replicaVersions,
                        syncBackups, replicaIndex);
                sendSyncBackups += syncBackupSent;
            }
        }
        return sendSyncBackups;
    }

    private int sendSingleBackup(BackupAwareOperation backupAwareOp, InternalPartition partition,
                                 long[] replicaVersions, int syncBackups, int replica) {
        Operation backupOp = getBackupOperation(backupAwareOp);
        PartitionReplica target = partition.getReplica(replica);
        if (target != null) {
            if (skipSendingBackupToTarget(partition, target)) {
                return 0;
            }
            // Since there is only one backup, backup operation is sent to only one node.
            // If backup operation is converted to `Data`, there will be these operations as below:
            //      - a temporary memory allocation (byte[]) for `Data`
            //      - serialize backup operation to allocated memory
            //      - copy the temporary allocated memory (backup operation data) to output while serializing `Backup`
            // In this flow, there are two redundant operations (allocating temporary memory and copying it to output).
            // So in this case (there is only one backup), we don't convert backup operation to `Data` as temporary
            // before `Backup` is serialized but backup operation is already serialized directly into output
            // without any unnecessary memory allocation and copy when it is used as object inside `Backup`.
            if (backupOp instanceof TargetAware) {
                ((TargetAware) backupOp).setTarget(target.address());
            }

            boolean isSyncBackup = syncBackups == 1;

            Backup backup = newBackup(backupAwareOp, backupOp, replicaVersions, 1, isSyncBackup);
            outboundOperationHandler.send(backup, target.address());

            if (isSyncBackup) {
                return 1;
            }
        }
        return 0;
    }

    private Operation getBackupOperation(BackupAwareOperation backupAwareOp) {
        Operation backupOp = backupAwareOp.getBackupOperation();
        if (backupOp == null) {
            throw new IllegalArgumentException("Backup operation must not be null! " + backupAwareOp);
        }
        if (ASSERTION_ENABLED) {
            checkServiceNamespaces(backupAwareOp, backupOp);
        }

        Operation op = (Operation) backupAwareOp;
        // set service name of backup operation.
        // if getServiceName() method is overridden to return the same name
        // then this will have no effect.
        backupOp.setServiceName(op.getServiceName());
        backupOp.setNodeEngine(nodeEngine);
        return backupOp;
    }

    private void checkServiceNamespaces(BackupAwareOperation backupAwareOp, Operation backupOp) {
        Operation op = (Operation) backupAwareOp;
        Object service;
        try {
            service = op.getService();
        } catch (Exception ignored) {
            // operation doesn't know its service name
            return;
        }

        if (service instanceof FragmentedMigrationAwareService) {
            assert backupAwareOp instanceof ServiceNamespaceAware
                    : service + " is instance of FragmentedMigrationAwareService, "
                    + backupAwareOp + " should implement ServiceNamespaceAware!";

            assert backupOp instanceof ServiceNamespaceAware
                    : service + " is instance of FragmentedMigrationAwareService, "
                    + backupOp + " should implement ServiceNamespaceAware!";
        } else {
            assert !(backupAwareOp instanceof ServiceNamespaceAware)
                    : service + " is NOT instance of FragmentedMigrationAwareService, "
                    + backupAwareOp + " should NOT implement ServiceNamespaceAware!";

            assert !(backupOp instanceof ServiceNamespaceAware)
                    : service + " is NOT instance of FragmentedMigrationAwareService, "
                    + backupOp + " should NOT implement ServiceNamespaceAware!";
        }
    }

    private static Backup newBackup(BackupAwareOperation backupAwareOp, Object backupOp, long[] replicaVersions,
                                    int replicaIndex, boolean respondBack) {
        Operation op = (Operation) backupAwareOp;
        Backup backup;
        if (backupOp instanceof Operation) {
            backup = new Backup((Operation) backupOp, op.getCallerAddress(), replicaVersions, respondBack, op.getClientCallId());
        } else if (backupOp instanceof Data) {
            backup = new Backup((Data) backupOp, op.getCallerAddress(), replicaVersions, respondBack, op.getClientCallId());
        } else {
            throw new IllegalArgumentException("Only 'Data' or 'Operation' typed backup operation is supported!");
        }

        backup.setPartitionId(op.getPartitionId()).setReplicaIndex(replicaIndex).setCallerUuid(op.getCallerUuid());
        if (hasActiveInvocation(op)) {
            setCallId(backup, op.getCallId());
        }
        return backup;
    }

    private boolean skipSendingBackupToTarget(InternalPartition partition, PartitionReplica target) {
        ClusterServiceImpl clusterService = node.getClusterService();

        assert !target.isIdentical(nodeEngine.getLocalMember()) : "Could not send backup operation, because "
                + target + " is local member itself! Local-member: " + clusterService.getLocalMember()
                + ", " + partition;

        if (clusterService.getMember(target.address(), target.uuid()) == null) {
            if (logger.isFinestEnabled()) {
                if (clusterService.isMissingMember(target.address(), target.uuid())) {
                    logger.finest("Could not send backup operation, because " + target + " is a missing member. "
                            + partition);
                } else {
                    logger.finest("Could not send backup operation, because " + target + " is not a known member. "
                            + partition);
                }
            }
            return true;
        }
        return false;
    }
}
