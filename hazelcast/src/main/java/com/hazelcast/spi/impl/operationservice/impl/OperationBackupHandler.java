/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;

import static com.hazelcast.internal.partition.InternalPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.spi.OperationAccessor.setCallId;
import static java.lang.Math.min;

/**
 * Responsible for creating a backups of an operation.
 */
final class OperationBackupHandler {

    private final Node node;
    private final OperationServiceImpl operationService;
    private final NodeEngineImpl nodeEngine;
    private final BackpressureRegulator backpressureRegulator;

    public OperationBackupHandler(OperationServiceImpl operationService) {
        this.operationService = operationService;
        this.node = operationService.node;
        this.nodeEngine = operationService.nodeEngine;
        this.backpressureRegulator = operationService.backpressureRegulator;
    }

    public int backup(BackupAwareOperation backupAwareOp) throws Exception {
        int requestedSyncBackups = requestedSyncBackups(backupAwareOp);
        int requestedAsyncBackups = requestedAsyncBackups(backupAwareOp);
        int requestedTotalBackups = requestedTotalBackups(backupAwareOp);
        if (requestedTotalBackups == 0) {
            return 0;
        }

        Operation op = (Operation) backupAwareOp;
        InternalPartitionService partitionService = node.getPartitionService();
        long[] replicaVersions = partitionService.incrementPartitionReplicaVersions(op.getPartitionId(),
                requestedTotalBackups);

        boolean syncForced = backpressureRegulator.isSyncForced(backupAwareOp);

        int syncBackups = syncBackups(requestedSyncBackups, requestedAsyncBackups, syncForced);
        int asyncBackups = asyncBackups(requestedSyncBackups, requestedAsyncBackups, syncForced);

        // TODO: This could cause a problem with back pressure
        if (!op.returnsResponse()) {
            asyncBackups += syncBackups;
            syncBackups = 0;
        }

        return makeBackups(backupAwareOp, op.getPartitionId(), replicaVersions, syncBackups, asyncBackups);
    }

    int syncBackups(int requestedSyncBackups, int requestedAsyncBackups, boolean syncForced) {
        if (syncForced) {
            // if force sync enabled, then the sum of the backups
            requestedSyncBackups += requestedAsyncBackups;
        }

        InternalPartitionService partitionService = node.getPartitionService();
        int maxBackupCount = partitionService.getMaxBackupCount();
        return min(maxBackupCount, requestedSyncBackups);
    }

    int asyncBackups(int requestedSyncBackups, int requestedAsyncBackups, boolean syncForced) {
        if (syncForced || requestedAsyncBackups == 0) {
            // if syncForced, then there will never be any async backups (they are forced to become sync)
            // if there are no asyncBackups then we are also done.
            return 0;
        }

        InternalPartitionService partitionService = node.getPartitionService();
        int maxBackupCount = partitionService.getMaxBackupCount();
        return min(maxBackupCount - requestedSyncBackups, requestedAsyncBackups);
    }

    private int requestedSyncBackups(BackupAwareOperation op) {
        int backups = op.getSyncBackupCount();

        if (backups < 0) {
            throw new IllegalArgumentException("Can't create backup for Operation:" + op
                    + ", sync backup count can't be smaller than 0, but found: " + backups);
        }

        if (backups > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Can't create backup for Operation:" + op
                    + ", sync backup count can't be larger than " + MAX_BACKUP_COUNT
                    + ", but found: " + backups);
        }
        return backups;
    }

    private int requestedAsyncBackups(BackupAwareOperation op) {
        int backups = op.getAsyncBackupCount();

        if (backups < 0) {
            throw new IllegalArgumentException("Can't create backup for Operation:" + op
                    + ", async backup count can't be smaller than 0, but found: " + backups);
        }

        if (backups > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Can't create backup for Operation:" + op
                    + ", async backup count can't be larger than " + MAX_BACKUP_COUNT
                    + ", but found: " + backups);
        }

        return backups;
    }

    private int requestedTotalBackups(BackupAwareOperation op) {
        int backups = op.getSyncBackupCount() + op.getAsyncBackupCount();

        if (backups > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Can't create backup for Operation:" + op
                    + ", the sum of async and sync backups is larger than " + MAX_BACKUP_COUNT
                    + ", sync backup count is " + op.getSyncBackupCount()
                    + ", async backup count is " + op.getAsyncBackupCount());
        }

        return backups;
    }

    private int makeBackups(BackupAwareOperation backupAwareOp, int partitionId, long[] replicaVersions,
                            int syncBackups, int asyncBackups) {
        int sendSyncBackups = 0;

        InternalPartitionService partitionService = node.getPartitionService();
        InternalPartition partition = partitionService.getPartition(partitionId);

        for (int replicaIndex = 1; replicaIndex <= syncBackups + asyncBackups; replicaIndex++) {
            Address target = partition.getReplicaAddress(replicaIndex);

            if (target == null) {
                continue;
            }

            assertNoBackupOnPrimaryMember(partition, target);

            boolean isSyncBackup = replicaIndex <= syncBackups;

            Backup backup = newBackup(backupAwareOp, replicaVersions, replicaIndex, isSyncBackup);
            operationService.send(backup, target);

            if (isSyncBackup) {
                sendSyncBackups++;
            }
        }

        return sendSyncBackups;
    }

    private Backup newBackup(BackupAwareOperation backupAwareOp, long[] replicaVersions,
                             int replicaIndex, boolean isSyncBackup) {
        Operation op = (Operation) backupAwareOp;
        Operation backupOp = newBackupOperation(backupAwareOp, replicaIndex);
        Data backupOpData = nodeEngine.getSerializationService().toData(backupOp);

        Backup backup = new Backup(backupOpData, op.getCallerAddress(), replicaVersions, isSyncBackup);
        backup.setPartitionId(op.getPartitionId())
                .setReplicaIndex(replicaIndex)
                .setServiceName(op.getServiceName())
                .setCallerUuid(nodeEngine.getLocalMember().getUuid());
        setCallId(backup, op.getCallId());

        return backup;
    }

    private Operation newBackupOperation(BackupAwareOperation backupAwareOp, int replicaIndex) {
        Operation backupOp = backupAwareOp.getBackupOperation();
        if (backupOp == null) {
            throw new IllegalArgumentException("Backup operation should not be null! " + backupAwareOp);
        }

        Operation op = (Operation) backupAwareOp;
        backupOp.setPartitionId(op.getPartitionId())
                .setReplicaIndex(replicaIndex)
                .setServiceName(op.getServiceName());
        return backupOp;
    }

    /**
     * Verifies that the backup of a partition doesn't end up at the member that also has the primary.
     */
    private void assertNoBackupOnPrimaryMember(InternalPartition partition, Address target) {
        if (target.equals(node.getThisAddress())) {
            throw new IllegalStateException("Normally shouldn't happen! Owner node and backup node "
                    + "are the same! " + partition);
        }
    }
}
