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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
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
    private final BackpressureRegulator backpressureRegulator;

    OperationBackupHandler(OperationServiceImpl operationService) {
        this.operationService = operationService;
        this.node = operationService.node;
        this.backpressureRegulator = operationService.backpressureRegulator;
    }

    /**
     * Sends the appropriate backups. This call will not wait till the backups have ACK'ed.
     *
     * If this call is made with a none BackupAwareOperation, then 0 is returned.
     *
     * @param op the Operation to backup.
     * @return the number of ACKS required to complete the invocation.
     * @throws Exception if there is any exception sending the backups.
     */
    int sendBackups(Operation op) throws Exception {
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

    int sendBackups0(BackupAwareOperation backupAwareOp) throws Exception {
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

    private int makeBackups(BackupAwareOperation backupAwareOp,
                            int partitionId,
                            long[] replicaVersions,
                            int syncBackups,
                            int asyncBackups) {
        int totalBackups = syncBackups + asyncBackups;

        InternalPartitionService partitionService = node.getPartitionService();
        InternalPartition partition = partitionService.getPartition(partitionId);

        // number of acks that needs to be send. Each sync backup, is 1 ack.
        int acks = 0;

        // we don't need to clone the backupOp because when the Backup is send, it is serialized. So this
        // backupOp instance is effectively readonly. Only on cloned (when the Backup is deserialized)
        // backupOp instances, operation are executed.
        Operation backupOp = newBackupOperation(backupAwareOp);

        for (int replicaIndex = 1; replicaIndex <= totalBackups; replicaIndex++) {
            Address target = partition.getReplicaAddress(replicaIndex);

            if (target == null) {
                continue;
            }

            assertNoBackupOnPrimaryMember(partition, target);

            boolean isSyncBackup = replicaIndex <= syncBackups;

            Backup backup = newBackup(backupAwareOp, backupOp, replicaVersions, replicaIndex, isSyncBackup);
            operationService.send(backup, target);

            if (isSyncBackup) {
                acks++;
            }
        }

        return acks;
    }

    private Operation newBackupOperation(BackupAwareOperation backupAwareOp) {
        Operation backupOp = backupAwareOp.getBackupOperation();
        if (backupOp == null) {
            throw new IllegalArgumentException("Backup operation should not be null! " + backupAwareOp);
        }
        Operation op = (Operation) backupAwareOp;
        // set service name of backup operation.
        // if getServiceName() method is overridden to return the same name then this will have no effect.
        backupOp.setServiceName(op.getServiceName());
        // we set the callId on the backupOp so that Backup knows who to send a response to. We don't want to set a callId
        // on the Backup since the Backup itself will not respond to the primary
        setCallId(backupOp, op.getCallId());
        return backupOp;
    }

    private Backup newBackup(BackupAwareOperation backupAwareOp,
                             Operation backupOp,
                             long[] replicaVersions,
                             int replicaIndex,
                             boolean sync) {
        Operation op = (Operation) backupAwareOp;
        Backup backup = new Backup(backupOp, op.getCallerAddress(), replicaVersions, sync);

        backup.setPartitionId(op.getPartitionId())
                .setReplicaIndex(replicaIndex);

        return backup;
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
