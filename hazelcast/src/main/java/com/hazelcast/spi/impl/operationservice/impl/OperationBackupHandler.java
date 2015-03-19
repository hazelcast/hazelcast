package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static java.lang.Math.min;

/**
 * Responsible for creating a backups of an operation.
 */
final class OperationBackupHandler {

    private OperationServiceImpl operationService;

    public OperationBackupHandler(OperationServiceImpl operationService) {
        this.operationService = operationService;
    }

    public int backup(BackupAwareOperation backupAwareOp) throws Exception {
        int requestedSyncBackupCount = backupAwareOp.getSyncBackupCount() > 0
                ? min(InternalPartition.MAX_BACKUP_COUNT, backupAwareOp.getSyncBackupCount()) : 0;

        int requestedAsyncBackupCount = backupAwareOp.getAsyncBackupCount() > 0
                ? min(InternalPartition.MAX_BACKUP_COUNT - requestedSyncBackupCount,
                backupAwareOp.getAsyncBackupCount()) : 0;

        int totalRequestedBackupCount = requestedSyncBackupCount + requestedAsyncBackupCount;
        if (totalRequestedBackupCount == 0) {
            return 0;
        }

        Operation op = (Operation) backupAwareOp;
        InternalPartitionService partitionService = operationService.node.getPartitionService();
        long[] replicaVersions = partitionService.incrementPartitionReplicaVersions(op.getPartitionId(),
                totalRequestedBackupCount);

        int maxPossibleBackupCount = partitionService.getMaxBackupCount();
        int syncBackupCount = min(maxPossibleBackupCount, requestedSyncBackupCount);
        int asyncBackupCount = min(maxPossibleBackupCount - syncBackupCount, requestedAsyncBackupCount);

        int totalBackupCount = syncBackupCount + asyncBackupCount;
        if (totalBackupCount == 0) {
            return 0;
        }

        if (!op.returnsResponse()) {
            syncBackupCount = 0;
        }

        return makeBackups(backupAwareOp, op.getPartitionId(), replicaVersions, syncBackupCount, totalBackupCount);
    }

    private int makeBackups(BackupAwareOperation backupAwareOp, int partitionId, long[] replicaVersions,
                            int syncBackupCount, int totalBackupCount) {
        Boolean backPressureNeeded = null;
        int sentSyncBackupCount = 0;
        InternalPartitionService partitionService = operationService.node.getPartitionService();
        InternalPartition partition = partitionService.getPartition(partitionId);
        for (int replicaIndex = 1; replicaIndex <= totalBackupCount; replicaIndex++) {
            Address target = partition.getReplicaAddress(replicaIndex);
            if (target == null) {
                continue;
            }

            assertNoBackupOnPrimaryMember(partition, target);

            boolean isSyncBackup = true;
            if (replicaIndex > syncBackupCount) {
                // it is an async-backup

                if (backPressureNeeded == null) {
                    // back-pressure was not yet calculated, so lets calculate it. Once it is calculated
                    // we'll use that value for all the backups for the 'backupAwareOp'.
                    backPressureNeeded = operationService.backPressureService.isBackPressureNeeded((Operation) backupAwareOp);
                }

                if (!backPressureNeeded) {
                    // only when no back-pressure is needed, then the async-backup is allowed to be async.
                    isSyncBackup = false;
                }
            }

            Backup backup = newBackup(backupAwareOp, replicaVersions, replicaIndex, isSyncBackup);
            operationService.send(backup, target);

            if (isSyncBackup) {
                sentSyncBackupCount++;
            }
        }
        return sentSyncBackupCount;
    }

    private Backup newBackup(BackupAwareOperation backupAwareOp, long[] replicaVersions,
                             int replicaIndex, boolean isSyncBackup) {
        Operation op = (Operation) backupAwareOp;
        Operation backupOp = initBackupOperation(backupAwareOp, replicaIndex);
        Data backupOpData = operationService.nodeEngine.getSerializationService().toData(backupOp);
        Backup backup = new Backup(backupOpData, op.getCallerAddress(), replicaVersions, isSyncBackup);
        backup.setPartitionId(op.getPartitionId())
                .setReplicaIndex(replicaIndex)
                .setServiceName(op.getServiceName())
                .setCallerUuid(operationService.nodeEngine.getLocalMember().getUuid());
        setCallId(backup, op.getCallId());
        return backup;
    }

    private Operation initBackupOperation(BackupAwareOperation backupAwareOp, int replicaIndex) {
        Operation backupOp = backupAwareOp.getBackupOperation();
        if (backupOp == null) {
            throw new IllegalArgumentException("Backup operation should not be null!");
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
        if (target.equals(operationService.node.getThisAddress())) {
            throw new IllegalStateException("Normally shouldn't happen! Owner node and backup node "
                    + "are the same! " + partition);
        }
    }
}
