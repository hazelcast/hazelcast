package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;

import static com.hazelcast.partition.InternalPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.spi.OperationAccessor.setCallId;
import static java.lang.Math.min;

/**
 * Responsible for making the backups for a {@link com.hazelcast.spi.BackupAwareOperation}.
 */
final class OperationBackupHandler {

    private final Node node;
    private final OperationServiceImpl operationService;
    private final NodeEngineImpl nodeEngine;

    public OperationBackupHandler(OperationServiceImpl operationService) {
        this.operationService = operationService;
        this.node = operationService.node;
        this.nodeEngine = operationService.nodeEngine;
    }

    /**
     * Makes the backup.
     *
     * The return value contains the number of sync backups. Normally this is determined by the
     * {@link BackupAwareOperation#getSyncBackupCount()}, but if the {@link Operation#isSyncForced()}, the async backups
     * are forced to become sync.
     *
     * @param backupAwareOp the BackupAwareOperation
     * @return the number of backups that are sync.
     * @throws Exception
     */
    public int backup(BackupAwareOperation backupAwareOp) throws Exception {
        // first we calculate the desired number of backups by asking the operation.
        int requestedSyncBackups = requestedSyncBackups(backupAwareOp);
        int requestedAsyncBackups = requestedAsyncBackups(backupAwareOp);
        int requestedTotalBackups = requestedTotalBackups(backupAwareOp);

        if (requestedTotalBackups == 0) {
            // if there are no backups, we are done.
            return 0;
        }

        Operation op = (Operation) backupAwareOp;
        InternalPartitionService partitionService = operationService.node.getPartitionService();
        long[] replicaVersions = partitionService.incrementPartitionReplicaVersions(op.getPartitionId(),
                requestedTotalBackups);

        // The requested number of backups could be larger than the actual number of backups e.g when the cluster
        // is too small the store the number of backups. Also we need to deal with overriding asynchronous behavior
        // in case we need to force syncs due to back-pressure.
        int actualSyncBackups = actualSyncBackups(requestedSyncBackups, requestedAsyncBackups, op.isSyncForced());
        int actualAsyncBackups = actualAsyncBackups(requestedSyncBackups, requestedAsyncBackups, op.isSyncForced());
        int actualTotalBackups = actualSyncBackups + actualAsyncBackups;

        // todo:
        // we need to rethink this part. Because it doesn't make much sense if an operation has sync backups, but doesn't
        // send a response. Probably this is a user error we should address.
        if (!op.isSyncForced() && !op.returnsResponse()) {
            actualSyncBackups = 0;
        }

        return makeBackups(backupAwareOp, op.getPartitionId(), replicaVersions, actualSyncBackups, actualTotalBackups);
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
                    + ", sync backup count is " +  op.getSyncBackupCount()
                    + ", async backup count is " + op.getAsyncBackupCount());
        }

        return backups;
    }

    int actualSyncBackups(int syncBackupCount, int asyncBackupCount, boolean forceSync) {
        if (forceSync) {
            // if force sync enabled, then the sum of the backups
            syncBackupCount += asyncBackupCount;
        }

        if (syncBackupCount == 0) {
            // if there are no sync backups, we are done and can return 0.
            return 0;
        }

        InternalPartitionService partitionService = node.getPartitionService();
        int maxBackupCount = partitionService.getMaxBackupCount();
        return min(maxBackupCount, syncBackupCount);
    }

    int actualAsyncBackups(int syncBackupCount, int asyncBackupCount, boolean forceSync) {
        if (forceSync || asyncBackupCount == 0) {
            // if forceSync is enabled, then there will never be any async backups (they are forced to become sync)
            // if there are no asyncBackups then we are also done.
            return 0;
        }

        InternalPartitionService partitionService = node.getPartitionService();
        int maxBackupCount = partitionService.getMaxBackupCount();
        return min(maxBackupCount - syncBackupCount, asyncBackupCount);
    }

    private int makeBackups(BackupAwareOperation backupAwareOp, int partitionId, long[] replicaVersions,
                            int syncBackupCount, int totalBackupCount) {
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
                isSyncBackup = false;
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
