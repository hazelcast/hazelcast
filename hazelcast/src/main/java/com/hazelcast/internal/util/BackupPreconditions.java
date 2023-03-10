package com.hazelcast.internal.util;

import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;

public class BackupPreconditions {
    /**
     * Tests if the newBackupCount count is valid.
     *
     * @param newBackupCount          the number of sync backups
     * @param currentAsyncBackupCount the current number of async backups
     * @return the newBackupCount
     * @throws IllegalArgumentException if newBackupCount is smaller than 0, or larger than the maximum
     *                                            number of backups.
     */
    public static int checkBackupCount(int newBackupCount, int currentAsyncBackupCount) {
        if (newBackupCount < 0) {
            throw new IllegalArgumentException("backup-count can't be smaller than 0");
        }

        if (currentAsyncBackupCount < 0) {
            throw new IllegalArgumentException("async-backup-count can't be smaller than 0");
        }

        if (newBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("backup-count can't be larger than than " + MAX_BACKUP_COUNT);
        }

        if (newBackupCount + currentAsyncBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("the sum of backup-count and async-backup-count can't be larger than than "
                    + MAX_BACKUP_COUNT);
        }

        return newBackupCount;
    }

    /**
     * Tests if the newAsyncBackupCount count is valid.
     *
     * @param currentBackupCount  the current number of backups
     * @param newAsyncBackupCount the new number of async backups
     * @return the newAsyncBackupCount
     * @throws IllegalArgumentException if asyncBackupCount is smaller than 0, or larger than the maximum
     *                                            number of backups.
     */
    public static int checkAsyncBackupCount(int currentBackupCount, int newAsyncBackupCount) {
        if (currentBackupCount < 0) {
            throw new IllegalArgumentException("backup-count can't be smaller than 0");
        }

        if (newAsyncBackupCount < 0) {
            throw new IllegalArgumentException("async-backup-count can't be smaller than 0");
        }

        if (newAsyncBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("async-backup-count can't be larger than than " + MAX_BACKUP_COUNT);
        }

        if (currentBackupCount + newAsyncBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("the sum of backup-count and async-backup-count can't be larger than than "
                    + MAX_BACKUP_COUNT);
        }

        return newAsyncBackupCount;
    }
}
