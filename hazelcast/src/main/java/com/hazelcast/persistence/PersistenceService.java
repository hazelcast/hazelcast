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

package com.hazelcast.persistence;

/**
 * Service for interacting with Persistence. For example - starting cluster-wide data backups, determining the
 * local backup state and interrupting a currently running local backup.
 *
 * @since 5.0
 */
public interface PersistenceService {
    /** The prefix for each persistence backup directory. The backup sequence is appended to this prefix. */
    String BACKUP_DIR_PREFIX = "backup-";

    /**
     * Attempts to perform a cluster-wide data backup. Each node will create a directory under the defined backup dir
     * with the name {@link #BACKUP_DIR_PREFIX} followed by the cluster time defined by this node.
     * The backup request is performed transactionally. This method will throw an exception if another request (transaction)
     * is already in progress. If a node is already performing a backup (there is a file indicating a backup is in progress),
     * the node will only log a warning and ignore the backup request.
     *
     * @since 5.0
     */
    void backup();

    /**
     * Attempts to perform a cluster-wide data backup. Each node will create a directory under the defined backup dir
     * with the name {@link #BACKUP_DIR_PREFIX} followed by the {@code backupSeq}.
     * The backup request is performed transactionally. This method will throw an exception if another request (transaction)
     * is already in progress. If a node is already performing a backup (there is a file indicating a backup is in progress),
     * the node will only log a warning and ignore the backup request.
     *
     * @param backupSeq the suffix of the backup directory for this cluster backup
     * @since 5.0
     */
    void backup(long backupSeq);

    /**
     * Returns the local backup task status (not the cluster backup status).
     *
     * @since 5.0
     */
    BackupTaskStatus getBackupTaskStatus();

    /**
     * Interrupts the local backup task if one is currently running. The contents of the target backup directory will be left
     * as-is.
     *
     * @since 5.0
     */
    void interruptLocalBackupTask();

    /**
     * Interrupts the backup tasks on each cluster member if one is currently running. The contents of the target backup
     * directories will be left as-is.
     *
     * @since 5.0
     */
    void interruptBackupTask();

    /**
     * Returns if backup is enabled.
     *
     * @return {@code true} if backup is enabled, {@code false} otherwise
     * @since 5.0
     */
    boolean isBackupEnabled();

    /**
     * Returns the persistence backup directory.
     *
     * @return persistence backup directory if backup is enabled, {@code null} otherwise.
     * @since 5.0
     */
    String getBackupDirectory();
}
