/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hotrestart;

/**
 * Service for interacting with Hot Restart. For example - starting cluster-wide hot restart data backups, determining the
 * local backup state and interrupting a currently running local hot restart backup.
 */
public interface HotRestartService {
    /** The prefix for each hot restart backup directory. The backup sequence is appended to this prefix. */
    String BACKUP_DIR_PREFIX = "backup-";

    /**
     * Attempts to perform a cluster hot restart data backup. Each node will create a directory under the defined backup dir
     * with the name {@link #BACKUP_DIR_PREFIX} followed by the cluster time defined by this node.
     * The backup request is performed transactionally. This method will throw an exception if an another request (transaction)
     * is already in progress. If a node is already performing a backup (there is a file indicating a backup is in progress),
     * the node will only log a warning and ignore the backup request.
     */
    void backup();

    /**
     * Attempts to perform a cluster hot restart data backup. Each node will create a directory under the defined backup dir
     * with the name {@link #BACKUP_DIR_PREFIX} followed by the {@code backupSeq}.
     * The backup request is performed transactionally. This method will throw an exception if an another request (transaction)
     * is already in progress. If a node is already performing a backup (there is a file indicating a backup is in progress),
     * the node will only log a warning and ignore the backup request.
     *
     * @param backupSeq the suffix of the backup directory for this cluster hot restart backup
     */
    void backup(long backupSeq);

    /**
     * Returns the local hot restart backup task status (not the cluster backup status).
     */
    BackupTaskStatus getBackupTaskStatus();

    /**
     * Interrupts the local backup task if one is currently running. The contents of the target backup directory will be left
     * as-is.
     */
    void interruptLocalBackupTask();

    /**
     * Interrupts the backup tasks on each cluster member if one is currently running. The contents of the target backup
     * directories will be left as-is.
     */
    void interruptBackupTask();

    /**
     * Returns if hot backup is enabled.
     *
     * @return {@code true} if hot backup is enabled, {@code false} otherwise
     */
    boolean isHotBackupEnabled();

    /**
     * Returns the hot restart backup directory.
     *
     * @return hot restart backup directory if hot backup is enabled, {@code null} otherwise.
     */
    String getBackupDirectory();
}
