/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.persistence.PersistenceService;

/**
 * @deprecated since 5.0 because of renaming purposes.
 * Please use {@link com.hazelcast.persistence.PersistenceService} instead.
 */
@Deprecated
public interface HotRestartService {
    /** The prefix for each hot restart backup directory. The backup sequence is appended to this prefix. */
    String BACKUP_DIR_PREFIX = "backup-";

    /**
     * @deprecated since 5.0 because of renaming purposes.
     * Please use {@link PersistenceService#backup()} instead.
     */
    @Deprecated
    void backup();

    /**
     * @deprecated since 5.0 because of renaming purposes.
     * Please use {@link PersistenceService#backup(long)} instead.
     */
    @Deprecated
    void backup(long backupSeq);

    /**
     * @deprecated since 5.0 because of renaming purposes.
     * Please use {@link PersistenceService#getBackupTaskStatus()} instead.
     */
    @Deprecated
    BackupTaskStatus getBackupTaskStatus();

    /**
     * @deprecated since 5.0 because of renaming purposes.
     * Please use {@link PersistenceService#interruptLocalBackupTask()} instead.
     */
    @Deprecated
    void interruptLocalBackupTask();

    /**
     * @deprecated since 5.0 because of renaming purposes.
     * Please use {@link PersistenceService#interruptBackupTask()} instead.
     */
    @Deprecated
    void interruptBackupTask();

    /**
     * @deprecated since 5.0 because of renaming purposes.
     * Please use {@link PersistenceService#isHotBackupEnabled()} instead.
     */
    @Deprecated
    boolean isHotBackupEnabled();

    /**
     * @deprecated since 5.0 because of renaming purposes.
     * Please use {@link PersistenceService#getBackupDirectory()} instead.
     */
    @Deprecated
    String getBackupDirectory();
}
