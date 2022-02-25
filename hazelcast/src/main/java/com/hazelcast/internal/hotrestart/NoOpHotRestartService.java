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

package com.hazelcast.internal.hotrestart;

import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.persistence.BackupTaskState;
import com.hazelcast.persistence.BackupTaskStatus;

/**
 * Empty implementation of HotRestartService to avoid null checks. This will provide default behaviour when hot restart is
 * not available or not enabled.
 */
public class NoOpHotRestartService implements HotRestartService {
    private static final BackupTaskStatus NO_TASK_BACKUP_STATUS = new BackupTaskStatus(BackupTaskState.NO_TASK, 0, 0);

    @Override
    public void backup() {
    }

    @Override
    public void backup(long backupSeq) {
    }

    @Override
    public BackupTaskStatus getBackupTaskStatus() {
        return NO_TASK_BACKUP_STATUS;
    }

    @Override
    public void interruptLocalBackupTask() {
    }

    @Override
    public void interruptBackupTask() {
    }

    @Override
    public boolean isHotBackupEnabled() {
        return false;
    }

    @Override
    public boolean isBackupEnabled() {
        return false;
    }

    @Override
    public String getBackupDirectory() {
        return null;
    }
}
