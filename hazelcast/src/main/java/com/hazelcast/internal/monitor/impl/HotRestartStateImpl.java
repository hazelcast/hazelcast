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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.monitor.HotRestartState;
import com.hazelcast.persistence.BackupTaskState;
import com.hazelcast.persistence.BackupTaskStatus;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getString;

public class HotRestartStateImpl implements HotRestartState {

    private BackupTaskStatus backupTaskStatus;
    private boolean isHotBackupEnabled;
    private String backupDirectory;

    public HotRestartStateImpl() {
    }

    public HotRestartStateImpl(BackupTaskStatus backupTaskStatus, boolean isHotBackupEnabled, String backupDirectory) {
        this.backupTaskStatus = backupTaskStatus;
        this.isHotBackupEnabled = isHotBackupEnabled;
        this.backupDirectory = backupDirectory;
    }

    @Override
    public BackupTaskStatus getBackupTaskStatus() {
        return backupTaskStatus;
    }

    @Override
    public boolean isHotBackupEnabled() {
        return this.isHotBackupEnabled;
    }

    @Override
    public String getBackupDirectory() {
        return backupDirectory;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        if (backupTaskStatus != null) {
            root.add("backupTaskState", backupTaskStatus.getState().name());
            root.add("backupTaskCompleted", backupTaskStatus.getCompleted());
            root.add("backupTaskTotal", backupTaskStatus.getTotal());
            root.add("isHotBackupEnabled", isHotBackupEnabled);
            root.add("backupDirectory", backupDirectory);
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        final String jsonBackupTaskState = getString(json, "backupTaskState", null);
        final int jsonBackupTaskCompleted = getInt(json, "backupTaskCompleted", 0);
        final int jsonBackupTaskTotal = getInt(json, "backupTaskTotal", 0);
        backupTaskStatus = jsonBackupTaskState != null ? new BackupTaskStatus(BackupTaskState.valueOf(jsonBackupTaskState),
                jsonBackupTaskCompleted, jsonBackupTaskTotal) : null;
        isHotBackupEnabled = getBoolean(json, "isHotBackupEnabled", false);
        backupDirectory = getString(json, "backupDirectory", null);
    }

    @Override
    public String toString() {
        return "HotRestartStateImpl{backupTaskStatus=" + backupTaskStatus
                + ", isHotBackupEnabled=" + isHotBackupEnabled
                + ", backupDirectory=" + backupDirectory
                + '}';
    }
}
