/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.monitor.HotRestartState;
import com.hazelcast.util.JsonUtil;

import static com.hazelcast.util.JsonUtil.getString;

public class HotRestartStateImpl implements HotRestartState {

    private BackupTaskStatus backupTaskStatus;

    private boolean isHotBackupEnabled;

    public HotRestartStateImpl() {
    }

    public HotRestartStateImpl(BackupTaskStatus backupTaskStatus, boolean isHotBackupEnabled) {
        this.backupTaskStatus = backupTaskStatus;
        this.isHotBackupEnabled = isHotBackupEnabled;
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
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        if (backupTaskStatus != null) {
            root.add("backupTaskState", backupTaskStatus.getState().name());
            root.add("backupTaskCompleted", backupTaskStatus.getCompleted());
            root.add("backupTaskTotal", backupTaskStatus.getTotal());
            root.add("isHotBackupEnabled", isHotBackupEnabled);
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        final String jsonBackupTaskState = getString(json, "backupTaskState", null);
        final int jsonBackupTaskCompleted = JsonUtil.getInt(json, "backupTaskCompleted", 0);
        final int jsonBackupTaskTotal = JsonUtil.getInt(json, "backupTaskTotal", 0);
        backupTaskStatus = jsonBackupTaskState != null ? new BackupTaskStatus(BackupTaskState.valueOf(jsonBackupTaskState),
                jsonBackupTaskCompleted, jsonBackupTaskTotal) : null;
        isHotBackupEnabled = JsonUtil.getBoolean(json, "isHotBackupEnabled", false);
    }

    @Override
    public String toString() {
        return "HotRestartStateImpl{backupTaskStatus=" + backupTaskStatus
                + ", isHotBackupEnabled" + isHotBackupEnabled
                + '}';
    }
}
