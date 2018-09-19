/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.monitor.HotRestartState;

import static com.hazelcast.util.JsonUtil.getString;

public class HotRestartStateImpl implements HotRestartState {

    private String backupDirectory;

    public HotRestartStateImpl() {
    }

    public HotRestartStateImpl(String backupDirectory) {
        this.backupDirectory = backupDirectory;
    }

    @Override
    public String getBackupDirectory() {
        return backupDirectory;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        if (backupDirectory != null) {
            root.add("backupDirectory", backupDirectory);
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        backupDirectory = getString(json, "backupDirectory", null);
    }

    @Override
    public String toString() {
        return "HotRestartStateImpl{"
                + "backupDirectory=" + backupDirectory
                + '}';
    }
}
