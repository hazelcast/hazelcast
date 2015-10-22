/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;
/**
 * Configuration for wan replication.
 */
public class WanReplicationConfig {

    String name;
    List<WanTargetClusterConfig> targetClusterConfigs;

    /**
     * This property is only valid when used with WAN Batch replication, Enterprise Only
     * When enabled, only the latest {@link com.hazelcast.wan.WanReplicationEvent} of a key is sent to target
     */
    boolean snapshotEnabled;

    public List<WanTargetClusterConfig> getTargetClusterConfigs() {
        return targetClusterConfigs;
    }

    public WanReplicationConfig addTargetClusterConfig(WanTargetClusterConfig wanTargetClusterConfig) {
        if (targetClusterConfigs == null) {
            targetClusterConfigs = new ArrayList<WanTargetClusterConfig>(2);
        }
        targetClusterConfigs.add(wanTargetClusterConfig);
        return this;
    }

    public WanReplicationConfig setTargetClusterConfigs(List<WanTargetClusterConfig> list) {
        targetClusterConfigs = list;
        return this;
    }

    public String getName() {
        return name;
    }

    public WanReplicationConfig setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }

    public void setSnapshotEnabled(boolean snapshotEnabled) {
        this.snapshotEnabled = snapshotEnabled;
    }

    @Override
    public String toString() {
        return "WanReplicationConfig"
                + "{name='" + name + '\''
                + ", snapshotEnabled=" + snapshotEnabled
                + ", targetClusterConfigs=" + targetClusterConfigs
                + '}';
    }
}
