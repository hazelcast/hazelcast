/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.raft;

import com.hazelcast.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftServiceConfig {

    private RaftMetadataGroupConfig metadataGroupConfig;

    private RaftConfig raftConfig = new RaftConfig();

    private final Map<String, RaftGroupConfig> groupConfigs = new HashMap<String, RaftGroupConfig>();

    public RaftServiceConfig() {
    }

    public RaftServiceConfig(RaftServiceConfig config) {
        this.metadataGroupConfig = new RaftMetadataGroupConfig(config.metadataGroupConfig);
        this.raftConfig = new RaftConfig(config.raftConfig);
        for (RaftGroupConfig groupConfig : config.groupConfigs.values()) {
            addGroupConfig(new RaftGroupConfig(groupConfig));
        }
    }

    public RaftConfig getRaftConfig() {
        return raftConfig;
    }

    public RaftServiceConfig setRaftConfig(RaftConfig raftConfig) {
        this.raftConfig = raftConfig;
        return this;
    }

    public RaftMetadataGroupConfig getMetadataGroupConfig() {
        return metadataGroupConfig;
    }

    public RaftServiceConfig setMetadataGroupConfig(RaftMetadataGroupConfig metadataGroupConfig) {
        this.metadataGroupConfig = metadataGroupConfig;
        return this;
    }

    public Map<String, RaftGroupConfig> getGroupConfigs() {
        return groupConfigs;
    }

    public RaftGroupConfig getGroupConfig(String name) {
        return groupConfigs.get(name);
    }

    public RaftServiceConfig addGroupConfig(RaftGroupConfig groupConfig) {
        Preconditions.checkTrue(groupConfigs.containsKey(groupConfig.getName()),
                "Group config '" + groupConfig.getName() + "' already exists!");
        groupConfigs.put(groupConfig.getName(), groupConfig);
        return this;
    }
}
