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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftConfig {

    private static final long DEFAULT_SESSION_TTL = 30;
    private static final long DEFAULT_HEARTBEAT_INTERVAL = TimeUnit.SECONDS.toMillis(5);

    private RaftMetadataGroupConfig metadataGroupConfig;

    private RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig();

    private final Map<String, RaftGroupConfig> groupConfigs = new HashMap<String, RaftGroupConfig>();

    private long sessionTimeToLiveSeconds = DEFAULT_SESSION_TTL;

    private long sessionHeartbeatIntervalMillis = DEFAULT_HEARTBEAT_INTERVAL;

    public RaftConfig() {
    }

    public RaftConfig(RaftConfig config) {
        this.metadataGroupConfig = new RaftMetadataGroupConfig(config.metadataGroupConfig);
        this.raftAlgorithmConfig = new RaftAlgorithmConfig(config.raftAlgorithmConfig);
        for (RaftGroupConfig groupConfig : config.groupConfigs.values()) {
            addGroupConfig(new RaftGroupConfig(groupConfig));
        }
        this.sessionTimeToLiveSeconds = config.sessionTimeToLiveSeconds;
        this.sessionHeartbeatIntervalMillis = config.sessionHeartbeatIntervalMillis;
    }

    public RaftAlgorithmConfig getRaftAlgorithmConfig() {
        return raftAlgorithmConfig;
    }

    public RaftConfig setRaftAlgorithmConfig(RaftAlgorithmConfig raftAlgorithmConfig) {
        this.raftAlgorithmConfig = raftAlgorithmConfig;
        return this;
    }

    public RaftMetadataGroupConfig getMetadataGroupConfig() {
        return metadataGroupConfig;
    }

    public RaftConfig setMetadataGroupConfig(RaftMetadataGroupConfig metadataGroupConfig) {
        this.metadataGroupConfig = metadataGroupConfig;
        return this;
    }

    public Map<String, RaftGroupConfig> getGroupConfigs() {
        return groupConfigs;
    }

    public RaftGroupConfig getGroupConfig(String name) {
        return groupConfigs.get(name);
    }

    public RaftConfig addGroupConfig(RaftGroupConfig groupConfig) {
        checkFalse(groupConfigs.containsKey(groupConfig.getName()),
                "Group config '" + groupConfig.getName() + "' already exists!");
        groupConfigs.put(groupConfig.getName(), groupConfig);
        return this;
    }

    public long getSessionTimeToLiveSeconds() {
        return sessionTimeToLiveSeconds;
    }

    public RaftConfig setSessionTimeToLiveSeconds(long sessionTimeToLiveSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session TTL should be greater than zero!");
        checkTrue(TimeUnit.SECONDS.toMillis(sessionTimeToLiveSeconds) > sessionHeartbeatIntervalMillis,
                "Session timeout should be greater than heartbeat interval!");
        this.sessionTimeToLiveSeconds = sessionTimeToLiveSeconds;
        return this;
    }

    public long getSessionHeartbeatIntervalMillis() {
        return sessionHeartbeatIntervalMillis;
    }

    public RaftConfig setSessionHeartbeatIntervalMillis(long sessionHeartbeatIntervalMillis) {
        checkPositive(sessionTimeToLiveSeconds, "Session heartbeat interval should be greater than zero!");
        checkTrue(TimeUnit.SECONDS.toMillis(sessionTimeToLiveSeconds) > sessionHeartbeatIntervalMillis,
                "Session TTL should be greater than heartbeat interval!");
        this.sessionHeartbeatIntervalMillis = sessionHeartbeatIntervalMillis;
        return this;
    }
}
