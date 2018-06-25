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

package com.hazelcast.config.raft;

import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftConfig {

    /**
     * Name of the default group if no group name is specified by Raft data structures.
     */
    public static final String DEFAULT_RAFT_GROUP_NAME = "default";

    private static final int DEFAULT_SESSION_TTL_SECONDS = 30;

    private static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5;


    private int cpNodeCount;

    private int groupSize;

    private RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig();

    private int sessionTimeToLiveSeconds = DEFAULT_SESSION_TTL_SECONDS;

    private int sessionHeartbeatIntervalSeconds = DEFAULT_HEARTBEAT_INTERVAL_SECONDS;

    /**
     * When enabled, an append request fails if the target member (leader) leaves the cluster.
     * At this point result of append request is indeterminate, it may have been replicated by the leader
     * to some of the followers.
     */
    private boolean failOnIndeterminateOperationState;

    private long missingRaftMemberRemovalSeconds;

    public RaftConfig() {
    }

    public RaftConfig(RaftConfig config) {
        this.cpNodeCount = config.cpNodeCount;
        this.groupSize = config.groupSize;
        this.raftAlgorithmConfig = new RaftAlgorithmConfig(config.raftAlgorithmConfig);
        this.sessionTimeToLiveSeconds = config.sessionTimeToLiveSeconds;
        this.sessionHeartbeatIntervalSeconds = config.sessionHeartbeatIntervalSeconds;
        this.failOnIndeterminateOperationState = config.failOnIndeterminateOperationState;
        this.missingRaftMemberRemovalSeconds = config.missingRaftMemberRemovalSeconds;
    }

    public int getCpNodeCount() {
        return cpNodeCount;
    }

    public RaftConfig setCpNodeCount(int cpNodeCount) {
        checkTrue(cpNodeCount >= 2, "CP subsystem must have at least 2 members");
        checkTrue(groupSize <= cpNodeCount,
                "The group size parameter cannot be bigger than the number of the cp node count");
        this.cpNodeCount = cpNodeCount;
        return this;
    }

    public int getGroupSize() {
        return groupSize > 0 ? groupSize : cpNodeCount;
    }

    public RaftConfig setGroupSize(int groupSize) {
        checkTrue(groupSize == 0 || groupSize >= 2, "Raft groups must have at least 2 members");
        checkTrue(groupSize <= cpNodeCount,
                "The group size parameter cannot be bigger than the number of the cp node count");
        this.groupSize = groupSize;
        return this;
    }

    public RaftAlgorithmConfig getRaftAlgorithmConfig() {
        return raftAlgorithmConfig;
    }

    public RaftConfig setRaftAlgorithmConfig(RaftAlgorithmConfig raftAlgorithmConfig) {
        this.raftAlgorithmConfig = raftAlgorithmConfig;
        return this;
    }

    public long getSessionTimeToLiveSeconds() {
        return sessionTimeToLiveSeconds;
    }

    public RaftConfig setSessionTimeToLiveSeconds(int sessionTimeToLiveSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session TTL should be greater than zero!");
        checkTrue(sessionTimeToLiveSeconds > sessionHeartbeatIntervalSeconds,
                "Session timeout must be greater than heartbeat interval!");
        this.sessionTimeToLiveSeconds = sessionTimeToLiveSeconds;
        return this;
    }

    public int getSessionHeartbeatIntervalSeconds() {
        return sessionHeartbeatIntervalSeconds;
    }

    public RaftConfig setSessionHeartbeatIntervalSeconds(int sessionHeartbeatIntervalSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session heartbeat interval should be greater than zero!");
        checkTrue(sessionTimeToLiveSeconds > sessionHeartbeatIntervalSeconds,
                "Session TTL must be greater than heartbeat interval!");
        checkTrue(missingRaftMemberRemovalSeconds == 0 || sessionTimeToLiveSeconds <= missingRaftMemberRemovalSeconds,
                "Session TTL must be smaller than or equal to missingRaftMemberRemovalSeconds!");
        this.sessionHeartbeatIntervalSeconds = sessionHeartbeatIntervalSeconds;
        return this;
    }

    public boolean isFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    public RaftConfig setFailOnIndeterminateOperationState(boolean failOnIndeterminateOperationState) {
        this.failOnIndeterminateOperationState = failOnIndeterminateOperationState;
        return this;
    }

    public long getMissingRaftMemberRemovalSeconds() {
        return missingRaftMemberRemovalSeconds;
    }

    public RaftConfig setMissingRaftMemberRemovalSeconds(long missingRaftMemberRemovalSeconds) {
        checkTrue(missingRaftMemberRemovalSeconds == 0 || missingRaftMemberRemovalSeconds >= sessionTimeToLiveSeconds,
                "missingRaftMemberRemovalSeconds must be either 0 or greater than or equal to session TTL");
        this.missingRaftMemberRemovalSeconds = missingRaftMemberRemovalSeconds;
        return this;
    }
}
