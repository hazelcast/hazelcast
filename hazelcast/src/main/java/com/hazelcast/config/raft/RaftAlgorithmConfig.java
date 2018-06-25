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

/**
 * Configuration for Raft algorithm.
 */
public class RaftAlgorithmConfig {

    /**
     * Default leader election timeout in millis. See {@link #leaderElectionTimeoutInMillis}.
     */
    public static final long DEFAULT_LEADER_ELECTION_TIMEOUT_IN_MILLIS = 2000;

    /**
     * Default leader heartbeat period in millis. See {@link #leaderHeartbeatPeriodInMillis}.
     */
    public static final long DEFAULT_LEADER_HEARTBEAT_PERIOD_IN_MILLIS = 5000;

    /**
     * Default max append request entry count. See {@link #appendRequestMaxEntryCount}.
     */
    public static final int DEFAULT_APPEND_REQUEST_MAX_ENTRY_COUNT = 50;

    /**
     * Default commit index advance to initiate a snapshot. See {@link #commitIndexAdvanceCountToSnapshot}.
     */
    public static final int DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_SNAPSHOT = 1000;

    /**
     * Default max allowed uncommitted entry count. See {@link #uncommittedEntryCountToRejectNewAppends}.
     */
    public static final int DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS = 100;

    /**
     * Leader election timeout in milliseconds.
     * If a candidate cannot win majority of the votes in time, a new election round is initiated.
     */
    private long leaderElectionTimeoutInMillis = DEFAULT_LEADER_ELECTION_TIMEOUT_IN_MILLIS;

    /**
     * Period for leader to send heartbeat messages to its followers.
     */
    private long leaderHeartbeatPeriodInMillis = DEFAULT_LEADER_HEARTBEAT_PERIOD_IN_MILLIS;

    /**
     * Max entry count that can be sent in a single batch of append entries request.
     */
    private int appendRequestMaxEntryCount = DEFAULT_APPEND_REQUEST_MAX_ENTRY_COUNT;

    /**
     * Number of commits to initiate a new snapshot after the last snapshot's index.
     */
    private int commitIndexAdvanceCountToSnapshot = DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_SNAPSHOT;

    /**
     * Max number of allowed uncommitted entries before rejecting new append requests
     */
    private int uncommittedEntryCountToRejectNewAppends = DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS;

    public RaftAlgorithmConfig() {
    }

    public RaftAlgorithmConfig(RaftAlgorithmConfig config) {
        this.leaderElectionTimeoutInMillis = config.leaderElectionTimeoutInMillis;
        this.leaderHeartbeatPeriodInMillis = config.leaderHeartbeatPeriodInMillis;
        this.appendRequestMaxEntryCount = config.appendRequestMaxEntryCount;
        this.commitIndexAdvanceCountToSnapshot = config.commitIndexAdvanceCountToSnapshot;
        this.uncommittedEntryCountToRejectNewAppends = config.uncommittedEntryCountToRejectNewAppends;
    }

    public long getLeaderElectionTimeoutInMillis() {
        return leaderElectionTimeoutInMillis;
    }

    public RaftAlgorithmConfig setLeaderElectionTimeoutInMillis(long leaderElectionTimeoutInMillis) {
        checkPositive(leaderElectionTimeoutInMillis, "leader election timeout in millis: "
                + leaderElectionTimeoutInMillis + " should be positive");
        this.leaderElectionTimeoutInMillis = leaderElectionTimeoutInMillis;
        return this;
    }

    public long getLeaderHeartbeatPeriodInMillis() {
        return leaderHeartbeatPeriodInMillis;
    }

    public RaftAlgorithmConfig setLeaderHeartbeatPeriodInMillis(long leaderHeartbeatPeriodInMillis) {
        checkPositive(leaderHeartbeatPeriodInMillis, "leader heartbeat period in millis: "
                + leaderHeartbeatPeriodInMillis + " should be positive");
        this.leaderHeartbeatPeriodInMillis = leaderHeartbeatPeriodInMillis;
        return this;
    }

    public int getAppendRequestMaxEntryCount() {
        return appendRequestMaxEntryCount;
    }

    public RaftAlgorithmConfig setAppendRequestMaxEntryCount(int appendRequestMaxEntryCount) {
        checkPositive(appendRequestMaxEntryCount, "append request max entry count: " + appendRequestMaxEntryCount
                + " should be positive");
        this.appendRequestMaxEntryCount = appendRequestMaxEntryCount;
        return this;
    }

    public int getCommitIndexAdvanceCountToSnapshot() {
        return commitIndexAdvanceCountToSnapshot;
    }

    public RaftAlgorithmConfig setCommitIndexAdvanceCountToSnapshot(int commitIndexAdvanceCountToSnapshot) {
        checkPositive(commitIndexAdvanceCountToSnapshot, "commit index advance count to snapshot: "
                + commitIndexAdvanceCountToSnapshot + " should be positive");
        this.commitIndexAdvanceCountToSnapshot = commitIndexAdvanceCountToSnapshot;
        return this;
    }

    public int getUncommittedEntryCountToRejectNewAppends() {
        return uncommittedEntryCountToRejectNewAppends;
    }

    public RaftAlgorithmConfig setUncommittedEntryCountToRejectNewAppends(int uncommittedEntryCountToRejectNewAppends) {
        checkPositive(uncommittedEntryCountToRejectNewAppends, "uncommitted entry count to reject new appends: "
                + uncommittedEntryCountToRejectNewAppends + " should be positive");
        this.uncommittedEntryCountToRejectNewAppends = uncommittedEntryCountToRejectNewAppends;
        return this;
    }

}
