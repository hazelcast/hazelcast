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

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration for Raft groups.
 */
public class RaftConfig {

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
     * with {@link com.hazelcast.raft.exception.CannotReplicateException}.
     */
    private int uncommittedEntryCountToRejectNewAppends = DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS;

    /**
     * When enabled, an append request fails if the target member (leader) leaves the cluster.
     * At this point result of append request is indeterminate, it may have been replicated by the leader
     * to some of the followers.
     */
    private boolean failOnIndeterminateOperationState;

    /**
     * Enabled appending a no-op entry to the log when a new leader is elected.
     * <p>
     * See <a href="https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J">
     * <i>Bug in single-server membership changes</i></a> post by Diego Ongaro for more info.
     */
    private boolean appendNopEntryOnLeaderElection;

    public RaftConfig() {
    }

    public RaftConfig(RaftConfig config) {
        this.leaderElectionTimeoutInMillis = config.leaderElectionTimeoutInMillis;
        this.leaderHeartbeatPeriodInMillis = config.leaderHeartbeatPeriodInMillis;
        this.appendRequestMaxEntryCount = config.appendRequestMaxEntryCount;
        this.commitIndexAdvanceCountToSnapshot = config.commitIndexAdvanceCountToSnapshot;
        this.uncommittedEntryCountToRejectNewAppends = config.uncommittedEntryCountToRejectNewAppends;
        this.failOnIndeterminateOperationState = config.failOnIndeterminateOperationState;
        this.appendNopEntryOnLeaderElection = config.appendNopEntryOnLeaderElection;
    }

    public long getLeaderElectionTimeoutInMillis() {
        return leaderElectionTimeoutInMillis;
    }

    public RaftConfig setLeaderElectionTimeoutInMillis(long leaderElectionTimeoutInMillis) {
        checkPositive(leaderElectionTimeoutInMillis, "leader election timeout in millis: "
                + leaderElectionTimeoutInMillis + " should be positive");
        this.leaderElectionTimeoutInMillis = leaderElectionTimeoutInMillis;
        return this;
    }

    public long getLeaderHeartbeatPeriodInMillis() {
        return leaderHeartbeatPeriodInMillis;
    }

    public RaftConfig setLeaderHeartbeatPeriodInMillis(long leaderHeartbeatPeriodInMillis) {
        checkPositive(leaderHeartbeatPeriodInMillis, "leader heartbeat period in millis: "
                + leaderHeartbeatPeriodInMillis + " should be positive");
        this.leaderHeartbeatPeriodInMillis = leaderHeartbeatPeriodInMillis;
        return this;
    }

    public int getAppendRequestMaxEntryCount() {
        return appendRequestMaxEntryCount;
    }

    public RaftConfig setAppendRequestMaxEntryCount(int appendRequestMaxEntryCount) {
        checkPositive(appendRequestMaxEntryCount, "append request max entry count: " + appendRequestMaxEntryCount
                + " should be positive");
        this.appendRequestMaxEntryCount = appendRequestMaxEntryCount;
        return this;
    }

    public int getCommitIndexAdvanceCountToSnapshot() {
        return commitIndexAdvanceCountToSnapshot;
    }

    public RaftConfig setCommitIndexAdvanceCountToSnapshot(int commitIndexAdvanceCountToSnapshot) {
        checkPositive(commitIndexAdvanceCountToSnapshot, "commit index advance count to snapshot: "
                + commitIndexAdvanceCountToSnapshot + " should be positive");
        this.commitIndexAdvanceCountToSnapshot = commitIndexAdvanceCountToSnapshot;
        return this;
    }

    public int getUncommittedEntryCountToRejectNewAppends() {
        return uncommittedEntryCountToRejectNewAppends;
    }

    public RaftConfig setUncommittedEntryCountToRejectNewAppends(int uncommittedEntryCountToRejectNewAppends) {
        checkPositive(uncommittedEntryCountToRejectNewAppends, "uncommitted entry count to reject new appends: "
                + uncommittedEntryCountToRejectNewAppends + " should be positive");
        this.uncommittedEntryCountToRejectNewAppends = uncommittedEntryCountToRejectNewAppends;
        return this;
    }

    public boolean isFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    public RaftConfig setFailOnIndeterminateOperationState(boolean failOnIndeterminateOperationState) {
        this.failOnIndeterminateOperationState = failOnIndeterminateOperationState;
        return this;
    }

    public boolean isAppendNopEntryOnLeaderElection() {
        return appendNopEntryOnLeaderElection;
    }

    public RaftConfig setAppendNopEntryOnLeaderElection(boolean appendNopEntryOnLeaderElection) {
        this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;
        return this;
    }
}
