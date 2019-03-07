/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.cp;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration for Hazelcast's implementation of the Raft consensus algorithm
 */
public class RaftAlgorithmConfig {

    /**
     * Default leader election timeout in millis.
     * See {@link #leaderElectionTimeoutInMillis}.
     */
    public static final long DEFAULT_LEADER_ELECTION_TIMEOUT_IN_MILLIS = 2000;

    /**
     * Default leader heartbeat period in millis.
     * See {@link #leaderHeartbeatPeriodInMillis}.
     */
    public static final long DEFAULT_LEADER_HEARTBEAT_PERIOD_IN_MILLIS = 5000;

    /**
     * Default max append request entry count.
     * See {@link #appendRequestMaxEntryCount}.
     */
    public static final int DEFAULT_APPEND_REQUEST_MAX_ENTRY_COUNT = 100;

    /**
     * Default commit index advance to initiate a snapshot.
     * See {@link #commitIndexAdvanceCountToSnapshot}.
     */
    public static final int DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_SNAPSHOT = 10000;

    /**
     * Default max allowed uncommitted entry count.
     * See {@link #uncommittedEntryCountToRejectNewAppends}.
     */
    public static final int DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS = 100;

    /**
     * Default max number of missed heartbeats to trigger a new leader election.
     */
    public static final int DEFAULT_MAX_MISSED_LEADER_HEARTBEAT_COUNT = 5;

    /**
     * Default append request backoff timeout in millis.
     */
    public static final long DEFAULT_APPEND_REQUEST_BACKOFF_TIMEOUT_IN_MILLIS = 100;

    /**
     * Leader election timeout in milliseconds. If a candidate cannot win
     * majority of the votes in time, a new election round is initiated.
     */
    private long leaderElectionTimeoutInMillis = DEFAULT_LEADER_ELECTION_TIMEOUT_IN_MILLIS;

    /**
     * Period in milliseconds for a leader to send heartbeat messages to
     * its followers
     */
    private long leaderHeartbeatPeriodInMillis = DEFAULT_LEADER_HEARTBEAT_PERIOD_IN_MILLIS;

    /**
     * Maximum number of missed leader heartbeats to trigger
     * a new leader election
     */
    private int maxMissedLeaderHeartbeatCount = DEFAULT_MAX_MISSED_LEADER_HEARTBEAT_COUNT;

    /**
     * Maximum number of entries that can be sent in a single batch of
     * append entries request
     */
    private int appendRequestMaxEntryCount = DEFAULT_APPEND_REQUEST_MAX_ENTRY_COUNT;

    /**
     * Number of new commits to initiate a new snapshot after
     * the last snapshot
     */
    private int commitIndexAdvanceCountToSnapshot = DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_SNAPSHOT;

    /**
     * Maximum number of uncommitted entries in the leader's Raft log before
     * temporarily rejecting new requests of callers.
     */
    private int uncommittedEntryCountToRejectNewAppends = DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS;

    /**
     * Timeout in milliseconds for append request backoff. After the leader
     * sends an append request to a follower, it will not send a subsequent
     * append request until the follower responds to the former request
     * or this timeout occurs.
     */
    private long appendRequestBackoffTimeoutInMillis = DEFAULT_APPEND_REQUEST_BACKOFF_TIMEOUT_IN_MILLIS;

    public RaftAlgorithmConfig() {
    }

    public RaftAlgorithmConfig(RaftAlgorithmConfig config) {
        this.leaderElectionTimeoutInMillis = config.leaderElectionTimeoutInMillis;
        this.leaderHeartbeatPeriodInMillis = config.leaderHeartbeatPeriodInMillis;
        this.appendRequestMaxEntryCount = config.appendRequestMaxEntryCount;
        this.commitIndexAdvanceCountToSnapshot = config.commitIndexAdvanceCountToSnapshot;
        this.uncommittedEntryCountToRejectNewAppends = config.uncommittedEntryCountToRejectNewAppends;
        this.maxMissedLeaderHeartbeatCount = config.maxMissedLeaderHeartbeatCount;
        this.appendRequestBackoffTimeoutInMillis = config.appendRequestBackoffTimeoutInMillis;
    }

    public long getLeaderElectionTimeoutInMillis() {
        return leaderElectionTimeoutInMillis;
    }

    public RaftAlgorithmConfig setLeaderElectionTimeoutInMillis(long leaderElectionTimeoutInMillis) {
        checkPositive(leaderElectionTimeoutInMillis, "leader election timeout in millis: "
                + leaderElectionTimeoutInMillis + " must be positive!");
        this.leaderElectionTimeoutInMillis = leaderElectionTimeoutInMillis;
        return this;
    }

    public long getLeaderHeartbeatPeriodInMillis() {
        return leaderHeartbeatPeriodInMillis;
    }

    public RaftAlgorithmConfig setLeaderHeartbeatPeriodInMillis(long leaderHeartbeatPeriodInMillis) {
        checkPositive(leaderHeartbeatPeriodInMillis, "leader heartbeat period in millis: "
                + leaderHeartbeatPeriodInMillis + " must be positive!");
        this.leaderHeartbeatPeriodInMillis = leaderHeartbeatPeriodInMillis;
        return this;
    }

    public int getAppendRequestMaxEntryCount() {
        return appendRequestMaxEntryCount;
    }

    public RaftAlgorithmConfig setAppendRequestMaxEntryCount(int appendRequestMaxEntryCount) {
        checkPositive(appendRequestMaxEntryCount, "append request max entry count: " + appendRequestMaxEntryCount
                + " must be positive!");
        this.appendRequestMaxEntryCount = appendRequestMaxEntryCount;
        return this;
    }

    public int getCommitIndexAdvanceCountToSnapshot() {
        return commitIndexAdvanceCountToSnapshot;
    }

    public RaftAlgorithmConfig setCommitIndexAdvanceCountToSnapshot(int commitIndexAdvanceCountToSnapshot) {
        checkPositive(commitIndexAdvanceCountToSnapshot, "commit index advance count to snapshot: "
                + commitIndexAdvanceCountToSnapshot + " must be positive!");
        this.commitIndexAdvanceCountToSnapshot = commitIndexAdvanceCountToSnapshot;
        return this;
    }

    public int getUncommittedEntryCountToRejectNewAppends() {
        return uncommittedEntryCountToRejectNewAppends;
    }

    public RaftAlgorithmConfig setUncommittedEntryCountToRejectNewAppends(int uncommittedEntryCountToRejectNewAppends) {
        checkPositive(uncommittedEntryCountToRejectNewAppends, "uncommitted entry count to reject new appends: "
                + uncommittedEntryCountToRejectNewAppends + " must be positive!");
        this.uncommittedEntryCountToRejectNewAppends = uncommittedEntryCountToRejectNewAppends;
        return this;
    }

    public int getMaxMissedLeaderHeartbeatCount() {
        return maxMissedLeaderHeartbeatCount;
    }

    public RaftAlgorithmConfig setMaxMissedLeaderHeartbeatCount(int maxMissedLeaderHeartbeatCount) {
        checkPositive(maxMissedLeaderHeartbeatCount, "max missed leader heartbeat count must be positive!");
        this.maxMissedLeaderHeartbeatCount = maxMissedLeaderHeartbeatCount;
        return this;
    }

    public long getAppendRequestBackoffTimeoutInMillis() {
        return appendRequestBackoffTimeoutInMillis;
    }

    public RaftAlgorithmConfig setAppendRequestBackoffTimeoutInMillis(long appendRequestBackoffTimeoutInMillis) {
        checkPositive(appendRequestBackoffTimeoutInMillis, "append request backoff timeout must be positive!");
        this.appendRequestBackoffTimeoutInMillis = appendRequestBackoffTimeoutInMillis;
        return this;
    }
}
