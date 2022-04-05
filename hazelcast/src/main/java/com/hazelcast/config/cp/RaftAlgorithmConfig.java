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

package com.hazelcast.config.cp;

import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configuration for Hazelcast's implementation of the Raft consensus algorithm
 */
public class RaftAlgorithmConfig {

    /**
     * The default leader election timeout in millis.
     * See {@link #leaderElectionTimeoutInMillis}.
     */
    public static final long DEFAULT_LEADER_ELECTION_TIMEOUT_IN_MILLIS = 2000;

    /**
     * The default leader heartbeat period in millis.
     * See {@link #leaderHeartbeatPeriodInMillis}.
     */
    public static final long DEFAULT_LEADER_HEARTBEAT_PERIOD_IN_MILLIS = 5000;

    /**
     * The default maximum append request entry count.
     * See {@link #appendRequestMaxEntryCount}.
     */
    public static final int DEFAULT_APPEND_REQUEST_MAX_ENTRY_COUNT = 100;

    /**
     * The default commit index advance count on Raft log to take a snapshot.
     * See {@link #commitIndexAdvanceCountToSnapshot}.
     */
    public static final int DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_SNAPSHOT = 10000;

    /**
     * The default maximum allowed uncommitted entry count.
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
     * majority of the votes in time, a new leader election round is initiated.
     */
    private long leaderElectionTimeoutInMillis = DEFAULT_LEADER_ELECTION_TIMEOUT_IN_MILLIS;

    /**
     * Duration in milliseconds for a Raft leader node to send periodic
     * heartbeat messages to its followers in order to denote its liveliness.
     * Periodic heartbeat messages are actually append entries requests and
     * can contain log entries for the lagging followers. If a too small value
     * is set, heartbeat messages are sent from Raft leaders to followers too
     * frequently and it can cause an unnecessary usage of CPU and network.
     */
    private long leaderHeartbeatPeriodInMillis = DEFAULT_LEADER_HEARTBEAT_PERIOD_IN_MILLIS;

    /**
     * Maximum number of missed Raft leader heartbeats for a follower to
     * trigger a new leader election round. For instance, if
     * {@link #leaderHeartbeatPeriodInMillis} is 1 second and this value is set
     * to 5, then a follower triggers a new leader election round if 5 seconds
     * pass after the last heartbeat message of the current Raft leader node.
     * If this duration is too small, new leader election rounds can be
     * triggered unnecessarily if the current Raft leader temporarily slows
     * down or a network congestion occurs. If it is too large, it takes longer
     * to detect failures of Raft leaders.
     */
    private int maxMissedLeaderHeartbeatCount = DEFAULT_MAX_MISSED_LEADER_HEARTBEAT_COUNT;

    /**
     * Maximum number of Raft log entries that can be sent as a batch
     * in a single append entries request. In Hazelcast's Raft consensus
     * algorithm implementation, a Raft leader maintains a separate replication
     * pipeline for each follower. It sends a new batch of Raft log entries to
     * a follower after the follower acknowledges the last append entries
     * request sent by the leader.
     */
    private int appendRequestMaxEntryCount = DEFAULT_APPEND_REQUEST_MAX_ENTRY_COUNT;

    /**
     * Number of new commits to initiate a new snapshot after the last snapshot
     * taken by the local Raft node. This value must be configured wisely as it
     * effects performance of the system in multiple ways. If a small value is
     * set, it means that snapshots are taken too frequently and Raft nodes keep
     * a very short Raft log. If snapshots are large and CP Subsystem
     * Persistence is enabled, this can create an unnecessary overhead on IO
     * performance. Moreover, a Raft leader can send too many snapshots to
     * followers and this can create an unnecessary overhead on network.
     * On the other hand, if a very large value is set, it can create a memory
     * overhead since Raft log entries are going to be kept in memory until
     * the next snapshot.
     */
    private int commitIndexAdvanceCountToSnapshot = DEFAULT_COMMIT_INDEX_ADVANCE_COUNT_TO_SNAPSHOT;

    /**
     * Maximum number of uncommitted log entries in the leader's Raft log
     * before temporarily rejecting new requests of callers. Since Raft leaders
     * send log entries to followers in batches, they accumulate incoming
     * requests in order to improve the throughput. You can configure this
     * field by considering your degree of concurrency in your callers.
     * For instance, if you have at most 1000 threads sending requests to
     * a Raft leader, you can set this field to 1000 so that callers do not
     * get retry responses unnecessarily.
     */
    private int uncommittedEntryCountToRejectNewAppends = DEFAULT_UNCOMMITTED_ENTRY_COUNT_TO_REJECT_NEW_APPENDS;

    /**
     * Timeout duration in milliseconds to apply backoff on append entries
     * requests. After a Raft leader sends an append entries request to
     * a follower, it will not send a subsequent append entries request either
     * until the follower responds or this timeout occurs. Backoff durations
     * are increased exponentially if followers remain unresponsive.
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
        checkPositive("leaderElectionTimeoutInMillis", leaderElectionTimeoutInMillis);
        this.leaderElectionTimeoutInMillis = leaderElectionTimeoutInMillis;
        return this;
    }

    public long getLeaderHeartbeatPeriodInMillis() {
        return leaderHeartbeatPeriodInMillis;
    }

    public RaftAlgorithmConfig setLeaderHeartbeatPeriodInMillis(long leaderHeartbeatPeriodInMillis) {
        checkPositive("leaderHeartbeatPeriodInMillis", leaderHeartbeatPeriodInMillis);
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
        checkPositive("appendRequestBackoffTimeoutInMillis", appendRequestBackoffTimeoutInMillis);
        this.appendRequestBackoffTimeoutInMillis = appendRequestBackoffTimeoutInMillis;
        return this;
    }

    @Override
    public String toString() {
        return "RaftAlgorithmConfig{" + "leaderElectionTimeoutInMillis=" + leaderElectionTimeoutInMillis
                + ", leaderHeartbeatPeriodInMillis=" + leaderHeartbeatPeriodInMillis + ", maxMissedLeaderHeartbeatCount="
                + maxMissedLeaderHeartbeatCount + ", appendRequestMaxEntryCount=" + appendRequestMaxEntryCount
                + ", commitIndexAdvanceCountToSnapshot=" + commitIndexAdvanceCountToSnapshot
                + ", uncommittedEntryCountToRejectNewAppends=" + uncommittedEntryCountToRejectNewAppends
                + ", appendRequestBackoffTimeoutInMillis=" + appendRequestBackoffTimeoutInMillis + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RaftAlgorithmConfig that = (RaftAlgorithmConfig) o;
        return leaderElectionTimeoutInMillis == that.leaderElectionTimeoutInMillis
                && leaderHeartbeatPeriodInMillis == that.leaderHeartbeatPeriodInMillis
                && maxMissedLeaderHeartbeatCount == that.maxMissedLeaderHeartbeatCount
                && appendRequestMaxEntryCount == that.appendRequestMaxEntryCount
                && commitIndexAdvanceCountToSnapshot == that.commitIndexAdvanceCountToSnapshot
                && uncommittedEntryCountToRejectNewAppends == that.uncommittedEntryCountToRejectNewAppends
                && appendRequestBackoffTimeoutInMillis == that.appendRequestBackoffTimeoutInMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderElectionTimeoutInMillis, leaderHeartbeatPeriodInMillis, maxMissedLeaderHeartbeatCount,
                appendRequestMaxEntryCount, commitIndexAdvanceCountToSnapshot, uncommittedEntryCountToRejectNewAppends,
                appendRequestBackoffTimeoutInMillis);
    }
}
