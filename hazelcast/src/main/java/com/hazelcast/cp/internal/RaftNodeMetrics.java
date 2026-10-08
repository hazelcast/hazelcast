/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.internal.raft.impl.RaftRole;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeUnit;

import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_LEADERSHIP_AVERAGE_TIME_MS;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_AVAILABLE_LOG_CAPACITY;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_COMMIT_INDEX;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_LAST_APPLIED;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_LAST_LOG_INDEX;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_LAST_LOG_TERM;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_ELECTED_LEADER_COUNT;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_SNAPSHOT_BUILD_COUNT;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_SNAPSHOT_BUILD_DURATION_MS;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_SNAPSHOT_INDEX;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_SNAPSHOT_TRANSFER_COUNT;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_SNAPSHOT_TRANSFER_DURATION_MS;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_NODE_TERM;

/**
 * Container object for single RaftNode metrics.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class RaftNodeMetrics {

    public final RaftRole role;

    @Probe(name = "memberCount")
    public final int memberCount;

    @Probe(name = CP_METRIC_RAFT_NODE_TERM)
    public final int term;

    @Probe(name = CP_METRIC_RAFT_NODE_COMMIT_INDEX)
    public final long commitIndex;

    @Probe(name = CP_METRIC_RAFT_NODE_LAST_APPLIED)
    public final long lastApplied;

    @Probe(name = CP_METRIC_RAFT_NODE_LAST_LOG_TERM)
    public final long lastLogTerm;

    @Probe(name = CP_METRIC_RAFT_NODE_SNAPSHOT_INDEX)
    public final long snapshotIndex;

    @Probe(name = CP_METRIC_RAFT_NODE_LAST_LOG_INDEX)
    public final long lastLogIndex;

    @Probe(name = CP_METRIC_RAFT_NODE_AVAILABLE_LOG_CAPACITY)
    public final long availableLogCapacity;

    @Probe(name = CP_METRIC_RAFT_NODE_LEADERSHIP_AVERAGE_TIME_MS,
            unit = ProbeUnit.MS)
    public final long averageTimeAsLeaderMs;

    @Probe(name = CP_METRIC_RAFT_NODE_ELECTED_LEADER_COUNT,
            unit = ProbeUnit.COUNT)
    public final long electedLeaderCount;

    @Probe(name = CP_METRIC_RAFT_NODE_SNAPSHOT_BUILD_DURATION_MS,
            unit = ProbeUnit.MS)
    public final long snapshotBuildDurationMs;

    @Probe(name = CP_METRIC_RAFT_NODE_SNAPSHOT_BUILD_COUNT,
            unit = ProbeUnit.COUNT)
    public final long snapshotBuildCount;

    @Probe(name = CP_METRIC_RAFT_NODE_SNAPSHOT_TRANSFER_DURATION_MS,
            unit = ProbeUnit.MS)
    public final long snapshotTransferDurationMs;

    @Probe(name = CP_METRIC_RAFT_NODE_SNAPSHOT_TRANSFER_COUNT,
            unit = ProbeUnit.COUNT)
    public final long snapshotTransferCount;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public RaftNodeMetrics(RaftRole role, int memberCount, int term, long commitIndex, long lastApplied,
                           long lastLogTerm, long snapshotIndex, long lastLogIndex, long availableLogCapacity,
                           RaftState.LeadershipStats leadershipStats, long snapshotBuildDurationMs, long snapshotBuildCount,
                           long snapshotTransferDurationMs, long snapshotTransferCount) {
        this.role = role;
        this.memberCount = memberCount;
        this.term = term;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.lastLogTerm = lastLogTerm;
        this.snapshotIndex = snapshotIndex;
        this.lastLogIndex = lastLogIndex;
        this.availableLogCapacity = availableLogCapacity;
        this.averageTimeAsLeaderMs = leadershipStats.getAvgTimeAsLeaderMs();
        this.electedLeaderCount = leadershipStats.getElectedLeaderCount();
        this.snapshotBuildDurationMs = snapshotBuildDurationMs;
        this.snapshotBuildCount = snapshotBuildCount;
        this.snapshotTransferDurationMs = snapshotTransferDurationMs;
        this.snapshotTransferCount = snapshotTransferCount;
    }
}
