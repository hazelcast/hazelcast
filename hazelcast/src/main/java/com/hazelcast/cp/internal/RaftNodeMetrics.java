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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.internal.raft.impl.RaftRole;
import com.hazelcast.internal.metrics.Probe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_NODE_AVAILABLE_LOG_CAPACITY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_NODE_COMMIT_INDEX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_NODE_LAST_APPLIED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_NODE_LAST_LOG_INDEX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_NODE_LAST_LOG_TERM;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_NODE_SNAPSHOT_INDEX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_NODE_TERM;

/**
 * Container object for single RaftNode metrics.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
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

    public RaftNodeMetrics(RaftRole role, int memberCount, int term, long commitIndex, long lastApplied,
            long lastLogTerm, long snapshotIndex, long lastLogIndex, long availableLogCapacity) {
        this.role = role;
        this.memberCount = memberCount;
        this.term = term;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.lastLogTerm = lastLogTerm;
        this.snapshotIndex = snapshotIndex;
        this.lastLogIndex = lastLogIndex;
        this.availableLogCapacity = availableLogCapacity;
    }
}
