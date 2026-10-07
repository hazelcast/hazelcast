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

package com.hazelcast.internal.metrics;

public abstract class CPMetricDescriptorConstants {
    // ===[CP SUBSYSTEM]================================================
    public static final String CP_PREFIX_RAFT = "raft";
    public static final String CP_PREFIX_RAFT_GROUP = "raft.group";
    public static final String CP_PREFIX_RAFT_METADATA = "raft.metadata";
    public static final String CP_DISCRIMINATOR_GROUPID = "groupId";
    public static final String CP_TAG_NAME = "name";
    public static final String CP_TAG_GROUP = "group";
    public static final String CP_METRIC_METADATA_RAFT_GROUP_MANAGER_GROUPS = "groups";
    public static final String CP_METRIC_METADATA_RAFT_GROUP_MANAGER_ACTIVE_MEMBERS = "activeMembers";
    public static final String CP_METRIC_METADATA_RAFT_GROUP_MANAGER_ACTIVE_MEMBERS_COMMIT_INDEX = "activeMembersCommitIndex";
    public static final String CP_METRIC_RAFT_NODE_TERM = "term";
    public static final String CP_METRIC_RAFT_NODE_COMMIT_INDEX = "commitIndex";
    public static final String CP_METRIC_RAFT_NODE_LAST_APPLIED = "lastApplied";
    public static final String CP_METRIC_RAFT_NODE_LAST_LOG_TERM = "lastLogTerm";
    public static final String CP_METRIC_RAFT_NODE_SNAPSHOT_INDEX = "snapshotIndex";
    public static final String CP_METRIC_RAFT_NODE_LAST_LOG_INDEX = "lastLogIndex";
    public static final String CP_METRIC_RAFT_NODE_AVAILABLE_LOG_CAPACITY = "availableLogCapacity";
    public static final String CP_METRIC_RAFT_NODE_LEADERSHIP_AVERAGE_TIME_MS
            = "leadership.averageTimeMs";
    public static final String CP_METRIC_RAFT_NODE_ELECTED_LEADER_COUNT
            = "leadership.electedCount";
    public static final String CP_METRIC_RAFT_NODE_SNAPSHOT_BUILD_DURATION_MS
            = "snapshotBuild.durationMs";
    public static final String CP_METRIC_RAFT_NODE_SNAPSHOT_BUILD_COUNT
            = "snapshotBuild.count";
    public static final String CP_METRIC_RAFT_NODE_SNAPSHOT_TRANSFER_DURATION_MS
            = "snapshotTransfer.durationMs";
    public static final String CP_METRIC_RAFT_NODE_SNAPSHOT_TRANSFER_COUNT
            = "snapshotTransfer.count";
    public static final String CP_METRIC_RAFT_SERVICE_NODES = "nodes";
    public static final String CP_METRIC_RAFT_SERVICE_DESTROYED_GROUP_IDS = "destroyedGroupIds";
    public static final String CP_METRIC_RAFT_SERVICE_TERMINATED_RAFT_NODE_GROUP_IDS = "terminatedRaftNodeGroupIds";
    public static final String CP_METRIC_RAFT_SERVICE_MISSING_MEMBERS = "missingMembers";
    public static final String CP_METRIC_SUMMARY_DESTROYED_COUNT = "destroyed.count";
    public static final String CP_METRIC_SUMMARY_LIVE_COUNT = "live.count";
    public static final String CP_METRIC_SESSION_COUNT = "sessionCount";
}
