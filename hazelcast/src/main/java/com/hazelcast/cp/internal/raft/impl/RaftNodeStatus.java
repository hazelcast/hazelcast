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

package com.hazelcast.cp.internal.raft.impl;

/**
 * Represents status of a {@link RaftNode} during its lifecycle.
 * <p>
 * Initially, a Raft node starts as {@link #ACTIVE}.
 */
public enum RaftNodeStatus {
    /**
     * Initial state of a node. When {@code ACTIVE} node operates normally.
     */
    ACTIVE,

    /**
     * During membership changes, node statuses become
     * {@code UPDATING_GROUP_MEMBER_LIST} and they apply requested change once
     * the entry is appended to the log. Once log is committed, if the related
     * node is the being removed from group, status becomes
     * {@link #STEPPED_DOWN}, otherwise {@link #ACTIVE}.
     */
    UPDATING_GROUP_MEMBER_LIST,

    /**
     * When a node is removed from the cluster after a membership change is
     * committed, its status becomes {@code STEPPED_DOWN}.
     */
    STEPPED_DOWN,

    /**
     * When a Raft group is being terminated, all nodes' statuses in that group
     * become {@code TERMINATING}. Once termination process is completed, then
     * statuses become {@link #TERMINATED}.
     */
    TERMINATING,

    /**
     * When a Raft group is terminated completely, all nodes' statuses in
     * that group become {@code TERMINATED}.
     */
    TERMINATED

}
