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

package com.hazelcast.cp.internal.raft.impl;

/**
 * Represents status of a {@link RaftNode} during its lifecycle.
 * <p>
 * Initially, a Raft node starts as {@link #ACTIVE}.
 */
public enum RaftNodeStatus {

    /**
     * Initial state of a Raft node before it is started.
     */
    INITIAL,

    /**
     * The normal operation mode of Raft node.
     */
    ACTIVE,

    /**
     * During membership changes, Raft node statuses become
     * {@code UPDATING_GROUP_MEMBER_LIST} and they apply requested change once
     * the entry is appended to the log. Once log is committed, if the related
     * Raft node is the being removed from group, status becomes
     * {@link #STEPPED_DOWN}, otherwise {@link #ACTIVE}.
     */
    UPDATING_GROUP_MEMBER_LIST,

    /**
     * When a Raft node is removed from the cluster after a membership change
     * is committed, its status becomes {@code STEPPED_DOWN}.
     */
    STEPPED_DOWN,

    /**
     * When a Raft group is being destroyed, all Raft nodes' statuses in that
     * group become {@code TERMINATING}. Once the Raft group destroy process
     * is completed, then the statuses become {@link #TERMINATED}.
     */
    TERMINATING,

    /**
     * When a Raft group is destroyed, all Raft nodes' statuses in that group
     * become {@code TERMINATED} after the destroy process is done. Moreover,
     * a Raft node can move to {@code TERMINATED} status if the Hazelcast node
     * containing this Raft node is shutting down.
     */
    TERMINATED

}
