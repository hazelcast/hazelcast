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
 * Represents role of a member in a Raft group.
 * <p>
 * At any given time each member is in one of three roles: {@link #LEADER},
 * {@link #FOLLOWER}, or {@link #CANDIDATE}.
 * <p>
 * Normally, there is exactly one leader and all of the other members are
 * followers. But during leader election some of the followers can become
 * candidates.
 */
public enum RaftRole {

    /**
     * Followers are passive, they issue no requests on their own
     * but simply respond to requests from leaders and candidates.
     */
    FOLLOWER,

    /**
     * Candidate is used to elect a new leader. When a candidate wins votes of
     * majority, it becomes the leader. Otherwise it becomes follower
     * or candidate again.
     */
    CANDIDATE,

    /**
     * The leader handles all client requests (append entry, membership change,
     * etc.) and replicates them to followers.
     */
    LEADER
}
