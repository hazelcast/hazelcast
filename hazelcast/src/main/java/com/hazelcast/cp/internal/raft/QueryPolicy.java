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

package com.hazelcast.cp.internal.raft;

/**
 * Policy to query committed Raft state without appending a log entry
 * but in some cases giving up strong consistency guarantees.
 */
public enum QueryPolicy {

    /**
     * Query committed Raft state locally only on Raft group leader.
     * <p>
     * If the leader is split from the group and a new leader is elected
     * already, stale values can be read until it realizes the split.
     * <p>
     * This policy is likely to hit more recent data when compared to
     * {@link #ANY_LOCAL}.
     */
    LEADER_LOCAL,

    /**
     * Query committed Raft state locally on any Raft group member.
     * <p>
     * Reading stale value is possible when a follower lags behind the leader
     * or is split from rest of the group.
     * <p>
     * {@link #LEADER_LOCAL} should be preferred if it's important to read
     * up-to-date data mostly.
     */
    ANY_LOCAL,

    /**
     * Query Raft state in a linearizable manner, either by appending
     * a log entry to Raft log or using the algorithm defined in
     * <i>6.4 Processing read-only queries more efficiently</i>
     * section of Raft dissertation.
     */
    LINEARIZABLE
}
