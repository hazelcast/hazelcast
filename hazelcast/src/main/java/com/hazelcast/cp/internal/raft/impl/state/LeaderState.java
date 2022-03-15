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

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.internal.util.Clock;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Mutable state maintained by the leader of the Raft group. Leader keeps
 * a {@link FollowerState} object for each follower.
 *
 * @see FollowerState
 */
public class LeaderState {

    private final Map<RaftEndpoint, FollowerState> followerStates = new HashMap<>();
    private final QueryState queryState = new QueryState();
    private long flushedLogIndex;

    LeaderState(Collection<RaftEndpoint> remoteMembers, long lastLogIndex) {
        for (RaftEndpoint follower : remoteMembers) {
            followerStates.put(follower, new FollowerState(0L, lastLogIndex + 1));
        }
        flushedLogIndex = lastLogIndex;
    }

    /**
     * Add a new follower with the leader's {@code lastLogIndex}.
     * Follower's {@code nextIndex} will be set to {@code lastLogIndex + 1}
     * and {@code matchIndex} to 0.
     */
    public void add(RaftEndpoint follower, long lastLogIndex) {
        assert !followerStates.containsKey(follower) : "Already known follower " + follower;
        followerStates.put(follower, new FollowerState(0L, lastLogIndex + 1));
    }

    /**
     * Removes a follower from leader maintained state.
     */
    public void remove(RaftEndpoint follower) {
        FollowerState removed = followerStates.remove(follower);
        queryState.removeAck(follower);
        assert removed != null : "Unknown follower " + follower;
    }

    /**
     * Returns an array of match indices for all followers.
     * Additionally an empty slot is added at the end of indices array for leader itself.
     */
    public long[] matchIndices() {
        // Leader index is appended at the end of array in AppendSuccessResponseHandlerTask
        // That's why we add one more empty slot.
        long[] indices = new long[followerStates.size() + 1];
        int ix = 0;
        for (FollowerState state : followerStates.values()) {
            indices[ix++] = state.matchIndex();
        }
        return indices;
    }

    public FollowerState getFollowerState(RaftEndpoint follower) {
        FollowerState followerState = followerStates.get(follower);
        assert followerState != null : "Unknown follower " + follower;
        return followerState;
    }

    public Map<RaftEndpoint, FollowerState> getFollowerStates() {
        return followerStates;
    }

    public QueryState queryState() {
        return queryState;
    }

    public long queryRound() {
        return queryState.queryRound();
    }

    public void flushedLogIndex(long flushedLogIndex) {
        assert flushedLogIndex > this.flushedLogIndex;
        this.flushedLogIndex = flushedLogIndex;
    }

    public long flushedLogIndex() {
        return flushedLogIndex;
    }

    /**
     * Returns the earliest append response ack timestamp of the majority nodes
     */
    public long majorityAppendRequestAckTimestamp(int majority) {
        long[] ackTimes = new long[followerStates.size() + 1];
        int i = 0;
        ackTimes[i] = Clock.currentTimeMillis();
        for (FollowerState followerState : followerStates.values()) {
            ackTimes[++i] = followerState.appendRequestAckTimestamp();
        }

        Arrays.sort(ackTimes);

        return ackTimes[ackTimes.length - majority];
    }

}
