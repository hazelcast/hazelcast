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

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.cluster.Endpoint;

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

    private final Map<Endpoint, FollowerState> followerStates = new HashMap<>();
    private final QueryState queryState = new QueryState();

    LeaderState(Collection<Endpoint> remoteMembers, long lastLogIndex) {
        for (Endpoint follower : remoteMembers) {
            followerStates.put(follower, new FollowerState(0L, lastLogIndex + 1));
        }
    }

    /**
     * Add a new follower with the leader's {@code lastLogIndex}.
     * Follower's {@code nextIndex} will be set to {@code lastLogIndex + 1}
     * and {@code matchIndex} to 0.
     */
    public void add(Endpoint follower, long lastLogIndex) {
        assert !followerStates.containsKey(follower) : "Already known follower " + follower;
        followerStates.put(follower, new FollowerState(0L, lastLogIndex + 1));
    }

    /**
     * Removes a follower from leader maintained state.
     */
    public void remove(Endpoint follower) {
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

    public FollowerState getFollowerState(Endpoint follower) {
        FollowerState followerState = followerStates.get(follower);
        assert followerState != null : "Unknown follower " + follower;
        return followerState;
    }

    public Map<Endpoint, FollowerState> getFollowerStates() {
        return followerStates;
    }

    public QueryState queryState() {
        return queryState;
    }

    public long queryRound() {
        return queryState.queryRound();
    }
}
