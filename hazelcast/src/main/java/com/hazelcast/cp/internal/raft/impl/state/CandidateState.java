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

import java.util.HashSet;
import java.util.Set;

/**
 * Mutable state maintained by each candidate during pre-voting &amp; voting phases
 */
public class CandidateState {

    private final int majority;
    private final Set<RaftEndpoint> voters = new HashSet<RaftEndpoint>();

    CandidateState(int majority) {
        this.majority = majority;
    }

    /**
     * Persists vote for the endpoint during election.
     * This method is idempotent, multiple votes from the same point are
     * counted only once.
     *
     * @return false if endpoint is already voted, true otherwise
     */
    public boolean grantVote(RaftEndpoint address) {
        return voters.add(address);
    }

    /**
     * Returns the number of expected majority of the votes.
     */
    public int majority() {
        return majority;
    }

    /**
     * Returns current granted number of the votes.
     */
    public int voteCount() {
        return voters.size();
    }

    /**
     * Returns true if majority of the votes are granted, false otherwise.
     */
    public boolean isMajorityGranted() {
        return voteCount() >= majority();
    }
}
