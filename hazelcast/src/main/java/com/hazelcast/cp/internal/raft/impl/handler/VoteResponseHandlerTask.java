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

package com.hazelcast.cp.internal.raft.impl.handler;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftRole;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.state.CandidateState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.CANDIDATE;

/**
 * Handles {@link VoteResponse} sent by {@link VoteRequestHandlerTask}.
 * <p>
 * Changes node to {@link RaftRole#LEADER} if if majority of the nodes
 * grants vote for this term via {@link RaftState#toLeader()}.
 * <p>
 * Appends a no-op entry if
 * {@link RaftIntegration#getAppendedEntryOnLeaderElection()} is enabled.
 * <p>
 * See <i>5.2 Leader election</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see VoteResponse
 * @see VoteRequest
 */
public class VoteResponseHandlerTask extends AbstractResponseHandlerTask {
    private final VoteResponse resp;

    public VoteResponseHandlerTask(RaftNodeImpl raftNode, VoteResponse response) {
        super(raftNode);
        this.resp = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = raftNode.state();

        if (state.role() != CANDIDATE) {
            logger.info("Ignored " + resp + ". We are not CANDIDATE anymore.");
            return;
        }

        if (resp.term() > state.term()) {
            logger.info("Demoting to FOLLOWER from current term: " + state.term() + " to new term: " + resp.term()
                    + " after " + resp);
            raftNode.toFollower(resp.term());
            return;
        }

        if (resp.term() < state.term()) {
            logger.warning("Stale " + resp + " is received, current term: " + state.term());
            return;
        }

        CandidateState candidateState = state.candidateState();
        if (resp.granted() && candidateState.grantVote(resp.voter())) {
            logger.info("Vote granted from " + resp.voter() + " for term: " + state.term()
                    + ", number of votes: " + candidateState.voteCount() + ", majority: " + candidateState.majority());
        }

        if (candidateState.isMajorityGranted()) {
            logger.info("We are the LEADER!");
            raftNode.toLeader();
        }
    }

    @Override
    protected RaftEndpoint sender() {
        return resp.voter();
    }
}
