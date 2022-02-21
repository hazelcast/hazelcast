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
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.state.CandidateState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.LeaderElectionTask;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;

/**
 * Handles {@link PreVoteResponse} sent by {@link PreVoteRequestHandlerTask}.
 * <p>
 * Initiates leader election by executing {@link LeaderElectionTask}
 * if majority grants vote for this pre-voting term.
 *
 * @see PreVoteResponse
 */
public class PreVoteResponseHandlerTask extends AbstractResponseHandlerTask {
    private final PreVoteResponse resp;

    public PreVoteResponseHandlerTask(RaftNodeImpl raftNode, PreVoteResponse response) {
        super(raftNode);
        this.resp = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = raftNode.state();

        if (state.role() != FOLLOWER) {
            logger.info("Ignored " + resp + ". We are not FOLLOWER anymore.");
            return;
        }

        if (resp.term() < state.term()) {
            logger.warning("Stale " + resp + " is received, current term: " + state.term());
            return;
        }

        CandidateState preCandidateState = state.preCandidateState();
        if (preCandidateState == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring " + resp + ". We are not interested in pre-votes anymore.");
            }
            return;
        }

        if (resp.granted() && preCandidateState.grantVote(resp.voter())) {
            logger.info("Pre-vote granted from " + resp.voter() + " for term: " + resp.term()
                    + ", number of votes: " + preCandidateState.voteCount() + ", majority: " + preCandidateState.majority());
        }

        if (preCandidateState.isMajorityGranted()) {
            logger.info("We have the majority during pre-vote phase. Let's start real election!");
            new LeaderElectionTask(raftNode, false).run();
        }
    }

    @Override
    protected RaftEndpoint sender() {
        return resp.voter();
    }
}
