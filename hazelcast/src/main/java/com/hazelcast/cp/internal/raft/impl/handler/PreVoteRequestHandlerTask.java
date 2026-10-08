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

package com.hazelcast.cp.internal.raft.impl.handler;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.PreVoteTask;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;

/**
 * Handles {@link PreVoteRequest} and responds to the sender
 * with a {@link PreVoteResponse}. Pre-voting is initiated by
 * {@link PreVoteTask}.
 * <p>
 * Grants vote or rejects the request as if responding to
 * a {@link com.hazelcast.cp.internal.raft.impl.dto.VoteRequest}
 * but differently Raft state is not mutated/updated, this task is
 * completely read-only.
 *
 * @see PreVoteRequest
 * @see PreVoteResponse
 * @see PreVoteTask
 */
public class PreVoteRequestHandlerTask extends RaftNodeStatusAwareTask implements Runnable {
    private final PreVoteRequest req;

    public PreVoteRequestHandlerTask(RaftNodeImpl raftNode, PreVoteRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();
        RaftEndpoint localEndpoint = localMember();

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > req.nextTerm()) {
            logger.info("Rejecting " + req + " since current term: " + state.term() + " is bigger");
            raftNode.send(new PreVoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        // Reply false if the leader is available (leader stickiness)
        if (raftNode.isLeaderAvailable()) {
            logger.info("Rejecting " + req + " since received append entries recently.");
            raftNode.send(new PreVoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        RaftLog raftLog = state.log();
        long localLastTerm = raftLog.lastLogOrSnapshotTerm();
        long localLastIndex = raftLog.lastLogOrSnapshotIndex();

        if (localLastTerm > req.lastLogTerm()) {
            logger.info("Rejecting " + req + " since our last log term: " + localLastTerm + " is greater");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        if (localLastTerm == req.lastLogTerm() && localLastIndex > req.lastLogIndex()) {
            logger.info("Rejecting " + req + " since our last log index: " + localLastIndex + " is greater");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        // Optimization for auto-step-down-leaders:
        // At this point, candidate's log is not worse than ours.
        // If logs are exactly equal, and the candidate is an auto-step-down member
        // while we are a normal (leader-capable) member, then reject the prevote
        // so a leader-capable node is preferred in tie situations.
        boolean logsEqual = localLastTerm == req.lastLogTerm()
                        && localLastIndex == req.lastLogIndex();
        if (logsEqual && req.isAutoStepDownLeader()
                && !raftNode.autoStepDownWhenLeaderForGroup()) {
            logger.fine("Rejecting " + req + " from auto step-down candidate: " + req.candidate()
                    + " with equal log; preferring non-auto-step-down member as leader");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        logger.info("Granted pre-vote for " + req);
        raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), true), req.candidate());
    }
}
