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
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.PreVoteTask;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;
import com.hazelcast.internal.util.Clock;

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

        // Reply false if last AppendEntries call was received less than election timeout ago (leader stickiness)
        if (raftNode.lastAppendEntriesTimestamp() > Clock.currentTimeMillis() - raftNode.getLeaderElectionTimeoutInMillis()) {
            logger.info("Rejecting " + req + " since received append entries recently.");
            raftNode.send(new PreVoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        RaftLog raftLog = state.log();
        if (raftLog.lastLogOrSnapshotTerm() > req.lastLogTerm()) {
            logger.info("Rejecting " + req + " since our last log term: " + raftLog.lastLogOrSnapshotTerm() + " is greater");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        if (raftLog.lastLogOrSnapshotTerm() == req.lastLogTerm() && raftLog.lastLogOrSnapshotIndex() > req.lastLogIndex()) {
            logger.info("Rejecting " + req + " since our last log index: " + raftLog.lastLogOrSnapshotIndex() + " is greater");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        logger.info("Granted pre-vote for " + req);
        raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), true), req.candidate());
    }
}
