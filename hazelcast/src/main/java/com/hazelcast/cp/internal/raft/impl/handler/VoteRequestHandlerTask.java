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
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.LeaderElectionTask;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;
import com.hazelcast.internal.util.Clock;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;

/**
 * Handles {@link VoteRequest} sent by a candidate. Responds with
 * a {@link VoteResponse} to the sender. Leader election is initiated by
 * {@link LeaderElectionTask}.
 * <p>
 * See <i>5.2 Leader election</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see VoteRequest
 * @see VoteResponse
 * @see LeaderElectionTask
 */
public class VoteRequestHandlerTask extends RaftNodeStatusAwareTask implements Runnable {
    private final VoteRequest req;

    public VoteRequestHandlerTask(RaftNodeImpl raftNode, VoteRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    // Justification: It is easier to follow the RequestVoteRPC logic in a single method
    protected void innerRun() {
        RaftState state = raftNode.state();
        RaftEndpoint localMember = localMember();

        // Reply false if last AppendEntries call was received less than election timeout ago (leader stickiness)
        // (Raft thesis - Section 4.2.3) This check conflicts with the leadership transfer mechanism,
        // in which a server legitimately starts an election without waiting an election timeout.
        // Those VoteRequest objects are marked with a special flag ("disruptive") to bypass leader stickiness.
        // Also if request comes from the current leader, then stickiness check is skipped.
        // Since current leader may have restarted by recovering its persistent state.
        long leaderElectionTimeoutDeadline = Clock.currentTimeMillis() - raftNode.getLeaderElectionTimeoutInMillis();
        if (!req.isDisruptive() && raftNode.lastAppendEntriesTimestamp() > leaderElectionTimeoutDeadline
                && !req.candidate().equals(state.leader())) {
            logger.info("Rejecting " + req + " since received append entries recently.");
            raftNode.send(new VoteResponse(localMember, state.term(), false), req.candidate());
            return;
        }

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > req.term()) {
            logger.info("Rejecting " + req + " since current term: " + state.term() + " is bigger");
            raftNode.send(new VoteResponse(localMember, state.term(), false), req.candidate());
            return;
        }

        if (state.term() < req.term()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            if (state.role() != FOLLOWER) {
                logger.info("Demoting to FOLLOWER after " + req + " since current term: " + state.term() + " is smaller");
            } else {
                logger.info("Moving to new term: " + req.term() + " from current term: " + state.term() + " after " + req);
            }

            raftNode.toFollower(req.term());
        }

        if (state.leader() != null && !req.candidate().equals(state.leader())) {
            logger.warning("Rejecting " + req + " since we have a leader: " + state.leader());
            raftNode.send(new VoteResponse(localMember, req.term(), false), req.candidate());
            return;
        }

        if (state.votedFor() != null) {
            boolean granted = (req.candidate().equals(state.votedFor()));
            if (granted) {
                logger.info("Vote granted for duplicate" + req);
            } else {
                logger.info("Duplicate " + req + ". currently voted-for: " + state.votedFor());
            }
            raftNode.send(new VoteResponse(localMember, req.term(), granted), req.candidate());
            return;
        }

        RaftLog raftLog = state.log();
        if (raftLog.lastLogOrSnapshotTerm() > req.lastLogTerm()) {
            logger.info("Rejecting " + req + " since our last log term: " + raftLog.lastLogOrSnapshotTerm() + " is greater");
            raftNode.send(new VoteResponse(localMember, req.term(), false), req.candidate());
            return;
        }

        if (raftLog.lastLogOrSnapshotTerm() == req.lastLogTerm() && raftLog.lastLogOrSnapshotIndex() > req.lastLogIndex()) {
            logger.info("Rejecting " + req + " since our last log index: " + raftLog.lastLogOrSnapshotIndex() + " is greater");
            raftNode.send(new VoteResponse(localMember, req.term(), false), req.candidate());
            return;
        }

        logger.info("Granted vote for " + req);
        state.persistVote(req.term(), req.candidate());

        raftNode.send(new VoteResponse(localMember, req.term(), true), req.candidate());
    }
}
