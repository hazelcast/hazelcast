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

package com.hazelcast.cp.internal.raft.impl.task;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;

import java.util.Collection;

/**
 * PreVoteTask is scheduled when current leader is null, unreachable
 * or unknown. It sends {@link PreVoteRequest}s to other members to receive
 * make sure majority is reachable and ready to elect a new leader.
 * <p>
 * Also a {@link PreVoteTimeoutTask} is scheduled with a
 * {@link RaftNodeImpl#getLeaderElectionTimeoutInMillis()} delay to trigger
 * pre-voting if a leader is not available yet.
 */
public class PreVoteTask extends RaftNodeStatusAwareTask implements Runnable {

    private int term;

    public PreVoteTask(RaftNodeImpl raftNode, int term) {
        super(raftNode);
        this.term = term;
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.fine("No new pre-vote phase, we already have a LEADER: " + state.leader());
            return;
        } else if (state.term() != term) {
            logger.fine("No new pre-vote phase for term= " + term + " because of new term: " + state.term());
            return;
        }

        Collection<RaftEndpoint> remoteMembers = state.remoteMembers();
        if (remoteMembers.isEmpty()) {
            logger.fine("Remote members is empty. No need for pre-voting.");
            return;
        }

        state.initPreCandidateState();
        int nextTerm = state.term() + 1;
        RaftLog log = state.log();
        PreVoteRequest request = new PreVoteRequest(localMember(), nextTerm, log.lastLogOrSnapshotTerm(),
                log.lastLogOrSnapshotIndex());

        logger.info("Pre-vote started for next term: " + request.nextTerm() + ", last log index: " + request.lastLogIndex()
                + ", last log term: " + request.lastLogTerm());
        raftNode.printMemberState();

        for (RaftEndpoint endpoint : remoteMembers) {
            raftNode.send(request, endpoint);
        }

        schedulePreVoteTimeout();
    }

    private void schedulePreVoteTimeout() {
        raftNode.schedule(new PreVoteTimeoutTask(raftNode, term), raftNode.getLeaderElectionTimeoutInMillis());
    }
}
