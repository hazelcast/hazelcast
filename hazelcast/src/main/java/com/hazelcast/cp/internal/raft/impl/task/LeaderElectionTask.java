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
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.handler.PreVoteResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;

/**
 * LeaderElectionTask is scheduled when current leader is null, unreachable
 * or unknown by {@link PreVoteResponseHandlerTask} after a follower receives
 * votes from at least majority. Local member becomes a candidate using
 * {@link RaftState#toCandidate(boolean)} and sends {@link VoteRequest}s to
 * other members.
 * <p>
 * Also a {@link LeaderElectionTimeoutTask} is scheduled with a
 * {@link RaftNodeImpl#getLeaderElectionTimeoutInMillis()} delay to trigger
 * leader election if one is not elected yet.
 */
public class LeaderElectionTask extends RaftNodeStatusAwareTask implements Runnable {

    private boolean disruptive;

    public LeaderElectionTask(RaftNodeImpl raftNode, boolean disruptive) {
        super(raftNode);
        this.disruptive = disruptive;
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.warning("No new election round, we already have a LEADER: " + state.leader());
            return;
        }

        VoteRequest request = state.toCandidate(disruptive);
        logger.info("Leader election started for term: " + request.term() + ", last log index: " + request.lastLogIndex()
                + ", last log term: " + request.lastLogTerm());
        raftNode.printMemberState();

        for (RaftEndpoint endpoint : state.remoteMembers()) {
            raftNode.send(request, endpoint);
        }

        scheduleLeaderElectionTimeout();
    }

    private void scheduleLeaderElectionTimeout() {
        raftNode.schedule(new LeaderElectionTimeoutTask(raftNode), raftNode.getLeaderElectionTimeoutInMillis());
    }
}
