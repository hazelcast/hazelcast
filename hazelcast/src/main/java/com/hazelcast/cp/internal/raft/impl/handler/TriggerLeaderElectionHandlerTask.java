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

import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.LeaderElectionTask;
import com.hazelcast.cp.internal.raft.impl.task.LeadershipTransferTask;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;

/**
 * Handles {@link TriggerLeaderElection} sent by {@link LeadershipTransferTask}
 * <p>
 * Initiates leader election by executing {@link LeaderElectionTask}
 * if this node accepts the requester as the leader and the local Raft log is
 * up-to-date with the leader's Raft log.
 *
 * @see TriggerLeaderElection
 */
public class TriggerLeaderElectionHandlerTask extends RaftNodeStatusAwareTask implements Runnable {

    private final TriggerLeaderElection req;

    public TriggerLeaderElectionHandlerTask(RaftNodeImpl raftNode, TriggerLeaderElection req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        if (logger.isFineEnabled()) {
            logger.fine("Received " + req);
        }

        RaftState state = raftNode.state();

        // Verify the term and the leader.
        // If the requesting leader is legit,
        // I will eventually accept it as the leader with a periodic append request.
        // Once I pass this if block, I know that I am follower and my log is same
        // with the leader's log.
        if (!(req.term() == state.term() && req.leader().equals(state.leader()))) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring " + req + " since term: " + state.term() + " and leader: " + state.leader());
            }

            return;
        }

        // Verify the last log entry
        LogEntry entry = state.log().lastLogOrSnapshotEntry();
        if (!(entry.index() == req.lastLogIndex() && entry.term() == req.lastLogTerm())) {
            if (logger.isFineEnabled()) {
                logger.fine("Could not accept leadership transfer because local Raft log is not same with the current leader. "
                        + "Last log entry: " + entry + ", request: " + req);
            }

            return;
        }

        // I will send a disruptive VoteRequest to bypass leader stickiness
        logger.info("Starting a new leader election since the current leader: " + req.leader() + " in term: " + req.term()
                + " asked for a leadership transfer!");
        state.leader(null);
        new LeaderElectionTask(raftNode, true).run();
    }

}
