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

import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;

/**
 * PreVoteTimeoutTask is scheduled by {@link PreVoteTask} to trigger pre-voting
 * again if this node is still a follower and a leader is not available after
 * leader election timeout.
 */
public class PreVoteTimeoutTask extends RaftNodeStatusAwareTask implements Runnable {

    private int term;

    PreVoteTimeoutTask(RaftNodeImpl raftNode, int term) {
        super(raftNode);
        this.term = term;
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();
        // Remove previously set preCandidateState.
        // Since it's now obsolete,
        // either a new pre-vote round will begin
        // or pre-vote phase will cease.
        state.removePreCandidateState();

        if (state.role() != FOLLOWER) {
            return;
        }
        logger.fine("Pre-vote for term: " + state.term() + " has timed out!");
        new PreVoteTask(raftNode, term).run();
    }
}
