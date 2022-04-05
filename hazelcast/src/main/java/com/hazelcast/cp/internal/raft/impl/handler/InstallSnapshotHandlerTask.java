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
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;

/**
 * Handles {@link InstallSnapshot} request sent by leader. Responds with
 * an {@link AppendSuccessResponse} if snapshot is installed, responds with
 * an {@link AppendFailureResponse} otherwise.
 * <p>
 * See <i>7 Log compaction</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see InstallSnapshot
 * @see AppendSuccessResponse
 * @see AppendFailureResponse
 */
public class InstallSnapshotHandlerTask extends RaftNodeStatusAwareTask implements Runnable {

    private final InstallSnapshot req;

    public InstallSnapshotHandlerTask(RaftNodeImpl raftNode, InstallSnapshot req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        if (logger.isFineEnabled()) {
            logger.fine("Received " + req);
        }

        RaftState state = raftNode.state();
        SnapshotEntry snapshot = req.snapshot();

        // Reply false if term < currentTerm (§5.1)
        if (req.term() < state.term()) {
            if (logger.isFineEnabled()) {
                logger.warning("Stale snapshot: " + req + " received in current term: " + state.term());
            }

            raftNode.send(new AppendFailureResponse(localMember(), state.term(), snapshot.index() + 1), req.leader());
            return;
        }

        // Transform into follower if a newer term is seen or another node wins the election of the current term
        if (req.term() > state.term() || state.role() != FOLLOWER) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

            logger.info("Demoting to FOLLOWER from current role: " + state.role() + ", term: " + state.term()
                    + " to new term: " + req.term() + " and leader: " + req.leader());

            raftNode.toFollower(req.term());
        }

        if (!req.leader().equals(state.leader())) {
            logger.info("Setting leader: " + req.leader());
            raftNode.leader(req.leader());
        }

        raftNode.updateLastAppendEntriesTimestamp();

        if (raftNode.installSnapshot(snapshot)) {
            raftNode.send(new AppendSuccessResponse(localMember(), req.term(), snapshot.index(), req.queryRound()), req.leader());
        }
    }
}
