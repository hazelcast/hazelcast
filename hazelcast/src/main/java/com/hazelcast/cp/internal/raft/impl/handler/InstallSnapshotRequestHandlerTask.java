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

import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;
import static com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse.SKIPPED_FOLLOWER_NEXT_INDEX;
import static java.lang.String.format;

/**
 * Processes an {@link InstallSnapshotRequest} sent by the leader, responding with an
 * {@link AppendSuccessResponse} if the snapshot is successfully installed, or an
 * {@link AppendFailureResponse} if installation fails.
 * <p>
 * If snapshot chunks are missing, it may also trigger an {@link
 * com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse}.
 * <p>
 * For more details, refer to the <i>7 Log Compaction</i> section of the paper
 * <i>In Search of an Understandable Consensus Algorithm</i> by Diego Ongaro and John Ousterhout.
 *
 * @see InstallSnapshotRequest
 * @see com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse
 * @see AppendSuccessResponse
 * @see AppendFailureResponse
 */
public class InstallSnapshotRequestHandlerTask extends RaftNodeStatusAwareTask implements Runnable {

    private final InstallSnapshotRequest req;

    public InstallSnapshotRequestHandlerTask(RaftNodeImpl raftNode, InstallSnapshotRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        logger.fine("Received %s", req);

        RaftState state = raftNode.state();
        SnapshotChunk newChunk = req.snapshotChunk();

        // Reply false if term < currentTerm (§5.1)
        if (req.term() < state.term()) {
            logger.fine("Stale snapshot: %s received in current term: %d"
                    + ", snapshotIndex: %d", req, state.term(), newChunk.index());

            raftNode.send(new AppendFailureResponse(localMember(), state.term(), newChunk.index() + 1,
                    req.flowControlSequenceNumber(), SKIPPED_FOLLOWER_NEXT_INDEX), req.leader());
            return;
        }

        // Transform into follower if a newer term is seen or another node wins the election of the current term
        if (req.term() > state.term() || state.role() != FOLLOWER) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

            logger.info("Demoting to FOLLOWER from current role: " + state.role() + ", term: " + state.term()
                    + " to new term: " + req.term() + " and leader: " + req.leader());

            raftNode.toFollower(req.term());
        }

        logger.info(format("Received snapshot chunk from leaderUuid=%s, %s",
                req.leader().getUuid(), newChunk));

        if (!req.leader().equals(state.leader())) {
            logger.info("Setting leader: " + req.leader());
            raftNode.leader(req.leader());
        }

        raftNode.updateLastAppendEntriesTimestamp();

        raftNode.getChunkedSnapshotInstaller().processChunk(req);
    }
}
