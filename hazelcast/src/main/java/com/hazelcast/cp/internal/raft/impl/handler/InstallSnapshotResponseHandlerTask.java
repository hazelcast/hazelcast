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
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse;
import com.hazelcast.cp.internal.raft.impl.state.FollowerState;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.QueryState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;

/**
 * Handles an {@link InstallSnapshotResponse} sent by a follower,
 * indicating the snapshot chunks the follower is missing.
 * <br>
 * This operation is applicable only when {@code RaftState#role()} is {@code LEADER}.
 *
 * @see InstallSnapshotResponse
 */
public class InstallSnapshotResponseHandlerTask extends AbstractResponseHandlerTask implements Runnable {

    private final InstallSnapshotResponse response;

    public InstallSnapshotResponseHandlerTask(RaftNodeImpl raftNode,
                                              InstallSnapshotResponse response) {
        super(raftNode);
        this.response = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = raftNode.state();

        if (state.role() != LEADER) {
            logger.warning("Ignored " + response + ". We are not LEADER anymore.");
            return;
        }

        assert response.term() <= state.term() : "Invalid "
                + response + " for current term: " + state.term();

        logger.fine("Received %s", response);

        RaftEndpoint follower = response.follower();
        LeaderState leaderState = state.leaderState();
        QueryState queryState = leaderState.queryState();

        if (queryState.tryAck(response.queryRound(), follower)) {
            logger.fine("Ack from %s for query round: %d", follower, response.queryRound());
        }
        raftNode.tryRunQueries();

        FollowerState followerState = leaderState.getFollowerState(sender());
        followerState.appendRequestAckReceived(response.flowControlSequenceNumber());

        raftNode.sendNextSnapshotChunk(sender(), response.snapshotIndex(), response.requestedChunkNumber());
    }

    @Override
    protected RaftEndpoint sender() {
        return response.follower();
    }
}
