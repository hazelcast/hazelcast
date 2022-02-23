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
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.state.FollowerState;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;

/**
 * Handles {@link AppendFailureResponse} sent by
 * {@link AppendRequestHandlerTask} after an append-entries request
 * or {@link InstallSnapshotHandlerTask} after an install snapshot request.
 * <p>
 * Decrements {@code nextIndex} of the follower by 1 if the response is valid.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendSuccessResponse
 * @see AppendFailureResponse
 */
public class AppendFailureResponseHandlerTask extends AbstractResponseHandlerTask {

    private final AppendFailureResponse resp;

    public AppendFailureResponseHandlerTask(RaftNodeImpl raftNode, AppendFailureResponse response) {
        super(raftNode);
        this.resp = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = raftNode.state();

        if (state.role() != LEADER) {
            logger.warning(resp + " is ignored since we are not LEADER.");
            return;
        }

        if (resp.term() > state.term()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.info("Demoting to FOLLOWER after " + resp + " from current term: " + state.term());
            raftNode.toFollower(resp.term());
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Received " + resp);
        }

        if (updateNextIndex(state)) {
            raftNode.sendAppendRequest(resp.follower());
        }
    }

    private boolean updateNextIndex(RaftState state) {
        LeaderState leaderState = state.leaderState();
        FollowerState followerState = leaderState.getFollowerState(resp.follower());

        long nextIndex = followerState.nextIndex();
        long matchIndex = followerState.matchIndex();

        if (resp.expectedNextIndex() == nextIndex) {
            // Received a response for the last append request. Resetting the flag...
            followerState.appendRequestAckReceived();

            // this is the response of the request I have sent for this nextIndex
            nextIndex--;
            if (nextIndex <= matchIndex) {
                logger.severe("Cannot decrement next index: " + nextIndex + " below match index: " + matchIndex
                        + " for follower: " + resp.follower());
                return false;
            }

            if (logger.isFineEnabled()) {
                logger.fine("Updating next index: " + nextIndex + " for follower: " + resp.follower());
            }
            followerState.nextIndex(nextIndex);
            return true;
        }

        return false;
    }

    @Override
    protected RaftEndpoint sender() {
        return resp.follower();
    }
}
