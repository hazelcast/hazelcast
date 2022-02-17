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
import com.hazelcast.cp.internal.raft.impl.state.QueryState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;

/**
 * Handles {@link AppendSuccessResponse} sent by
 * {@link AppendRequestHandlerTask} after an append-entries request or
 * {@link InstallSnapshotHandlerTask} after an install snapshot request.
 * <p>
 * Advances {@link RaftState#commitIndex()} according to {@code matchIndex}es
 * of followers.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendSuccessResponse
 * @see AppendFailureResponse
 */
public class AppendSuccessResponseHandlerTask extends AbstractResponseHandlerTask {
    private final AppendSuccessResponse resp;

    public AppendSuccessResponseHandlerTask(RaftNodeImpl raftNode, AppendSuccessResponse response) {
        super(raftNode);
        this.resp = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = raftNode.state();

        if (state.role() != LEADER) {
            logger.warning("Ignored " + resp + ". We are not LEADER anymore.");
            return;
        }

        assert resp.term() <= state.term() : "Invalid " + resp + " for current term: " + state.term();

        if (logger.isFineEnabled()) {
            logger.fine("Received " + resp);
        }

        if (updateFollowerIndices(state)) {
            if (!raftNode.tryAdvanceCommitIndex()) {
                trySendAppendRequest(state);
            }
        } else {
            raftNode.tryRunQueries();
        }

        checkIfQueryAckNeeded(state);
    }

    private boolean updateFollowerIndices(RaftState state) {
        // If successful: update nextIndex and matchIndex for follower (§5.3)

        RaftEndpoint follower = resp.follower();
        LeaderState leaderState = state.leaderState();
        FollowerState followerState = leaderState.getFollowerState(follower);
        QueryState queryState = leaderState.queryState();

        if (queryState.tryAck(resp.queryRound(), follower)) {
            if (logger.isFineEnabled()) {
                logger.fine("Ack from " + follower + " for query round: " + resp.queryRound());
            }
        }

        long matchIndex = followerState.matchIndex();
        long followerLastLogIndex = resp.lastLogIndex();

        if (followerLastLogIndex > matchIndex) {
            // Received a response for the last append request. Resetting the flag...
            followerState.appendRequestAckReceived();

            long newNextIndex = followerLastLogIndex + 1;
            followerState.matchIndex(followerLastLogIndex);
            followerState.nextIndex(newNextIndex);

            if (logger.isFineEnabled()) {
                logger.fine("Updated match index: " + followerLastLogIndex + " and next index: " + newNextIndex
                        + " for follower: " + follower);
            }

            return true;
        } else if (followerLastLogIndex == matchIndex) {
            // Received a response for the last append request. Resetting the flag...
            followerState.appendRequestAckReceived();
        } else if (logger.isFineEnabled()) {
            logger.fine("Will not update match index for follower: " + follower + ". follower last log index: "
                    + followerLastLogIndex + ", match index: " + matchIndex);
        }

        return false;
    }

    private void checkIfQueryAckNeeded(RaftState state) {
        QueryState queryState = state.leaderState().queryState();
        if (queryState.isAckNeeded(resp.follower(), state.majority())) {
            raftNode.sendAppendRequest(resp.follower());
        }
    }

    private void trySendAppendRequest(RaftState state) {
        long followerLastLogIndex = resp.lastLogIndex();
        if (state.log().lastLogOrSnapshotIndex() > followerLastLogIndex || state.commitIndex() == followerLastLogIndex) {
            // If the follower is still missing some log entries or has not learnt the latest commit index yet,
            // then send another append request.
            raftNode.sendAppendRequest(resp.follower());
        }
    }

    @Override
    protected RaftEndpoint sender() {
        return resp.follower();
    }
}
