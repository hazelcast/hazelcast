/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Endpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.FollowerState;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.QueryState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.internal.util.SimpleCompletableFuture;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;
import static java.util.Arrays.sort;

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
 * @see com.hazelcast.cp.internal.raft.impl.dto.AppendRequest
 * @see com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse
 * @see com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse
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

        if (!updateFollowerIndices(state)) {
            tryRunQueries(state);
            return;
        }

        if (!tryAdvanceCommitIndex(state)) {
            trySendAppendRequest(state);
        }
    }

    private boolean updateFollowerIndices(RaftState state) {
        // If successful: update nextIndex and matchIndex for follower (§5.3)

        Endpoint follower = resp.follower();
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
            followerState.resetAppendRequestBackoff();

            long newNextIndex = followerLastLogIndex + 1;
            followerState.matchIndex(followerLastLogIndex);
            followerState.nextIndex(newNextIndex);

            if (logger.isFineEnabled()) {
                logger.fine("Updated match index: " + followerLastLogIndex + " and next index: " + newNextIndex
                        + " for follower: " + follower);
            }

            return true;
        } else if (followerLastLogIndex < matchIndex) {
            if (logger.isFineEnabled()) {
                logger.fine("Will not update match index for follower: " + follower + ". follower last log index: "
                        + followerLastLogIndex + ", match index: " + matchIndex);
            }
        }

        return false;
    }

    private long findQuorumMatchIndex(RaftState state) {
        LeaderState leaderState = state.leaderState();
        long[] indices = leaderState.matchIndices();

        // if the leader is leaving, it should not count its vote for quorum...
        if (raftNode.state().isKnownMember(localMember())) {
            indices[indices.length - 1] = state.log().lastLogOrSnapshotIndex();
        } else {
            // Remove the last empty slot reserved for leader index
            indices = Arrays.copyOf(indices, indices.length - 1);
        }

        sort(indices);

        long quorumMatchIndex = indices[(indices.length - 1) / 2];
        if (logger.isFineEnabled()) {
            logger.fine("Quorum match index: " + quorumMatchIndex + ", indices: " + Arrays.toString(indices));
        }

        return quorumMatchIndex;
    }

    private boolean tryAdvanceCommitIndex(RaftState state) {
        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4)
        long quorumMatchIndex = findQuorumMatchIndex(state);
        long commitIndex = state.commitIndex();
        RaftLog raftLog = state.log();
        for (; quorumMatchIndex > commitIndex; quorumMatchIndex--) {
            // Only log entries from the leader’s current term are committed by counting replicas; once an entry
            // from the current term has been committed in this way, then all prior entries are committed indirectly
            // because of the Log Matching Property.
            LogEntry entry = raftLog.getLogEntry(quorumMatchIndex);
            if (entry.term() == state.term()) {
                commitEntries(state, quorumMatchIndex);
                return true;
            } else if (logger.isFineEnabled()) {
                logger.fine("Cannot commit " + entry + " since an entry from the current term: " + state.term() + " is needed.");
            }
        }
        return false;
    }

    private void commitEntries(RaftState state, long commitIndex) {
        if (logger.isFineEnabled()) {
            logger.fine("Setting commit index: " + commitIndex);
        }
        state.commitIndex(commitIndex);
        raftNode.broadcastAppendRequest();
        raftNode.applyLogEntries();
        tryRunQueries(state);
    }

    private void tryRunQueries(RaftState state) {
        QueryState queryState = state.leaderState().queryState();
        if (queryState.queryCount() == 0) {
            return;
        }

        long commitIndex = state.commitIndex();
        if (!queryState.isMajorityAcked(commitIndex, state.majority())) {
            return;
        } else if (queryState.isAckNeeded(resp.follower(), state.majority())) {
            raftNode.sendAppendRequest(resp.follower());
            return;
        }

        Collection<Tuple2<Object, SimpleCompletableFuture>> operations = queryState.operations();

        if (logger.isFineEnabled()) {
            logger.fine("Running " + operations.size() + " queries at commit index: " + commitIndex
                    + ", query round: " + queryState.queryRound());
        }

        for (Tuple2<Object, SimpleCompletableFuture> t : operations) {
            raftNode.runQuery(t.element1, t.element2);
        }

        queryState.reset();
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
    protected Endpoint sender() {
        return resp.follower();
    }
}
