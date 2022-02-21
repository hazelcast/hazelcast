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

import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.command.RaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.state.QueryState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.UUID;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;

/**
 * QueryTask is executed to query/read Raft state without appending log entry.
 * It's scheduled by {@link RaftNodeImpl#query(Object, QueryPolicy)}.
 *
 * @see QueryPolicy
 */
public class QueryTask implements Runnable {
    private final RaftNodeImpl raftNode;
    private final Object operation;
    private final QueryPolicy queryPolicy;
    private final InternalCompletableFuture resultFuture;
    private final ILogger logger;

    public QueryTask(RaftNodeImpl raftNode, Object operation, QueryPolicy policy, InternalCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.logger = raftNode.getLogger(getClass());
        this.queryPolicy = policy;
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        try {
            if (!verifyOperation()) {
                return;
            }

            if (!verifyRaftNodeStatus()) {
                return;
            }

            switch (queryPolicy) {
                case LEADER_LOCAL:
                    handleLeaderLocalRead();
                    break;
                case ANY_LOCAL:
                    handleAnyLocalRead();
                    break;
                case LINEARIZABLE:
                    handleLinearizableRead();
                    break;
                default:
                    resultFuture.completeExceptionally(new IllegalArgumentException("Invalid query policy: " + queryPolicy));
            }
        } catch (Throwable t) {
            logger.severe(queryPolicy + " query failed", t);
            RaftEndpoint leader = raftNode.getLeader();
            UUID leaderUuid = leader != null ? leader.getUuid() : null;
            resultFuture.completeExceptionally(new CPSubsystemException("Internal failure", t, leaderUuid));
        }
    }

    private void handleLeaderLocalRead() {
        RaftState state = raftNode.state();
        if (state.role() != LEADER) {
            resultFuture.completeExceptionally(
                    new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), state.leader()));
            return;
        }

        // TODO: We can reject the query, if leader is not able to reach majority of the followers

        handleAnyLocalRead();
    }

    private void handleAnyLocalRead() {
        RaftState state = raftNode.state();
        if (logger.isFineEnabled()) {
            logger.fine("Querying: " + operation + " with policy: " + queryPolicy + " in term: " + state.term());
        }

        // TODO: We can reject the query, if follower have not received any heartbeat recently

        raftNode.runQuery(operation, resultFuture);
    }

    private void handleLinearizableRead() {
        if (!raftNode.isLinearizableReadOptimizationEnabled()) {
            new ReplicateTask(raftNode, operation, resultFuture).run();
            return;
        }

        RaftState state = raftNode.state();
        if (state.role() != LEADER) {
            resultFuture.completeExceptionally(
                    new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), state.leader()));
            return;
        }

        if (!raftNode.canQueryLinearizable()) {
            resultFuture.completeExceptionally(new CannotReplicateException(state.leader()));
            return;
        }

        long commitIndex = state.commitIndex();
        QueryState queryState = state.leaderState().queryState();

        if (logger.isFineEnabled()) {
            logger.fine("Adding query at commit index: " + commitIndex + ", query round: " + queryState.queryRound());
        }

        if (queryState.addQuery(commitIndex, operation, resultFuture) == 1) {
            raftNode.broadcastAppendRequest();
        }
    }

    private boolean verifyOperation() {
        if (operation instanceof RaftGroupCmd) {
            resultFuture.completeExceptionally(new IllegalArgumentException("cannot run query: " + operation));
            return false;
        }

        return true;
    }

    private boolean verifyRaftNodeStatus() {
        switch (raftNode.getStatus()) {
            case INITIAL:
                resultFuture.completeExceptionally(new CannotReplicateException(null));
                return false;
            case TERMINATED:
            case STEPPED_DOWN:
                resultFuture.completeExceptionally(
                        new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), null));
                return false;
            default:
                return true;
        }
    }

}
