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

package com.hazelcast.cp.internal.raft.impl.task;

import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.command.RaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.internal.util.SimpleCompletableFuture;
import com.hazelcast.logging.ILogger;

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
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public QueryTask(RaftNodeImpl raftNode, Object operation, QueryPolicy policy, SimpleCompletableFuture resultFuture) {
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
                    new ReplicateTask(raftNode, operation, resultFuture).run();
                    break;
                default:
                    resultFuture.setResult(new IllegalArgumentException("Invalid query policy: " + queryPolicy));
            }
        } catch (Throwable t) {
            logger.severe(queryPolicy + " query failed", t);
            resultFuture.setResult(new CPSubsystemException("Internal failure", raftNode.getLeader(), t));
        }
    }

    private void handleLeaderLocalRead() {
        RaftState state = raftNode.state();
        if (state.role() != LEADER) {
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), state.leader()));
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

        raftNode.runQueryOperation(operation, resultFuture);
    }

    private boolean verifyOperation() {
        if (operation instanceof RaftGroupCmd) {
            resultFuture.setResult(new IllegalArgumentException("cannot run query: " + operation));
            return false;
        }

        return true;
    }

    private boolean verifyRaftNodeStatus() {
        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            resultFuture.setResult(new CPGroupDestroyedException(raftNode.getGroupId()));
            return false;
        } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), null));
            return false;
        }

        return true;
    }

}
