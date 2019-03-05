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
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.internal.util.SimpleCompletableFuture;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.UPDATING_GROUP_MEMBER_LIST;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;

/**
 * ReplicateTask is executed to append an operation to Raft log
 * and replicate the new entry to followers. It's scheduled by
 * {@link com.hazelcast.cp.internal.raft.impl.RaftNode#replicate(Object)}
 * or by {@link MembershipChangeTask} for membership changes.
 * <p>
 * If this node is not the leader, future is immediately notified with
 * {@link NotLeaderException}.
 * <p>
 * If replication of the operation is not allowed at the moment
 * (see {@link RaftNodeImpl#canReplicateNewEntry(Object)}), the future is
 * immediately notified with {@link CannotReplicateException}.
 */
public class ReplicateTask implements Runnable {
    private final RaftNodeImpl raftNode;
    private final Object operation;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public ReplicateTask(RaftNodeImpl raftNode, Object operation, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.logger = raftNode.getLogger(getClass());
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        try {
            if (!verifyRaftNodeStatus()) {
                return;
            }

            RaftState state = raftNode.state();
            if (state.role() != LEADER) {
                resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), state.leader()));
                return;
            }

            if (!raftNode.canReplicateNewEntry(operation)) {
                resultFuture.setResult(new CannotReplicateException(raftNode.getLocalMember()));
                return;
            }

            if (logger.isFineEnabled()) {
                logger.fine("Replicating: " + operation + " in term: " + state.term());
            }

            RaftLog log = state.log();

            if (!log.checkAvailableCapacity(1)) {
                resultFuture.setResult(new IllegalStateException("Not enough capacity in RaftLog!"));
                return;
            }

            long newEntryLogIndex = log.lastLogOrSnapshotIndex() + 1;
            raftNode.registerFuture(newEntryLogIndex, resultFuture);
            log.appendEntries(new LogEntry(state.term(), newEntryLogIndex, operation));

            preApplyRaftGroupCmd(newEntryLogIndex, operation);

            raftNode.broadcastAppendRequest();
        } catch (Throwable t) {
            logger.severe(operation + " could not be replicated to leader: " + raftNode.getLocalMember(), t);
            resultFuture.setResult(new CPSubsystemException("Internal failure", raftNode.getLeader(), t));
        }
    }

    private boolean verifyRaftNodeStatus() {
        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            resultFuture.setResult(new CPGroupDestroyedException(raftNode.getGroupId()));
            logger.fine("Won't run " + operation + ", since raft node is terminated");
            return false;
        } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
            logger.fine("Won't run " + operation + ", since raft node is stepped down");
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), null));
            return false;
        }

        return true;
    }

    private void preApplyRaftGroupCmd(long logIndex, Object operation) {
        if (operation instanceof DestroyRaftGroupCmd) {
            raftNode.setStatus(RaftNodeStatus.TERMINATING);
        } else if (operation instanceof UpdateRaftGroupMembersCmd) {
            raftNode.setStatus(UPDATING_GROUP_MEMBER_LIST);
            UpdateRaftGroupMembersCmd op = (UpdateRaftGroupMembersCmd) operation;
            raftNode.updateGroupMembers(logIndex, op.getMembers());
            // TODO following suggestions are optimizations. No rush for impl'ing them...
            // TODO update quorum match indices...
            // TODO if the Raft group shrinks, we can move the commit index forward...
        }
    }

}
