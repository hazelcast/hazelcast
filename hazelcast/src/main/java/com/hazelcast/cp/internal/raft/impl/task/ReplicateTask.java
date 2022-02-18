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
import com.hazelcast.cp.internal.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.UUID;

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
    private final InternalCompletableFuture resultFuture;
    private final ILogger logger;

    public ReplicateTask(RaftNodeImpl raftNode, Object operation, InternalCompletableFuture resultFuture) {
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
                resultFuture.completeExceptionally(
                        new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalMember(), state.leader()));
                return;
            }

            if (!raftNode.canReplicateNewEntry(operation)) {
                resultFuture.completeExceptionally(new CannotReplicateException(raftNode.getLocalMember()));
                return;
            }

            if (logger.isFineEnabled()) {
                logger.fine("Replicating: " + operation + " in term: " + state.term());
            }

            RaftLog log = state.log();

            if (!log.checkAvailableCapacity(1)) {
                resultFuture.completeExceptionally(new IllegalStateException("Not enough capacity in RaftLog!"));
                return;
            }

            long newEntryLogIndex = log.lastLogOrSnapshotIndex() + 1;
            raftNode.registerFuture(newEntryLogIndex, resultFuture);
            log.appendEntries(new LogEntry(state.term(), newEntryLogIndex, operation));

            preApplyRaftGroupCmd(newEntryLogIndex, operation);

            raftNode.broadcastAppendRequest();
        } catch (Throwable t) {
            logger.severe(operation + " could not be replicated to leader: " + raftNode.getLocalMember(), t);
            RaftEndpoint leader = raftNode.getLeader();
            UUID leaderUuid = leader != null ? leader.getUuid() : null;
            resultFuture.completeExceptionally(new CPSubsystemException("Internal failure", t, leaderUuid));
        }
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
