package com.hazelcast.raft.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.operation.ApplyRaftGroupMembersOp;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.operation.RaftCommandOperation;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.operation.TerminateRaftGroupOp;

/**
 * ReplicateTask is executed to append a {@link RaftOperation} to Raft log
 * and replicate the new entry to followers. It's scheduled by
 * {@link com.hazelcast.raft.impl.RaftNode#replicate(RaftOperation)}
 * or by {@link MembershipChangeTask} for membership changes.
 * <p>
 * If this node is not the leader, future is immediately notified with {@link NotLeaderException}.
 * <p>
 * If replication of the operation is not allowed at the moment (see {@link RaftNodeImpl#canReplicateNewEntry(RaftOperation)}),
 * future is immediately notified with {@link CannotReplicateException}.
 */
public class ReplicateTask implements Runnable {
    private final RaftNodeImpl raftNode;
    private final RaftOperation operation;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public ReplicateTask(RaftNodeImpl raftNode, RaftOperation operation, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.logger = raftNode.getLogger(getClass());
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        if (!verifyServiceName()) {
            return;
        }

        if (!verifyRaftNodeStatus()) {
            return;
        }

        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalEndpoint(), state.leader()));
            return;
        }

        if (!raftNode.canReplicateNewEntry(operation)) {
            resultFuture.setResult(new CannotReplicateException(raftNode.getLocalEndpoint()));
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Replicating: " + operation + " in term: " + state.term());
        }

        long newEntryLogIndex = state.log().lastLogOrSnapshotIndex() + 1;
        raftNode.registerFuture(newEntryLogIndex, resultFuture);
        state.log().appendEntries(new LogEntry(state.term(), newEntryLogIndex, operation));

        handleInternalRaftOperation(newEntryLogIndex, operation);

        raftNode.broadcastAppendRequest();
    }

    private boolean verifyServiceName() {
        if (!(operation instanceof RaftCommandOperation || raftNode.getServiceName().equals(operation.getServiceName()))) {
            resultFuture.setResult(new IllegalArgumentException("operation: " + operation + "  service name: "
                    + operation.getServiceName() + " is different than expected service name: " + raftNode.getServiceName()));
            return false;
        }

        return true;
    }

    private boolean verifyRaftNodeStatus() {
        if (raftNode.getStatus() == RaftNodeStatus.TERMINATED) {
            resultFuture.setResult(new RaftGroupTerminatedException());
            logger.fine("Won't run " + operation + ", since raft node is terminated");
            return false;
        } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
            logger.fine("Won't run " + operation + ", since raft node is stepped down");
            resultFuture.setResult(new NotLeaderException(raftNode.getGroupId(), raftNode.getLocalEndpoint(), null));
            return false;
        }

        return true;
    }

    private void handleInternalRaftOperation(long logIndex, RaftOperation operation) {
        if (operation instanceof TerminateRaftGroupOp) {
            raftNode.setStatus(RaftNodeStatus.TERMINATING);
        } else if (operation instanceof ApplyRaftGroupMembersOp) {
            raftNode.setStatus(RaftNodeStatus.CHANGING_MEMBERSHIP);
            ApplyRaftGroupMembersOp op = (ApplyRaftGroupMembersOp) operation;
            raftNode.updateGroupMembers(logIndex, op.getMembers());
            // TODO update quorum match indices...
            // TODO if the raft group shrinks, we can move the commit index forward...
        }
    }

}
