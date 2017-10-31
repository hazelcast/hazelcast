package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.NotLeaderException;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public class ReplicateTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final RaftOperation operation;
    private final SimpleCompletableFuture resultFuture;
    private final ILogger logger;

    public ReplicateTask(RaftNode raftNode, RaftOperation operation, SimpleCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.operation = operation;
        this.logger = raftNode.getLogger(getClass());
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        if (state.role() != RaftRole.LEADER) {
            resultFuture.setResult(new NotLeaderException());
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Replicating: " + operation + " in term: " + state.term());
        }

        assert state.role() == RaftRole.LEADER;

        int logIndex = state.log().lastLogIndex() + 1;
        raftNode.registerFuture(logIndex, resultFuture);
        state.log().appendEntries(new LogEntry(state.term(), logIndex, operation));
        raftNode.broadcastAppendRequest();
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
