package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LeaderElectionTimeoutTask implements StripedRunnable {
    private final RaftNode raftNode;

    public LeaderElectionTimeoutTask(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void run() {
        if (raftNode.state().role() != RaftRole.CANDIDATE) {
            return;
        }
        raftNode.getLogger(getClass()).warning("Leader election for term: " + raftNode.state().term() + " has timed out!");
        new LeaderElectionTask(raftNode).run();
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
