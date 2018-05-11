package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;

/**
 * LeaderElectionTimeoutTask is scheduled by {@link LeaderElectionTask}
 * to trigger leader election again if one is not elected after leader election timeout.
 */
public class LeaderElectionTimeoutTask extends RaftNodeStatusAwareTask implements Runnable {

    public LeaderElectionTimeoutTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
        if (raftNode.state().role() != RaftRole.CANDIDATE) {
            return;
        }
        logger.warning("Leader election for term: " + raftNode.state().term() + " has timed out!");
        new LeaderElectionTask(raftNode).run();
    }
}
