package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;

/**
 * PreVoteTimeoutTask is scheduled by {@link PreVoteTask}
 * to trigger pre-voting again if this node is still a follower
 * and a leader is not available after leader election timeout.
 */
public class PreVoteTimeoutTask extends RaftNodeStatusAwareTask implements Runnable {

    public PreVoteTimeoutTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
        if (raftNode.state().role() != RaftRole.FOLLOWER) {
            return;
        }
        logger.info("Pre-vote for term: " + raftNode.state().term() + " has timed out!");
        new PreVoteTask(raftNode).run();
    }
}
