package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.state.RaftState;

/**
 * LeaderElectionTask is scheduled when current leader is null, unreachable or unknown
 * by {@link com.hazelcast.raft.impl.handler.PreVoteResponseHandlerTask} after a follower receives votes
 * from at least majority.
 * Local member becomes a candidate using {@link RaftState#toCandidate()} and sends {@link VoteRequest}s to other members.
 * <p>
 * Also a {@link LeaderElectionTimeoutTask} is scheduled with a {@link RaftNodeImpl#getLeaderElectionTimeoutInMillis()}
 * delay to trigger leader election if one is not elected yet.
 */
public class LeaderElectionTask extends RaftNodeStatusAwareTask implements Runnable {

    public LeaderElectionTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.warning("No new election round, we already have a LEADER: " + state.leader());
            return;
        }

        VoteRequest request = state.toCandidate();
        logger.info("Leader election started for term: " + request.term() + ", last log index: " + request.lastLogIndex()
                + ", last log term: " + request.lastLogTerm());
        raftNode.printMemberState();

        for (RaftMember endpoint : state.remoteMembers()) {
            raftNode.send(request, endpoint);
        }

        scheduleLeaderElectionTimeout();
    }

    private void scheduleLeaderElectionTimeout() {
        raftNode.schedule(new LeaderElectionTimeoutTask(raftNode), raftNode.getLeaderElectionTimeoutInMillis());
    }
}
