package com.hazelcast.raft.impl.task;

import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.RaftState;

import java.util.Collection;

/**
 * PreVoteTask is scheduled when current leader is null, unreachable or unknown.
 * It sends {@link PreVoteRequest}s to other members to receive make sure majority is reachable
 * and ready to elect a new leader.
 * <p>
 * Also a {@link PreVoteTimeoutTask} is scheduled with a {@link RaftNodeImpl#getLeaderElectionTimeoutInMillis()}
 * delay to trigger pre-voting if a leader is not available yet.
 */
public class PreVoteTask extends RaftNodeStatusAwareTask implements Runnable {

    public PreVoteTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();

        if (state.leader() != null) {
            logger.info("No new pre-vote phase, we already have a LEADER: " + state.leader());
            return;
        }

        Collection<RaftMember> remoteMembers = state.remoteMembers();
        if (remoteMembers.isEmpty()) {
            logger.fine("Remote members is empty. No need for pre-voting.");
            return;
        }


        state.initPreCandidateState();
        int nextTerm = state.term() + 1;
        RaftLog log = state.log();
        PreVoteRequest request = new PreVoteRequest(raftNode.getLocalMember(), nextTerm,
                log.lastLogOrSnapshotTerm(), log.lastLogOrSnapshotIndex());

        logger.info("Pre-vote started for next term: " + request.nextTerm() + ", last log index: " + request.lastLogIndex()
                + ", last log term: " + request.lastLogTerm());
        raftNode.printMemberState();

        for (RaftMember endpoint : remoteMembers) {
            raftNode.send(request, endpoint);
        }

        schedulePreVoteTimeout();
    }

    private void schedulePreVoteTimeout() {
        raftNode.schedule(new PreVoteTimeoutTask(raftNode), raftNode.getLeaderElectionTimeoutInMillis());
    }
}
