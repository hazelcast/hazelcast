package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.state.CandidateState;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.task.LeaderElectionTask;

/**
 * Handles {@link PreVoteResponse} sent by {@link PreVoteRequestHandlerTask}.
 * <p>
 * Initiates leader election by executing {@link LeaderElectionTask}
 * if majority grants vote for this pre-voting term.
 *
 * @see PreVoteResponse
 */
public class PreVoteResponseHandlerTask extends AbstractResponseHandlerTask {
    private final PreVoteResponse resp;

    public PreVoteResponseHandlerTask(RaftNodeImpl raftNode, PreVoteResponse response) {
        super(raftNode);
        this.resp = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = raftNode.state();

        if (state.role() != RaftRole.FOLLOWER) {
            logger.info("Ignored " + resp + ". We are not FOLLOWER anymore.");
            return;
        }

        if (resp.term() < state.term()) {
            logger.warning("Stale " + resp + " is received, current term: " + state.term());
            return;
        }

        CandidateState preCandidateState = state.preCandidateState();
        if (preCandidateState == null) {
            logger.fine("Ignoring " + resp + ". We are not interested in pre-votes anymore.");
            return;
        }

        if (resp.granted() && preCandidateState.grantVote(resp.voter())) {
            logger.info("Pre-vote granted from " + resp.voter() + " for term: " + resp.term()
                    + ", number of votes: " + preCandidateState.voteCount() + ", majority: " + preCandidateState.majority());
        }

        if (preCandidateState.isMajorityGranted()) {
            logger.info("We have the majority during pre-vote phase. Let's start real election!");
            new LeaderElectionTask(raftNode).run();
        }
    }

    @Override
    protected RaftEndpoint senderEndpoint() {
        return resp.voter();
    }
}
