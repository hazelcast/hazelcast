package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.operation.NopEntryOp;
import com.hazelcast.raft.impl.state.CandidateState;
import com.hazelcast.raft.impl.state.RaftState;

/**
 * Handles {@link VoteResponse} sent by {@link VoteRequestHandlerTask}.
 * <p>
 * Changes node to {@link RaftRole#LEADER} if if majority of the nodes grants vote for this term
 * via {@link RaftState#toLeader()}.
 * <p>
 * Appends a no-op entry if {@link com.hazelcast.raft.RaftConfig#appendNopEntryOnLeaderElection} is enabled.
 * <p>
 * See <i>5.2 Leader election</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see VoteResponse
 * @see com.hazelcast.raft.impl.dto.VoteRequest
 */
public class VoteResponseHandlerTask extends AbstractResponseHandlerTask {
    private final VoteResponse resp;

    public VoteResponseHandlerTask(RaftNodeImpl raftNode, VoteResponse response) {
        super(raftNode);
        this.resp = response;
    }

    @Override
    protected void handleResponse() {
        RaftState state = raftNode.state();

        if (state.role() != RaftRole.CANDIDATE) {
            logger.info("Ignored " + resp + ". We are not CANDIDATE anymore.");
            return;
        }

        if (resp.term() > state.term()) {
            logger.info("Demoting to FOLLOWER from current term: " + state.term() + " to new term: " + resp.term()
                    + " after " + resp);
            state.toFollower(resp.term());
            raftNode.printMemberState();
            return;
        }

        if (resp.term() < state.term()) {
            logger.warning("Stale " + resp + " is received, current term: " + state.term());
            return;
        }

        CandidateState candidateState = state.candidateState();
        if (resp.granted() && candidateState.grantVote(resp.voter())) {
            logger.info("Vote granted from " + resp.voter() + " for term: " + state.term()
                    + ", number of votes: " + candidateState.voteCount() + ", majority: " + candidateState.majority());
        }

        if (candidateState.isMajorityGranted()) {
            logger.info("We are the LEADER!");
            state.toLeader();
            appendNopEntry();
            raftNode.printMemberState();
            raftNode.scheduleHeartbeat();
        }
    }

    private void appendNopEntry() {
        if (raftNode.shouldAppendNopEntryOnLeaderElection()) {
            RaftState state = raftNode.state();
            RaftLog log = state.log();
            log.appendEntries(new LogEntry(state.term(), log.lastLogOrSnapshotIndex() + 1, new NopEntryOp()));
        }
    }

    @Override
    protected RaftEndpoint senderEndpoint() {
        return resp.voter();
    }
}
