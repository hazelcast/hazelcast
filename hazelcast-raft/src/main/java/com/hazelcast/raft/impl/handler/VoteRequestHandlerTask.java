package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import com.hazelcast.util.Clock;

/**
 * Handles {@link VoteRequest} sent by a candidate. Responds with a {@link VoteResponse} to the sender.
 * Leader election is initiated by {@link com.hazelcast.raft.impl.task.LeaderElectionTask}.
 * <p>
 * See <i>5.2 Leader election</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see VoteRequest
 * @see VoteResponse
 * @see com.hazelcast.raft.impl.task.LeaderElectionTask
 */
public class VoteRequestHandlerTask extends RaftNodeStatusAwareTask implements Runnable {
    private final VoteRequest req;

    public VoteRequestHandlerTask(RaftNodeImpl raftNode, VoteRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();
        RaftMember localEndpoint = raftNode.getLocalMember();

        // Reply false if last AppendEntries call was received less than election timeout ago (leader stickiness)
        if (raftNode.lastAppendEntriesTimestamp() > Clock.currentTimeMillis() - raftNode.getLeaderElectionTimeoutInMillis()) {
            logger.info("Rejecting " + req + " since received append entries recently.");
            raftNode.send(new VoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > req.term()) {
            logger.info("Rejecting " + req + " since current term: " + state.term() + " is bigger");
            raftNode.send(new VoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        if (state.term() < req.term()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            if (state.role() != RaftRole.FOLLOWER) {
                logger.info("Demoting to FOLLOWER after " + req + " since current term: " + state.term() + " is smaller");
            } else {
                logger.info("Moving to new term: " + req.term() + " from current term: " + state.term() + " after " + req);
            }

            state.toFollower(req.term());
            raftNode.printMemberState();
        }

        if (state.leader() != null && !req.candidate().equals(state.leader())) {
            logger.warning("Rejecting " + req + " since we have a leader: " + state.leader());
            raftNode.send(new VoteResponse(localEndpoint, req.term(), false), req.candidate());
            return;
        }

        if (state.lastVoteTerm() == req.term() && state.votedFor() != null) {
            boolean granted = (req.candidate().equals(state.votedFor()));
            if (granted) {
                logger.info("Vote granted for duplicate" + req);
            } else {
                logger.info("Duplicate " + req + ". currently voted-for: " + state.votedFor());
            }
            raftNode.send(new VoteResponse(localEndpoint, req.term(), granted), req.candidate());
            return;
        }

        RaftLog raftLog = state.log();
        if (raftLog.lastLogOrSnapshotTerm() > req.lastLogTerm()) {
            logger.info("Rejecting " + req + " since our last log term: " + raftLog.lastLogOrSnapshotTerm() + " is greater");
            raftNode.send(new VoteResponse(localEndpoint, req.term(), false), req.candidate());
            return;
        }

        if (raftLog.lastLogOrSnapshotTerm() == req.lastLogTerm() && raftLog.lastLogOrSnapshotIndex() > req.lastLogIndex()) {
            logger.info("Rejecting " + req + " since our last log index: " + raftLog.lastLogOrSnapshotIndex() + " is greater");
            raftNode.send(new VoteResponse(localEndpoint, req.term(), false), req.candidate());
            return;
        }

        logger.info("Granted vote for " + req);
        state.persistVote(req.term(), req.candidate());

        raftNode.send(new VoteResponse(localEndpoint, req.term(), true), req.candidate());
    }
}
