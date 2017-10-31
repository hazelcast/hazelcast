package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * TODO: Javadoc Pending...
 *
 */
public class VoteRequestHandlerTask implements StripedRunnable {
    private final ILogger logger;
    private RaftNode raftNode;
    private final VoteRequest req;

    public VoteRequestHandlerTask(RaftNode raftNode, VoteRequest req) {
        this.raftNode = raftNode;
        this.req = req;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        if (!state.isKnownEndpoint(req.candidate())) {
            logger.warning("Ignored " + req + " since candidate is unknown to us");
            return;
        }

        RaftEndpoint localEndpoint = raftNode.getLocalEndpoint();

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
        if (raftLog.lastLogTerm() > req.lastLogTerm()) {
            logger.info("Rejecting " + req + " since our last log term: " + raftLog.lastLogTerm() + " is greater");
            raftNode.send(new VoteResponse(localEndpoint, req.term(), false), req.candidate());
            return;
        }

        if (raftLog.lastLogTerm() == req.lastLogTerm() && raftLog.lastLogIndex() > req.lastLogIndex()) {
            logger.info("Rejecting " + req + " since our last log index: " + raftLog.lastLogIndex() + " is greater");
            raftNode.send(new VoteResponse(localEndpoint, req.term(), false), req.candidate());
            return;
        }

        logger.info("Granted vote for " + req);
        state.persistVote(req.term(), req.candidate());

        raftNode.send(new VoteResponse(localEndpoint, req.term(), true), req.candidate());
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
