package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.util.executor.StripedRunnable;

public class AppendFailureResponseHandlerTask implements StripedRunnable {

    private final RaftNode raftNode;
    private final AppendFailureResponse resp;
    private final ILogger logger;

    public AppendFailureResponseHandlerTask(RaftNode raftNode, AppendFailureResponse response) {
        this.raftNode = raftNode;
        this.resp = response;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        if (!state.isKnownEndpoint(resp.follower())) {
            logger.warning(resp + " is ignored since sender is unknown to us");
            return;
        }

        if (state.role() != RaftRole.LEADER) {
            logger.warning(resp + " is ignored since we are not LEADER.");
            return;
        }

        if (resp.term() > state.term()) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.info("Demoting to FOLLOWER after " + resp + " from current term: " + state.term());
            state.toFollower(resp.term());
            raftNode.invalidateFuturesFrom(state.commitIndex() + 1);
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Received " + resp);
        }

        if (updateNextIndex(state)) {
            raftNode.sendAppendRequest(resp.follower());
        }
    }

    private boolean updateNextIndex(RaftState state) {
        LeaderState leaderState = state.leaderState();
        int nextIndex = leaderState.getNextIndex(resp.follower());
        int matchIndex = leaderState.getMatchIndex(resp.follower());

        if (resp.expectedNextIndex() == nextIndex) {
            // this is the response of the request I have sent for this nextIndex
            nextIndex--;
            if (nextIndex <= matchIndex) {
                logger.severe("Cannot decrement next index: " + nextIndex + " below match index: " + matchIndex
                        + " for follower: " + resp.follower());
                return false;
            }

            if (logger.isFineEnabled()) {
                logger.info("Updating next index: " + nextIndex + " for follower: " + resp.follower());
            }
            leaderState.setNextIndex(resp.follower(), nextIndex);
            return true;
        }

        return false;
    }
}
