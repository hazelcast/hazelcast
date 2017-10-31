package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Arrays;
import java.util.Collection;

import static java.util.Arrays.sort;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendSuccessResponseHandlerTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final AppendSuccessResponse resp;
    private final ILogger logger;

    public AppendSuccessResponseHandlerTask(RaftNode raftNode, AppendSuccessResponse response) {
        this.raftNode = raftNode;
        this.resp = response;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        RaftState state = raftNode.state();
        if (!state.isKnownEndpoint(resp.follower())) {
            logger.warning("Ignored " + resp + ", since sender is unknown to us");
            return;
        }

        if (state.role() != RaftRole.LEADER) {
            logger.warning("Ignored " + resp + ". We are not LEADER anymore.");
            return;
        }

        assert resp.term() <= state.term() : "Invalid " + resp + " for current term: " + state.term();

        if (logger.isFineEnabled()) {
            logger.fine("Received " + resp);
        }

        // If successful: update nextIndex and matchIndex for follower (§5.3)
        updateFollowerIndices(state);

        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4)
        int quorumMatchIndex = findQuorumMatchIndex(state);
        int commitIndex = state.commitIndex();
        RaftLog raftLog = state.log();
        for (; quorumMatchIndex > commitIndex; quorumMatchIndex--) {
            // Only log entries from the leader’s current term are committed by counting replicas; once an entry
            // from the current term has been committed in this way, then all prior entries are committed indirectly
            // because of the Log Matching Property.
            LogEntry entry = raftLog.getEntry(quorumMatchIndex);
            if (entry.term() == state.term()) {
                commitEntries(state, quorumMatchIndex);
                break;
            } else if (logger.isFineEnabled()) {
                logger.fine("Cannot commit " + entry + " since an entry from the current term: " + state.term() + " is needed.");
            }
        }
    }

    private void updateFollowerIndices(RaftState state) {
        RaftEndpoint follower = resp.follower();
        LeaderState leaderState = state.leaderState();
        int matchIndex = leaderState.getMatchIndex(follower);
        int followerLastLogIndex = resp.lastLogIndex();

        if (followerLastLogIndex > matchIndex) {
            int newNextIndex = followerLastLogIndex + 1;
            logger.info("Updating match index: " + followerLastLogIndex + " and next index: " + newNextIndex
                    + " for follower: " + follower);
            leaderState.setMatchIndex(follower, followerLastLogIndex);
            leaderState.setNextIndex(follower, newNextIndex);
        } else if (followerLastLogIndex < matchIndex) {
            logger.warning("Will not update match index for follower: " + follower + ". follower last log index: "
                    + followerLastLogIndex + ", match index: " + matchIndex);
        }
    }

    private int findQuorumMatchIndex(RaftState state) {
        LeaderState leaderState = state.leaderState();
        Collection<Integer> matchIndices = leaderState.matchIndices();
        int[] indices = new int[matchIndices.size() + 1];
        indices[0] = state.log().lastLogIndex();

        int k = 1;
        for (int index : matchIndices) {
            indices[k++] = index;
        }
        sort(indices);

        int quorumMatchIndex = indices[(indices.length - 1) / 2];
        if (logger.isFineEnabled()) {
            logger.fine("Quorum match index: " + quorumMatchIndex + ", indices: " + Arrays.toString(indices));
        }

        return quorumMatchIndex;
    }

    private void commitEntries(RaftState state, int commitIndex) {
        logger.info("Setting commit index: " + commitIndex);
        state.commitIndex(commitIndex);
        raftNode.broadcastAppendRequest();
        raftNode.processLogs();
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
