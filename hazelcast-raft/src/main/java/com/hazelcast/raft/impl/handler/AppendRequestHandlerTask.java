package com.hazelcast.raft.impl.handler;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.min;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendRequestHandlerTask implements StripedRunnable {
    private final RaftNode raftNode;
    private final AppendRequest req;
    private final ILogger logger;

    public AppendRequestHandlerTask(RaftNode raftNode, AppendRequest req) {
        this.raftNode = raftNode;
        this.req = req;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public void run() {
        if (logger.isFineEnabled()) {
            logger.fine("Received " + req);
        }

        RaftState state = raftNode.state();
        if (!state.isKnownEndpoint(req.leader())) {
            logger.warning("Ignored " + req + ", since sender is unknown to us");
            return;
        }

        // Reply false if term < currentTerm (§5.1)
        if (req.term() < state.term()) {
            logger.warning("Stale " + req + " received in current term: " + state.term());
            raftNode.send(createFailureResponse(state.term()), req.leader());
            return;
        }

        RaftLog raftLog = state.log();

        // Increase the term if we see a newer one, also transition to follower
        // if we ever get an appendEntries call
        if (req.term() > state.term() || state.role() != RaftRole.FOLLOWER) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.info("Demoting to FOLLOWER from current term: " + state.term() + " to new term: " + req.term() + " and leader: " + req.leader());
            state.toFollower(req.term());
            state.leader(req.leader());
            raftNode.invalidateFuturesFrom(state.commitIndex() + 1);
            raftNode.send(createFailureResponse(req.term()), req.leader());
            return;
        }

        if (!req.leader().equals(state.leader())) {
            logger.info("Setting leader: " + req.leader());
            state.leader(req.leader());
        }

        // Verify the last log entry
        if (req.prevLogIndex() > 0) {
            int lastLogIndex = raftLog.lastLogIndex();
            int lastLogTerm = raftLog.lastLogTerm();

            int prevLogTerm;
            if (req.prevLogIndex() == lastLogIndex) {
                prevLogTerm = lastLogTerm;
            } else {
                // Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                LogEntry prevLog = raftLog.getEntry(req.prevLogIndex());
                if (prevLog == null) {
                    logger.warning("Failed to get previous log index for " + req + ", last log index: " + lastLogIndex);
                    raftNode.send(createFailureResponse(req.term()), req.leader());
                    return;
                }
                prevLogTerm = prevLog.term();
            }

            if (req.prevLogTerm() != prevLogTerm) {
                logger.warning("Previous log term of " + req + " is different than ours: " + prevLogTerm);
                raftNode.send(createFailureResponse(req.term()), req.leader());
                return;
            }
        }

        // Process any new entries
        if (req.entryCount() > 0) {
            // Delete any conflicting entries, skip any duplicates
            int lastLogIndex = raftLog.lastLogIndex();

            LogEntry[] newEntries = null;
            for (int i = 0; i < req.entryCount(); i++) {
                LogEntry reqEntry = req.entries()[i];

                if (reqEntry.index() > lastLogIndex) {
                    newEntries = Arrays.copyOfRange(req.entries(), i, req.entryCount());
                    break;
                }

                LogEntry localEntry = raftLog.getEntry(reqEntry.index());

                assert localEntry != null : "Entry not found on log index: " + reqEntry.index() + " for " + req;

                // If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it (§5.3)
                if (reqEntry.term() != localEntry.term()) {
                    List<LogEntry> truncated = raftLog.truncateEntriesFrom(reqEntry.index());
                    if (logger.isFineEnabled()) {
                        logger.warning("Truncated " + truncated.size() + " entries from entry index: " + reqEntry.index() + " => "
                                + truncated);
                    } else {
                        logger.warning("Truncated " + truncated.size() + " entries from entry index: " + reqEntry.index());
                    }

                    raftNode.invalidateFuturesFrom(reqEntry.index());

                    //                            if (entry.index <= r.configurations.latestIndex) {
                    //                                r.configurations.latest = r.configurations.committed
                    //                                r.configurations.latestIndex = r.configurations.committedIndex
                    //                            }
                    newEntries = Arrays.copyOfRange(req.entries(), i, req.entryCount());
                    break;
                }
            }

            if (newEntries != null && newEntries.length > 0) {
                // Append any new entries not already in the log
                if (logger.isFineEnabled()) {
                    logger.fine("Appending " + newEntries.length + " entries: " + Arrays.toString(newEntries));
                }

                raftLog.appendEntries(newEntries);

                // Handle any new configuration changes
                //                        for _, newEntry := range newEntries {
                //                            r.processConfigurationLogEntry(newEntry)
                //                        }
            }
        }

        // Update the commit index
        if (req.leaderCommitIndex() > state.commitIndex()) {
            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            int newCommitIndex = min(req.leaderCommitIndex(), raftLog.lastLogIndex());
            logger.info("Setting commit index: " + newCommitIndex);
            state.commitIndex(newCommitIndex);
            //                    if r.configurations.latestIndex <= newCommitIndex {
            //                        r.configurations.committed = r.configurations.latest
            //                        r.configurations.committedIndex = r.configurations.latestIndex
            //                    }
            raftNode.processLogs();
        }

        int lastLogIndex = req.prevLogIndex() + req.entryCount();
        AppendSuccessResponse resp = new AppendSuccessResponse(raftNode.getLocalEndpoint(), state.term(), lastLogIndex);
        raftNode.send(resp, req.leader());
    }

    private AppendFailureResponse createFailureResponse(int term) {
        return new AppendFailureResponse(raftNode.getLocalEndpoint(), term, req.prevLogIndex() + 1);
    }

    @Override
    public int getKey() {
        return raftNode.getStripeKey();
    }
}
