package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftNodeStatus;
import com.hazelcast.raft.impl.RaftRole;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.command.ApplyRaftGroupMembersCmd;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import com.hazelcast.raft.command.DestroyRaftGroupCmd;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.min;
import static java.util.Arrays.asList;

/**
 * Handles {@link AppendRequest} sent by the leader. Responds with an {@link AppendSuccessResponse} if append
 * is successful, responds with an {@link AppendFailureResponse} otherwise.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendSuccessResponse
 * @see AppendFailureResponse
 */
public class AppendRequestHandlerTask extends RaftNodeStatusAwareTask implements Runnable {
    private final AppendRequest req;

    public AppendRequestHandlerTask(RaftNodeImpl raftNode, AppendRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        if (logger.isFineEnabled()) {
            logger.fine("Received " + req);
        }

        RaftState state = raftNode.state();

        // Reply false if term < currentTerm (§5.1)
        if (req.term() < state.term()) {
            logger.warning("Stale " + req + " received in current term: " + state.term());
            raftNode.send(createFailureResponse(state.term()), req.leader());
            return;
        }

        RaftLog raftLog = state.log();

        // Transform into follower if a newer term is seen or another node wins the election of the current term
        if (req.term() > state.term() || state.role() != RaftRole.FOLLOWER) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.info("Demoting to FOLLOWER from current role: " + state.role() + ", term: " + state.term()
                    + " to new term: " + req.term() + " and leader: " + req.leader());
            state.toFollower(req.term());
            raftNode.printMemberState();
        }

        if (!req.leader().equals(state.leader())) {
            logger.info("Setting leader: " + req.leader());
            state.leader(req.leader());
            raftNode.printMemberState();
        }

        // Verify the last log entry
        if (req.prevLogIndex() > 0) {
            long lastLogIndex = raftLog.lastLogOrSnapshotIndex();
            int lastLogTerm = raftLog.lastLogOrSnapshotTerm();

            int prevLogTerm;
            if (req.prevLogIndex() == lastLogIndex) {
                prevLogTerm = lastLogTerm;
            } else {
                // Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
                LogEntry prevLog = raftLog.getLogEntry(req.prevLogIndex());
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
            long lastLogIndex = raftLog.lastLogOrSnapshotIndex();

            LogEntry[] newEntries = null;
            for (int i = 0; i < req.entryCount(); i++) {
                LogEntry reqEntry = req.entries()[i];

                if (reqEntry.index() > lastLogIndex) {
                    newEntries = Arrays.copyOfRange(req.entries(), i, req.entryCount());
                    break;
                }

                LogEntry localEntry = raftLog.getLogEntry(reqEntry.index());

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
                    handleRaftGroupCmd(truncated, true);

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
                handleRaftGroupCmd(asList(newEntries), false);
            }
        }

        long lastLogIndex = req.prevLogIndex() + req.entryCount();

        // Update the commit index
        if (req.leaderCommitIndex() > state.commitIndex()) {
            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            long newCommitIndex = min(req.leaderCommitIndex(), lastLogIndex);
            logger.fine("Setting commit index: " + newCommitIndex);
            state.commitIndex(newCommitIndex);
            raftNode.applyLogEntries();
        }

        raftNode.updateLastAppendEntriesTimestamp();
        AppendSuccessResponse resp = new AppendSuccessResponse(raftNode.getLocalMember(), state.term(), lastLogIndex);
        raftNode.send(resp, req.leader());
    }

    private void handleRaftGroupCmd(List<LogEntry> entries, boolean revert) {
        for (LogEntry entry : entries) {
            if (entry.operation() instanceof DestroyRaftGroupCmd) {
                RaftNodeStatus status = revert ? RaftNodeStatus.ACTIVE : RaftNodeStatus.TERMINATING;
                raftNode.setStatus(status);
                return;
            } else if (entry.operation() instanceof ApplyRaftGroupMembersCmd) {
                RaftNodeStatus status = revert ? RaftNodeStatus.ACTIVE : RaftNodeStatus.CHANGING_MEMBERSHIP;
                raftNode.setStatus(status);
                if (revert) {
                    raftNode.resetGroupMembers();
                } else {
                    ApplyRaftGroupMembersCmd op = (ApplyRaftGroupMembersCmd) entry.operation();
                    raftNode.updateGroupMembers(entry.index(), op.getMembers());
                }
                return;
            }
        }
    }

    private AppendFailureResponse createFailureResponse(int term) {
        return new AppendFailureResponse(raftNode.getLocalMember(), term, req.prevLogIndex() + 1);
    }
}
