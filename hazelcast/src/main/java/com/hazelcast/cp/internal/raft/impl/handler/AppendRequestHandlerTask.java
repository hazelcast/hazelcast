/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.raft.impl.handler;

import com.hazelcast.cp.internal.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.cp.internal.raft.command.RaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;
import static java.lang.Math.min;

/**
 * Handles {@link AppendRequest} sent by the leader. Responds with
 * an {@link AppendSuccessResponse} if append is successful, responds with
 * an {@link AppendFailureResponse} otherwise.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
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
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength",
                       "checkstyle:nestedifdepth"})
    // Justification: It is easier to follow the AppendEntriesRPC logic in a single method
    protected void innerRun() {
        if (logger.isFineEnabled()) {
            logger.fine("Received " + req);
        }

        RaftState state = raftNode.state();

        // Reply false if term < currentTerm (§5.1)
        if (req.term() < state.term()) {
            if (logger.isFineEnabled()) {
                logger.warning("Stale " + req + " received in current term: " + state.term());
            }

            raftNode.send(createFailureResponse(state.term()), req.leader());
            return;
        }

        // Transform into follower if a newer term is seen or another node wins the election of the current term
        if (req.term() > state.term() || state.role() != FOLLOWER) {
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
            logger.info("Demoting to FOLLOWER from current role: " + state.role() + ", term: " + state.term()
                    + " to new term: " + req.term() + " and leader: " + req.leader());
            raftNode.toFollower(req.term());
        }

        if (!req.leader().equals(state.leader())) {
            logger.info("Setting leader: " + req.leader());
            raftNode.leader(req.leader());
        }

        RaftLog raftLog = state.log();

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
                    if (logger.isFineEnabled()) {
                        logger.warning("Failed to get previous log index for " + req + ", last log index: " + lastLogIndex);
                    }

                    raftNode.send(createFailureResponse(req.term()), req.leader());
                    return;
                }
                prevLogTerm = prevLog.term();
            }

            if (req.prevLogTerm() != prevLogTerm) {
                if (logger.isFineEnabled()) {
                    logger.warning("Previous log term of " + req + " is different than ours: " + prevLogTerm);
                }

                raftNode.send(createFailureResponse(req.term()), req.leader());
                return;
            }
        }

        int truncatedAppendRequestEntryCount = 0;
        LogEntry[] newEntries = null;
        // Process any new entries
        if (req.entryCount() > 0) {
            // Delete any conflicting entries, skip any duplicates
            long lastLogIndex = raftLog.lastLogOrSnapshotIndex();

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
                    List<LogEntry> truncatedEntries = raftLog.deleteEntriesFrom(reqEntry.index());
                    if (logger.isFineEnabled()) {
                        logger.warning("Truncated " + truncatedEntries.size() + " entries from entry index: "
                                + reqEntry.index() + " => " + truncatedEntries);
                    } else {
                        logger.warning("Truncated " + truncatedEntries.size() + " entries from entry index: "
                                + reqEntry.index());
                    }

                    raftNode.invalidateFuturesFrom(reqEntry.index());
                    revertPreAppliedRaftGroupCmd(truncatedEntries);
                    newEntries = Arrays.copyOfRange(req.entries(), i, req.entryCount());
                    raftLog.flush();
                    break;
                }
            }

            if (newEntries != null && newEntries.length > 0) {
                if (raftLog.availableCapacity() < newEntries.length) {
                    if (logger.isFineEnabled()) {
                        logger.warning("Truncating " + newEntries.length + " entries to " + raftLog.availableCapacity()
                                + " to fit into the available capacity of the Raft log");
                    }

                    truncatedAppendRequestEntryCount = newEntries.length - raftLog.availableCapacity();
                    newEntries = Arrays.copyOf(newEntries, raftLog.availableCapacity());
                }

                // Append any new entries not already in the log
                if (logger.isFineEnabled()) {
                    logger.fine("Appending " + newEntries.length + " entries: " + Arrays.toString(newEntries));
                }

                raftLog.appendEntries(newEntries);
                raftLog.flush();
            }
        }

        // I cannot use raftLog.lastLogOrSnapshotIndex() for lastLogIndex because my log may contain
        // some uncommitted entries from the previous leader and those entries will be truncated soon
        // I can only send a response based on how many entries I have appended from this append request
        long lastLogIndex = req.prevLogIndex() + req.entryCount() - truncatedAppendRequestEntryCount;
        long oldCommitIndex = state.commitIndex();

        // Update the commit index
        if (req.leaderCommitIndex() > oldCommitIndex) {
            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            long newCommitIndex = min(req.leaderCommitIndex(), lastLogIndex);
            if (logger.isFineEnabled()) {
                logger.fine("Setting commit index: " + newCommitIndex);
            }
            state.commitIndex(newCommitIndex);
        }

        raftNode.updateLastAppendEntriesTimestamp();

        try {
            AppendSuccessResponse resp = new AppendSuccessResponse(localMember(), state.term(), lastLogIndex, req.queryRound());
            raftNode.send(resp, req.leader());
        } finally {
            if (state.commitIndex() > oldCommitIndex) {
                raftNode.applyLogEntries();
            }
            if (newEntries != null) {
                preApplyRaftGroupCmd(newEntries, state.commitIndex());
            }
        }
    }


    private void preApplyRaftGroupCmd(LogEntry[] entries, long commitIndex) {
        // There can be at most one appended & not-committed command in the log
        for (LogEntry entry : entries) {
            Object operation = entry.operation();
            if (entry.index() <= commitIndex || !(operation instanceof RaftGroupCmd)) {
                continue;
            }

            if (operation instanceof DestroyRaftGroupCmd) {
                raftNode.setStatus(RaftNodeStatus.TERMINATING);
            } else if (operation instanceof UpdateRaftGroupMembersCmd) {
                raftNode.setStatus(RaftNodeStatus.UPDATING_GROUP_MEMBER_LIST);
                UpdateRaftGroupMembersCmd op = (UpdateRaftGroupMembersCmd) operation;
                raftNode.updateGroupMembers(entry.index(), op.getMembers());
            } else {
                assert false : "Invalid command: " + operation + " in " + raftNode.getGroupId();
            }

            return;
        }
    }

    private void revertPreAppliedRaftGroupCmd(List<LogEntry> entries) {
        // I am reverting appended-but-uncommitted entries and there can be at most 1 uncommitted Raft command...
        List<LogEntry> commandEntries = new ArrayList<>();
        for (LogEntry entry : entries) {
            if (entry.operation() instanceof RaftGroupCmd) {
                commandEntries.add(entry);
            }
        }

        assert commandEntries.size() <= 1 : "Reverted command entries: " + commandEntries;

        for (LogEntry entry : entries) {
            if (entry.operation() instanceof DestroyRaftGroupCmd) {
                raftNode.setStatus(RaftNodeStatus.ACTIVE);
            } else if (entry.operation() instanceof UpdateRaftGroupMembersCmd) {
                raftNode.setStatus(RaftNodeStatus.ACTIVE);
                raftNode.resetGroupMembers();
            }
        }
    }

    private AppendFailureResponse createFailureResponse(int term) {
        return new AppendFailureResponse(localMember(), term, req.prevLogIndex() + 1);
    }
}
