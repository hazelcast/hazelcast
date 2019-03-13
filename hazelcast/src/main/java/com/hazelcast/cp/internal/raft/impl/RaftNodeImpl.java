/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.LeaderDemotedException;
import com.hazelcast.cp.exception.StaleAppendRequestException;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.cp.internal.raft.command.RaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.handler.AppendFailureResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.AppendRequestHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.AppendSuccessResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.InstallSnapshotHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.PreVoteRequestHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.PreVoteResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.VoteRequestHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.VoteResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.state.FollowerState;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.MembershipChangeTask;
import com.hazelcast.cp.internal.raft.impl.task.PreVoteTask;
import com.hazelcast.cp.internal.raft.impl.task.QueryTask;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;
import com.hazelcast.cp.internal.raft.impl.task.ReplicateTask;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.internal.util.SimpleCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.Clock;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.ACTIVE;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.TERMINATED;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.TERMINATING;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.UPDATING_GROUP_MEMBER_LIST;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link RaftNode}.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class RaftNodeImpl implements RaftNode {

    private static final int LEADER_ELECTION_TIMEOUT_RANGE = 1000;
    private static final long RAFT_NODE_INIT_DELAY_MILLIS = 500;
    private static final float RATIO_TO_KEEP_LOGS_AFTER_SNAPSHOT = 0.1f;

    private final CPGroupId groupId;
    private final ILogger logger;
    private final RaftState state;
    private final RaftIntegration raftIntegration;
    private final Endpoint localMember;
    private final Long2ObjectHashMap<SimpleCompletableFuture> futures = new Long2ObjectHashMap<SimpleCompletableFuture>();

    private final long heartbeatPeriodInMillis;
    private final int leaderElectionTimeout;
    private final int maxUncommittedEntryCount;
    private final int appendRequestMaxEntryCount;
    private final int commitIndexAdvanceCountToSnapshot;
    private final int maxMissedLeaderHeartbeatCount;
    private final long appendRequestBackoffTimeoutInMillis;
    private final int maxNumberOfLogsToKeepAfterSnapshot;
    private final Runnable appendRequestBackoffResetTask;

    private long lastAppendEntriesTimestamp;
    private boolean appendRequestBackoffResetTaskScheduled;
    private volatile RaftNodeStatus status = ACTIVE;

    public RaftNodeImpl(CPGroupId groupId, Endpoint localMember, Collection<Endpoint> members,
                        RaftAlgorithmConfig raftAlgorithmConfig, RaftIntegration raftIntegration) {
        checkNotNull(groupId);
        checkNotNull(localMember);
        checkNotNull(members);
        this.groupId = groupId;
        this.raftIntegration = raftIntegration;
        this.localMember = localMember;
        this.maxUncommittedEntryCount = raftAlgorithmConfig.getUncommittedEntryCountToRejectNewAppends();
        this.appendRequestMaxEntryCount = raftAlgorithmConfig.getAppendRequestMaxEntryCount();
        this.commitIndexAdvanceCountToSnapshot = raftAlgorithmConfig.getCommitIndexAdvanceCountToSnapshot();
        this.leaderElectionTimeout = (int) raftAlgorithmConfig.getLeaderElectionTimeoutInMillis();
        this.heartbeatPeriodInMillis = raftAlgorithmConfig.getLeaderHeartbeatPeriodInMillis();
        this.maxMissedLeaderHeartbeatCount = raftAlgorithmConfig.getMaxMissedLeaderHeartbeatCount();
        this.maxNumberOfLogsToKeepAfterSnapshot = (int) (commitIndexAdvanceCountToSnapshot * RATIO_TO_KEEP_LOGS_AFTER_SNAPSHOT);
        this.appendRequestBackoffTimeoutInMillis = raftAlgorithmConfig.getAppendRequestBackoffTimeoutInMillis();
        int logCapacity = commitIndexAdvanceCountToSnapshot + maxUncommittedEntryCount + maxNumberOfLogsToKeepAfterSnapshot;
        this.state = new RaftState(groupId, localMember, members, logCapacity);
        this.logger = getLogger(RaftNode.class);
        this.appendRequestBackoffResetTask = new AppendRequestBackoffResetTask();
    }

    public ILogger getLogger(Class clazz) {
        String name = state.name();
        return raftIntegration.getLogger(clazz.getName() + "(" + name + ")");
    }

    @Override
    public CPGroupId getGroupId() {
        return groupId;
    }

    @Override
    public Endpoint getLocalMember() {
        return localMember;
    }

    // It reads the most recent write to the volatile leader field, however leader might be already changed.
    @Override
    public Endpoint getLeader() {
        return state.leader();
    }

    // It reads the volatile status field
    @Override
    public RaftNodeStatus getStatus() {
        return status;
    }

    @Override
    public Collection<Endpoint> getInitialMembers() {
        return state.initialMembers();
    }

    @Override
    public Collection<Endpoint> getCommittedMembers() {
        return state.committedGroupMembers().members();
    }

    @Override
    public Collection<Endpoint> getAppliedMembers() {
        return state.lastGroupMembers().members();
    }

    @Override
    public void forceSetTerminatedStatus() {
        execute(new Runnable() {
            @Override
            public void run() {
                if (!isTerminatedOrSteppedDown()) {
                    setStatus(TERMINATED);
                    if (localMember.equals(state.leader())) {
                        invalidateFuturesFrom(state.commitIndex() + 1);
                    }
                }
            }
        });
    }

    /**
     * Starts the periodic tasks, such as voting, leader failure-detection, snapshot handling.
     */
    public void start() {
        if (!raftIntegration.isReady()) {
            raftIntegration.schedule(new Runnable() {
                @Override
                public void run() {
                    start();
                }
            }, RAFT_NODE_INIT_DELAY_MILLIS, MILLISECONDS);
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Starting raft node: " + localMember + " for " + groupId
                    + " with " + state.memberCount() + " members: " + state.members());
        }
        raftIntegration.execute(new PreVoteTask(this, 0));

        scheduleLeaderFailureDetection();
    }

    @Override
    public void handlePreVoteRequest(PreVoteRequest request) {
        execute(new PreVoteRequestHandlerTask(this, request));
    }

    @Override
    public void handlePreVoteResponse(PreVoteResponse response) {
        execute(new PreVoteResponseHandlerTask(this, response));
    }

    @Override
    public void handleVoteRequest(VoteRequest request) {
        execute(new VoteRequestHandlerTask(this, request));
    }

    @Override
    public void handleVoteResponse(VoteResponse response) {
        execute(new VoteResponseHandlerTask(this, response));
    }

    @Override
    public void handleAppendRequest(AppendRequest request) {
        execute(new AppendRequestHandlerTask(this, request));
    }

    @Override
    public void handleAppendResponse(AppendSuccessResponse response) {
        execute(new AppendSuccessResponseHandlerTask(this, response));
    }

    @Override
    public void handleAppendResponse(AppendFailureResponse response) {
        execute(new AppendFailureResponseHandlerTask(this, response));
    }

    @Override
    public void handleInstallSnapshot(InstallSnapshot request) {
        execute(new InstallSnapshotHandlerTask(this, request));
    }

    @Override
    public ICompletableFuture replicate(Object operation) {
        SimpleCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new ReplicateTask(this, operation, resultFuture));
        return resultFuture;
    }

    @Override
    public ICompletableFuture replicateMembershipChange(Endpoint member, MembershipChangeMode mode) {
        SimpleCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new MembershipChangeTask(this, resultFuture, member, mode));
        return resultFuture;
    }

    @Override
    public ICompletableFuture replicateMembershipChange(Endpoint member, MembershipChangeMode mode,
                                                        long groupMembersCommitIndex) {
        SimpleCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new MembershipChangeTask(this, resultFuture, member, mode, groupMembersCommitIndex));
        return resultFuture;
    }

    @Override
    public ICompletableFuture query(Object operation, QueryPolicy queryPolicy) {
        SimpleCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new QueryTask(this, operation, queryPolicy, resultFuture));
        return resultFuture;
    }

    // It reads the volatile status field
    @Override
    public boolean isTerminatedOrSteppedDown() {
        return status == TERMINATED || status == STEPPED_DOWN;
    }

    public void setStatus(RaftNodeStatus newStatus) {
        if (this.status == TERMINATED || this.status == STEPPED_DOWN) {
            throw new IllegalStateException("Cannot set status: " + newStatus + " since already " + this.status);
        }

        RaftNodeStatus prevStatus = this.status;
        this.status = newStatus;

        if (prevStatus != newStatus) {
            if (newStatus == ACTIVE) {
                logger.info("Status is set to: " + newStatus);
            } else {
                logger.warning("Status is set to: " + newStatus);
            }
        }

        raftIntegration.onNodeStatusChange(newStatus);
    }

    /**
     * Returns a randomized leader election timeout in milliseconds based on configured timeout.
     *
     * @see RaftAlgorithmConfig#getLeaderElectionTimeoutInMillis()
     */
    public long getLeaderElectionTimeoutInMillis() {
        return RandomPicker.getInt(leaderElectionTimeout, leaderElectionTimeout + LEADER_ELECTION_TIMEOUT_RANGE);
    }

    /**
     * Returns the entry to be appended if the no-op entry append on leader election feature is enabled.
     */
    public Object getAppendedEntryOnLeaderElection() {
        return raftIntegration.getAppendedEntryOnLeaderElection();
    }

    /**
     * Returns true if a new entry with the operation is allowed to be replicated.
     * This method can be invoked only when the local Raft node is the leader.
     * <p/>
     * Replication is not allowed, when;
     * <ul>
     * <li>Node is terminating, terminated or stepped down. See {@link RaftNodeStatus}.</li>
     * <li>Raft log contains max allowed uncommitted entry count.
     * See {@link RaftAlgorithmConfig#getUncommittedEntryCountToRejectNewAppends()}.</li>
     * <li>The operation is a {@link RaftGroupCmd} and there's an ongoing membership change in group.</li>
     * <li>The operation is a membership change operation and there's no committed entry in this term yet.
     * See {@link RaftIntegration#getAppendedEntryOnLeaderElection()} ()}.</li>
     * </ul>
     */
    public boolean canReplicateNewEntry(Object operation) {
        if (isTerminatedOrSteppedDown()) {
            return false;
        }

        RaftLog log = state.log();
        long lastLogIndex = log.lastLogOrSnapshotIndex();
        long commitIndex = state.commitIndex();
        if (lastLogIndex - commitIndex >= maxUncommittedEntryCount) {
            return false;
        }

        if (status == TERMINATING) {
            return false;
        } else if (status == UPDATING_GROUP_MEMBER_LIST) {
            return state.lastGroupMembers().isKnownMember(getLocalMember()) && !(operation instanceof RaftGroupCmd);
        }

        if (operation instanceof UpdateRaftGroupMembersCmd) {
            // the leader must have committed an entry in its term to make a membership change
            // https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J

            // last committed entry is either in the last snapshot or still in the log
            LogEntry lastCommittedEntry = commitIndex == log.snapshotIndex() ? log.snapshot() : log.getLogEntry(commitIndex);
            assert lastCommittedEntry != null;

            return lastCommittedEntry.term() == state.term();
        }

        return true;
    }

    /**
     * Schedules periodic leader failure detection task.
     */
    private void scheduleLeaderFailureDetection() {
        schedule(new LeaderFailureDetectionTask(), getLeaderElectionTimeoutInMillis());
    }

    /**
     * Schedules periodic heartbeat task when a new leader is elected.
     */
    public void scheduleHeartbeat() {
        broadcastAppendRequest();
        schedule(new HeartbeatTask(), heartbeatPeriodInMillis);
    }

    public void send(PreVoteRequest request, Endpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(PreVoteResponse response, Endpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(VoteRequest request, Endpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(VoteResponse response, Endpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(AppendRequest request, Endpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(AppendSuccessResponse response, Endpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(AppendFailureResponse response, Endpoint target) {
        raftIntegration.send(response, target);
    }

    /**
     * Broadcasts append-entries request to all group members according to their nextIndex parameters.
     */
    public void broadcastAppendRequest() {
        for (Endpoint follower : state.remoteMembers()) {
            sendAppendRequest(follower);
        }
        updateLastAppendEntriesTimestamp();
    }

    /**
     * Sends an append-entries request to the follower member.
     * <p>
     * Log entries between follower's known nextIndex and latest appended entry index are sent in a batch.
     * Batch size can be {@link RaftAlgorithmConfig#getAppendRequestMaxEntryCount()} at most.
     * <p>
     * If follower's nextIndex is behind the latest snapshot index, then {@link InstallSnapshot} request is sent.
     * <p>
     * If leader doesn't know follower's matchIndex (if {@code matchIndex == 0}), then an empty append-entries is sent
     * to save bandwidth until leader learns the matchIndex of the follower.
     * <p>
     * If log entries contains multiple membership change entries, then entries batch is split to send only a single
     * membership change in single append-entries request.
     */
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    public void sendAppendRequest(Endpoint follower) {
        if (!raftIntegration.isReachable(follower)) {
            return;
        }

        RaftLog raftLog = state.log();
        LeaderState leaderState = state.leaderState();
        FollowerState followerState = leaderState.getFollowerState(follower);
        if (followerState.isAppendRequestBackoffSet()) {
            // The follower still has not sent a response for the last append request.
            // We will send a new append request either when the follower sends a response
            // or a back-off timeout occurs.
            return;
        }

        long nextIndex = followerState.nextIndex();

        // if the first log entry to be sent is put into the snapshot, check if we still keep it in the log
        // if we still keep that log entry and its previous entry, we don't need to send a snapshot
        if (nextIndex <= raftLog.snapshotIndex()
                && (!raftLog.containsLogEntry(nextIndex) || (nextIndex > 1 && !raftLog.containsLogEntry(nextIndex - 1)))) {
            InstallSnapshot installSnapshot = new InstallSnapshot(localMember, state.term(), raftLog.snapshot());
            if (logger.isFineEnabled()) {
                logger.fine("Sending " + installSnapshot + " to " + follower + " since next index: " + nextIndex
                        + " <= snapshot index: " + raftLog.snapshotIndex());
            }
            followerState.setMaxAppendRequestBackoff();
            scheduleAppendAckResetTask();
            raftIntegration.send(installSnapshot, follower);
            return;
        }

        int prevEntryTerm = 0;
        long prevEntryIndex = 0;
        LogEntry[] entries;
        boolean setAppendRequestBackoff = true;

        if (nextIndex > 1) {
            prevEntryIndex = nextIndex - 1;
            LogEntry prevEntry = (raftLog.snapshotIndex() == prevEntryIndex)
                    ? raftLog.snapshot() : raftLog.getLogEntry(prevEntryIndex);
            assert prevEntry != null : "Prev entry index: " + prevEntryIndex + ", snapshot: " + raftLog.snapshotIndex();
            prevEntryTerm = prevEntry.term();

            long matchIndex = followerState.matchIndex();
            if (matchIndex == 0) {
                // Until the leader has discovered where it and the follower's logs match,
                // the leader can send AppendEntries with no entries (like heartbeats) to save bandwidth.
                // We still need to enable append request backoff here because we do not want to bombard
                // the follower before we learn its match index
                entries = new LogEntry[0];
            } else if (nextIndex <= raftLog.lastLogOrSnapshotIndex()) {
                // Then, once the matchIndex immediately precedes the nextIndex,
                // the leader should begin to send the actual entries
                long end = min(nextIndex + appendRequestMaxEntryCount, raftLog.lastLogOrSnapshotIndex());
                entries = raftLog.getEntriesBetween(nextIndex, end);
            } else {
                // The follower has caught up with the leader. Sending an empty append request as a heartbeat...
                entries = new LogEntry[0];
                setAppendRequestBackoff = false;
            }
        } else if (nextIndex == 1 && raftLog.lastLogOrSnapshotIndex() > 0) {
            // Entries will be sent to the follower for the first time...
            long end = min(nextIndex + appendRequestMaxEntryCount, raftLog.lastLogOrSnapshotIndex());
            entries = raftLog.getEntriesBetween(nextIndex, end);
        } else {
            // There is no entry in the Raft log. Sending an empty append request as a heartbeat...
            entries = new LogEntry[0];
            setAppendRequestBackoff = false;
        }

        AppendRequest request = new AppendRequest(getLocalMember(), state.term(), prevEntryTerm, prevEntryIndex,
                state.commitIndex(), entries);

        if (logger.isFineEnabled()) {
            logger.fine("Sending " + request + " to " + follower + " with next index: " + nextIndex);
        }

        if (setAppendRequestBackoff) {
            followerState.setAppendRequestBackoff();
            scheduleAppendAckResetTask();
        }

        send(request, follower);
    }

    /**
     * Applies committed log entries between {@code lastApplied} and {@code commitIndex}, if there's any available.
     * If new entries are applied, {@link RaftState}'s {@code lastApplied} field is updated.
     *
     * @see RaftState#lastApplied()
     * @see RaftState#commitIndex()
     */
    public void applyLogEntries() {
        // Reject logs we've applied already
        long commitIndex = state.commitIndex();
        long lastApplied = state.lastApplied();

        if (commitIndex == lastApplied) {
            return;
        }

        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
        assert commitIndex > lastApplied
                : "commit index: " + commitIndex + " cannot be smaller than last applied: " + lastApplied;

        // Apply all the preceding logs
        RaftLog raftLog = state.log();
        for (long idx = state.lastApplied() + 1; idx <= commitIndex; idx++) {
            LogEntry entry = raftLog.getLogEntry(idx);
            if (entry == null) {
                String msg = "Failed to get log entry at index: " + idx;
                logger.severe(msg);
                throw new AssertionError(msg);
            }

            applyLogEntry(entry);

            // Update the lastApplied index
            state.lastApplied(idx);
        }

        assert status != TERMINATED || commitIndex == raftLog.lastLogOrSnapshotIndex()
                : "commit index: " + commitIndex + " must be equal to " + raftLog.lastLogOrSnapshotIndex() + " on termination.";

        if (state.role() == LEADER || state.role() == FOLLOWER) {
            takeSnapshotIfCommitIndexAdvanced();
        }
    }

    /**
     * Applies the log entry by executing operation attached and set execution result to
     * the related future if any available.
     */
    private void applyLogEntry(LogEntry entry) {
        if (logger.isFineEnabled()) {
            logger.fine("Processing " + entry);
        }

        Object response = null;
        Object operation = entry.operation();
        if (operation instanceof RaftGroupCmd) {
            if (operation instanceof DestroyRaftGroupCmd) {
                setStatus(TERMINATED);
            } else if (operation instanceof UpdateRaftGroupMembersCmd) {
                if (state.lastGroupMembers().index() < entry.index()) {
                    setStatus(UPDATING_GROUP_MEMBER_LIST);
                    UpdateRaftGroupMembersCmd op = (UpdateRaftGroupMembersCmd) operation;
                    updateGroupMembers(entry.index(), op.getMembers());
                }

                assert status == UPDATING_GROUP_MEMBER_LIST : "STATUS: " + status;
                assert state.lastGroupMembers().index() == entry.index();

                state.commitGroupMembers();
                UpdateRaftGroupMembersCmd cmd = (UpdateRaftGroupMembersCmd) operation;
                if (cmd.getMember().equals(localMember) && cmd.getMode() == MembershipChangeMode.REMOVE) {
                    setStatus(STEPPED_DOWN);
                    // If I am the leader, I may have some waiting futures whose operations are already committed
                    // but responses are not decided yet. When I leave the cluster after my shutdown, invocations
                    // of those futures will receive MemberLeftException and retry. However, if I have an invocation
                    // during the shutdown process, its future will not complete unless I notify it here.
                    // Although LeaderDemotedException is designed for another case, we use it here since
                    // invocations internally retry when they receive LeaderDemotedException.
                    invalidateFuturesUntil(entry.index() - 1, new LeaderDemotedException(localMember, null));
                } else {
                    setStatus(ACTIVE);
                }
                response = entry.index();
            } else {
                response = new IllegalArgumentException("Invalid command: " + operation);
            }
        } else {
            response = raftIntegration.runOperation(operation, entry.index());
        }

        if (response == PostponedResponse.INSTANCE) {
            // postpone sending response
            return;
        }
        completeFuture(entry.index(), response);
    }

    public void updateLastAppendEntriesTimestamp() {
        lastAppendEntriesTimestamp = Clock.currentTimeMillis();
    }

    public long lastAppendEntriesTimestamp() {
        return lastAppendEntriesTimestamp;
    }

    public RaftState state() {
        return state;
    }

    /**
     * Executes query operation sets execution result to the future.
     */
    public void runQueryOperation(Object operation, SimpleCompletableFuture resultFuture) {
        long commitIndex = state.commitIndex();
        Object result = raftIntegration.runOperation(operation, commitIndex);
        resultFuture.setResult(result);
    }

    /**
     * Executes task using {@link RaftIntegration#execute(Runnable)}.
     */
    public void execute(Runnable task) {
        raftIntegration.execute(task);
    }

    /**
     * Schedules task using {@link RaftIntegration#schedule(Runnable, long, TimeUnit)}.
     */
    public void schedule(Runnable task, long delayInMillis) {
        if (isTerminatedOrSteppedDown()) {
            return;
        }

        raftIntegration.schedule(task, delayInMillis, MILLISECONDS);
    }

    /**
     * Registers the future for the appended entry with its {@code entryIndex}.
     */
    public void registerFuture(long entryIndex, SimpleCompletableFuture future) {
        SimpleCompletableFuture f = futures.put(entryIndex, future);
        assert f == null : "Future object is already registered for entry index: " + entryIndex;
    }

    /**
     * Completes the future registered with {@code entryIndex}.
     */
    public void completeFuture(long entryIndex, Object response) {
        SimpleCompletableFuture f = futures.remove(entryIndex);
        if (f != null) {
            f.setResult(response);
        }
    }

    /**
     * Invalidates futures registered with indexes {@code >= entryIndex}. Note that {@code entryIndex} is inclusive.
     * {@link LeaderDemotedException} is set a result to futures.
     */
    public void invalidateFuturesFrom(long entryIndex) {
        int count = 0;
        Iterator<Entry<Long, SimpleCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, SimpleCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index >= entryIndex) {
                entry.getValue().setResult(new LeaderDemotedException(localMember, state.leader()));
                iterator.remove();
                count++;
            }
        }

        if (count > 0) {
            logger.warning("Invalidated " + count + " futures from log index: " + entryIndex);
        }
    }

    /**
     * Invalidates futures registered with indexes {@code <= entryIndex}. Note that {@code entryIndex} is inclusive.
     * {@link StaleAppendRequestException} is set a result to futures.
     */
    private void invalidateFuturesUntil(long entryIndex, Object response) {
        int count = 0;
        Iterator<Entry<Long, SimpleCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, SimpleCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index <= entryIndex) {
                entry.getValue().setResult(response);
                iterator.remove();
                count++;
            }
        }

        if (count > 0) {
            logger.warning("Invalidated " + count + " futures until log index: " + entryIndex);
        }
    }

    /**
     * Takes a snapshot if {@code commitIndex} advanced equal to or more than
     * {@link RaftAlgorithmConfig#getCommitIndexAdvanceCountToSnapshot()}.
     * <p>
     * Snapshot is not created if there's an ongoing membership change or Raft group is being destroyed.
     */
    private void takeSnapshotIfCommitIndexAdvanced() {
        long commitIndex = state.commitIndex();
        if ((commitIndex - state.log().snapshotIndex()) < commitIndexAdvanceCountToSnapshot) {
            return;
        }

        if (isTerminatedOrSteppedDown()) {
            // If the status is UPDATING_MEMBER_LIST or TERMINATING, it means the status is normally ACTIVE
            // and there is an appended but not committed RaftGroupCmd.
            // If the status is TERMINATED or STEPPED_DOWN, then there will not be any new appends.
            return;
        }

        RaftLog log = state.log();
        Object snapshot = raftIntegration.takeSnapshot(commitIndex);
        if (snapshot instanceof Throwable) {
            Throwable t = (Throwable) snapshot;
            logger.severe("Could not take snapshot at commit index: " + commitIndex, t);
            return;
        }

        int snapshotTerm = log.getLogEntry(commitIndex).term();
        RaftGroupMembers members = state.committedGroupMembers();
        SnapshotEntry snapshotEntry = new SnapshotEntry(snapshotTerm, commitIndex, snapshot, members.index(), members.members());

        long minMatchIndex = 0L;
        LeaderState leaderState = state.leaderState();
        if (leaderState != null) {
            long[] indices = leaderState.matchIndices();
            // Last slot is reserved for leader index,
            // and always zero. That's why we are skipping it.
            Arrays.sort(indices, 0, indices.length - 1);
            minMatchIndex = indices[0];
        }

        long truncateLogsUpToIndex = max(commitIndex - maxNumberOfLogsToKeepAfterSnapshot, minMatchIndex);
        int truncated = log.setSnapshot(snapshotEntry, truncateLogsUpToIndex);

        if (logger.isFineEnabled()) {
            logger.fine(snapshotEntry + " is taken, " + truncated + " entries are truncated.");
        }
    }

    /**
     * Restores the snapshot sent by the leader if it's not applied before.
     *
     * @return true if snapshot is restores, false otherwise.
     */
    public boolean installSnapshot(SnapshotEntry snapshot) {
        long commitIndex = state.commitIndex();
        if (commitIndex > snapshot.index()) {
            logger.info("Ignored stale " + snapshot + ", commit index at: " + commitIndex);
            return false;
        } else if (commitIndex == snapshot.index()) {
            logger.info("Ignored " + snapshot + " since commit index is same.");
            return true;
        }

        state.commitIndex(snapshot.index());
        int truncated = state.log().setSnapshot(snapshot);
        if (truncated > 0) {
            logger.info(truncated + " entries are truncated to install " + snapshot);
        }

        raftIntegration.restoreSnapshot(snapshot.operation(), snapshot.index());

        // If I am installing a snapshot, it means I am still present in the last member list,
        // but it is possible that the last entry I appended before the snapshot could be a membership change.
        // Because of this, I need to update my status.
        // Nevertheless, I may not be present in the restored member list, which is ok.

        setStatus(ACTIVE);
        state.restoreGroupMembers(snapshot.groupMembersLogIndex(), snapshot.groupMembers());
        printMemberState();

        state.lastApplied(snapshot.index());
        invalidateFuturesUntil(snapshot.index(), new StaleAppendRequestException(state.leader()));
        logger.info(snapshot + " is installed.");

        return true;
    }

    public void printMemberState() {
        CPGroupId groupId = state.groupId();
        StringBuilder sb = new StringBuilder("\n\nCP Group Members {")
                .append("groupId: ").append(groupId.name()).append("(").append(groupId.id()).append(")")
                .append(", size:").append(state.memberCount())
                .append(", term:").append(state.term())
                .append(", logIndex:").append(state.membersLogIndex())
                .append("} [");

        for (Endpoint member : state.members()) {
            sb.append("\n\t").append(member);
            if (localMember.equals(member)) {
                sb.append(" - ").append(state.role()).append(" this");
            } else if (member.equals(state.leader())) {
                sb.append(" - ").append(LEADER);
            }
        }
        sb.append("\n]\n");
        logger.info(sb.toString());
    }

    /**
     * Updates Raft group members.
     *
     * @see RaftState#updateGroupMembers(long, Collection)
     */
    public void updateGroupMembers(long logIndex, Collection<Endpoint> members) {
        state.updateGroupMembers(logIndex, members);
        printMemberState();
    }

    /**
     * Resets Raft group members back.
     *
     * @see RaftState#resetGroupMembers()
     */
    public void resetGroupMembers() {
        state.resetGroupMembers();
        printMemberState();
    }

    /**
     * Schedules a task to reset append request backoff flags,
     * if not scheduled already.
     *
     * @see RaftAlgorithmConfig#getAppendRequestBackoffTimeoutInMillis()
     */
    private void scheduleAppendAckResetTask() {
        if (appendRequestBackoffResetTaskScheduled) {
            return;
        }

        appendRequestBackoffResetTaskScheduled = true;
        schedule(appendRequestBackoffResetTask, appendRequestBackoffTimeoutInMillis);
    }

    /**
     * Periodic heartbeat task, which is scheduled on leader only with {@link #heartbeatPeriodInMillis} delay,
     * and sends heartbeat messages (append-entries) if no append-entries request is sent
     * since {@link #lastAppendEntriesTimestamp}.
     */
    private class HeartbeatTask extends RaftNodeStatusAwareTask {
        HeartbeatTask() {
            super(RaftNodeImpl.this);
        }

        @Override
        protected void innerRun() {
            if (state.role() == LEADER) {
                if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - heartbeatPeriodInMillis) {
                    broadcastAppendRequest();
                }

                scheduleHeartbeat();
            }
        }
    }

    /**
     * Leader failure detection task checks whether leader exists and is reachable. Runs pre-vote mechanism
     * if leader doesn't exist or is unreachable or is an unknown member.
     */
    private class LeaderFailureDetectionTask extends RaftNodeStatusAwareTask {
        LeaderFailureDetectionTask() {
            super(RaftNodeImpl.this);
        }

        @Override
        protected void innerRun() {
            try {
                Endpoint leader = state.leader();
                if (leader == null) {
                    if (state.role() == FOLLOWER) {
                        logger.warning("We are FOLLOWER and there is no current leader. Will start new election round...");
                        runPreVoteTask();
                    }
                } else if (!raftIntegration.isReachable(leader)) {
                    logger.warning("Current leader " + leader + " is not reachable. Will start new election round...");
                    resetLeaderAndStartElection();
                } else if (isLeaderHeartbeatTimedOut()) {
                    // Even though leader endpoint is reachable by raft-integration,
                    // leader itself may be crashed and another member may be restarted on the same endpoint.
                    logger.warning("Current leader " + leader + "'s heartbeats are timed-out. Will start new election round...");
                    resetLeaderAndStartElection();
                } else if (!state.committedGroupMembers().isKnownMember(leader)) {
                    logger.warning("Current leader " + leader + " is not member anymore. Will start new election round...");
                    resetLeaderAndStartElection();
                }
            } finally {
                scheduleLeaderFailureDetection();
            }
        }

        private boolean isLeaderHeartbeatTimedOut() {
            long missedHeartbeatThreshold = maxMissedLeaderHeartbeatCount * heartbeatPeriodInMillis;
            return lastAppendEntriesTimestamp + missedHeartbeatThreshold < Clock.currentTimeMillis();
        }

        final void resetLeaderAndStartElection() {
            state.leader(null);
            printMemberState();
            runPreVoteTask();
        }

        private void runPreVoteTask() {
            if (state.preCandidateState() == null) {
                new PreVoteTask(RaftNodeImpl.this, state.term()).run();
            }
        }
    }

    /**
     * If the append request backoff flag is set for any follower, this task resets
     * the flag, sends a new append request, and schedules itself again.
     */
    private class AppendRequestBackoffResetTask extends RaftNodeStatusAwareTask {
        AppendRequestBackoffResetTask() {
            super(RaftNodeImpl.this);
        }

        @Override
        protected void innerRun() {
            appendRequestBackoffResetTaskScheduled = false;
            LeaderState leaderState = state.leaderState();

            if (leaderState != null) {
                Map<Endpoint, FollowerState> followerStates = leaderState.getFollowerStates();
                for (Entry<Endpoint, FollowerState> entry : followerStates.entrySet()) {
                    FollowerState followerState = entry.getValue();
                    if (!followerState.isAppendRequestBackoffSet()) {
                        continue;
                    }
                    if (followerState.completeAppendRequestBackoffRound()) {
                        // This follower has not sent a response to the last append request.
                        // Send another append request
                        sendAppendRequest(entry.getKey());
                    }
                    // Schedule the task again, we still have backoff flag set followers
                    scheduleAppendAckResetTask();
                }
            }
        }
    }
}
