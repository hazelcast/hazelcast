/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
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
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.handler.AppendFailureResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.AppendRequestHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.AppendSuccessResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.InstallSnapshotRequestHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.InstallSnapshotResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.PreVoteRequestHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.PreVoteResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.TriggerLeaderElectionHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.VoteRequestHandlerTask;
import com.hazelcast.cp.internal.raft.impl.handler.VoteResponseHandlerTask;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.NopRaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raft.impl.state.FollowerState;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.QueryState;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raft.impl.task.InitLeadershipTransferTask;
import com.hazelcast.cp.internal.raft.impl.task.MembershipChangeTask;
import com.hazelcast.cp.internal.raft.impl.task.PreVoteTask;
import com.hazelcast.cp.internal.raft.impl.task.QueryTask;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;
import com.hazelcast.cp.internal.raft.impl.task.ReplicateTask;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil.createChunksFromSnapshot;
import static com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil.extractChunksFrom;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.ACTIVE;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.INITIAL;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.TERMINATED;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.TERMINATING;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.UPDATING_GROUP_MEMBER_LIST;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;
import static com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry.isNonInitial;
import static com.hazelcast.cp.internal.raft.impl.state.RaftState.newRaftState;
import static com.hazelcast.cp.internal.raft.impl.state.RaftState.restoreRaftState;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Arrays.sort;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Implementation of {@link RaftNode}.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class RaftNodeImpl implements RaftNode {

    private static final int LEADER_ELECTION_TIMEOUT_RANGE = 1000;
    private static final long RAFT_NODE_INIT_DELAY_MILLIS = 500;
    private static final float RATIO_TO_KEEP_LOGS_AFTER_SNAPSHOT = 0.1f;
    private static final long METADATA_CHUNK_INDEX = -1L;
    private static final int METADATA_CHUNK_NUMBER = 0;

    protected final CPGroupId groupId;
    protected final ILogger logger;
    protected final RaftState state;
    protected final RaftIntegration raftIntegration;
    private final ChunkedSnapshotInstaller chunkedSnapshotInstaller;
    private final Long2ObjectHashMap<InternalCompletableFuture> futures = new Long2ObjectHashMap<>();

    private final long heartbeatPeriodInMillis;
    private final int leaderElectionTimeout;
    private final int maxUncommittedEntryCount;
    private final int appendRequestMaxEntryCount;
    private final int commitIndexAdvanceCountToSnapshot;
    private final int maxMissedLeaderHeartbeatCount;
    private final long appendRequestBackoffTimeoutInMillis;
    private final int maxNumberOfLogsToKeepAfterSnapshot;
    private final Runnable appendRequestBackoffResetTask;
    private final Runnable flushTask;
    private final RaftStateStore stateStore;

    private long lastAppendEntriesTimestamp;
    private long appendFailureResponsesSent;
    private boolean appendRequestBackoffResetTaskScheduled;
    private boolean flushTaskSubmitted;
    private long lastSnapshotBuildDurationMs;
    private long snapshotBuildCount;

    private volatile RaftNodeStatus status = INITIAL;

    @SuppressWarnings("checkstyle:executablestatementcount")
    RaftNodeImpl(CPGroupId groupId, RaftEndpoint localMember, Collection<RaftEndpoint> members, RaftStateStore stateStore,
                 RaftAlgorithmConfig raftAlgorithmConfig, RaftIntegration raftIntegration) {
        checkNotNull(groupId);
        checkNotNull(localMember);
        checkNotNull(members);
        checkNotNull(stateStore);
        checkNotNull(raftAlgorithmConfig);
        checkNotNull(raftIntegration);
        this.groupId = groupId;
        this.raftIntegration = raftIntegration;
        this.maxUncommittedEntryCount = raftAlgorithmConfig.getUncommittedEntryCountToRejectNewAppends();
        this.appendRequestMaxEntryCount = raftAlgorithmConfig.getAppendRequestMaxEntryCount();
        this.commitIndexAdvanceCountToSnapshot = raftAlgorithmConfig.getCommitIndexAdvanceCountToSnapshot();
        this.leaderElectionTimeout = (int) raftAlgorithmConfig.getLeaderElectionTimeoutInMillis();
        this.heartbeatPeriodInMillis = raftAlgorithmConfig.getLeaderHeartbeatPeriodInMillis();
        this.maxMissedLeaderHeartbeatCount = raftAlgorithmConfig.getMaxMissedLeaderHeartbeatCount();
        this.maxNumberOfLogsToKeepAfterSnapshot = (int) (commitIndexAdvanceCountToSnapshot * RATIO_TO_KEEP_LOGS_AFTER_SNAPSHOT);
        this.appendRequestBackoffTimeoutInMillis = raftAlgorithmConfig.getAppendRequestBackoffTimeoutInMillis();
        int logCapacity = commitIndexAdvanceCountToSnapshot + maxUncommittedEntryCount + maxNumberOfLogsToKeepAfterSnapshot;
        this.state = newRaftState(groupId, localMember, members, logCapacity, stateStore);
        this.logger = getLogger(RaftNode.class);
        this.appendRequestBackoffResetTask = new AppendRequestBackoffResetTask();
        this.stateStore = stateStore;
        if (stateStore instanceof NopRaftStateStore) {
            this.flushTask = null;
            this.flushTaskSubmitted = true;
        } else {
            this.flushTask = new FlushTask();
        }
        this.chunkedSnapshotInstaller = new ChunkedSnapshotInstaller(this);
    }

    @SuppressWarnings("checkstyle:executablestatementcount")
    RaftNodeImpl(CPGroupId groupId, RestoredRaftState restoredState, RaftStateStore stateStore,
                 RaftAlgorithmConfig config, RaftIntegration raftIntegration) {
        checkNotNull(groupId);
        checkNotNull(stateStore);
        checkNotNull(raftIntegration);
        checkNotNull(groupId);
        this.groupId = groupId;
        this.raftIntegration = raftIntegration;
        this.maxUncommittedEntryCount = config.getUncommittedEntryCountToRejectNewAppends();
        this.appendRequestMaxEntryCount = config.getAppendRequestMaxEntryCount();
        this.commitIndexAdvanceCountToSnapshot = config.getCommitIndexAdvanceCountToSnapshot();
        this.leaderElectionTimeout = (int) config.getLeaderElectionTimeoutInMillis();
        this.heartbeatPeriodInMillis = config.getLeaderHeartbeatPeriodInMillis();
        this.maxMissedLeaderHeartbeatCount = config.getMaxMissedLeaderHeartbeatCount();
        this.maxNumberOfLogsToKeepAfterSnapshot = (int) (commitIndexAdvanceCountToSnapshot * RATIO_TO_KEEP_LOGS_AFTER_SNAPSHOT);
        this.appendRequestBackoffTimeoutInMillis = config.getAppendRequestBackoffTimeoutInMillis();
        int logCapacity = commitIndexAdvanceCountToSnapshot + maxUncommittedEntryCount + maxNumberOfLogsToKeepAfterSnapshot;
        this.state = restoreRaftState(groupId, restoredState, logCapacity, stateStore);
        this.logger = getLogger(RaftNode.class);
        this.appendRequestBackoffResetTask = new AppendRequestBackoffResetTask();
        this.stateStore = stateStore;
        if (stateStore instanceof NopRaftStateStore) {
            this.flushTask = null;
            this.flushTaskSubmitted = true;
        } else {
            this.flushTask = new FlushTask();
        }
        this.chunkedSnapshotInstaller = new ChunkedSnapshotInstaller(this);
    }

    /**
     * Creates a new Raft node with an empty initial state.
     */
    public static RaftNodeImpl newRaftNode(CPGroupId groupId, RaftEndpoint localMember, Collection<RaftEndpoint> members,
                                           RaftAlgorithmConfig config, RaftIntegration integration) {
        return new RaftNodeImpl(groupId, checkNotNull(localMember), checkNotNull(members), NopRaftStateStore.INSTANCE, config,
                integration);
    }

    /**
     * Creates a new Raft node with an empty initial state
     * and a {@link RaftStateStore} to persist Raft state changes
     */
    public static RaftNodeImpl newRaftNode(CPGroupId groupId, RaftEndpoint localMember, Collection<RaftEndpoint> members,
                                           RaftAlgorithmConfig config, RaftIntegration integration,
                                           RaftStateStore raftStateStore) {
        return new RaftNodeImpl(groupId, checkNotNull(localMember), checkNotNull(members), raftStateStore, config,
                integration);
    }

    /**
     * Creates a new Raft node with restored Raft state
     */
    public static RaftNodeImpl restoreRaftNode(CPGroupId groupId, RestoredRaftState restoredState, RaftAlgorithmConfig config,
                                               RaftIntegration integration) {
        return new RaftNodeImpl(groupId, restoredState, NopRaftStateStore.INSTANCE, config, integration);
    }

    /**
     * Creates a new Raft node with restored Raft state
     * and a {@link RaftStateStore} to persist Raft state changes
     */
    public static RaftNodeImpl restoreRaftNode(CPGroupId groupId, RestoredRaftState restoredState, RaftAlgorithmConfig config,
                                               RaftIntegration integration, RaftStateStore raftStateStore) {
        return new RaftNodeImpl(groupId, restoredState, raftStateStore, config, integration);
    }

    public ChunkedSnapshotInstaller getChunkedSnapshotInstaller() {
        return chunkedSnapshotInstaller;
    }

    /**
     * Wall-clock duration (ms) of the most recently completed local snapshot build including failed attempts.
     * Holds its last value between builds.
     */
    public long getLastSnapshotBuildDurationMs() {
        return lastSnapshotBuildDurationMs;
    }

    /**
     * Cumulative number of local snapshot builds taken so far, including failed attempts.
     */
    public long getSnapshotBuildCount() {
        return snapshotBuildCount;
    }

    public RaftIntegration getRaftIntegration() {
        return raftIntegration;
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
    public RaftEndpoint getLocalMember() {
        return state.localEndpoint();
    }

    @Override
    public RaftEndpoint getLeader() {
        // Reads the most recent write to the volatile leader field, however leader might be already changed.
        return state.leader();
    }

    @Override
    public RaftNodeStatus getStatus() {
        // Reads the volatile status field
        return status;
    }

    @Override
    public Collection<RaftEndpoint> getInitialMembers() {
        return state.initialMembers();
    }

    @Override
    public Collection<RaftEndpoint> getCommittedMembers() {
        return state.committedGroupMembers().members();
    }

    @Override
    public Collection<RaftEndpoint> getAppliedMembers() {
        return state.lastGroupMembers().members();
    }

    @Override
    public InternalCompletableFuture forceSetTerminatedStatus() {
        InternalCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        if (isTerminatedOrSteppedDown()) {
            if (logger.isFineEnabled()) {
                logger.fine("Already stepped down or terminated, not setting `TERMINATED` status.");
            }
            resultFuture.complete(null);
            return resultFuture;
        }

        execute(() -> {
            Throwable failure = null;
            try {
                if (isTerminatedOrSteppedDown()) {
                    return;
                } else if (status == INITIAL) {
                    setStatus(TERMINATED);
                    return;
                }

                invalidateFuturesFrom(state.commitIndex() + 1);
                LeaderState leaderState = state.leaderState();
                if (leaderState != null) {
                    for (BiTuple<Object, InternalCompletableFuture> t : leaderState.queryState().operations()) {
                        t.element2.completeExceptionally(new LeaderDemotedException(state.localEndpoint(), null));
                    }
                }
                state.completeLeadershipTransfer(new LeaderDemotedException(state.localEndpoint(), null));
                setStatus(TERMINATED);
            } catch (Throwable t) {
                failure = t;

                logger.severe("Failure during force-termination", t);
                if (status != TERMINATED && status != STEPPED_DOWN) {
                    setStatus(TERMINATED);
                }
            } finally {
                if (failure == null) {
                    resultFuture.complete(null);
                } else {
                    resultFuture.completeExceptionally(failure);
                }
            }
        });

        return resultFuture;
    }

    /**
     * Starts the periodic tasks, such as voting, leader failure-detection, snapshot handling.
     */
    public void start() {
        if (status == TERMINATED) {
            logger.warning("Not starting since already terminated...");
            return;
        }

        if (status != INITIAL) {
            throw new IllegalStateException("Cannot start RaftNode when " + status);
        }

        if (!raftIntegration.isReady()) {
            raftIntegration.schedule(this::start, RAFT_NODE_INIT_DELAY_MILLIS, MILLISECONDS);
            return;
        }

        logger.fine("Starting Raft node: " + state.localEndpoint() + " for " + groupId
                + " with " + state.memberCount() + " members: " + state.members());

        execute(() -> {
            if (status == TERMINATED) {
                logger.warning("Not starting since already terminated...");
                return;
            }

            if (status != INITIAL) {
                throw new IllegalStateException("Cannot start RaftNode when " + status);
            }

            initRestoredState();
            try {
                state.init();
            } catch (IOException e) {
                logger.severe("Raft node start failed!", e);
                setStatus(TERMINATED);
                return;
            }

            new PreVoteTask(RaftNodeImpl.this, 0).run();
            scheduleLeaderFailureDetection();

            // status could be UPDATING_GROUP_MEMBER_LIST after restoring Raft state
            // so we only switch to ACTIVE only if status is INITIAL
            if (status == INITIAL) {
                setStatus(ACTIVE);
            }
        });
    }

    private void closeStateStore() {
        try {
            state.stateStore().close();
        } catch (IOException e) {
            logger.severe(e);
        }
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
    public void handleInstallSnapshotRequest(InstallSnapshotRequest request) {
        execute(new InstallSnapshotRequestHandlerTask(this, request));
    }

    @Override
    public void handleInstallSnapshotResponse(InstallSnapshotResponse response) {
        execute(new InstallSnapshotResponseHandlerTask(this, response));
    }

    @Override
    public void handleTriggerLeaderElection(TriggerLeaderElection request) {
        execute(new TriggerLeaderElectionHandlerTask(this, request));
    }

    @Override
    public void handleTriggerLeadershipRebalance() {
        // no-op
    }

    @Override
    public InternalCompletableFuture replicate(Object operation) {
        InternalCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        execute(createReplicationTask(operation, resultFuture));
        return resultFuture;
    }

    public ReplicateTask createReplicationTask(Object operation, InternalCompletableFuture resultFuture) {
        return new ReplicateTask(this, operation, resultFuture);
    }

    @Override
    public InternalCompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeMode mode) {
        InternalCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        execute(new MembershipChangeTask(this, resultFuture, member, mode));
        return resultFuture;
    }

    @Override
    public InternalCompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeMode mode,
                                                               long groupMembersCommitIndex) {
        InternalCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new MembershipChangeTask(this, resultFuture, member, mode, groupMembersCommitIndex));
        return resultFuture;
    }

    @Override
    public InternalCompletableFuture query(Object operation, QueryPolicy queryPolicy) {
        InternalCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new QueryTask(this, operation, queryPolicy, resultFuture));
        return resultFuture;
    }

    @Override
    public InternalCompletableFuture transferLeadership(RaftEndpoint endpoint) {
        InternalCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new InitLeadershipTransferTask(this, endpoint, resultFuture));
        return resultFuture;
    }

    @Override
    public boolean autoStepDownWhenLeaderForGroup() {
        return false;
    }

    @Override
    public boolean isTerminatedOrSteppedDown() {
        // Reads the volatile status field
        return status == TERMINATED || status == STEPPED_DOWN;
    }

    public void setStatus(RaftNodeStatus newStatus) {
        if (this.status == TERMINATED || this.status == STEPPED_DOWN) {
            throw new IllegalStateException("Cannot set status: " + newStatus + " since already " + this.status);
        }

        RaftNodeStatus prevStatus = this.status;

        if (prevStatus != newStatus) {
            Level level = Level.WARNING;
            if (newStatus == ACTIVE) {
                level = Level.INFO;
            } else if ((newStatus == TERMINATED || newStatus == STEPPED_DOWN) && prevStatus != INITIAL) {
                closeStateStore();
            }
            //Status should be set to `TERMINATED` or `STEPPED_DOWN` after `closeStateStore`
            this.status = newStatus;

            logger.log(level, "Status is set to: " + newStatus);
            raftIntegration.onNodeStatusChange(newStatus);
        }
    }

    private void groupDestroyed() {
        if (status != TERMINATED) {
            closeStateStore();
            //Status should be set to `TERMINATED` after `closeStateStore`
            status = TERMINATED;
            logger.warning("Status is set to: " + TERMINATED + " on group destroyed");
        }
        raftIntegration.onGroupDestroyed(groupId);
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
     * Returns true if a new entry with the operation is currently allowed to
     * be replicated. This method can be invoked only when the local Raft node
     * is the leader.
     * <p>
     * Replication is not allowed, when;
     * <ul>
     * <li>Node is terminating, terminated or stepped down. See {@link RaftNodeStatus}.</li>
     * <li>Raft log contains max allowed uncommitted entry count.
     * See {@link RaftAlgorithmConfig#getUncommittedEntryCountToRejectNewAppends()}.</li>
     * <li>The operation is a {@link RaftGroupCmd} and there's an ongoing membership change in group.</li>
     * <li>The operation is a membership change operation and there's no committed entry in this term yet.
     * See {@link RaftIntegration#getAppendedEntryOnLeaderElection()}.</li>
     * <li>There is an ongoing leadership transfer.</li>
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

        return state.leadershipTransferState() == null;
    }

    /**
     * Returns true if a new query is currently allowed to be executed without
     * appending to the Raft log. This method can be invoked only when
     * the local Raft node is the leader.
     * <p>
     * A new linearizable query execution is not allowed, when;
     * <ul>
     * <li>Node is terminating, terminated or stepped down.
     * See {@link RaftNodeStatus}.</li>
     * <li>If the leader has not yet marked an entry from its current term
     * committed. See Section 6.4 of Raft Dissertation.</li>
     * <li>There are already
     * {@link RaftAlgorithmConfig#getUncommittedEntryCountToRejectNewAppends()}
     * queries waiting to be executed.</li>
     * </ul>
     */
    public boolean canQueryLinearizable() {
        if (isTerminatedOrSteppedDown()) {
            return false;
        }

        long commitIndex = state.commitIndex();
        RaftLog log = state.log();

        // If the leader has not yet marked an entry from its current term committed, it waits until it has done so. (§6.4)
        // last committed entry is either in the last snapshot or still in the log
        LogEntry lastCommittedEntry = commitIndex == log.snapshotIndex() ? log.snapshot() : log.getLogEntry(commitIndex);
        assert lastCommittedEntry != null;

        if (lastCommittedEntry.term() != state.term()) {
            return false;
        }

        // We can execute multiple queries at one-shot without appending to the Raft log,
        // and we use the maxUncommittedEntryCount configuration parameter to upper-bound
        // the number of queries that are collected until the heartbeat round is done.
        QueryState queryState = state.leaderState().queryState();
        return queryState.queryCount() < maxUncommittedEntryCount;
    }

    /**
     * Returns true if the linearizable read optimization is enabled.
     */
    public boolean isLinearizableReadOptimizationEnabled() {
        return raftIntegration.isLinearizableReadOptimizationEnabled();
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
    private void scheduleHeartbeat() {
        schedule(new HeartbeatTask(), heartbeatPeriodInMillis);
    }

    public void send(PreVoteRequest request, RaftEndpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(PreVoteResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(VoteRequest request, RaftEndpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(VoteResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(AppendSuccessResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(AppendFailureResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
        appendFailureResponsesSent++;
    }

    public void send(TriggerLeaderElection request, RaftEndpoint target) {
        raftIntegration.send(request, target);
    }

    /**
     * Broadcasts append-entries request to all group members according to their nextIndex parameters.
     */
    public void broadcastAppendRequest() {
        for (RaftEndpoint follower : state.remoteMembers()) {
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
     * If follower's nextIndex is behind the latest snapshot index, then {@link InstallSnapshotRequest} request is sent.
     * <p>
     * If leader doesn't know follower's matchIndex (if {@code matchIndex == 0}), then an empty append-entries is sent
     * to save bandwidth until leader learns the matchIndex of the follower.
     */
    public void sendAppendRequest(RaftEndpoint follower) {
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

        if (shouldSendSnapshot(raftLog, nextIndex)) {
            sendSnapshotRequest(follower);
            return;
        }

        sendAppendEntries(follower, raftLog, leaderState, followerState, nextIndex);
    }

    private boolean shouldSendSnapshot(RaftLog raftLog, long nextIndex) {
        // if the first log entry to be sent is put into the snapshot,
        // check if we still keep it in the log if we still keep that log
        // entry and its previous entry, we don't need to send a snapshot
        return nextIndex <= raftLog.snapshotIndex()
                && (!raftLog.containsLogEntry(nextIndex)
                || (nextIndex > 1 && !raftLog.containsLogEntry(nextIndex - 1)));
    }

    private void sendSnapshotRequest(RaftEndpoint follower) {
        initiateFollowerSnapshotSync(follower);
    }

    /**
     * Triggers a follower to synchronize with the leader's current snapshot.
     * <p>
     * This is done by calling the main snapshot-sending method with special
     * parameters. This signals that the follower is out of date and should be
     * sent snapshot metadata, which will cause it to reset and begin a new
     * full synchronization.
     *
     * @param follower The endpoint of the follower that needs to sync up.
     */
    private void initiateFollowerSnapshotSync(RaftEndpoint follower) {
        logger.fine("Forcing a full snapshot sync for follower %s", follower);

        sendNextSnapshotChunk(follower, METADATA_CHUNK_INDEX, METADATA_CHUNK_NUMBER);
    }

    public void sendNextSnapshotChunk(RaftEndpoint follower, long requestedSnapshotIndex, int requestedChunkNumber) {
        // 1. Get the current state and snapshot details from the leader.
        final LeaderState leaderState = state.leaderState();
        final SnapshotEntry snapshotEntry = state.log().snapshot();

        if (extractChunksFrom(snapshotEntry).isEmpty()) {
            throw new IllegalStateException("No chunks extracted from snapshot");
        }

        final long currentSnapshotIndex = snapshotEntry.index();

        // 2. Handle cases where the follower's request is invalid or outdated.
        // Guard Clause: Follower is asking for a snapshot
        // that is newer than ours. This shouldn't happen.
        if (requestedSnapshotIndex > currentSnapshotIndex) {
            logger.warning(format("Ignoring request from %s for a future snapshot. "
                            + "Follower wants index %d, but we only have index %d.",
                    follower, requestedSnapshotIndex, currentSnapshotIndex));
            return;
        }

        // 3. Determine which chunk to send: metadata or actual data.
        SnapshotChunk chunkToSend;
        // If the follower is behind, it needs to start over. We send only
        // the metadata of our *new* snapshot. This prompts the follower
        // to reset its state and request the new snapshot from scratch.
        // see ChunkedSnapshotInstaller#validateAndStoreChunk#isFromNewerSnapshot
        if (requestedSnapshotIndex < currentSnapshotIndex) {
            logger.info(format("Follower %s is behind (wants index %d, we have %d). "
                            + "Sending current snapshot metadata to force a reset.",
                    follower, requestedSnapshotIndex, currentSnapshotIndex));
            chunkToSend = createMetadataChunk(snapshotEntry);
        } else {
            // Happy Path: The follower is requesting
            // a chunk of the current snapshot.
            List<SnapshotChunk> snapshotChunks = extractChunksFrom(snapshotEntry);
            chunkToSend = snapshotChunks.get(requestedChunkNumber);
            logger.fine("Sending snapshot chunk %d (0-based) of %d to follower %s for index %d.",
                    requestedChunkNumber, chunkToSend.chunkCount(), follower, currentSnapshotIndex);
        }

        // This check handles cases where the chunk couldn't be created or found.
        if (chunkToSend == null) {
            logger.severe(format("Failed to get snapshot chunk %d (0-based) for follower %s at index %d. Aborting.",
                    requestedChunkNumber, follower, currentSnapshotIndex));
            return;
        }

        // 4. Build and send the snapshot request to the follower.
        final FollowerState followerState = leaderState.getFollowerState(follower);
        final InstallSnapshotRequest request = new InstallSnapshotRequest(
                state.localEndpoint(),
                state.term(),
                chunkToSend,
                leaderState.queryRound(),
                followerState.setMaxAppendRequestBackoff()
        );

        raftIntegration.send(request, follower);
        scheduleAppendAckResetTask();
    }

    private static SnapshotChunk createMetadataChunk(SnapshotEntry snapshotEntry) {
        List<SnapshotChunk> chunks = extractChunksFrom(snapshotEntry);
        return ((SnapshotChunk) new SnapshotChunk()
                .setChunkCount(chunks.size())
                .setChunkNumber(METADATA_CHUNK_NUMBER)
                .setGroupMembers(snapshotEntry.groupMembers())
                .setGroupMembersLogIndex(snapshotEntry.groupMembersLogIndex())
                .setSnapshotTerm(snapshotEntry.term())
                .setIndex(snapshotEntry.index())
                .setOperation(null));
    }

    private void sendAppendEntries(RaftEndpoint follower, RaftLog raftLog,
                                   LeaderState leaderState, FollowerState followerState,
                                   long nextIndex) {
        final LogEntry[] entries = prepareLogEntries(raftLog, followerState, nextIndex);
        final boolean shouldBackoff = shouldSetBackoff(raftLog, followerState, nextIndex);

        if (shouldBackoff) {
            followerState.setAppendRequestBackoff();
        }

        final long prevEntryIndex = nextIndex > 1 ? nextIndex - 1 : 0;
        final int prevEntryTerm = getPreviousEntryTerm(raftLog, prevEntryIndex);

        AppendRequest request = new AppendRequest(
                getLocalMember(),
                state.term(),
                prevEntryTerm,
                prevEntryIndex,
                state.commitIndex(),
                entries,
                leaderState.queryRound(),
                followerState.flowControlSequenceNumber()
        );

        logger.fine("Sending %s to %s with next index: %d", request, follower, nextIndex);

        raftIntegration.send(request, follower);

        if (shouldTriggerFlush(entries, leaderState)) {
            submitFlushTask();
        }

        if (shouldBackoff) {
            scheduleAppendAckResetTask();
        }
    }

    private LogEntry[] prepareLogEntries(RaftLog raftLog, FollowerState followerState, long nextIndex) {
        // Return empty array for heartbeats when no entries are needed
        if (nextIndex > 1 && followerState.matchIndex() == 0) {
            // Until the leader has discovered where it and the follower's
            // logs match, the leader can send AppendEntries with no
            // entries (like heartbeats) to save bandwidth. We still need
            // to enable append request backoff here because we do not want
            // to bombard the follower before we learn its match index
            return new LogEntry[0];
        }

        if (nextIndex > raftLog.lastLogOrSnapshotIndex()) {
            // Follower is caught up with leader, send empty heartbeat
            return new LogEntry[0];
        }

        if (raftLog.lastLogOrSnapshotIndex() == 0 && nextIndex == 1) {
            // No entries in Raft log, send empty heartbeat
            return new LogEntry[0];
        }

        // Send entries to follower, either for first time or when
        // the matchIndex immediately precedes the nextIndex,
        // the leader should begin to send the actual entries
        long end = Math.min(nextIndex + appendRequestMaxEntryCount, raftLog.lastLogOrSnapshotIndex());
        return raftLog.getEntriesBetween(nextIndex, end);
    }

    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    private boolean shouldSetBackoff(RaftLog raftLog, FollowerState followerState, long nextIndex) {
        return (nextIndex == 1 && raftLog.lastLogOrSnapshotIndex() > 0)
                || (nextIndex > 1 && followerState.matchIndex() == 0)
                || (nextIndex > 1 && nextIndex <= raftLog.lastLogOrSnapshotIndex());
    }

    private int getPreviousEntryTerm(RaftLog raftLog, long prevEntryIndex) {
        if (prevEntryIndex == 0) {
            return 0;
        }
        LogEntry prevEntry = raftLog.snapshotIndex() == prevEntryIndex
                ? raftLog.snapshot()
                : raftLog.getLogEntry(prevEntryIndex);
        if (prevEntry == null) {
            throw new IllegalStateException(
                    format("Previous entry not found: index=%d, snapshot=%d",
                            prevEntryIndex, raftLog.snapshotIndex()));
        }
        return prevEntry.term();
    }

    private boolean shouldTriggerFlush(LogEntry[] entries, LeaderState leaderState) {
        // if I am sending any non-flushed entry to the follower, I should trigger the flush task.
        // I hope that I will flush before receiving append responses from half of the followers...
        // This is a very critical optimization because
        // it makes the leader and followers flush in parallel...
        return entries.length > 0
                && entries[entries.length - 1].index() > leaderState.flushedLogIndex();
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
            logger.fine("Processing %s", entry);
        }

        Object response = null;
        Object operation = entry.operation();
        if (operation instanceof RaftGroupCmd) {
            if (operation instanceof DestroyRaftGroupCmd) {
                groupDestroyed();
            } else if (operation instanceof UpdateRaftGroupMembersCmd op) {
                if (state.lastGroupMembers().index() < entry.index()) {
                    setStatus(UPDATING_GROUP_MEMBER_LIST);
                    updateGroupMembers(entry.index(), op.getMembers());
                }

                assert status == UPDATING_GROUP_MEMBER_LIST : "STATUS: " + status;
                assert state.lastGroupMembers().index() == entry.index();

                state.commitGroupMembers();
                UpdateRaftGroupMembersCmd cmd = op;
                if (cmd.getMember().equals(state.localEndpoint()) && cmd.getMode() == MembershipChangeMode.REMOVE) {
                    setStatus(STEPPED_DOWN);
                    // If I am the leader, I may have some waiting futures whose operations are already committed
                    // but responses are not decided yet. When I leave the cluster after my shutdown, invocations
                    // of those futures will receive MemberLeftException and retry. However, if I have an invocation
                    // during the shutdown process, its future will not complete unless I notify it here.
                    // Although LeaderDemotedException is designed for another case, we use it here since
                    // invocations internally retry when they receive LeaderDemotedException.
                    invalidateFuturesUntil(entry.index() - 1, new LeaderDemotedException(state.localEndpoint(), null));
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

    public RaftState state() {
        return state;
    }

    /**
     * Executes query operation sets execution result to the future.
     */
    public void runQuery(Object operation, InternalCompletableFuture resultFuture) {
        Object result = raftIntegration.runOperation(operation, state.commitIndex());
        resultFuture.complete(result);
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
    public void registerFuture(long entryIndex, InternalCompletableFuture future) {
        InternalCompletableFuture f = futures.put(entryIndex, future);
        assert f == null : "Future object is already registered for entry index: " + entryIndex;
    }

    /**
     * Completes the future registered with {@code entryIndex}.
     */
    public void completeFuture(long entryIndex, Object response) {
        InternalCompletableFuture f = futures.remove(entryIndex);
        if (f != null) {
            if (response instanceof Throwable throwable) {
                f.completeExceptionally(throwable);
            } else {
                f.complete(response);
            }
        }
    }

    /**
     * Invalidates futures registered with indexes {@code >= entryIndex}. Note that {@code entryIndex} is inclusive.
     * {@link LeaderDemotedException} is set a result to futures.
     */
    public void invalidateFuturesFrom(long entryIndex) {
        int count = 0;
        Iterator<Entry<Long, InternalCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, InternalCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index >= entryIndex) {
                entry.getValue().completeExceptionally(new LeaderDemotedException(state.localEndpoint(), state.leader()));
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
     */
    private void invalidateFuturesUntil(long entryIndex, Throwable response) {
        int count = 0;
        Iterator<Entry<Long, InternalCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, InternalCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index <= entryIndex) {
                entry.getValue().completeExceptionally(response);
                iterator.remove();
                count++;
            }
        }

        if (count > 0) {
            logger.warning("Invalidated " + count + " futures until log index: " + entryIndex);
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    private void takeSnapshotIfCommitIndexAdvanced() {
        final long commitIndex = state.commitIndex();
        if ((commitIndex - state.log().snapshotIndex()) < commitIndexAdvanceCountToSnapshot) {
            return;
        }

        if (isTerminatedOrSteppedDown()) {
            // If the status is TERMINATING, it means the status is normally ACTIVE
            // and there is an appended but not committed RaftGroupCmd.
            // If the status is TERMINATED or STEPPED_DOWN, then there will not be any new appends.
            return;
        }

        logger.fine("Taking chunked snapshot at commit index: %d", commitIndex);

        long buildStartNanos = System.nanoTime();
        try {
            Object snapshot;
            try {
                snapshot = raftIntegration.takeServiceSnapshots(commitIndex);
            } catch (Throwable t) {
                logger.severe("Could not take snapshot at commit index: " + commitIndex, t);
                return;
            }

            RaftLog log = state.log();
            int snapshotTerm = log.getLogEntry(commitIndex).term();
            RaftGroupMembers members = state.committedGroupMembers();

            List<SnapshotChunk> chunksFromSnapshot = createChunksFromSnapshot(
                    raftIntegration,
                    snapshotTerm,
                    commitIndex,
                    members.members(),
                    members.index(),
                    snapshot,
                    logger
            );

            logger.fine("SnapshotChunk creation result: created %d chunks", chunksFromSnapshot.size());

            try {
                persistAndFlushSnapshotChunks(chunksFromSnapshot, commitIndex);
            } catch (IOException e) {
                throw new HazelcastException(e);
            }

            snapshot = chunksFromSnapshot;

            long highestLogIndexToTruncate = commitIndex - maxNumberOfLogsToKeepAfterSnapshot;
            LeaderState leaderState = state.leaderState();
            if (leaderState != null) {
                long[] matchIndices = leaderState.matchIndices();
                // Last slot is reserved for leader index and always zero.

                // If there is at least one follower with unknown match index,
                // its log can be close to the leader's log so we are keeping the old log entries.
                boolean allMatchIndicesKnown = Arrays.stream(matchIndices, 0, matchIndices.length - 1)
                        .noneMatch(i -> i == 0);

                if (allMatchIndicesKnown) {
                    // Otherwise, we will keep the log entries until the minimum match index
                    // that is bigger than (commitIndex - maxNumberOfLogsToKeepAfterSnapshot).
                    // If there is no such follower (all of the minority followers are far behind),
                    // then there is no need to keep the old log entries.
                    highestLogIndexToTruncate = Arrays.stream(matchIndices)
                            // No need to keep any log entry if all followers are up to date
                            .filter(i -> i < commitIndex)
                            .filter(i -> i > commitIndex - maxNumberOfLogsToKeepAfterSnapshot)
                            // We should not delete the smallest matchIndex
                            .map(i -> i - 1)
                            .sorted()
                            .findFirst()
                            .orElse(commitIndex);
                }
            }

            // we flushed the snapshot to the storage.
            // it is safe to modify the memory state now.

            SnapshotEntry snapshotEntry = new SnapshotEntry(snapshotTerm, commitIndex,
                    snapshot, members.index(), members.members());

            int truncatedEntryCount = log.setSnapshot(snapshotEntry, highestLogIndexToTruncate, true);

            logger.fine("%s is taken, %d entries are truncated.", snapshotEntry, truncatedEntryCount);
        } finally {
            lastSnapshotBuildDurationMs = NANOSECONDS.toMillis(System.nanoTime() - buildStartNanos);
            snapshotBuildCount++;
        }
    }

    private void persistAndFlushSnapshotChunks(List<SnapshotChunk> chunksFromSnapshot, long snapshotIndex)
            throws IOException {

        for (SnapshotChunk chunk : chunksFromSnapshot) {
            try {
                stateStore.persistSnapshotChunk(chunk);
            } catch (IOException e) {
                throw new HazelcastException(format("Persist failed at snapshot index: %d, chunkNumber: %d(0-based)",
                        chunk.index(), chunk.chunkNumber()), e);
            }
        }

        try {
            stateStore.flushLogs();
        } catch (IOException e) {
            throw new HazelcastException(format("Flush failed at snapshot index: %d", snapshotIndex), e);
        }
    }

    /**
     * Restores the snapshot sent by the leader if it's not applied before.
     *
     * @return true if snapshot is restored, false otherwise.
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
        RaftLog raftLog = state.log();
        int truncated = raftLog.setSnapshot(snapshot);
        raftLog.flush();

        if (truncated > 0) {
            logger.info(truncated + " entries are truncated to install " + snapshot);
        }

        raftIntegration.restoreSnapshot(snapshot, snapshot.index());

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

    private void initRestoredState() {
        SnapshotEntry snapshotEntry = state.log().snapshot();

        if (isNonInitial(snapshotEntry)) {
            printMemberState();
            raftIntegration.restoreSnapshot(snapshotEntry, snapshotEntry.index());
            if (logger.isFineEnabled()) {
                logger.info(snapshotEntry + " is restored.");
            } else {
                logger.info("Snapshot is restored at commitIndex=" + snapshotEntry.index());
            }
        }

        applyRestoredRaftGroupCommands(snapshotEntry);
    }

    private void applyRestoredRaftGroupCommands(SnapshotEntry snapshot) {
        // If there is a single Raft group command after the last snapshot,
        // here we cannot know if the that command is committed or not so we
        // just "pre-apply" that command without committing it.
        // If there are multiple Raft group commands, it is definitely known
        // that all the command up to the last command are committed,
        // but the last command may not be committed.
        // This conclusion boils down to the fact that once you append a Raft
        // group command, you cannot append a new one before committing it.

        RaftLog log = state.log();
        LogEntry committedEntry = null;
        LogEntry lastAppliedEntry = null;

        for (long i = snapshot != null ? snapshot.index() + 1 : 1; i <= log.lastLogOrSnapshotIndex(); i++) {
            LogEntry entry = log.getLogEntry(i);
            assert entry != null : "index: " + i;
            if (entry.operation() instanceof RaftGroupCmd) {
                committedEntry = lastAppliedEntry;
                lastAppliedEntry = entry;
            }
        }

        if (committedEntry != null) {
            state.commitIndex(committedEntry.index());
            applyLogEntries();
        }

        if (lastAppliedEntry != null) {
            if (lastAppliedEntry.operation() instanceof UpdateRaftGroupMembersCmd) {
                setStatus(UPDATING_GROUP_MEMBER_LIST);
                Collection<RaftEndpoint> members = ((UpdateRaftGroupMembersCmd) lastAppliedEntry.operation()).getMembers();
                updateGroupMembers(lastAppliedEntry.index(), members);
            } else if (lastAppliedEntry.operation() instanceof DestroyRaftGroupCmd) {
                setStatus(TERMINATING);
            } else {
                throw new IllegalStateException("Invalid group command for restore: " + lastAppliedEntry);
            }
        }
    }

    public void printMemberState() {
        CPGroupId groupId = state.groupId();
        StringBuilder sb = new StringBuilder("\n\nCP Group Members {")
                .append("groupId: ").append(groupId.getName()).append("(").append(groupId.getId()).append(")")
                .append(", size:").append(state.memberCount())
                .append(", term:").append(state.term())
                .append(", logIndex:").append(state.membersLogIndex())
                .append("} [");

        for (RaftEndpoint member : state.members()) {
            CPMember cpMember = raftIntegration.getCPMember(member);
            sb.append("\n\t").append(cpMember != null ? cpMember : member);
            if (state.localEndpoint().equals(member)) {
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
    public void updateGroupMembers(long logIndex, Collection<RaftEndpoint> members) {
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

    private void submitFlushTask() {
        if (flushTaskSubmitted) {
            return;
        }

        flushTaskSubmitted = true;
        raftIntegration.submit(flushTask);
    }

    /**
     * Switches this node to follower role by clearing the known leader
     * endpoint and (pre) candidate states, and updating the term. If this Raft
     * node was leader before switching to the follower state, it may have some
     * queries waiting to be executed. Those queries are also failed with
     * {@link LeaderDemotedException}. After the state switch,
     * {@link RaftIntegration#onNodeStatusChange(RaftNodeStatus)} is called.
     *
     * @param term the new term to switch
     */
    public void toFollower(int term) {
        LeaderState leaderState = state.leaderState();
        if (leaderState != null) {
            for (BiTuple<Object, InternalCompletableFuture> t : leaderState.queryState().operations()) {
                t.element2.completeExceptionally(new LeaderDemotedException(state.localEndpoint(), null));
            }
        }

        // Suppress pre-voting for one leader election timeout after this demotion.
        // Otherwise, if no leader is known, LeaderFailureDetectionTask immediately starts
        // another election, causing repeated term increments and election churn.
        state.toFollower(term, Clock.currentTimeMillis() + getLeaderElectionTimeoutInMillis());
        printMemberState();
    }

    /**
     * Updates the known leader endpoint and calls
     * {@link RaftIntegration#onNodeStatusChange(RaftNodeStatus)}.
     *
     * @param endpoint the new leader endpoint
     */
    public void leader(RaftEndpoint endpoint) {
        state.leader(endpoint);
        printMemberState();
    }

    /**
     * Switches this Raft node to the leader role by performing the following
     * steps:
     * <ul>
     * <li>Setting the local endpoint as the current leader,</li>
     * <li>Clearing (pre)candidate states,</li>
     * <li>Initializing the leader state for the current members,</li>
     * <li>Appending an operation to the Raft log if enabled,</li>
     * <li>Scheduling the periodic heartbeat task,</li>
     * <li>Printing the member state.</li>
     * </ul>
     */
    public void toLeader() {
        state.toLeader();
        appendEntryAfterLeaderElection();
        printMemberState();
        broadcastAppendRequest();
        scheduleHeartbeat();
        raftIntegration.onNewLeaderElected(groupId);
    }

    private long findQuorumMatchIndex() {
        LeaderState leaderState = state.leaderState();
        long[] indices = leaderState.matchIndices();

        // if the leader is leaving, it should not count its vote for quorum...
        if (state.isKnownMember(state.localEndpoint())) {
            // Raft dissertation Section 10.2.1:
            // The leader may even commit an entry before it has been written to its own disk,
            // if a majority of followers have written it to their disks; this is still safe.
            long leaderIndex = flushTask == null ? state.log().lastLogOrSnapshotIndex() : leaderState.flushedLogIndex();
            indices[indices.length - 1] = leaderIndex;
        } else {
            // Remove the last empty slot reserved for leader index
            indices = Arrays.copyOf(indices, indices.length - 1);
        }

        sort(indices);

        long quorumMatchIndex = indices[(indices.length - 1) / 2];
        if (logger.isFineEnabled()) {
            logger.fine("Quorum match index: %s, indices: %s", quorumMatchIndex, Arrays.toString(indices));
        }

        return quorumMatchIndex;
    }

    public boolean tryAdvanceCommitIndex() {
        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4)
        long quorumMatchIndex = findQuorumMatchIndex();
        long commitIndex = state.commitIndex();
        RaftLog raftLog = state.log();
        for (; quorumMatchIndex > commitIndex; quorumMatchIndex--) {
            // Only log entries from the leader’s current term are committed by counting replicas; once an entry
            // from the current term has been committed in this way, then all prior entries are committed indirectly
            // because of the Log Matching Property.
            LogEntry entry = raftLog.getLogEntry(quorumMatchIndex);
            if (entry.term() == state.term()) {
                commitEntries(quorumMatchIndex);
                return true;
            } else if (logger.isFineEnabled()) {
                logger.fine("Cannot commit %s since an entry from the current term: %s is needed.", entry, state.term());
            }
        }
        return false;
    }

    private void commitEntries(long commitIndex) {
        if (logger.isFineEnabled()) {
            logger.fine("Setting commit index: %s", commitIndex);
        }
        state.commitIndex(commitIndex);

        if (status == ACTIVE) {
            applyLogEntries();
            tryRunQueries();
        } else {
            tryRunQueries();
            applyLogEntries();
        }

        broadcastAppendRequest();
    }

    public boolean tryRunQueries() {
        QueryState queryState = state.leaderState().queryState();
        if (queryState.queryCount() == 0) {
            return false;
        }

        long commitIndex = state.commitIndex();
        if (!queryState.isMajorityAcked(commitIndex, state.majority())) {
            return true;
        }

        Collection<BiTuple<Object, InternalCompletableFuture>> operations = queryState.operations();

        if (logger.isFineEnabled()) {
            logger.fine("Running " + operations.size() + " queries at commit index: " + commitIndex
                    + ", query round: " + queryState.queryRound());
        }

        for (BiTuple<Object, InternalCompletableFuture> t : operations) {
            runQuery(t.element1, t.element2);
        }

        queryState.reset();
        return false;
    }

    private void appendEntryAfterLeaderElection() {
        Object entry = raftIntegration.getAppendedEntryOnLeaderElection();
        if (entry != null) {
            RaftLog log = state.log();
            log.appendEntries(new LogEntry(state.term(), log.lastLogOrSnapshotIndex() + 1, entry));
        }
    }

    public boolean isHeartbeatTimedOut(long timestamp) {
        long missedHeartbeatThreshold = maxMissedLeaderHeartbeatCount * heartbeatPeriodInMillis;
        return timestamp + missedHeartbeatThreshold < Clock.currentTimeMillis();
    }

    public boolean isHeartbeatTimedOut() {
        return isHeartbeatTimedOut(lastAppendEntriesTimestamp);
    }

    public boolean isLeaderAvailable() {
        RaftEndpoint leader = state.leader();
        return (leader != null) && raftIntegration.isReachable(leader) && !isHeartbeatTimedOut();
    }

    // Used for testing to validate raft log reconciliation; see RaftLogReconciliationTest
    public long getAppendFailureResponsesSent() {
        return appendFailureResponsesSent;
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
                if (isHeartbeatTimedOut(state.leaderState().majorityAppendRequestAckTimestamp(state.majority()))) {
                    logger.warning("Demoting to " + FOLLOWER + " since not received acks from majority recently...");
                    toFollower(state.term());
                    invalidateFuturesUntil(state.log().lastLogOrSnapshotIndex(), new StaleAppendRequestException(null));
                    return;
                }

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
                if (state.role() == LEADER) {
                    return;
                }

                RaftEndpoint leader = state.leader();
                if (leader == null) {
                    if (state.role() == FOLLOWER && !state.isPreVoteBackoffActive()) {
                        logger.warning("We are FOLLOWER and there is no current leader. Will start new election round...");
                        runPreVoteTask();
                    }
                } else if (!raftIntegration.isReachable(leader)) {
                    logger.warning("Current leader " + leader + " is not reachable. Will start new election round...");
                    resetLeaderAndStartElection();
                } else if (isHeartbeatTimedOut()) {
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

        final void resetLeaderAndStartElection() {
            leader(null);
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
            if (leaderState == null) {
                return;
            }

            Map<RaftEndpoint, FollowerState> followerStates = leaderState.getFollowerStates();
            for (Entry<RaftEndpoint, FollowerState> entry : followerStates.entrySet()) {
                FollowerState followerState = entry.getValue();
                if (followerState.isAppendRequestBackoffSet()) {
                    if (followerState.completeAppendRequestBackoffRound()) {
                        // This follower has not sent a response to the last append request.
                        // Send another append request
                        sendAppendRequest(entry.getKey());
                    } else {
                        // Schedule the task again, we still have backoff flag set followers
                        scheduleAppendAckResetTask();
                    }
                }
            }
        }
    }

    private class FlushTask extends RaftNodeStatusAwareTask {
        FlushTask() {
            super(RaftNodeImpl.this);
        }

        @Override
        protected void innerRun() {
            flushTaskSubmitted = false;
            RaftLog log = state.log();
            log.flush();
            LeaderState leaderState = state.leaderState();
            if (leaderState != null) {
                leaderState.flushedLogIndex(log.lastLogOrSnapshotIndex());
                tryAdvanceCommitIndex();
            }
        }
    }

    @Override
    public String toString() {
        return "RaftNode{" + "groupId=" + groupId + ", status=" + status + ", localEndpoint=" + state.localEndpoint() + '}';
    }
}
