package com.hazelcast.raft.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.CannotRunLocalQueryException;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.StaleAppendRequestException;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.handler.AppendFailureResponseHandlerTask;
import com.hazelcast.raft.impl.handler.AppendRequestHandlerTask;
import com.hazelcast.raft.impl.handler.AppendSuccessResponseHandlerTask;
import com.hazelcast.raft.impl.handler.InstallSnapshotHandlerTask;
import com.hazelcast.raft.impl.handler.PreVoteRequestHandlerTask;
import com.hazelcast.raft.impl.handler.PreVoteResponseHandlerTask;
import com.hazelcast.raft.impl.handler.VoteRequestHandlerTask;
import com.hazelcast.raft.impl.handler.VoteResponseHandlerTask;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.log.SnapshotEntry;
import com.hazelcast.raft.impl.command.ApplyRaftGroupMembersCmd;
import com.hazelcast.raft.impl.log.NopEntry;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.task.MembershipChangeTask;
import com.hazelcast.raft.impl.task.PreVoteTask;
import com.hazelcast.raft.impl.task.QueryTask;
import com.hazelcast.raft.impl.task.ReplicateTask;
import com.hazelcast.raft.impl.util.PostponedResponse;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.command.RaftGroupCmd;
import com.hazelcast.raft.command.TerminateRaftGroupCmd;
import com.hazelcast.util.Clock;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.impl.RaftNodeStatus.ACTIVE;
import static com.hazelcast.raft.impl.RaftNodeStatus.CHANGING_MEMBERSHIP;
import static com.hazelcast.raft.impl.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.raft.impl.RaftNodeStatus.TERMINATED;
import static com.hazelcast.raft.impl.RaftNodeStatus.TERMINATING;
import static java.lang.Math.min;

/**
 * Implementation of {@link RaftNode}.
 */
public class RaftNodeImpl implements RaftNode {

    private static final long SNAPSHOT_TASK_PERIOD_IN_SECONDS = 1;
    private static final int LEADER_ELECTION_TIMEOUT_RANGE = 1000;

    private final RaftGroupId groupId;
    private final ILogger logger;
    private final RaftState state;
    private final RaftIntegration raftIntegration;
    private final RaftEndpoint localEndpoint;
    private final Long2ObjectHashMap<SimpleCompletableFuture> futures = new Long2ObjectHashMap<SimpleCompletableFuture>();

    private final long heartbeatPeriodInMillis;
    private final int leaderElectionTimeout;
    private final int maxUncommittedEntryCount;
    private final int appendRequestMaxEntryCount;
    private final int commitIndexAdvanceCountToSnapshot;
    private final boolean appendNopEntryOnLeaderElection;

    private long lastAppendEntriesTimestamp;
    private volatile RaftNodeStatus status = ACTIVE;

    public RaftNodeImpl(RaftGroupId groupId, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints,
                        RaftConfig raftConfig, RaftIntegration raftIntegration) {
        this.groupId = groupId;
        this.raftIntegration = raftIntegration;
        this.localEndpoint = localEndpoint;
        this.state = new RaftState(groupId, localEndpoint, endpoints);
        this.logger = getLogger(RaftNode.class);
        this.maxUncommittedEntryCount = raftConfig.getUncommittedEntryCountToRejectNewAppends();
        this.appendRequestMaxEntryCount = raftConfig.getAppendRequestMaxEntryCount();
        this.commitIndexAdvanceCountToSnapshot = raftConfig.getCommitIndexAdvanceCountToSnapshot();
        this.leaderElectionTimeout = (int) raftConfig.getLeaderElectionTimeoutInMillis();
        this.heartbeatPeriodInMillis = raftConfig.getLeaderHeartbeatPeriodInMillis();
        this.appendNopEntryOnLeaderElection = raftConfig.isAppendNopEntryOnLeaderElection();
    }

    public ILogger getLogger(Class clazz) {
        String name = state.name();
        return raftIntegration.getLogger(clazz.getName() + "(" + name + ")");
    }

    @Override
    public RaftGroupId getGroupId() {
        return groupId;
    }

    @Override
    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    // It reads the most recent write to the volatile leader field, however leader might be already changed.
    @Override
    public RaftEndpoint getLeader() {
        return state.leader();
    }

    // It reads the volatile status field
    @Override
    public RaftNodeStatus getStatus() {
        return status;
    }

    @Override
    public void forceSetTerminatedStatus() {
        execute(new Runnable() {
            @Override
            public void run() {
                if (!isTerminatedOrSteppedDown()) {
                    setStatus(RaftNodeStatus.TERMINATED);
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
            }, 500, TimeUnit.MILLISECONDS);
            return;
        }

        logger.info("Starting raft node: " + localEndpoint + " for raft cluster: " + state.name()
                + " with members[" + state.memberCount() + "]: " + state.members());
        raftIntegration.execute(new PreVoteTask(this));

        scheduleLeaderFailureDetection();
        scheduleSnapshot();
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
    public ICompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeType change) {
        SimpleCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new MembershipChangeTask(this, resultFuture, member, change));
        return resultFuture;
    }

    @Override
    public ICompletableFuture replicateMembershipChange(RaftEndpoint member, MembershipChangeType change,
                                                        long groupMembersCommitIndex) {
        SimpleCompletableFuture resultFuture = raftIntegration.newCompletableFuture();
        raftIntegration.execute(new MembershipChangeTask(this, resultFuture, member, change, groupMembersCommitIndex));
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

    public void setStatus(RaftNodeStatus status) {
        if (this.status == TERMINATED || this.status == STEPPED_DOWN) {
            throw new IllegalStateException("Cannot set status: " + status + " since it is already " + this.status);
        }

        this.status = status;
        if (status == ACTIVE) {
            logger.info("Status is set to: " + status);
        } else {
            logger.warning("Status is set to: " + status);
        }
    }

    /**
     * Returns a randomized leader election timeout in milliseconds based on configured timeout.
     *
     * @see RaftConfig#leaderHeartbeatPeriodInMillis
     */
    public long getLeaderElectionTimeoutInMillis() {
        return RandomPicker.getInt(leaderElectionTimeout, leaderElectionTimeout + LEADER_ELECTION_TIMEOUT_RANGE);
    }

    /**
     * Returns true if a no-op entry should be appended when a new leader is elected.
     *
     * @see RaftConfig#appendNopEntryOnLeaderElection
     */
    public boolean shouldAppendNopEntryOnLeaderElection() {
        return appendNopEntryOnLeaderElection;
    }

    /**
     * Returns true if a new entry with the operation is allowed to be replicated.
     * <p/>
     * Replication is not allowed, when;
     * <ul>
     * <li>Node is terminating, terminated or stepped down. See {@link RaftNodeStatus}.</li>
     * <li>Raft log contains max allowed uncommitted entry count.
     * See {@link RaftConfig#uncommittedEntryCountToRejectNewAppends}.</li>
     * <li>The operation is a {@link RaftGroupCmd} and there's an ongoing membership change in group.</li>
     * <li>The operation is a membership change operation and there's no committed entry in this term yet.
     * See {@link #shouldAppendNopEntryOnLeaderElection()}.</li>
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
        } else if (status == CHANGING_MEMBERSHIP) {
            return !(operation instanceof RaftGroupCmd);
        }

        if (operation instanceof ApplyRaftGroupMembersCmd) {
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
     * Schedules periodic snapshot task.
     */
    private void scheduleSnapshot() {
        schedule(new SnapshotTask(), TimeUnit.SECONDS.toMillis(SNAPSHOT_TASK_PERIOD_IN_SECONDS));
    }

    /**
     * Schedules periodic heartbeat task when a new leader is elected.
     */
    public void scheduleHeartbeat() {
        broadcastAppendRequest();
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

    public void send(AppendRequest request, RaftEndpoint target) {
        raftIntegration.send(request, target);
    }

    public void send(AppendSuccessResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
    }

    public void send(AppendFailureResponse response, RaftEndpoint target) {
        raftIntegration.send(response, target);
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
     * Sends an append-entries request to the follower endpoint.
     * <p>
     * Log entries between follower's known nextIndex and latest appended entry index are sent in a batch.
     * Batch size can be {@link RaftConfig#appendRequestMaxEntryCount} at most.
     * <p>
     * If follower's nextIndex is behind the latest snapshot index, then {@link InstallSnapshot} request is sent.
     * <p>
     * If leader doesn't know follower's matchIndex (if {@code matchIndex == 0}), then an empty append-entries is sent
     * to save bandwidth until leader learns the matchIndex of the follower.
     * <p>
     * If log entries contains multiple membership change entries, then entries batch is split to send only a single
     * membership change in single append-entries request.
     */
    public void sendAppendRequest(RaftEndpoint follower) {
        RaftLog raftLog = state.log();
        LeaderState leaderState = state.leaderState();

        long nextIndex = leaderState.getNextIndex(follower);

        if (nextIndex <= raftLog.snapshotIndex()) {
            InstallSnapshot installSnapshot = new InstallSnapshot(localEndpoint, state.term(), raftLog.snapshot());
            if (logger.isFineEnabled()) {
                logger.fine("Sending " + installSnapshot + " to " + follower + " since next index: " + nextIndex
                        + " <= snapshot index: " + raftLog.snapshotIndex());
            }
            raftIntegration.send(installSnapshot, follower);
            return;
        }

        LogEntry prevEntry;
        LogEntry[] entries;
        if (nextIndex > 1) {
            long prevEntryIndex = nextIndex - 1;
            prevEntry = (prevEntryIndex == raftLog.snapshotIndex()) ? raftLog.snapshot() : raftLog.getLogEntry(prevEntryIndex);

            long matchIndex = leaderState.getMatchIndex(follower);
            if (matchIndex == 0 && nextIndex > (matchIndex + 1)) {
                // Until the leader has discovered where it and the follower's logs match,
                // the leader can send AppendEntries with no entries (like heartbeats) to save bandwidth.
                entries = new LogEntry[0];
            } else if (nextIndex <= raftLog.lastLogOrSnapshotIndex()) {
                // Then, once the matchIndex immediately precedes the nextIndex,
                // the leader should begin to send the actual entries
                long end = min(nextIndex + appendRequestMaxEntryCount, raftLog.lastLogOrSnapshotIndex());
                entries = raftLog.getEntriesBetween(nextIndex, end);
            } else {
                entries = new LogEntry[0];
            }
        } else if (nextIndex == 1 && raftLog.lastLogOrSnapshotIndex() > 0) {
            prevEntry = new LogEntry();
            long end = min(nextIndex + appendRequestMaxEntryCount, raftLog.lastLogOrSnapshotIndex());
            entries = raftLog.getEntriesBetween(nextIndex, end);
        } else {
            prevEntry = new LogEntry();
            entries = new LogEntry[0];
        }

        assert prevEntry != null : "Follower: " + follower + ", next index: " + nextIndex;

        if (prevEntry.index() < state.commitIndex()) {
            // send at most one ApplyRaftGroupMembersOp in single batch
            entries = trimEntriesIfContainsMultipleMembershipChanges(entries);
        }

        AppendRequest appendRequest = new AppendRequest(getLocalEndpoint(), state.term(), prevEntry.term(), prevEntry.index(),
                state.commitIndex(), entries);

        if (logger.isFineEnabled()) {
            logger.fine("Sending " + appendRequest + " to " + follower + " with next index: " + nextIndex);
        }

        send(appendRequest, follower);
    }

    /**
     * If log entries contains multiple membership change entries, then splits entries to send only a single
     * membership change in single append-entries request.
     */
    private LogEntry[] trimEntriesIfContainsMultipleMembershipChanges(LogEntry[] entries) {
        int trim = entries.length;
        boolean found = false;
        for (int i = 0; i < entries.length; i++) {
            LogEntry entry = entries[i];
            if (entry.operation() instanceof ApplyRaftGroupMembersCmd) {
                if (found) {
                    trim = i;
                    break;
                } else {
                    found = true;
                }
            }
        }

        if (trim < entries.length) {
            logger.fine("Trimming append entries up to index of the second ApplyRaftGroupMembersOp: " + trim);
            return Arrays.copyOf(entries, trim);
        }
        return entries;
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
        assert commitIndex > lastApplied : "commit index: " + commitIndex + " cannot be smaller than last applied: " + lastApplied;

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

        assert status != TERMINATED || commitIndex == raftLog.lastLogOrSnapshotIndex() :
                "commit index: " + commitIndex + " must be equal to " + raftLog.lastLogOrSnapshotIndex() + " on termination.";
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
        if (operation instanceof TerminateRaftGroupCmd) {
            assert status == TERMINATING;
            setStatus(TERMINATED);
        } else if (operation instanceof ApplyRaftGroupMembersCmd) {
            assert status == CHANGING_MEMBERSHIP : "STATUS: " + status;
            state.commitGroupMembers();
            if (state.members().contains(localEndpoint)) {
                setStatus(ACTIVE);
            } else {
                setStatus(STEPPED_DOWN);
            }
            response = entry.index();
        } else if (!(operation instanceof NopEntry)) {
            response = raftIntegration.runOperation(operation, entry.index());
        }

        if (response == PostponedResponse.INSTANCE) {
            // TODO: postpone sending response
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
     * Executes query operation sets execution result to the future. If there's no commit in Raft log yet,
     * then {@link CannotRunLocalQueryException} is set as result.
     */
    public void runQueryOperation(Object operation, SimpleCompletableFuture resultFuture) {
        long commitIndex = state.commitIndex();
        Object result = (commitIndex > 0) ? raftIntegration.runOperation(operation, commitIndex)
                : new CannotRunLocalQueryException(state.leader());
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

        raftIntegration.schedule(task, delayInMillis, TimeUnit.MILLISECONDS);
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
        Iterator<Map.Entry<Long, SimpleCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, SimpleCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index >= entryIndex) {
                entry.getValue().setResult(new LeaderDemotedException(localEndpoint, state.leader()));
                iterator.remove();
                count++;
            }
        }

        logger.warning("Invalidated " + count + " futures from log index: " + entryIndex);
    }

    /**
     * Invalidates futures registered with indexes {@code <= entryIndex}. Note that {@code entryIndex} is inclusive.
     * {@link StaleAppendRequestException} is set a result to futures.
     */
    private void invalidateFuturesUntil(long entryIndex) {
        int count = 0;
        Iterator<Map.Entry<Long, SimpleCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, SimpleCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index <= entryIndex) {
                entry.getValue().setResult(new StaleAppendRequestException(state.leader()));
                iterator.remove();
                count++;
            }
        }

        logger.warning("Invalidated " + count + " futures until log index: " + entryIndex);
    }

    /**
     * Takes a snapshot if {@code commitIndex} advanced equal to or more than
     * {@link RaftConfig#commitIndexAdvanceCountToSnapshot}.
     * <p>
     * Snapshot is not created if there's an ongoing membership change or raft group is being destroyed.
     */
    private void takeSnapshotIfCommitIndexAdvanced() {
        long commitIndex = state.commitIndex();
        if ((commitIndex - state.log().snapshotIndex()) < commitIndexAdvanceCountToSnapshot) {
            return;
        }

        // We don't support snapshots while there's a membership change or the raft group is being destroyed...
        if (status != ACTIVE) {
            return;
        }

        RaftLog log = state.log();
        Object snapshot = raftIntegration.takeSnapshot(commitIndex);
        if (snapshot instanceof Throwable) {
            Throwable t = (Throwable) snapshot;
            logger.severe("Could not take snapshot for " + groupId + " commit index: " + commitIndex, t);
            return;
        }

        LogEntry committedEntry = log.getLogEntry(commitIndex);
        SnapshotEntry snapshotEntry = new SnapshotEntry(committedEntry.term(), commitIndex, snapshot,
                state.membersLogIndex(), state.members());
        log.setSnapshot(snapshotEntry);

        logger.info("Snapshot: "  + snapshotEntry + " is taken.");
    }

    /**
     * Restores the snapshot sent by the leader if it's not applied before.
     *
     * @return true if snapshot is restores, false otherwise.
     */
    public boolean installSnapshot(SnapshotEntry snapshot) {
        long commitIndex = state.commitIndex();
        if (commitIndex > snapshot.index()) {
            logger.warning("Ignored stale snapshot: " + snapshot + ". commit index: " + commitIndex);
            return false;
        } else if (commitIndex == snapshot.index()) {
            logger.warning("Ignored snapshot: " + snapshot + " since commit index is same.");
            return false;
        }

        state.commitIndex(snapshot.index());
        List<LogEntry> truncated = state.log().setSnapshot(snapshot);
        if (logger.isFineEnabled()) {
            logger.fine(truncated.size() + " entries are truncated to install snapshot: " + snapshot + " => " + truncated);
        } else if (truncated.size() > 0) {
            logger.info(truncated.size() + " entries are truncated to install snapshot: " + snapshot);
        }

        raftIntegration.restoreSnapshot(snapshot.operation(), snapshot.index());

        // If I am installing a snapshot, it means I am still present in the last member list so I don't need to update status.
        // Nevertheless, I may not be present in the restored member list, which is ok.
        state.restoreGroupMembers(snapshot.groupMembersLogIndex(), snapshot.groupMembers());
        printMemberState();

        state.lastApplied(snapshot.index());

        invalidateFuturesUntil(snapshot.index());

        logger.info("Snapshot: " + snapshot + " is installed.");

        return true;
    }

    public void printMemberState() {
        RaftGroupId groupId = state.groupId();
        StringBuilder sb = new StringBuilder("\n\nRaft Members {")
                .append("groupId: ").append(groupId.name()).append("(").append(groupId.commitIndex()).append(")")
                .append(", size:").append(state.memberCount())
                .append(", term:").append(state.term())
                .append(", logIndex:").append(state.membersLogIndex())
                .append("} [");

        for (RaftEndpoint endpoint : state.members()) {
            sb.append("\n\t").append(endpoint);
            if (localEndpoint.equals(endpoint)) {
                sb.append(" - ").append(state.role()).append(" this");
            } else if (endpoint.equals(state.leader())) {
                sb.append(" - ").append(RaftRole.LEADER);
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
     * Periodic heartbeat task, which is scheduled on leader only with {@link #heartbeatPeriodInMillis} delay,
     * and sends heartbeat messages (append-entries) if no append-entries request is sent
     * since {@link #lastAppendEntriesTimestamp}.
     */
    private class HeartbeatTask implements Runnable {
        @Override
        public void run() {
            if (state.role() == RaftRole.LEADER) {
                if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - heartbeatPeriodInMillis) {
                    broadcastAppendRequest();
                }

                scheduleHeartbeat();
            }
        }
    }

    /**
     * Leader failure detection task checks whether leader exists and is reachable. Runs pre-vote mechanism
     * if leader doesn't exist or is unreachable or is an unknown endpoint.
     */
    private class LeaderFailureDetectionTask implements Runnable {
        @Override
        public void run() {
            try {
                RaftEndpoint leader = state.leader();
                if (leader == null) {
                    if (state.role() == RaftRole.FOLLOWER) {
                        logger.warning("We are FOLLOWER and there is no current leader. Will start new election round...");
                        runPreVoteTask();
                    }
                } else if (!raftIntegration.isReachable(leader)) {
                    logger.warning("Current leader " + leader + " is not reachable. Will start new election round...");
                    resetLeaderAndStartElection();
                } else if (!state.committedGroupMembers().isKnownEndpoint(leader)) {
                    logger.warning("Current leader " + leader + " is not member anymore. Will start new election round...");
                    resetLeaderAndStartElection();
                }
            } finally {
                scheduleLeaderFailureDetection();
            }
        }

        private void resetLeaderAndStartElection() {
            state.leader(null);
            printMemberState();
            runPreVoteTask();
        }

        private void runPreVoteTask() {
            if (state.preCandidateState() == null) {
                new PreVoteTask(RaftNodeImpl.this).run();
            }
        }
    }

    private class SnapshotTask implements Runnable {
        @Override
        public void run() {
            try {
                if (state.role() == RaftRole.LEADER || state.role() == RaftRole.FOLLOWER) {
                    takeSnapshotIfCommitIndexAdvanced();
                }
            } finally {
                scheduleSnapshot();
            }
        }
    }
}
