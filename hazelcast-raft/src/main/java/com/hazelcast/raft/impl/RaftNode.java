package com.hazelcast.raft.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.LeaderDemotedException;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.handler.AppendFailureResponseHandlerTask;
import com.hazelcast.raft.impl.handler.AppendRequestHandlerTask;
import com.hazelcast.raft.impl.handler.AppendSuccessResponseHandlerTask;
import com.hazelcast.raft.impl.handler.LeaderElectionTask;
import com.hazelcast.raft.impl.handler.ReplicateTask;
import com.hazelcast.raft.impl.handler.VoteRequestHandlerTask;
import com.hazelcast.raft.impl.handler.VoteResponseHandlerTask;
import com.hazelcast.raft.impl.log.LogEntry;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.LeaderState;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.impl.util.StripedExecutorConveyor;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.collection.Long2ObjectHashMap;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftNode {

    private static final int HEARTBEAT_PERIOD = 5;

    private final ILogger logger;
    private final RaftState state;
    private final StripedExecutor executor;
    private final RaftIntegration raftIntegration;
    private final RaftEndpoint localEndpoint;
    private final TaskScheduler taskScheduler;

    private final Long2ObjectHashMap<SimpleCompletableFuture> futures = new Long2ObjectHashMap<SimpleCompletableFuture>();
    private long lastAppendEntriesTimestamp;

    public RaftNode(String name, RaftEndpoint localEndpoint, Collection<RaftEndpoint> endpoints,
            RaftIntegration raftIntegration, StripedExecutor executor) {
        this.raftIntegration = raftIntegration;
        this.executor = executor;
        this.localEndpoint = localEndpoint;
        this.state = new RaftState(name, localEndpoint, endpoints);
        this.taskScheduler = raftIntegration.getTaskScheduler();
        this.logger = getLogger(getClass());
    }

    public ILogger getLogger(Class clazz) {
        String name = state.name();
        return raftIntegration.getLogger(clazz.getName() + "(" + name + ")");
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
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

    public void start() {
        if (raftIntegration.isJoined()) {
            logger.info("Starting raft node: " + localEndpoint + " for raft cluster: " + state.name()
                + " with members[" + state.memberCount() + "]: " + state.members());
            executor.execute(new LeaderElectionTask(this));
        } else {
            scheduleStart();
        }

        scheduleLeaderFailureDetection();
    }

    private void scheduleLeaderFailureDetection() {
        // TODO: Delay should be configurable.
        long delay = RandomPicker.getInt(1000, 1500);
        taskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                executor.execute(new LeaderFailureDetectionTask());
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void scheduleStart() {
        taskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                start();
            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    public int getStripeKey() {
        return state.name().hashCode();
    }

    public void scheduleLeaderLoop() {
        executor.execute(new HeartbeatTask());
    }

    public void broadcastAppendRequest() {
        for (RaftEndpoint follower : state.remoteMembers()) {
            sendAppendRequest(follower);
        }
        lastAppendEntriesTimestamp = Clock.currentTimeMillis();
    }

    public void sendAppendRequest(RaftEndpoint follower) {
        RaftLog raftLog = state.log();
        LeaderState leaderState = state.leaderState();

        int nextIndex = leaderState.getNextIndex(follower);

        LogEntry prevEntry;
        LogEntry[] entries;
        // TODO: define a max batch size
        if (nextIndex > 1) {
            prevEntry = raftLog.getEntry(nextIndex - 1);
            int matchIndex = leaderState.getMatchIndex(follower);
            if (matchIndex == 0 && nextIndex > (matchIndex + 1)) {
                // Until the leader has discovered where it and the follower's logs match,
                // the leader can send AppendEntries with no entries (like heartbeats) to save bandwidth.
                entries = new LogEntry[0];
            } else {
                // Then, once the matchIndex immediately precedes the nextIndex,
                // the leader should begin to send the actual entries
                entries = raftLog.getEntriesBetween(nextIndex, raftLog.lastLogIndex());
            }
        } else if (nextIndex == 1 && raftLog.lastLogIndex() > 0) {
            prevEntry = new LogEntry();
            entries = raftLog.getEntriesBetween(nextIndex, raftLog.lastLogIndex());
        } else {
            prevEntry = new LogEntry();
            entries = new LogEntry[0];
        }

        assert prevEntry != null : "Follower: " + follower + ", next index: " + nextIndex;

        AppendRequest appendRequest = new AppendRequest(getLocalEndpoint(), state.term(), prevEntry.term(), prevEntry.index(),
                state.commitIndex(), entries);

        if (logger.isFineEnabled()) {
            logger.fine("Sending " + appendRequest + " to " + follower + " with next index: " + nextIndex);
        }

        send(appendRequest, follower);
    }

    // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
    public void processLogs() {
        // Reject logs we've applied already
        int commitIndex = state.commitIndex();
        int lastApplied = state.lastApplied();

        if (commitIndex == lastApplied) {
            return;
        }

        assert commitIndex > lastApplied : "commit index: " + commitIndex + " cannot be smaller than last applied: " + lastApplied;

        // Apply all the preceding logs
        RaftLog raftLog = state.log();
        for (int idx = state.lastApplied() + 1; idx <= commitIndex; idx++) {
            LogEntry entry = raftLog.getEntry(idx);
            if (entry == null) {
                String msg = "Failed to get log entry at index: " + idx;
                logger.severe(msg);
                throw new AssertionError(msg);
            }

            processLog(entry);

            // Update the lastApplied index
            state.lastApplied(idx);
        }
    }

    private void processLog(LogEntry entry) {
        if (logger.isFineEnabled()) {
            logger.fine("Processing " + entry);
        }

        SimpleCompletableFuture future = futures.remove(entry.index());
        Object response = raftIntegration.runOperation(entry.operation(), entry.index());
        if (future != null) {
            future.setResult(response);
        }
    }

    public RaftState state() {
        return state;
    }

    public TaskScheduler taskScheduler() {
        return taskScheduler;
    }

    public Executor executor() {
        return executor;
    }

    public void handleVoteRequest(VoteRequest request) {
        executor.execute(new VoteRequestHandlerTask(this, request));
    }

    public void handleVoteResponse(VoteResponse response) {
        executor.execute(new VoteResponseHandlerTask(this, response));
    }

    public void handleAppendRequest(AppendRequest request) {
        executor.execute(new AppendRequestHandlerTask(this, request));
    }

    public void handleAppendResponse(AppendSuccessResponse response) {
        executor.execute(new AppendSuccessResponseHandlerTask(this, response));
    }

    public void handleAppendResponse(AppendFailureResponse response) {
        executor.execute(new AppendFailureResponseHandlerTask(this, response));
    }

    public void registerFuture(int entryIndex, SimpleCompletableFuture future) {
        SimpleCompletableFuture f = futures.put(entryIndex, future);
        assert f == null : "Future object is already registered for entry index: " + entryIndex;
    }

    public void invalidateFuturesFrom(int entryIndex) {
        int count = 0;
        Iterator<Map.Entry<Long, SimpleCompletableFuture>> iterator = futures.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, SimpleCompletableFuture> entry = iterator.next();
            long index = entry.getKey();
            if (index >= entryIndex) {
                entry.getValue().setResult(new LeaderDemotedException());
                iterator.remove();
                count++;
            }
        }

        logger.warning("Invalidated " + count + " futures from log index: " + entryIndex);
    }

    // for testing
    RaftState getState() {
        return state;
    }

    // for testing
    Executor getExecutor() {
        return new StripedExecutorConveyor(getStripeKey(), executor);
    }

    public ICompletableFuture replicate(RaftOperation operation) {
        SimpleCompletableFuture resultFuture = new SimpleCompletableFuture(raftIntegration.getExecutor(), logger);
        executor.execute(new ReplicateTask(this, operation, resultFuture));
        return resultFuture;
    }

    public RaftEndpoint getLeader() {
        // read leader might be stale, since it's accessed without any synchronization
        return state.leader();
    }

    private class HeartbeatTask implements StripedRunnable {

        @Override
        public void run() {
            if (state.role() == RaftRole.LEADER) {
                if (lastAppendEntriesTimestamp < Clock.currentTimeMillis() - TimeUnit.SECONDS.toMillis(HEARTBEAT_PERIOD)) {
                    broadcastAppendRequest();
                }

                scheduleNextRun();
            }
        }

        private void scheduleNextRun() {
            taskScheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    executor.execute(new HeartbeatTask());
                }
            }, HEARTBEAT_PERIOD, TimeUnit.SECONDS);
        }

        @Override
        public int getKey() {
            return getStripeKey();
        }
    }

    private class LeaderFailureDetectionTask implements StripedRunnable {
        @Override
        public int getKey() {
            return getStripeKey();
        }

        @Override
        public void run() {
            try {
                RaftEndpoint leader = state.leader();
                if (leader == null) {
                    if (state.role() == RaftRole.FOLLOWER) {
                        logger.warning("We are FOLLOWER and there is no current leader. Will start new election round...");
                        new LeaderElectionTask(RaftNode.this).run();
                    }
                } else if (!raftIntegration.isReachable(leader)) {
                    logger.warning("Current leader " + leader + " is not reachable. Will start new election round...");
                    state.leader(null);
                    new LeaderElectionTask(RaftNode.this).run();
                }
            } finally {
                scheduleLeaderFailureDetection();
            }
        }
    }
}
