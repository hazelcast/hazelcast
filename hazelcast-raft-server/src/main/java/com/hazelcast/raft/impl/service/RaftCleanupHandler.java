package com.hazelcast.raft.impl.service;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.MemberDoesNotExistException;
import com.hazelcast.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNodeStatus;
import com.hazelcast.raft.impl.service.LeavingRaftEndpointContext.RaftGroupLeavingEndpointContext;
import com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteDestroyRaftGroupsOp;
import com.hazelcast.raft.impl.service.operation.metadata.CompleteRemoveEndpointOp;
import com.hazelcast.raft.impl.service.operation.metadata.DestroyRaftNodesOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetDestroyingRaftGroupIds;
import com.hazelcast.raft.impl.service.operation.metadata.GetLeavingEndpointContextOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.raft.impl.util.Pair;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.operation.TerminateRaftGroupOp;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static com.hazelcast.raft.impl.service.RaftService.METADATA_GROUP_ID;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftCleanupHandler {

    static final long CLEANUP_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);
    private static final long CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(10);

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private volatile RaftInvocationManager invocationManager;

    RaftCleanupHandler(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
    }

    public void init() {
        if (getLocalEndpoint() == null) {
            return;
        }

        this.invocationManager = raftService.getInvocationManager();

        ExecutionService executionService = nodeEngine.getExecutionService();
        // scheduleWithRepetition skips subsequent execution if one is already running.
        executionService.scheduleWithRepetition(new GroupDestroyHandlerTask(), CLEANUP_TASK_PERIOD_IN_MILLIS,
                CLEANUP_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new RemoveEndpointHandlerTask(), CLEANUP_TASK_PERIOD_IN_MILLIS,
                CLEANUP_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
        executionService.scheduleWithRepetition(new CheckLocalRaftNodesTask(), CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS,
                CHECK_LOCAL_RAFT_NODES_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
    }

    private RaftEndpoint getLocalEndpoint() {
        return raftService.getMetadataManager().getLocalEndpoint();
    }

    private boolean shouldRunCleanupTask() {
        RaftNode raftNode = raftService.getRaftNode(RaftService.METADATA_GROUP_ID);
        // even if the local leader information is stale, it is fine.
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown() && getLocalEndpoint().equals(raftNode.getLeader());
    }

    private class CheckLocalRaftNodesTask implements Runnable {

        public void run() {
            for (RaftNode raftNode : raftService.getAllRaftNodes()) {
                final RaftGroupId groupId = raftNode.getGroupId();
                if (groupId.equals(METADATA_GROUP_ID)) {
                    continue;
                }

                if (raftNode.getStatus() == RaftNodeStatus.TERMINATED || raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
                    raftService.destroyRaftNode(groupId);
                    continue;
                }

                ICompletableFuture<RaftGroupInfo> f = query(METADATA_GROUP_ID, new GetRaftGroupOp(groupId));

                f.andThen(new ExecutionCallback<RaftGroupInfo>() {
                    @Override
                    public void onResponse(RaftGroupInfo groupInfo) {
                        if (groupInfo == null) {
                            logger.severe("Could not find raft group for local raft node of " + groupId);
                        } else if (groupInfo.status() == RaftGroupStatus.DESTROYED) {
                            raftService.destroyRaftNode(groupId);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.warning("Could not get raft group info of " + groupId, t);
                    }
                });
            }
        }

    }

    private class GroupDestroyHandlerTask implements Runnable {

        @Override
        public void run() {
            if (!shouldRunCleanupTask()) {
                return;
            }

            Collection<RaftGroupId> destroyingRaftGroupIds = getDestroyingRaftGroupIds();
            if (destroyingRaftGroupIds.isEmpty()) {
                return;
            }

            Map<RaftGroupId, Future<Object>> futures = new HashMap<RaftGroupId, Future<Object>>();
            for (final RaftGroupId groupId : destroyingRaftGroupIds) {
                Future<Object> future = invocationManager.invoke(groupId, new TerminateRaftGroupOp());
                futures.put(groupId, future);
            }

            final Set<RaftGroupId> terminatedGroupIds = new HashSet<RaftGroupId>();
            for (Entry<RaftGroupId, Future<Object>> e : futures.entrySet()) {
                if (isTerminated(e.getKey(), e.getValue())) {
                    terminatedGroupIds.add(e.getKey());
                }
            }

            if (terminatedGroupIds.isEmpty()) {
                return;
            }

            commitDestroyedRaftGroups(terminatedGroupIds);

            for (RaftGroupId groupId : terminatedGroupIds) {
                raftService.destroyRaftNode(groupId);
            }

            OperationService operationService = nodeEngine.getOperationService();
            for (RaftEndpoint endpoint : raftService.getAllEndpoints()) {
                if (!endpoint.equals(raftService.getLocalEndpoint())) {
                    operationService.send(new DestroyRaftNodesOp(terminatedGroupIds), endpoint.getAddress());
                }
            }
        }

        private Collection<RaftGroupId> getDestroyingRaftGroupIds() {
            Future<Collection<RaftGroupId>> f = query(METADATA_GROUP_ID, new GetDestroyingRaftGroupIds());

            try {
                return f.get();
            } catch (Exception e) {
                logger.severe("Cannot get destroying raft group ids", e);
                return Collections.emptyList();
            }
        }

        private boolean isTerminated(RaftGroupId groupId, Future<Object> future) {
            try {
                future.get();
                return true;
            }  catch (InterruptedException e) {
                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RaftGroupTerminatedException) {
                    return true;
                }

                logger.severe("Cannot get result of DESTROY commit to " + groupId, e);

                return false;
            }
        }

        private void commitDestroyedRaftGroups(final Set<RaftGroupId> destroyedGroupIds) {
            Future<Collection<RaftGroupId>> f = invocationManager.invoke(METADATA_GROUP_ID,
                    new CompleteDestroyRaftGroupsOp(destroyedGroupIds));

            try {
                f.get();
                logger.info("Terminated raft groups: " + destroyedGroupIds + " are committed.");
            } catch (Exception e) {
                logger.severe("Cannot commit terminated raft groups: " + destroyedGroupIds, e);
            }
        }
    }

    private class RemoveEndpointHandlerTask implements Runnable {

        private static final int NA_MEMBERS_COMMIT_INDEX = -1;

        @Override
        public void run() {
            if (!shouldRunCleanupTask()) {
                return;
            }

            LeavingRaftEndpointContext leavingEndpointContext = getLeavingRaftEndpointContext();
            if (leavingEndpointContext == null) {
                return;
            }

            handle(leavingEndpointContext);
        }

        private LeavingRaftEndpointContext getLeavingRaftEndpointContext() {
            Future<LeavingRaftEndpointContext> f = query(METADATA_GROUP_ID, new GetLeavingEndpointContextOp());

            try {
                return f.get();
            } catch (Exception e) {
                logger.severe("Cannot get leaving endpoint context", e);
                return null;
            }
        }

        private void handle(LeavingRaftEndpointContext leavingRaftEndpointContext) {
            final RaftEndpoint leavingEndpoint = leavingRaftEndpointContext.getEndpoint();
            logger.fine("Handling remove of " + leavingEndpoint + " => " + leavingRaftEndpointContext);

            Map<RaftGroupId, Future<Long>> joinFutures = new HashMap<RaftGroupId, Future<Long>>();
            Map<RaftGroupId, RaftGroupLeavingEndpointContext> leavingGroups = leavingRaftEndpointContext.getGroups();
            for (Entry<RaftGroupId, RaftGroupLeavingEndpointContext> e : leavingGroups.entrySet()) {
                final RaftGroupId groupId = e.getKey();
                final RaftGroupLeavingEndpointContext ctx = e.getValue();

                if (ctx.getSubstitute() == null) {
                    // no substitute is found
                    Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
                    ILogger logger = nodeEngine.getLogger(getClass());
                    SimpleCompletableFuture<Long> future = new SimpleCompletableFuture<Long>(executor, logger);
                    future.setResult(ctx.getMembersCommitIndex());
                    joinFutures.put(groupId, future);
                    continue;
                }

                logger.fine("Substituting " + leavingEndpoint + " with " + ctx.getSubstitute() + " in " + groupId);

                ICompletableFuture<Long> future = invocationManager.changeRaftGroupMembership(groupId,
                        ctx.getMembersCommitIndex(), ctx.getSubstitute(), MembershipChangeType.ADD);
                joinFutures.put(groupId, future);
            }

            Map<RaftGroupId, Future<Long>> leaveFutures = new HashMap<RaftGroupId, Future<Long>>();
            for (Entry<RaftGroupId, Future<Long>> entry : joinFutures.entrySet()) {
                final RaftGroupId groupId = entry.getKey();
                final RaftGroupLeavingEndpointContext ctx = leavingGroups.get(groupId);
                final long idx = getMemberAddCommitIndex(groupId, leavingEndpoint, ctx, entry.getValue());
                if (idx != NA_MEMBERS_COMMIT_INDEX) {
                    logger.info(ctx.getSubstitute() + " is added to " + groupId + " for " + leavingEndpoint
                            + " with new members commit index: " + idx);
                    ICompletableFuture<Long> future = invocationManager.changeRaftGroupMembership(groupId, idx,
                            leavingEndpoint, MembershipChangeType.REMOVE);
                    leaveFutures.put(groupId, future);
                }
            }

            Map<RaftGroupId, Pair<Long, Long>> leftGroups = new HashMap<RaftGroupId, Pair<Long, Long>>();
            for (Entry<RaftGroupId, Future<Long>> entry : leaveFutures.entrySet()) {
                RaftGroupId groupId = entry.getKey();
                RaftGroupLeavingEndpointContext ctx = leavingGroups.get(groupId);
                long idx = getMemberRemoveCommitIndex(groupId, leavingEndpoint, ctx, entry.getValue());
                if (idx != NA_MEMBERS_COMMIT_INDEX) {
                    logger.info(leavingEndpoint + " is removed from " + groupId + " with new members commit index: " + idx);
                    leftGroups.put(groupId, new Pair<Long, Long>(ctx.getMembersCommitIndex(), idx));
                }
            }

            completeRemoveOnMetadata(leavingEndpoint, leftGroups);
            removeFromMetadataGroup(leavingEndpoint);
        }

        private long getMemberAddCommitIndex(RaftGroupId groupId, RaftEndpoint leavingEndpoint,
                                            RaftGroupLeavingEndpointContext ctx, Future<Long> future) {
            try {
                return future.get();
            }  catch (InterruptedException e) {
                logger.severe("Cannot get MEMBER ADD result of " + ctx.getSubstitute() + " for " + leavingEndpoint
                        + " to " + groupId + " with members commit index: " + ctx.getMembersCommitIndex(), e);
                return NA_MEMBERS_COMMIT_INDEX;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MismatchingGroupMembersCommitIndexException) {
                    MismatchingGroupMembersCommitIndexException m = (MismatchingGroupMembersCommitIndexException) e.getCause();

                    String msg = "MEMBER ADD commit of " + ctx.getSubstitute() + " for " + leavingEndpoint + " to " + groupId
                            + " with members commit index: " + ctx.getMembersCommitIndex() + " failed. Actual group members: "
                            + m.getMembers() + " with commit index: " + m.getCommitIndex();

                    if (m.getMembers().size() != ctx.getMembers().size() + 1) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    // learnt group members must contain the substitute the and current members I know

                    if (!m.getMembers().contains(ctx.getSubstitute())) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    for (RaftEndpoint endpoint : ctx.getMembers()) {
                        if (!m.getMembers().contains(endpoint)) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }
                    }

                    return m.getCommitIndex();
                }

                logger.severe("Cannot get MEMBER ADD result of " + ctx.getSubstitute() + " for " + leavingEndpoint
                        + " to " + groupId + " with members commit index: " + ctx.getMembersCommitIndex(), e);
                return NA_MEMBERS_COMMIT_INDEX;
            }
        }

        private long getMemberRemoveCommitIndex(RaftGroupId groupId, RaftEndpoint leavingEndpoint,
                                               RaftGroupLeavingEndpointContext ctx, Future<Long> future) {
            try {
                return future.get();
            }  catch (InterruptedException e) {
                logger.severe("Cannot get MEMBER REMOVE result of " + leavingEndpoint + " to " + groupId, e);
                return NA_MEMBERS_COMMIT_INDEX;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof MismatchingGroupMembersCommitIndexException) {
                    MismatchingGroupMembersCommitIndexException m = (MismatchingGroupMembersCommitIndexException) e.getCause();

                    String msg = "MEMBER REMOVE commit of " + leavingEndpoint + " with substitute: " + ctx.getSubstitute()
                            + " to " + groupId + " failed. Actual group members: " + m.getMembers() + " with commit index: "
                            + m.getCommitIndex();

                    if (m.getMembers().contains(leavingEndpoint)) {
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    if (ctx.getSubstitute() != null) {
                        // I expect the substitute endpoint to be joined to the group
                        if (!m.getMembers().contains(ctx.getSubstitute())) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }

                        // I know the leaving endpoint has left the group and the substitute has joined.
                        // So member sizes must be same...
                        if (m.getMembers().size() != ctx.getMembers().size()) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }
                    } else if (m.getMembers().size() != (ctx.getMembers().size() - 1)) {
                        // if there is no substitute, I expect number of the learnt group members to be 1 less than
                        // the current members I know
                        logger.severe(msg);
                        return NA_MEMBERS_COMMIT_INDEX;
                    }

                    for (RaftEndpoint endpoint : ctx.getMembers()) {
                        // Other group members expect the leaving one and substitute must be still present...
                        if (!endpoint.equals(leavingEndpoint) && !m.getMembers().contains(endpoint)) {
                            logger.severe(msg);
                            return NA_MEMBERS_COMMIT_INDEX;
                        }
                    }

                    return m.getCommitIndex();
                }

                logger.severe("Cannot get MEMBER REMOVE result of " + leavingEndpoint + " to " + groupId, e);
                return NA_MEMBERS_COMMIT_INDEX;
            }
        }

        private void completeRemoveOnMetadata(final RaftEndpoint endpoint,
                                              final Map<RaftGroupId, Pair<Long, Long>> leftGroups) {
            ICompletableFuture<Object> future = invocationManager.invoke(METADATA_GROUP_ID,
                    new CompleteRemoveEndpointOp(endpoint, leftGroups));

            try {
                future.get();
                logger.info(endpoint + " is safe to remove");
            } catch (Exception e) {
                logger.severe("Cannot commit remove completion for: " + endpoint, e);
            }
        }

        private void removeFromMetadataGroup(final RaftEndpoint endpoint) {
            ICompletableFuture<Object> future = invocationManager.changeRaftGroupMembership(METADATA_GROUP_ID, endpoint,
                                                                   MembershipChangeType.REMOVE);

            try {
                future.get();
                logger.info(endpoint + " is removed from metadata cluster.");
            } catch (MemberDoesNotExistException e) {
                logger.fine("Cannot commit remove for: " + endpoint, e);
            } catch (Exception e) {
                logger.severe("Cannot commit remove for: " + endpoint, e);
            }
        }

    }

    private <T> ICompletableFuture<T> query(RaftGroupId groupId, RaftOperation raftOperation) {
        return invocationManager.query(groupId, raftOperation, QueryPolicy.LEADER_LOCAL);
    }
}
