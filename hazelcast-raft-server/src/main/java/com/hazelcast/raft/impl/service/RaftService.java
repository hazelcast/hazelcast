package com.hazelcast.raft.impl.service;

import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.RaftManagementService;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.service.exception.CannotRemoveMemberException;
import com.hazelcast.raft.impl.service.operation.metadata.AddRaftMemberOp;
import com.hazelcast.raft.impl.service.operation.metadata.CheckRemovedRaftMemberOp;
import com.hazelcast.raft.impl.service.operation.metadata.ForceDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerRebalanceRaftGroupsOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerRemoveRaftMemberOp;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.spi.GracefulShutdownAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.raft.impl.service.RaftCleanupHandler.CLEANUP_TASK_PERIOD_IN_MILLIS;
import static com.hazelcast.raft.impl.service.RaftMetadataManager.METADATA_GROUP_ID;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.util.Preconditions.checkState;
import static java.util.Collections.newSetFromMap;

/**
 * TODO: Javadoc Pending...
 */
public class RaftService implements ManagedService, SnapshotAwareService<MetadataSnapshot>,
        GracefulShutdownAwareService, MembershipAwareService, RaftManagementService {

    public static final String SERVICE_NAME = "hz:core:raft";

    private final ConcurrentMap<RaftGroupId, RaftNode> nodes = new ConcurrentHashMap<RaftGroupId, RaftNode>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final Set<RaftGroupId> destroyedGroupIds = newSetFromMap(new ConcurrentHashMap<RaftGroupId, Boolean>());

    private final RaftConfig config;
    private final RaftInvocationManager invocationManager;
    private final RaftMetadataManager metadataManager;

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        RaftConfig raftConfig = nodeEngine.getConfig().getRaftConfig();
        this.config = raftConfig != null ? new RaftConfig(raftConfig) : new RaftConfig();
        this.metadataManager = new RaftMetadataManager(nodeEngine, this, config.getMetadataGroupConfig());
        this.invocationManager = new RaftInvocationManager(nodeEngine, this);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        ClientExceptionFactory clientExceptionFactory = this.nodeEngine.getNode().clientEngine.getClientExceptionFactory();
        SessionExpiredException.register(clientExceptionFactory);
        metadataManager.initIfInitialRaftMember();
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public MetadataSnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        return metadataManager.takeSnapshot(groupId, commitIndex);
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, MetadataSnapshot snapshot) {
        metadataManager.restoreSnapshot(groupId, commitIndex, snapshot);
    }

    @Override
    public Collection<RaftGroupId> getRaftGroupIds() {
        return Collections.unmodifiableCollection(metadataManager.getRaftGroupIds());
    }

    @Override
    public RaftGroupInfo getRaftGroup(RaftGroupId id) {
        return metadataManager.getRaftGroup(id);
    }

    /**
     * this method is NOT idempotent and multiple invocations on the same member can break the whole system !!!
     */
    @Override
    public void resetAndInitRaftState() {
        // we should clear the current raft state before resetting the metadata manager
        for (RaftNode node : nodes.values()) {
            node.forceSetTerminatedStatus();
        }
        nodes.clear();
        destroyedGroupIds.clear();

        invocationManager.reset();
        metadataManager.reset();
    }

    /**
     * this method is idempotent
     */
    @Override
    public ICompletableFuture<Void> triggerRebalanceRaftGroups() {
        return invocationManager.invoke(METADATA_GROUP_ID, new TriggerRebalanceRaftGroupsOp());
    }

    @Override
    public ICompletableFuture<Void> triggerRaftMemberPromotion() {
        checkState(metadataManager.getLocalMember() == null, "We are already a Raft member!");

        RaftMemberImpl member = new RaftMemberImpl(nodeEngine.getLocalMember());
        logger.info("Adding new Raft member: " + member);
        ICompletableFuture<Void> future = invocationManager.invoke(METADATA_GROUP_ID, new AddRaftMemberOp(member));

        future.andThen(new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                metadataManager.init();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });
        return future;
    }

    /**
     * this method is idempotent
     */
    @Override
    public ICompletableFuture<Void> triggerRemoveRaftMember(RaftMember m) {
        RaftMemberImpl member = (RaftMemberImpl) m;
        ClusterService clusterService = nodeEngine.getClusterService();
        checkState(clusterService.isMaster(), "Only master can remove a Raft member!");
        checkState(clusterService.getMember(member.getAddress()) == null,
                "Cannot remove " + member + ", it is a live member!");

        return invokeTriggerRemoveMember(member);
    }

    /**
     * this method is idempotent
     */
    @Override
    public ICompletableFuture<Void> forceDestroyRaftGroup(RaftGroupId groupId) {
        return invocationManager.invoke(METADATA_GROUP_ID, new ForceDestroyRaftGroupOp(groupId));
    }

    @Override
    // TODO: close sessions & release resources/locks
    public boolean onShutdown(long timeout, TimeUnit unit) {
        RaftMemberImpl localMember = getLocalMember();
        if (localMember == null) {
            return true;
        }

        if (metadataManager.getActiveMembers().size() == 1) {
            logger.warning("I am the last...");
            return true;
        }

        logger.fine("Triggering remove member procedure for " + localMember);

        long remainingTimeNanos = unit.toNanos(timeout);
        long start = System.nanoTime();

        ensureTriggerShutdown(localMember, remainingTimeNanos);
        remainingTimeNanos -= (System.nanoTime() - start);

        // wait for us being replaced in all raft groups we are participating
        // and removed from all raft groups
        logger.fine("Waiting remove member procedure to be completed for " + localMember
                + ", remaining time: " + TimeUnit.NANOSECONDS.toMillis(remainingTimeNanos) + " ms.");
        while (remainingTimeNanos > 0) {
            if (isRemoved(localMember)) {
                logger.fine("Remove member procedure completed for " + localMember);
                return true;
            }
            try {
                Thread.sleep(CLEANUP_TASK_PERIOD_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            remainingTimeNanos -= CLEANUP_TASK_PERIOD_IN_MILLIS;
        }
        logger.fine("Remove member procedure NOT completed for " + localMember + " in " + unit.toMillis(timeout) + " ms.");
        return false;
    }

    private void ensureTriggerShutdown(RaftMemberImpl member, long remainingTimeNanos) {
        while (remainingTimeNanos > 0) {
            long start = System.nanoTime();
            try {
                // mark us as shutting-down in metadata
                Future<Void> future = invokeTriggerRemoveMember(member);
                future.get(remainingTimeNanos, TimeUnit.NANOSECONDS);
                logger.fine(member + " is marked as being removed.");
                return;
            } catch (CannotRemoveMemberException e) {
                remainingTimeNanos -= (System.nanoTime() - start);
                if (remainingTimeNanos <= 0) {
                    throw e;
                }
                logger.fine(e.getMessage());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        metadataManager.broadcastActiveMembers();
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    public RaftMetadataManager getMetadataManager() {
        return metadataManager;
    }

    public RaftInvocationManager getInvocationManager() {
        return invocationManager;
    }

    public void handlePreVoteRequest(RaftGroupId groupId, PreVoteRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.warning("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handlePreVoteRequest(request);
    }

    public void handlePreVoteResponse(RaftGroupId groupId, PreVoteResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.warning("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handlePreVoteResponse(response);
    }

    public void handleVoteRequest(RaftGroupId groupId, VoteRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.warning("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleVoteRequest(request);
    }

    public void handleVoteResponse(RaftGroupId groupId, VoteResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.warning("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleVoteResponse(response);
    }

    public void handleAppendEntries(RaftGroupId groupId, AppendRequest request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.warning("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleAppendRequest(request);
    }

    public void handleAppendResponse(RaftGroupId groupId, AppendSuccessResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.warning("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleAppendResponse(RaftGroupId groupId, AppendFailureResponse response) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.warning("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleSnapshot(RaftGroupId groupId, InstallSnapshot request) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            logger.warning("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleInstallSnapshot(request);
    }

    public Collection<RaftNode> getAllRaftNodes() {
        return new ArrayList<RaftNode>(nodes.values());
    }

    public RaftNode getRaftNode(RaftGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftNode getOrInitRaftNode(RaftGroupId groupId) {
        RaftNode node = nodes.get(groupId);
        if (node == null && !destroyedGroupIds.contains(groupId)) {
            logger.fine("There is no RaftNode for " + groupId + ". Asking to the metadata group...");
            nodeEngine.getExecutionService().execute(ASYNC_EXECUTOR, new InitializeRaftNodeTask(groupId));
        }
        return node;
    }

    public boolean isRaftGroupDestroyed(RaftGroupId groupId) {
        return destroyedGroupIds.contains(groupId);
    }

    public RaftConfig getConfig() {
        return config;
    }

    public RaftMemberImpl getLocalMember() {
        return metadataManager.getLocalMember();
    }

    public void createRaftNode(RaftGroupId groupId, Collection<RaftMember> members) {
        if (nodes.containsKey(groupId)) {
            logger.fine("Not creating RaftNode for " + groupId + " since it is already created...");
            return;
        }

        if (destroyedGroupIds.contains(groupId)) {
            logger.warning("Not creating RaftNode for " + groupId + " since it is already destroyed");
            return;
        }

        RaftIntegration raftIntegration = new NodeEngineRaftIntegration(nodeEngine, groupId);
        RaftNodeImpl node = new RaftNodeImpl(groupId, getLocalMember(), members, config.getRaftAlgorithmConfig(), raftIntegration);

        if (nodes.putIfAbsent(groupId, node) == null) {
            if (destroyedGroupIds.contains(groupId)) {
                node.forceSetTerminatedStatus();
                logger.warning("Not creating RaftNode for " + groupId + " since it is already destroyed");
                return;
            }

            node.start();
            logger.info("RaftNode created for: " + groupId + " with members: " + members);
        }
    }

    public void destroyRaftNode(RaftGroupId groupId) {
        destroyedGroupIds.add(groupId);
        RaftNode node = nodes.remove(groupId);
        if (node != null) {
            node.forceSetTerminatedStatus();
            logger.fine("Local raft node of " + groupId + " is destroyed.");
        }
    }

    private ICompletableFuture<Void> invokeTriggerRemoveMember(RaftMemberImpl member) {
        return invocationManager.invoke(METADATA_GROUP_ID, new TriggerRemoveRaftMemberOp(member));
    }

    private boolean isRemoved(RaftMemberImpl member) {
        Future<Boolean> f = invocationManager.query(METADATA_GROUP_ID, new CheckRemovedRaftMemberOp(member), LEADER_LOCAL);
        try {
            return f.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private class InitializeRaftNodeTask implements Runnable {
        private final RaftGroupId groupId;

        InitializeRaftNodeTask(RaftGroupId groupId) {
            this.groupId = groupId;
        }

        @Override
        public void run() {
            queryInitialMembersFromMetadataRaftGroup();
        }

        private void queryInitialMembersFromMetadataRaftGroup() {
            RaftOp op = new GetRaftGroupOp(groupId);
            ICompletableFuture<RaftGroupInfo> f = invocationManager.query(METADATA_GROUP_ID, op, LEADER_LOCAL);
            f.andThen(new ExecutionCallback<RaftGroupInfo>() {
                @Override
                public void onResponse(RaftGroupInfo group) {
                    if (group != null) {
                        if (group.members().contains(getLocalMember())) {
                            createRaftNode(groupId, group.initialMembers());
                        } else {
                            // I can be the member that is just added to the raft group...
                            queryInitialMembersFromTargetRaftGroup();
                        }
                    } else {
                        logger.warning("Cannot get initial members of: " + groupId + " from the metadata group");
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Cannot get initial members of: " + groupId + " from the metadata group", t);
                }
            });
        }

        void queryInitialMembersFromTargetRaftGroup() {
            RaftOp op = new GetInitialRaftGroupMembersIfCurrentGroupMemberOp(getLocalMember());
            ICompletableFuture<Collection<RaftMember>> f = invocationManager.query(groupId, op, LEADER_LOCAL);
            f.andThen(new ExecutionCallback<Collection<RaftMember>>() {
                @Override
                public void onResponse(Collection<RaftMember> initialMembers) {
                    createRaftNode(groupId, initialMembers);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Cannot get initial members of: " + groupId + " from the group itself", t);
                }
            });
        }
    }
}
