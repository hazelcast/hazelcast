/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.service;

import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.RaftManagementService;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftIntegration;
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
import com.hazelcast.raft.impl.service.operation.metadata.ForceDestroyRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveRaftGroupIdOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetActiveRaftMembersOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import com.hazelcast.raft.impl.service.operation.metadata.GetRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.RaftServicePreJoinOp;
import com.hazelcast.raft.impl.service.operation.metadata.TriggerRemoveRaftMemberOp;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.spi.GracefulShutdownAwareService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PreJoinAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.raft.impl.service.MetadataRaftGroupManager.METADATA_GROUP_ID;
import static com.hazelcast.raft.impl.service.RaftGroupMembershipManager.MANAGEMENT_TASK_PERIOD_IN_MILLIS;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO: Javadoc Pending...
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class RaftService implements ManagedService, SnapshotAwareService<MetadataRaftGroupSnapshot>, GracefulShutdownAwareService,
                                    MembershipAwareService, RaftManagementService, PreJoinAwareService {

    public static final String SERVICE_NAME = "hz:core:raft";

    private static final long REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS = 1;

    private final ConcurrentMap<RaftGroupId, RaftNode> nodes = new ConcurrentHashMap<RaftGroupId, RaftNode>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final Set<RaftGroupId> destroyedGroupIds = newSetFromMap(new ConcurrentHashMap<RaftGroupId, Boolean>());

    private final RaftConfig config;
    private final RaftInvocationManager invocationManager;
    private final MetadataRaftGroupManager metadataGroupManager;

    private final ConcurrentMap<RaftMemberImpl, Long> missingMembers = new ConcurrentHashMap<RaftMemberImpl, Long>();

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        RaftConfig raftConfig = nodeEngine.getConfig().getRaftConfig();
        this.config = raftConfig != null ? new RaftConfig(raftConfig) : new RaftConfig();
        this.metadataGroupManager = new MetadataRaftGroupManager(nodeEngine, this, config);
        this.invocationManager = new RaftInvocationManager(nodeEngine, this);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        ClientExceptionFactory clientExceptionFactory = this.nodeEngine.getNode().clientEngine.getClientExceptionFactory();
        SessionExpiredException.register(clientExceptionFactory);
        metadataGroupManager.initLocalRaftMemberOnStartup();
        if (config.getMissingRaftMemberRemovalSeconds() > 0) {
            nodeEngine.getExecutionService().scheduleWithRepetition(new RemoveMissingMemberTask(),
                    REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS, REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS, SECONDS);
        }
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    @Override
    public MetadataRaftGroupSnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        return metadataGroupManager.takeSnapshot(groupId, commitIndex);
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, MetadataRaftGroupSnapshot snapshot) {
        metadataGroupManager.restoreSnapshot(groupId, commitIndex, snapshot);
    }

    @Override
    public Collection<RaftGroupId> getRaftGroupIds() {
        return Collections.unmodifiableCollection(metadataGroupManager.getRaftGroupIds());
    }

    @Override
    public RaftGroupInfo getRaftGroup(RaftGroupId id) {
        return metadataGroupManager.getRaftGroup(id);
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
        metadataGroupManager.reset();
    }

    @Override
    public ICompletableFuture<Void> triggerRaftMemberPromotion() {
        checkState(metadataGroupManager.getLocalMember() == null, "We are already a Raft member!");

        RaftMemberImpl member = new RaftMemberImpl(nodeEngine.getLocalMember());
        logger.info("Adding new Raft member: " + member);
        ICompletableFuture<Void> future = invocationManager.invoke(METADATA_GROUP_ID, new AddRaftMemberOp(member));

        future.andThen(new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                metadataGroupManager.initPromotedRaftMember();
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

        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        final SimpleCompletableFuture<Void> future = new SimpleCompletableFuture<Void>(executor, logger);

        invokeTriggerRemoveMember(member).andThen(new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                future.setResult(response);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof CannotRemoveMemberException) {
                    t = new IllegalStateException(t.getMessage());
                }

                future.setResult(new ExecutionException(t));
            }
        });

        return future;
    }

    /**
     * this method is idempotent
     */
    @Override
    public ICompletableFuture<Void> forceDestroyRaftGroup(RaftGroupId groupId) {
        return invocationManager.invoke(METADATA_GROUP_ID, new ForceDestroyRaftGroupOp(groupId));
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        RaftMemberImpl localMember = getLocalMember();
        if (localMember == null) {
            return true;
        }

        if (metadataGroupManager.getActiveMembers().size() == 1) {
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
                Thread.sleep(MANAGEMENT_TASK_PERIOD_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            remainingTimeNanos -= MANAGEMENT_TASK_PERIOD_IN_MILLIS;
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
    public Operation getPreJoinOperation() {
        boolean master = nodeEngine.getClusterService().isMaster();
        return master ? new RaftServicePreJoinOp(metadataGroupManager.isDiscoveryCompleted()) : null;
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        metadataGroupManager.broadcastActiveMembers();
        updateMissingMembers();
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        updateMissingMembers();
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
        updateMissingMembers();
    }

    void updateMissingMembers() {
        if (config.getMissingRaftMemberRemovalSeconds() == 0 || !metadataGroupManager.isDiscoveryCompleted()) {
            return;
        }

        Collection<RaftMemberImpl> activeMembers = metadataGroupManager.getActiveMembers();
        missingMembers.keySet().retainAll(activeMembers);

        ClusterService clusterService = nodeEngine.getClusterService();
        for (RaftMemberImpl raftMember : activeMembers) {
            if (clusterService.getMember(raftMember.getAddress()) == null) {
                if (missingMembers.putIfAbsent(raftMember, System.currentTimeMillis()) == null) {
                    logger.warning(raftMember + " is not present in the cluster. It will be auto-removed after "
                            + config.getMissingRaftMemberRemovalSeconds() + " seconds.");
                }
            } else if (missingMembers.remove(raftMember) != null) {
                logger.info(raftMember + " is removed from the missing members list as it is in the cluster.");
            }
        }
    }

    Collection<RaftMemberImpl> getMissingMembers() {
        return Collections.unmodifiableSet(missingMembers.keySet());
    }

    public MetadataRaftGroupManager getMetadataGroupManager() {
        return metadataGroupManager;
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
        return metadataGroupManager.getLocalMember();
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

        RaftIntegration integration = new NodeEngineRaftIntegration(nodeEngine, groupId);
        RaftNodeImpl node = new RaftNodeImpl(groupId, getLocalMember(), members, config.getRaftAlgorithmConfig(), integration);

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

    public RaftGroupId createRaftGroupForProxy(String name) {
        String groupName = getGroupNameForProxy(name);

        try {
            RaftGroupId groupId = invocationManager
                    .<RaftGroupId>invoke(MetadataRaftGroupManager.METADATA_GROUP_ID, new GetActiveRaftGroupIdOp(groupName)).get();
            if (groupId != null) {
                return groupId;
            }

            return invocationManager.createRaftGroup(groupName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Could not create raft group: " + groupName);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Could not create raft group: " + groupName);
        }
    }

    private String getGroupNameForProxy(String name) {
        int i = name.indexOf("@");
        if (i == -1) {
            return RaftConfig.DEFAULT_RAFT_GROUP_NAME;
        }

        checkTrue(i < (name.length() - 1), "Custom group name cannot be empty string");
        checkTrue(name.indexOf("@", i +1) == -1, "Custom group name must be specified at most once");
        String groupName = name.substring(i + 1).trim();
        checkTrue(groupName.length() > 0, "Custom group name cannot be empty string");
        return groupName;
    }

    private ICompletableFuture<Void> invokeTriggerRemoveMember(RaftMemberImpl member) {
        return invocationManager.invoke(METADATA_GROUP_ID, new TriggerRemoveRaftMemberOp(member));
    }

    private boolean isRemoved(RaftMemberImpl member) {
        RaftOp op = new GetActiveRaftMembersOp();
        InternalCompletableFuture<List<RaftMemberImpl>> f = invocationManager.query(METADATA_GROUP_ID, op, LEADER_LOCAL);
        List<RaftMemberImpl> members = f.join();
        return !members.contains(member);
    }

    public static String getObjectNameForProxy(String name) {
        int i = name.indexOf("@");
        if (i == -1) {
            return name;
        }

        checkTrue(i < (name.length() - 1), "Object name cannot be empty string");
        checkTrue(name.indexOf("@", i +1) == -1, "Custom group name must be specified at most once");
        String objectName = name.substring(0, i).trim();
        checkTrue(objectName.length() > 0, "Object name cannot be empty string");
        return objectName;
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
            RaftMemberImpl localMember = getLocalMember();
            if (localMember == null) {
                return;
            }

            RaftOp op = new GetInitialRaftGroupMembersIfCurrentGroupMemberOp(localMember);
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

    private class RemoveMissingMemberTask implements Runnable {
        @Override
        public void run() {
            try {
                if (!nodeEngine.getClusterService().isMaster() || missingMembers.isEmpty()
                        || metadataGroupManager.getMembershipChangeContext() != null) {
                    return;
                }

                for (Entry<RaftMemberImpl, Long> e : missingMembers.entrySet()) {
                    long missingTimeSeconds = MILLISECONDS.toSeconds(System.currentTimeMillis() - e.getValue());
                    if (missingTimeSeconds >= config.getMissingRaftMemberRemovalSeconds()) {
                        RaftMemberImpl missingMember = e.getKey();
                        logger.info("Triggering auto-remove of " + missingMember + " since it is absent for "
                                + missingTimeSeconds + " seconds...");

                        triggerRemoveRaftMember(missingMember).get();

                        logger.info("Auto-removal of " + missingMember + " is successful.");

                        return;
                    }
            }
            } catch (Exception e) {
                logger.severe("RemoveMissingMembersTask failed", e);
            }
        }
    }

}
