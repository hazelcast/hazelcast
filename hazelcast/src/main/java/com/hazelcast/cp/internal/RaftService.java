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

package com.hazelcast.cp.internal;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.operation.RestartCPMemberOp;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.cp.internal.raftop.metadata.AddCPMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.ForceDestroyRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupByNameOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.RaftServicePreJoinOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerRemoveCPMemberOp;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
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
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.UuidUtil;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.RaftGroupMembershipManager.MANAGEMENT_TASK_PERIOD_IN_MILLIS;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;
import static com.hazelcast.util.ExceptionUtil.peel;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains {@link RaftNode} instances that run the Raft consensus algorithm
 * for the created CP groups. Also implements CP subsystem management methods.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class RaftService implements ManagedService, SnapshotAwareService<MetadataRaftGroupSnapshot>, GracefulShutdownAwareService,
                                    MembershipAwareService, CPSubsystemManagementService, PreJoinAwareService {

    public static final String SERVICE_NAME = "hz:core:raft";

    private static final long REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS = 1;

    private final ConcurrentMap<CPGroupId, RaftNode> nodes = new ConcurrentHashMap<CPGroupId, RaftNode>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final Set<CPGroupId> destroyedGroupIds = newSetFromMap(new ConcurrentHashMap<CPGroupId, Boolean>());
    private final CPSubsystemConfig config;
    private final RaftInvocationManager invocationManager;
    private final MetadataRaftGroupManager metadataGroupManager;
    private final ConcurrentMap<CPMemberInfo, Long> missingMembers = new ConcurrentHashMap<CPMemberInfo, Long>();

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        CPSubsystemConfig cpSubsystemConfig = nodeEngine.getConfig().getCPSubsystemConfig();
        this.config = cpSubsystemConfig != null ? new CPSubsystemConfig(cpSubsystemConfig) : new CPSubsystemConfig();
        checkCPSubsystemConfig(this.config);
        this.metadataGroupManager = new MetadataRaftGroupManager(nodeEngine, this, config);
        this.invocationManager = new RaftInvocationManager(nodeEngine, this);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        if (!metadataGroupManager.initLocalCPMemberOnStartup()) {
            return;
        }

        if (config.getMissingCPMemberAutoRemovalSeconds() > 0) {
            nodeEngine.getExecutionService().scheduleWithRepetition(new AutoRemoveMissingCPMemberTask(),
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
    public MetadataRaftGroupSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        return metadataGroupManager.takeSnapshot(groupId, commitIndex);
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, MetadataRaftGroupSnapshot snapshot) {
        metadataGroupManager.restoreSnapshot(groupId, commitIndex, snapshot);
    }

    public ICompletableFuture<Collection<CPGroupId>> getAllCPGroupIds() {
        return invocationManager.invoke(getMetadataGroupId(), new GetRaftGroupIdsOp());
    }

    @Override
    public ICompletableFuture<Collection<CPGroupId>> getCPGroupIds() {
        return invocationManager.invoke(getMetadataGroupId(), new GetActiveRaftGroupIdsOp());
    }

    public ICompletableFuture<CPGroup> getCPGroup(CPGroupId groupId) {
        return invocationManager.invoke(getMetadataGroupId(), new GetRaftGroupOp(groupId));
    }

    @Override
    public ICompletableFuture<CPGroup> getCPGroup(String name) {
        return invocationManager.invoke(getMetadataGroupId(), new GetActiveRaftGroupByNameOp(name));
    }

    @Override
    public ICompletableFuture<Void> restart() {
        final SimpleCompletableFuture<Void> future = newCompletableFuture();
        ClusterService clusterService = nodeEngine.getClusterService();
        final Collection<Member> members = clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR);

        if (!clusterService.isMaster()) {
            future.setResult(new IllegalStateException("Only master can restart CP subsystem!"));
            return future;
        }

        if (config.getCPMemberCount() > members.size() + 1) {
            future.setResult(new IllegalStateException("Not enough cluster members to restart CP subsystem. "
                    + "Required: " + config.getCPMemberCount() + ", available: " + (members.size() + 1)));
            return future;
        }

        ExecutionCallback<Void> callback = new ExecutionCallback<Void>() {
            final AtomicInteger latch = new AtomicInteger(members.size());
            volatile Throwable failure;

            @Override
            public void onResponse(Void response) {
                if (latch.decrementAndGet() == 0) {
                    future.setResult(failure != null ? new ExecutionException(failure) : response);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                failure = t;
                if (latch.decrementAndGet() == 0) {
                    future.setResult(new ExecutionException(t));
                }
            }
        };

        long seed = newSeed();
        logger.warning("Restarting CP subsystem with groupId seed: " + seed);
        InternalOperationService operationService = nodeEngine.getOperationService();
        for (Member member : members) {
            InternalCompletableFuture<Void> f =
                    operationService.invokeOnTarget(SERVICE_NAME, new RestartCPMemberOp(seed), member.getAddress());
            f.andThen(callback);
        }

        restartLocal(seed);
        return future;
    }

    private long newSeed() {
        long currentSeed = metadataGroupManager.getGroupIdSeed();
        long seed = Clock.currentTimeMillis();
        while (seed <= currentSeed) {
            seed++;
        }
        return seed;
    }

    public void restartLocal(long seed) {
        if (seed == 0L) {
            throw new IllegalArgumentException("Seed cannot be zero!");
        }
        if (seed == metadataGroupManager.getGroupIdSeed()) {
            // we have already seen this seed
            logger.severe("Ignoring restart request. Current groupId seed is already equal to " + seed);
            return;
        }

        // we should clear the current raft state before resetting the metadata manager
        resetLocalRaftState();
        invocationManager.reset();
        metadataGroupManager.restart(seed);
        logger.info("CP state is reset with groupId seed: " + seed);
    }

    private void resetLocalRaftState() {
        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(RaftRemoteService.class)) {
            InternalProxyService proxyService = nodeEngine.getProxyService();
            for (String objectName : proxyService.getDistributedObjectNames(serviceInfo.getName())) {
                proxyService.destroyLocalDistributedObject(serviceInfo.getName(), objectName, false);
            }

            if (serviceInfo.getService() instanceof RaftManagedService) {
                ((RaftManagedService) serviceInfo.getService()).onCPSubsystemRestart();
            }
        }

        for (RaftNode node : nodes.values()) {
            node.forceSetTerminatedStatus();
        }

        destroyedGroupIds.addAll(nodes.keySet());
        nodes.clear();
    }

    @Override
    public ICompletableFuture<Void> promoteToCPMember() {
        if (metadataGroupManager.getLocalMember() != null) {
            SimpleCompletableFuture<Void> f = newCompletableFuture();
            f.setResult(null);
            return f;
        }

        MemberImpl localMember = nodeEngine.getLocalMember();
        // Local member may be recovered during restart, for instance via Hot Restart,
        // but Raft state cannot be recovered back.
        // That's why we generate a new UUID while promoting a member to CP.
        // This new UUID generation can be removed when Hot Restart allows to recover Raft state.
        final CPMemberInfo member = new CPMemberInfo(UuidUtil.newUnsecureUuidString(), localMember.getAddress());
        logger.info("Adding new CP member: " + member);
        ICompletableFuture<Void> future = invocationManager.invoke(getMetadataGroupId(), new AddCPMemberOp(member));

        future.andThen(new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                metadataGroupManager.initPromotedCPMember(member);
            }

            @Override
            public void onFailure(Throwable t) {
            }
        });
        return future;
    }

    private SimpleCompletableFuture<Void> newCompletableFuture() {
        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        return new SimpleCompletableFuture<Void>(executor, logger);
    }

    /**
     * this method is idempotent
     */
    @Override
    public ICompletableFuture<Void> removeCPMember(final String cpMemberUuid) {
        final ClusterService clusterService = nodeEngine.getClusterService();
        final SimpleCompletableFuture<Void> future = newCompletableFuture();

        if (!clusterService.isMaster()) {
            future.setResult(new IllegalStateException("Only master can remove a CP member!"));
            return future;
        }

        final ExecutionCallback<Void> removeMemberCallback = new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void response) {
                future.setResult(response);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof CannotRemoveCPMemberException) {
                    t = new IllegalStateException(t.getMessage());
                }

                future.setResult(new ExecutionException(t));
            }
        };

        invocationManager.<Collection<CPMember>>invoke(getMetadataGroupId(), new GetActiveCPMembersOp())
                .andThen(new ExecutionCallback<Collection<CPMember>>() {
            @Override
            public void onResponse(Collection<CPMember> cpMembers) {
                CPMemberInfo cpMemberToRemove = null;
                for (CPMember cpMember : cpMembers) {
                    if (cpMember.getUuid().equals(cpMemberUuid)) {
                        cpMemberToRemove = (CPMemberInfo) cpMember;
                        break;
                    }
                }

                if (cpMemberToRemove == null) {
                    future.setResult(new ExecutionException(new IllegalArgumentException("No CPMember found with uuid: "
                            + cpMemberUuid)));
                    return;
                } else if (clusterService.getMember(cpMemberToRemove.getAddress()) != null) {
                    future.setResult(new ExecutionException(new IllegalArgumentException("Cannot remove: " + cpMemberToRemove
                            + ", it is a live member!")));
                    return;
                }

                invokeTriggerRemoveMember(cpMemberToRemove).andThen(removeMemberCallback);
            }

            @Override
            public void onFailure(Throwable t) {
                future.setResult(new ExecutionException(t));
            }
        });

        return future;
    }

    /**
     * this method is idempotent
     */
    @Override
    public ICompletableFuture<Void> forceDestroyCPGroup(String groupName) {
        return invocationManager.invoke(getMetadataGroupId(), new ForceDestroyRaftGroupOp(groupName));
    }

    @Override
    public ICompletableFuture<Collection<CPMember>> getCPMembers() {
        return invocationManager.invoke(getMetadataGroupId(), new GetActiveCPMembersOp());
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        CPMemberInfo localMember = getLocalMember();
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

    private void ensureTriggerShutdown(CPMemberInfo member, long remainingTimeNanos) {
        while (remainingTimeNanos > 0) {
            long start = System.nanoTime();
            try {
                // mark us as shutting-down in metadata
                Future<Void> future = invokeTriggerRemoveMember(member);
                future.get(remainingTimeNanos, TimeUnit.NANOSECONDS);
                logger.fine(member + " is marked as being removed.");
                return;
            } catch (CannotRemoveCPMemberException e) {
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
        if (config.getMissingCPMemberAutoRemovalSeconds() == 0 || !metadataGroupManager.isDiscoveryCompleted()) {
            return;
        }

        Collection<CPMemberInfo> activeMembers = metadataGroupManager.getActiveMembers();
        missingMembers.keySet().retainAll(activeMembers);

        ClusterService clusterService = nodeEngine.getClusterService();
        for (CPMemberInfo cpMember : activeMembers) {
            if (clusterService.getMember(cpMember.getAddress()) == null) {
                if (missingMembers.putIfAbsent(cpMember, System.currentTimeMillis()) == null) {
                    logger.warning(cpMember + " is not present in the cluster. It will be auto-removed after "
                            + config.getMissingCPMemberAutoRemovalSeconds() + " seconds.");
                }
            } else if (missingMembers.remove(cpMember) != null) {
                logger.info(cpMember + " is removed from the missing members list as it is in the cluster.");
            }
        }
    }

    Collection<CPMemberInfo> getMissingMembers() {
        return Collections.unmodifiableSet(missingMembers.keySet());
    }

    public Collection<CPGroupId> getCPGroupIdsLocally() {
        return metadataGroupManager.getGroupIds();
    }

    public CPGroupInfo getCPGroupLocally(CPGroupId groupId) {
        return metadataGroupManager.getRaftGroup(groupId);
    }

    public MetadataRaftGroupManager getMetadataGroupManager() {
        return metadataGroupManager;
    }

    public RaftInvocationManager getInvocationManager() {
        return invocationManager;
    }

    public void handlePreVoteRequest(CPGroupId groupId, PreVoteRequest request, CPMember target) {
        if (!isTargetLocalMember(request, target)) {
            return;
        }
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + request);
            }
            return;
        }
        node.handlePreVoteRequest(request);
    }

    public void handlePreVoteResponse(CPGroupId groupId, PreVoteResponse response, CPMember target) {
        if (!isTargetLocalMember(response, target)) {
            return;
        }
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + response);
            }
            return;
        }
        node.handlePreVoteResponse(response);
    }

    public void handleVoteRequest(CPGroupId groupId, VoteRequest request, CPMember target) {
        if (!isTargetLocalMember(request, target)) {
            return;
        }
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + request);
            }
            return;
        }
        node.handleVoteRequest(request);
    }

    public void handleVoteResponse(CPGroupId groupId, VoteResponse response, CPMember target) {
        if (!isTargetLocalMember(response, target)) {
            return;
        }
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + response);
            }
            return;
        }
        node.handleVoteResponse(response);
    }

    public void handleAppendEntries(CPGroupId groupId, AppendRequest request, CPMember target) {
        if (!isTargetLocalMember(request, target)) {
            return;
        }
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + request);
            }
            return;
        }
        node.handleAppendRequest(request);
    }

    public void handleAppendResponse(CPGroupId groupId, AppendSuccessResponse response, CPMember target) {
        if (!isTargetLocalMember(response, target)) {
            return;
        }
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + response);
            }
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleAppendResponse(CPGroupId groupId, AppendFailureResponse response, CPMember target) {
        if (!isTargetLocalMember(response, target)) {
            return;
        }
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + response);
            }
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleSnapshot(CPGroupId groupId, InstallSnapshot request, CPMember target) {
        if (!isTargetLocalMember(request, target)) {
            return;
        }
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + request);
            }
            return;
        }
        node.handleInstallSnapshot(request);
    }

    private boolean isTargetLocalMember(Object request, CPMember target) {
        if (!target.equals(metadataGroupManager.getLocalMember())) {
            if (logger.isFineEnabled()) {
                logger.fine("Won't handle " + request + ". We are not the expected target: " + target);
            }
            return false;
        }
        return true;
    }

    public Collection<RaftNode> getAllRaftNodes() {
        return new ArrayList<RaftNode>(nodes.values());
    }

    public RaftNode getRaftNode(CPGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftNode getOrInitRaftNode(CPGroupId groupId) {
        RaftNode node = nodes.get(groupId);
        if (node == null && !destroyedGroupIds.contains(groupId)) {
            logger.fine("RaftNode[" + groupId + "] does not exist. Asking to the METADATA CP group...");
            nodeEngine.getExecutionService().execute(ASYNC_EXECUTOR, new InitializeRaftNodeTask(groupId));
        }
        return node;
    }

    public boolean isRaftGroupDestroyed(CPGroupId groupId) {
        return destroyedGroupIds.contains(groupId);
    }

    public CPSubsystemConfig getConfig() {
        return config;
    }

    public CPMemberInfo getLocalMember() {
        return metadataGroupManager.getLocalMember();
    }

    public void createRaftNode(CPGroupId groupId, Collection<CPMemberInfo> members) {
        if (nodes.containsKey(groupId)) {
            return;
        }

        if (destroyedGroupIds.contains(groupId)) {
            logger.warning("Not creating RaftNode[" + groupId + "] since the CP group is already destroyed");
            return;
        }

        RaftIntegration integration = new NodeEngineRaftIntegration(nodeEngine, groupId);
        RaftAlgorithmConfig raftAlgorithmConfig = config.getRaftAlgorithmConfig();
        RaftNodeImpl node = new RaftNodeImpl(groupId, getLocalMember(), (Collection) members, raftAlgorithmConfig, integration);

        if (nodes.putIfAbsent(groupId, node) == null) {
            if (destroyedGroupIds.contains(groupId)) {
                node.forceSetTerminatedStatus();
                logger.warning("Not creating RaftNode[" + groupId + "] since the CP group is already destroyed");
                return;
            }

            node.start();
            logger.info("RaftNode[" + groupId + "] is created with " + members);
        }
    }

    public void destroyRaftNode(CPGroupId groupId) {
        destroyedGroupIds.add(groupId);
        RaftNode node = nodes.remove(groupId);
        if (node != null) {
            node.forceSetTerminatedStatus();
            if (logger.isFineEnabled()) {
                logger.fine("Local RaftNode[" + groupId + "] is destroyed.");
            }
        }
    }

    public RaftGroupId createRaftGroupForProxy(String name) {
        String groupName = getGroupNameForProxy(name);
        checkFalse(groupName.equalsIgnoreCase(METADATA_CP_GROUP_NAME), "CP data structures cannot run on the METADATA CP group!");

        try {
            RaftGroupId groupId = getGroupIdForProxy(name);
            if (groupId != null) {
                return groupId;
            }

            return invocationManager.createRaftGroup(groupName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Could not create CP group: " + groupName);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Could not create CP group: " + groupName);
        }
    }

    public RaftGroupId getGroupIdForProxy(String name) {
        String groupName = getGroupNameForProxy(name);
        RaftOp op = new GetActiveRaftGroupByNameOp(groupName);
        CPGroupInfo group = invocationManager.<CPGroupInfo>invoke(getMetadataGroupId(), op).join();
        return group != null ? group.id() : null;
    }

    private ICompletableFuture<Void> invokeTriggerRemoveMember(CPMemberInfo member) {
        return invocationManager.invoke(getMetadataGroupId(), new TriggerRemoveCPMemberOp(member));
    }

    private boolean isRemoved(CPMemberInfo member) {
        RaftOp op = new GetActiveCPMembersOp();
        InternalCompletableFuture<List<CPMemberInfo>> f = invocationManager.query(getMetadataGroupId(), op, LEADER_LOCAL);
        List<CPMemberInfo> members = f.join();
        return !members.contains(member);
    }

    public static String withoutDefaultGroupName(String name) {
        name = name.trim();
        int i = name.indexOf("@");
        if (i == -1) {
            return name;
        }

        checkTrue(name.indexOf("@", i + 1) == -1, "Custom group name must be specified at most once");
        String groupName = name.substring(i + 1);
        if (groupName.equals(DEFAULT_GROUP_NAME)) {
            return name.substring(0, i);
        }

        return name;
    }

    public static String getGroupNameForProxy(String name) {
        int i = name.indexOf("@");
        if (i == -1) {
            return DEFAULT_GROUP_NAME;
        }

        checkTrue(i < (name.length() - 1), "Custom CP group name cannot be empty string");
        checkTrue(name.indexOf("@", i + 1) == -1, "Custom group name must be specified at most once");
        String groupName = name.substring(i + 1).trim();
        checkTrue(groupName.length() > 0, "Custom CP group name cannot be empty string");
        return groupName;
    }

    public static String getObjectNameForProxy(String name) {
        int i = name.indexOf("@");
        if (i == -1) {
            return name;
        }

        checkTrue(i < (name.length() - 1), "Object name cannot be empty string");
        checkTrue(name.indexOf("@", i + 1) == -1,
                "Custom CP group name must be specified at most once");
        String objectName = name.substring(0, i).trim();
        checkTrue(objectName.length() > 0, "Object name cannot be empty string");
        return objectName;
    }

    public CPGroupId getMetadataGroupId() {
        return metadataGroupManager.getMetadataGroupId();
    }

    private class InitializeRaftNodeTask implements Runnable {
        private final CPGroupId groupId;

        InitializeRaftNodeTask(CPGroupId groupId) {
            this.groupId = groupId;
        }

        @Override
        public void run() {
            queryInitialMembersFromMetadataRaftGroup();
        }

        @SuppressWarnings("unchecked")
        private void queryInitialMembersFromMetadataRaftGroup() {
            RaftOp op = new GetRaftGroupOp(groupId);
            ICompletableFuture<CPGroupInfo> f = invocationManager.query(getMetadataGroupId(), op, LEADER_LOCAL);
            f.andThen(new ExecutionCallback<CPGroupInfo>() {
                @Override
                public void onResponse(CPGroupInfo group) {
                    if (group != null) {
                        if (group.memberImpls().contains(getLocalMember())) {
                            createRaftNode(groupId, (Collection) group.initialMembers());
                        } else {
                            // I can be the member that is just added to the raft group...
                            queryInitialMembersFromTargetRaftGroup();
                        }
                    } else if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the METADATA CP group");
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    RuntimeException cause = peel(t);
                    if (cause instanceof CPGroupDestroyedException) {
                        CPGroupId destroyedGroupId = ((CPGroupDestroyedException) cause).getGroupId();
                        destroyedGroupIds.add(destroyedGroupId);
                    }

                    if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the METADATA CP group", t);
                    }
                }
            });
        }

        void queryInitialMembersFromTargetRaftGroup() {
            CPMemberInfo localMember = getLocalMember();
            if (localMember == null) {
                return;
            }

            RaftOp op = new GetInitialRaftGroupMembersIfCurrentGroupMemberOp(localMember);
            ICompletableFuture<Collection<CPMemberInfo>> f = invocationManager.query(groupId, op, LEADER_LOCAL);
            f.andThen(new ExecutionCallback<Collection<CPMemberInfo>>() {
                @Override
                public void onResponse(Collection<CPMemberInfo> initialMembers) {
                    createRaftNode(groupId, initialMembers);
                }

                @Override
                public void onFailure(Throwable t) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the CP group itself", t);
                    }
                }
            });
        }
    }

    private class AutoRemoveMissingCPMemberTask implements Runnable {
        @Override
        public void run() {
            try {
                if (!nodeEngine.getClusterService().isMaster() || missingMembers.isEmpty()
                        || metadataGroupManager.getMembershipChangeContext() != null) {
                    return;
                }

                for (Entry<CPMemberInfo, Long> e : missingMembers.entrySet()) {
                    long missingTimeSeconds = MILLISECONDS.toSeconds(System.currentTimeMillis() - e.getValue());
                    if (missingTimeSeconds >= config.getMissingCPMemberAutoRemovalSeconds()) {
                        CPMemberInfo missingMember = e.getKey();
                        logger.warning("Removing " + missingMember + " since it is absent for " + missingTimeSeconds
                                + " seconds.");

                        removeCPMember(missingMember.getUuid()).get();

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
