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
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.cp.internal.raftop.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.AddCPMemberOp;
import com.hazelcast.cp.internal.raftop.metadata.ForceDestroyRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupByNameOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupIdsOp;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.RaftServicePreJoinOp;
import com.hazelcast.cp.internal.raftop.metadata.RemoveCPMemberOp;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.diagnostics.MetricsPlugin;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.util.SimpleCompletableFuture;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.RaftGroupMembershipManager.MANAGEMENT_TASK_PERIOD_IN_MILLIS;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeImpl.newRaftNode;
import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains {@link RaftNode} instances that run the Raft consensus algorithm
 * for the created CP groups. Also implements CP Subsystem management methods.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class RaftService implements ManagedService, SnapshotAwareService<MetadataRaftGroupSnapshot>, GracefulShutdownAwareService,
                                    MembershipAwareService, CPSubsystemManagementService, PreJoinAwareService,
                                    RaftNodeLifecycleAwareService {

    public static final String SERVICE_NAME = "hz:core:raft";

    private static final long REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS = 1;
    private static final int AWAIT_DISCOVERY_STEP_MILLIS = 10;

    private final ReadWriteLock nodeLock = new ReentrantReadWriteLock();
    @Probe
    private final ConcurrentMap<CPGroupId, RaftNode> nodes = new ConcurrentHashMap<CPGroupId, RaftNode>();
    private final ConcurrentMap<CPGroupId, RaftNodeMetrics> nodeMetrics = new ConcurrentHashMap<CPGroupId, RaftNodeMetrics>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    @Probe
    private final Set<CPGroupId> destroyedGroupIds = newSetFromMap(new ConcurrentHashMap<CPGroupId, Boolean>());
    @Probe
    private final Set<CPGroupId> steppedDownGroupIds = newSetFromMap(new ConcurrentHashMap<CPGroupId, Boolean>());
    private final CPSubsystemConfig config;
    private final RaftInvocationManager invocationManager;
    private final MetadataRaftGroupManager metadataGroupManager;
    @Probe
    private final ConcurrentMap<CPMemberInfo, Long> missingMembers = new ConcurrentHashMap<CPMemberInfo, Long>();
    private final int metricsPeriod;

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        CPSubsystemConfig cpSubsystemConfig = nodeEngine.getConfig().getCPSubsystemConfig();
        this.config = cpSubsystemConfig != null ? new CPSubsystemConfig(cpSubsystemConfig) : new CPSubsystemConfig();
        checkCPSubsystemConfig(this.config);
        this.invocationManager = new RaftInvocationManager(nodeEngine, this);
        this.metadataGroupManager = new MetadataRaftGroupManager(this.nodeEngine, this, config);

        MetricsRegistry metricsRegistry = this.nodeEngine.getMetricsRegistry();
        metricsRegistry.scanAndRegister(this, "raft");
        metricsRegistry.scanAndRegister(metadataGroupManager, "raft.metadata");
        this.metricsPeriod = nodeEngine.getProperties().getInteger(MetricsPlugin.PERIOD_SECONDS);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        if (!metadataGroupManager.init()) {
            return;
        }

        if (config.getMissingCPMemberAutoRemovalSeconds() > 0) {
            nodeEngine.getExecutionService().scheduleWithRepetition(new AutoRemoveMissingCPMemberTask(),
                    REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS, REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS, SECONDS);
        }

        MetricsRegistry metricsRegistry = this.nodeEngine.getMetricsRegistry();
        metricsRegistry.scheduleAtFixedRate(new PublishNodeMetricsTask(), metricsPeriod, SECONDS, ProbeLevel.INFO);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        if (getCPPersistenceService().isEnabled()) {
            List<Future> futures = new ArrayList<Future>(nodes.size());
            for (RaftNode raftNode : nodes.values()) {
                futures.add(raftNode.forceSetTerminatedStatus());
            }
            for (Future future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    logger.severe("Error while terminating RaftNode", e);
                }
            }
        }
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
        checkState(config.getCPMemberCount() > 0, "CP Subsystem is not enabled!");

        final SimpleCompletableFuture<Void> future = newCompletableFuture();
        ClusterService clusterService = nodeEngine.getClusterService();
        final Collection<Member> members = clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR);

        if (!clusterService.isMaster()) {
            return complete(future, new IllegalStateException("Only master can restart CP Subsystem!"));
        }

        if (config.getCPMemberCount() > members.size() + 1) {
            return complete(future, new IllegalStateException("Not enough cluster members to restart CP Subsystem! "
                    + "Required: " + config.getCPMemberCount() + ", available: " + (members.size() + 1)));
        }

        ExecutionCallback<Void> callback = new ExecutionCallback<Void>() {
            final AtomicInteger latch = new AtomicInteger(members.size());
            volatile Throwable failure;

            @Override
            public void onResponse(Void response) {
                if (latch.decrementAndGet() == 0) {
                    if (failure == null) {
                        future.setResult(response);
                    } else {
                        complete(future, failure);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                failure = t;
                if (latch.decrementAndGet() == 0) {
                    complete(future, t);
                }
            }
        };

        long seed = newSeed();
        logger.warning("Resetting CP Subsystem with groupId seed: " + seed);
        restartLocal(seed);

        InternalOperationService operationService = nodeEngine.getOperationService();
        for (Member member : members) {
            Operation op = new RestartCPMemberOp(seed);
            operationService.<Void>invokeOnTarget(SERVICE_NAME, op, member.getAddress()).andThen(callback);
        }

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

        nodeLock.writeLock().lock();
        try {
            // we should clear the current raft state before resetting the metadata manager
            resetLocalRaftState();

            getCPPersistenceService().reset();
            metadataGroupManager.restart(seed);
            logger.info("Local CP state is reset with groupId seed: " + seed);
        } finally {
            nodeLock.writeLock().unlock();
        }
    }

    private void resetLocalRaftState() {
        // node.forceSetTerminatedStatus() will trigger RaftNodeLifecycleAwareService.onRaftGroupDestroyed()
        // which will attempt to acquire the read lock on nodeLock. In order to prevent it, we first
        // add group ids into destroyedGroupIds to short-cut RaftNodeLifecycleAwareService.onRaftGroupDestroyed()

        List<ICompletableFuture> futures = new ArrayList<ICompletableFuture>(nodes.size());
        destroyedGroupIds.addAll(nodes.keySet());
        for (RaftNode node : nodes.values()) {
            ICompletableFuture f = node.forceSetTerminatedStatus();
            futures.add(f);
        }

        nodes.clear();

        for (ICompletableFuture future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                logger.fine(e);
            }
        }

        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(RaftRemoteService.class)) {
            if (serviceInfo.getService() instanceof RaftManagedService) {
                ((RaftManagedService) serviceInfo.getService()).onCPSubsystemRestart();
            }
        }

        nodeMetrics.clear();
        missingMembers.clear();
        invocationManager.reset();
    }

    @Override
    public ICompletableFuture<Void> promoteToCPMember() {
        final SimpleCompletableFuture<Void> future = newCompletableFuture();

        if (!metadataGroupManager.isDiscoveryCompleted()) {
            return complete(future, new IllegalStateException("CP Subsystem discovery is not completed yet!"));
        }

        if (nodeEngine.getLocalMember().isLiteMember()) {
            return complete(future, new IllegalStateException("Lite members cannot be promoted to CP member!"));
        }

        if (getLocalCPMember() != null) {
            future.setResult(null);
            return future;
        }

        MemberImpl localMember = nodeEngine.getLocalMember();
        // Local member may be recovered during restart, for instance via Hot Restart,
        // but Raft state cannot be recovered back.
        // That's why we generate a new UUID while promoting a member to CP.
        // This new UUID generation can be removed when Hot Restart allows to recover Raft state.
        final CPMemberInfo member = new CPMemberInfo(UuidUtil.newUnsecureUUID(), localMember.getAddress());
        logger.info("Adding new CP member: " + member);

        invocationManager.invoke(getMetadataGroupId(), new AddCPMemberOp(member))
                         .andThen(new ExecutionCallback<Object>() {
                             @Override
                             public void onResponse(Object response) {
                                 metadataGroupManager.initPromotedCPMember(member);
                                 future.setResult(response);
                             }

                             @Override
                             public void onFailure(Throwable t) {
                                 complete(future, t);
                             }
                         });
        return future;
    }

    private <T> SimpleCompletableFuture<T> newCompletableFuture() {
        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        return new SimpleCompletableFuture<T>(executor, logger);
    }

    @Override
    public ICompletableFuture<Void> removeCPMember(final String cpMemberUuidString) {
        final UUID cpMemberUuid = UUID.fromString(cpMemberUuidString);
        final ClusterService clusterService = nodeEngine.getClusterService();
        final SimpleCompletableFuture<Void> future = newCompletableFuture();

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

                complete(future, t);
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
                    complete(future, new IllegalArgumentException("No CPMember found with uuid: " + cpMemberUuidString));
                    return;
                } else {
                    Member member = clusterService.getMember(cpMemberToRemove.getAddress());
                    if (member != null) {
                        logger.warning("Only unreachable/crashed CP members should be removed. " + member + " is alive but "
                                + cpMemberToRemove + " with the same address is being removed.");
                    }
                }
                invokeTriggerRemoveMember(cpMemberToRemove).andThen(removeMemberCallback);
            }

            @Override
            public void onFailure(Throwable t) {
                complete(future, t);
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
    public boolean isDiscoveryCompleted() {
        return metadataGroupManager.isDiscoveryCompleted();
    }

    @Override
    public boolean awaitUntilDiscoveryCompleted(long timeout, TimeUnit timeUnit) throws InterruptedException {
        long timeoutMillis = timeUnit.toMillis(timeout);
        while (timeoutMillis > 0 && !metadataGroupManager.isDiscoveryCompleted()) {
            long sleepMillis = Math.min(AWAIT_DISCOVERY_STEP_MILLIS, timeoutMillis);
            Thread.sleep(sleepMillis);
            timeoutMillis -= sleepMillis;
        }
        return metadataGroupManager.isDiscoveryCompleted();
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        CPMemberInfo localMember = getLocalCPMember();
        if (localMember == null) {
            return true;
        }

        if (getCPPersistenceService().isEnabled()) {
            // When persistence is enabled, we do not remove this member from CP Subsystem.
            // Because it is supposed to recover by restoring disk data.
            return true;
        }

        logger.fine("Triggering remove member procedure for " + localMember);

        if (ensureCPMemberRemoved(localMember, unit.toNanos(timeout))) {
            return true;
        }

        logger.fine("Remove member procedure NOT completed for " + localMember + " in " + unit.toMillis(timeout) + " ms.");
        return false;
    }

    private boolean ensureCPMemberRemoved(CPMemberInfo member, long remainingTimeNanos) {
        while (remainingTimeNanos > 0) {
            long start = System.nanoTime();
            try {
                if (metadataGroupManager.getActiveMembers().size() == 1) {
                    logger.warning("I am one of the last 2 CP members...");
                    return true;
                }

                invokeTriggerRemoveMember(member).get();
                logger.fine(member + " is marked as being removed.");
                break;
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof CannotRemoveCPMemberException)) {
                    throw ExceptionUtil.rethrow(e);
                }
                remainingTimeNanos -= (System.nanoTime() - start);
                if (remainingTimeNanos <= 0) {
                    throw new IllegalStateException(e.getMessage());
                }
                try {
                    Thread.sleep(MANAGEMENT_TASK_PERIOD_IN_MILLIS);
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        return true;
    }

    @Override
    public Operation getPreJoinOperation() {
        // RU_COMPAT_3_11
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_12)) {
            return null;
        }
        if (config.getCPMemberCount() == 0) {
            return null;
        }
        boolean master = nodeEngine.getClusterService().isMaster();
        boolean discoveryCompleted = metadataGroupManager.isDiscoveryCompleted();
        RaftGroupId metadataGroupId = metadataGroupManager.getMetadataGroupId();
        return master ? new RaftServicePreJoinOp(discoveryCompleted, metadataGroupId) : null;
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        metadataGroupManager.broadcastActiveCPMembers();
        updateMissingMembers();
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        updateMissingMembers();
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    void updateMissingMembers() {
        if (config.getMissingCPMemberAutoRemovalSeconds() == 0 || !metadataGroupManager.isDiscoveryCompleted()
                || !isStartCompleted()) {
            return;
        }

        // since only the Metadata CP group members keep the active CP member list,
        // they will be the ones that keep track of missing CP members.
        Collection<CPMemberInfo> activeMembers = metadataGroupManager.getActiveMembers();
        missingMembers.keySet().retainAll(activeMembers);

        ClusterService clusterService = nodeEngine.getClusterService();
        for (CPMemberInfo cpMember : activeMembers) {
            if (clusterService.getMember(cpMember.getAddress()) == null) {
                if (missingMembers.putIfAbsent(cpMember, Clock.currentTimeMillis()) == null) {
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

    public MetadataRaftGroupManager getMetadataGroupManager() {
        return metadataGroupManager;
    }

    public RaftInvocationManager getInvocationManager() {
        return invocationManager;
    }

    public void handlePreVoteRequest(CPGroupId groupId, PreVoteRequest request, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, request, target);
        if (node != null) {
            node.handlePreVoteRequest(request);
        }
    }

    public void handlePreVoteResponse(CPGroupId groupId, PreVoteResponse response, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, response, target);
        if (node != null) {
            node.handlePreVoteResponse(response);
        }
    }

    public void handleVoteRequest(CPGroupId groupId, VoteRequest request, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, request, target);
        if (node != null) {
            node.handleVoteRequest(request);
        }

    }

    public void handleVoteResponse(CPGroupId groupId, VoteResponse response, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, response, target);
        if (node != null) {
            node.handleVoteResponse(response);
        }

    }

    public void handleAppendEntries(CPGroupId groupId, AppendRequest request, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, request, target);
        if (node != null) {
            node.handleAppendRequest(request);
        }
    }

    public void handleAppendResponse(CPGroupId groupId, AppendSuccessResponse response, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, response, target);
        if (node != null) {
            node.handleAppendResponse(response);
        }
    }

    public void handleAppendResponse(CPGroupId groupId, AppendFailureResponse response, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, response, target);
        if (node != null) {
            node.handleAppendResponse(response);
        }
    }

    public void handleSnapshot(CPGroupId groupId, InstallSnapshot request, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, request, target);
        if (node != null) {
            node.handleInstallSnapshot(request);
        }
    }

    public void handleTriggerLeaderElection(CPGroupId groupId, TriggerLeaderElection request, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, request, target);
        if (node != null) {
            node.handleTriggerLeaderElection(request);
        }
    }

    public Collection<RaftNode> getAllRaftNodes() {
        return new ArrayList<RaftNode>(nodes.values());
    }

    public RaftNode getRaftNode(CPGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftNode getOrInitRaftNode(CPGroupId groupId) {
        RaftNode node = nodes.get(groupId);
        if (node == null && isStartCompleted() && isDiscoveryCompleted() && !isRaftGroupDestroyed(groupId)) {
            logger.fine("RaftNode[" + groupId + "] does not exist. Asking to the METADATA CP group...");
            nodeEngine.getExecutionService().execute(ASYNC_EXECUTOR, new InitializeRaftNodeTask(groupId));
        }
        return node;
    }

    private RaftNode getOrInitRaftNodeIfTargetLocalCPMember(CPGroupId groupId, Object message, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNode(groupId);
        if (node == null) {
            if (logger.isFineEnabled()) {
                logger.warning("RaftNode[" + groupId + "] does not exist to handle: " + message);
            }
            return null;
        }

        if (!target.equals(node.getLocalMember())) {
            if (logger.isFineEnabled()) {
                logger.warning("Won't handle " + message + ". We are not the expected target: " + target + ", local endpoint: "
                        + node.getLocalMember());
            }
            return null;
        }

        return node;
    }

    boolean isStartCompleted() {
        return nodeEngine.getNode().getNodeExtension().isStartCompleted();
    }

    public boolean isRaftGroupDestroyed(CPGroupId groupId) {
        return destroyedGroupIds.contains(groupId);
    }

    public CPSubsystemConfig getConfig() {
        return config;
    }

    @Override
    public CPMemberInfo getLocalCPMember() {
        return metadataGroupManager.getLocalCPMember();
    }

    public RaftEndpoint getLocalCPEndpoint() {
        CPMemberInfo localCPMember = getLocalCPMember();
        return localCPMember != null ? localCPMember.toRaftEndpoint() : null;
    }

    public void createRaftNode(CPGroupId groupId, Collection<RaftEndpoint> members) {
        createRaftNode(groupId, members, getLocalCPEndpoint());
    }

    void createRaftNode(CPGroupId groupId, Collection<RaftEndpoint> members, RaftEndpoint localCPMember) {
        if (nodes.containsKey(groupId) || !isStartCompleted() || !hasSameSeed(groupId)) {
            return;
        }

        nodeLock.readLock().lock();
        try {
            if (destroyedGroupIds.contains(groupId)) {
                logger.warning("Not creating RaftNode[" + groupId + "] since the CP group is already destroyed");
                return;
            } else if (steppedDownGroupIds.contains(groupId)) {
                if (!nodeEngine.isRunning()) {
                    logger.fine("Not creating RaftNode[" + groupId + "] since the local CP member is already stepped down");
                    return;
                }
                steppedDownGroupIds.remove(groupId);
            }

            int partitionId = getCPGroupPartitionId(groupId);
            RaftIntegration integration = new NodeEngineRaftIntegration(nodeEngine, groupId, localCPMember, partitionId);
            RaftAlgorithmConfig raftAlgorithmConfig = config.getRaftAlgorithmConfig();
            RaftStateStore stateStore = getCPPersistenceService().createRaftStateStore((RaftGroupId) groupId, null);
            RaftNodeImpl node = newRaftNode(groupId, localCPMember, members, raftAlgorithmConfig, integration, stateStore);

            if (nodes.putIfAbsent(groupId, node) == null) {
                if (destroyedGroupIds.contains(groupId)) {
                    destroyRaftNode(node);
                    logger.warning("Not creating RaftNode[" + groupId + "] since the CP group is already destroyed");
                    return;
                }

                registerNodeMetrics(node);
                node.start();
                logger.info("RaftNode[" + groupId + "] is created with " + members);
            }
        } finally {
            nodeLock.readLock().unlock();
        }
    }

    CPPersistenceService getCPPersistenceService() {
        return nodeEngine.getNode().getNodeExtension().getCPPersistenceService();
    }

    public RaftNodeImpl restoreRaftNode(RaftGroupId groupId, RestoredRaftState restoredState, LogFileStructure logFileStructure) {
        int partitionId = getCPGroupPartitionId(groupId);
        RaftIntegration integration = new NodeEngineRaftIntegration(nodeEngine, groupId,
                restoredState.localEndpoint(), partitionId);
        RaftAlgorithmConfig raftAlgorithmConfig = config.getRaftAlgorithmConfig();
        RaftStateStore stateStore = getCPPersistenceService().createRaftStateStore(groupId, logFileStructure);
        RaftNodeImpl node = RaftNodeImpl.restoreRaftNode(
                groupId, restoredState, raftAlgorithmConfig, integration, stateStore);

        // no need to lock here...
        RaftNode prev = nodes.putIfAbsent(groupId, node);
        checkState(prev == null, "Could not restore " + groupId + " because its Raft node already exists!");

        registerNodeMetrics(node);
        node.start();
        logger.info("RaftNode[" + groupId + "] is restored.");
        return node;
    }

    private void registerNodeMetrics(RaftNodeImpl node) {
        RaftNodeMetrics metrics = new RaftNodeMetrics();
        CPGroupId groupId = node.getGroupId();
        nodeMetrics.put(groupId, metrics);

        MetricsRegistry metricsRegistry = nodeEngine.getMetricsRegistry();
        metricsRegistry.scanAndRegister(metrics, "raft." + groupId.name() + "(" + groupId.id() + ")");
    }

    private void deregisterNodeMetrics(CPGroupId groupId) {
        RaftNodeMetrics metrics = nodeMetrics.remove(groupId);
        if (metrics != null) {
            MetricsRegistry metricsRegistry = nodeEngine.getMetricsRegistry();
            metricsRegistry.deregister(metrics);
        }
    }

    private boolean hasSameSeed(CPGroupId groupId) {
        return getMetadataGroupId().seed() == ((RaftGroupId) groupId).seed();
    }

    public boolean updateInvocationManagerMembers(long groupIdSeed, long membersCommitIndex, Collection<CPMemberInfo> members) {
        return invocationManager.getRaftInvocationContext().setMembers(groupIdSeed, membersCommitIndex, members);
    }

    public void destroyRaftNode(CPGroupId groupId) {
        if (destroyedGroupIds.contains(groupId) || !hasSameSeed(groupId)) {
            return;
        }

        nodeLock.readLock().lock();
        try {
            if (destroyedGroupIds.contains(groupId)) {
                return;
            }

            destroyedGroupIds.add(groupId);
            RaftNode node = nodes.remove(groupId);
            deregisterNodeMetrics(groupId);
            if (node != null) {
                destroyRaftNode(node);
                if (logger.isFineEnabled()) {
                    logger.fine("Local RaftNode[" + groupId + "] is destroyed.");
                }
            }
        } finally {
            nodeLock.readLock().unlock();
        }
    }

    private void destroyRaftNode(RaftNode node) {
        final RaftGroupId groupId = (RaftGroupId) node.getGroupId();
        node.forceSetTerminatedStatus().andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                getCPPersistenceService().removeRaftStateStore(groupId);
            }

            @Override
            public void onFailure(Throwable t) {
                getCPPersistenceService().removeRaftStateStore(groupId);
            }
        });
    }


    public void stepDownRaftNode(CPGroupId groupId) {
        if (steppedDownGroupIds.contains(groupId) || !hasSameSeed(groupId)) {
            return;
        }

        nodeLock.readLock().lock();
        try {
            if (steppedDownGroupIds.contains(groupId)) {
                return;
            }

            RaftNode node = nodes.get(groupId);
            if (node != null && node.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
                steppedDownGroupIds.add(groupId);
                nodes.remove(groupId, node);
                deregisterNodeMetrics(groupId);
            }
        } finally {
            nodeLock.readLock().unlock();
        }
    }

    public RaftGroupId createRaftGroupForProxy(String name) {
        String groupName = getGroupNameForProxy(name);
        try {
            CPGroupSummary group = getGroupSummaryForProxy(groupName).join();
            if (group != null) {
                return (RaftGroupId) group.id();
            }
            return invocationManager.createRaftGroup(groupName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Could not create CP group: " + groupName);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Could not create CP group: " + groupName);
        }
    }

    public InternalCompletableFuture<RaftGroupId> createRaftGroupForProxyAsync(String name) {
        final String groupName = getGroupNameForProxy(name);
        final SimpleCompletableFuture<RaftGroupId> future = newCompletableFuture();

        InternalCompletableFuture<CPGroupSummary> groupIdFuture = getGroupSummaryForProxy(groupName);
        groupIdFuture.andThen(new ExecutionCallback<CPGroupSummary>() {
            @Override
            public void onResponse(CPGroupSummary response) {
                if (response != null) {
                    future.setResult(response.id());
                } else {
                    invocationManager.createRaftGroup(groupName)
                                     .andThen(new ExecutionCallback<RaftGroupId>() {
                                         @Override
                                         public void onResponse(RaftGroupId response) {
                                             future.setResult(response);
                                         }

                                         @Override
                                         public void onFailure(Throwable t) {
                                             complete(future, t);
                                         }
                                     });
                }
            }

            @Override
            public void onFailure(Throwable t) {
                complete(future, t);
            }
        });
        return future;
    }

    private InternalCompletableFuture<CPGroupSummary> getGroupSummaryForProxy(String groupName) {
        return invocationManager.query(getMetadataGroupId(), new GetActiveRaftGroupByNameOp(groupName), LINEARIZABLE);
    }

    private ICompletableFuture<Void> invokeTriggerRemoveMember(CPMemberInfo member) {
        return invocationManager.invoke(getMetadataGroupId(), new RemoveCPMemberOp(member));
    }

    private <T> SimpleCompletableFuture<T> complete(SimpleCompletableFuture<T> future, Throwable t) {
        if (!(t instanceof ExecutionException)) {
            t = new ExecutionException(t);
        }

        future.setResult(t);
        return future;
    }

    public static String withoutDefaultGroupName(String name) {
        name = name.trim();
        int i = name.indexOf("@");
        if (i == -1) {
            return name;
        }

        checkTrue(name.indexOf("@", i + 1) == -1, "Custom group name must be specified at most once");
        String groupName = name.substring(i + 1).trim();
        if (groupName.equalsIgnoreCase(DEFAULT_GROUP_NAME)) {
            return name.substring(0, i);
        }

        return name;
    }

    public static String getGroupNameForProxy(String name) {
        name = name.trim();
        int i = name.indexOf("@");
        if (i == -1) {
            return DEFAULT_GROUP_NAME;
        }

        checkTrue(i < (name.length() - 1), "Custom CP group name cannot be empty string");
        checkTrue(name.indexOf("@", i + 1) == -1, "Custom group name must be specified at most once");
        String groupName = name.substring(i + 1).trim();
        checkTrue(groupName.length() > 0, "Custom CP group name cannot be empty string");
        checkFalse(groupName.equalsIgnoreCase(METADATA_CP_GROUP_NAME), "CP data structures cannot run on the METADATA CP group!");
        return groupName.equalsIgnoreCase(DEFAULT_GROUP_NAME) ? DEFAULT_GROUP_NAME : groupName;
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

    public RaftGroupId getMetadataGroupId() {
        return metadataGroupManager.getMetadataGroupId();
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    public void handleActiveCPMembers(RaftGroupId receivedMetadataGroupId, long membersCommitIndex,
                                      Collection<CPMemberInfo> members) {
        // TODO [basri] should we check anything related to isStartCompleted() ?
        if (!metadataGroupManager.isDiscoveryCompleted()) {
            if (logger.isFineEnabled()) {
                logger.fine("Ignoring received active CP members: " + members + " since discovery is in progress.");
            }
            return;
        }

        checkNotNull(members);
        checkTrue(members.size() > 0, "Active CP members list cannot be empty");
        if (members.size() == 1) {
            logger.fine("There is one active CP member left: " + members);
            return;
        }

        CPMemberInfo localMember = getLocalCPMember();
        members = replaceLocalMemberIfAddressChanged(membersCommitIndex, members, localMember);

        if (updateInvocationManagerMembers(receivedMetadataGroupId.seed(), membersCommitIndex, members)) {
            if (logger.isFineEnabled()) {
                logger.fine("Handled new active CP members list: " + members + ", members commit index: " + membersCommitIndex
                        + ", METADATA group id seed: " + receivedMetadataGroupId.seed());
            }
        }

        RaftGroupId metadataGroupId = getMetadataGroupId();
        if (receivedMetadataGroupId.seed() < metadataGroupId.seed() || metadataGroupId.equals(receivedMetadataGroupId)) {
            return;
        }

        if (!isStartCompleted()) {
            if (!metadataGroupId.equals(receivedMetadataGroupId)) {
                logger.severe("Restored METADATA groupId: " + metadataGroupId + " is different than received METADATA groupId: "
                        + receivedMetadataGroupId + ". There must have been a CP Subsystem reset while this member was down...");
            }

            return;
        }

        if (getRaftNode(receivedMetadataGroupId) != null) {
            if (logger.isFineEnabled()) {
                logger.fine(localMember + " is already part of METADATA group but received active CP members!");
            }

            return;
        }

        if (!receivedMetadataGroupId.equals(metadataGroupId) && getRaftNode(metadataGroupId) != null) {
            logger.warning(localMember + " was part of " + metadataGroupId + ", but received active CP members for "
                    + receivedMetadataGroupId + ".");
            return;
        }

        metadataGroupManager.handleMetadataGroupId(receivedMetadataGroupId);
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    private Collection<CPMemberInfo> replaceLocalMemberIfAddressChanged(long membersCommitIndex, Collection<CPMemberInfo> members,
                                                                        CPMemberInfo localMember) {
        if (localMember != null && !members.contains(localMember)) {
            // If I am present in the received CP member list with another address, I replace my local member.
            // In addition, I will remove any other member that has my address.
            CPMemberInfo otherMember = null;
            CPMemberInfo staleLocalMember = null;
            for (CPMemberInfo m : members) {
                if (m.getAddress().equals(localMember.getAddress()) && !m.getUuid().equals(localMember.getUuid())) {
                    otherMember = m;
                } else if (!m.getAddress().equals(localMember.getAddress()) && m.getUuid().equals(localMember.getUuid())) {
                    staleLocalMember = m;
                }
            }

            if (otherMember != null || staleLocalMember != null) {
                members = new ArrayList<CPMemberInfo>(members);
                members.remove(otherMember);
                members.remove(staleLocalMember);
                if (logger.isFineEnabled()) {
                    // prints null if there is no other member with the same address but it is ok in a debug log...
                    logger.fine("Removing other member: " + otherMember + " in received CP members list: " + members
                            + " and commit index: " + membersCommitIndex);
                }
            }

            if (staleLocalMember != null) {
                members.add(localMember);
                if (logger.isFineEnabled()) {
                    logger.fine("Replacing stale local member: " + staleLocalMember + " with: " + localMember
                            + " in received CP members list: " + members + " and commit index: " + membersCommitIndex);
                }
            } else if (nodeEngine.getNode().isRunning()) {
                boolean missingAutoRemovalEnabled = config.getMissingCPMemberAutoRemovalSeconds() > 0;
                logger.severe("Local " + localMember + " is not part of received active CP members: " + members
                        + ". It seems local member is removed from CP Subsystem. "
                        + "Auto removal of missing members is " + (missingAutoRemovalEnabled ? "enabled." : "disabled."));
            }
        }

        return members;
    }

    @Override
    public void onRaftGroupDestroyed(CPGroupId groupId) {
        destroyRaftNode(groupId);
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
        stepDownRaftNode(groupId);
    }

    public int getCPGroupPartitionId(CPGroupId groupId) {
        assert groupId.id() >= 0 : "Invalid groupId: " + groupId;
        return (int) (groupId.id() % nodeEngine.getPartitionService().getPartitionCount());
    }

    // TODO: rename!
    public Collection<CPGroupId> getLeadershipGroups() {
        Collection<CPGroupId> groupIds = new ArrayList<CPGroupId>();
        RaftEndpoint localEndpoint = getLocalCPEndpoint();
        for (RaftNode raftNode : nodes.values()) {
            if (CPGroup.METADATA_CP_GROUP_NAME.equals(raftNode.getGroupId().name())) {
                // Ignore metadata group
                continue;
            }
            RaftEndpoint leader = raftNode.getLeader();
            if (leader != null && leader.equals(localEndpoint)) {
                groupIds.add(raftNode.getGroupId());
            }
        }
        return groupIds;
    }

    public ICompletableFuture transferLeadership(CPGroupId groupId, CPMemberInfo to) {
        RaftNode raftNode = getRaftNode(groupId);
        if (raftNode == null) {
            throw new IllegalStateException("RaftNode does not exist for group: " + groupId);
        }
        return raftNode.transferLeadership(to.toRaftEndpoint());
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

        private void queryInitialMembersFromMetadataRaftGroup() {
            RaftOp op = new GetRaftGroupOp(groupId);
            ICompletableFuture<CPGroupSummary> f = invocationManager.query(getMetadataGroupId(), op, LEADER_LOCAL);
            f.andThen(new ExecutionCallback<CPGroupSummary>() {
                @Override
                public void onResponse(CPGroupSummary group) {
                    if (group != null) {
                        if (group.members().contains(getLocalCPMember())) {
                            createRaftNode((RaftGroupId) groupId, group.initialMembers());
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
                    if (t instanceof CPGroupDestroyedException) {
                        CPGroupId destroyedGroupId = ((CPGroupDestroyedException) t).getGroupId();
                        destroyedGroupIds.add(destroyedGroupId);
                    }

                    if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the METADATA CP group", t);
                    }
                }
            });
        }

        void queryInitialMembersFromTargetRaftGroup() {
            RaftEndpoint localEndpoint = getLocalCPEndpoint();
            if (localEndpoint == null) {
                return;
            }

            RaftOp op = new GetInitialRaftGroupMembersIfCurrentGroupMemberOp(localEndpoint);
            ICompletableFuture<Collection<RaftEndpoint>> f = invocationManager.query(groupId, op, LEADER_LOCAL);
            f.andThen(new ExecutionCallback<Collection<RaftEndpoint>>() {
                @Override
                public void onResponse(Collection<RaftEndpoint> initialMembers) {
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
                if (!metadataGroupManager.isMetadataGroupLeader() || metadataGroupManager.getMembershipChangeSchedule() != null) {
                    return;
                }

                for (Entry<CPMemberInfo, Long> e : missingMembers.entrySet()) {
                    long missingTimeSeconds = MILLISECONDS.toSeconds(System.currentTimeMillis() - e.getValue());
                    if (missingTimeSeconds >= config.getMissingCPMemberAutoRemovalSeconds()) {
                        CPMemberInfo missingMember = e.getKey();
                        logger.warning("Removing " + missingMember + " since it is absent for " + missingTimeSeconds
                                + " seconds.");

                        removeCPMember(missingMember.getUuid().toString()).get();

                        logger.info("Auto-removal of " + missingMember + " is successful.");

                        return;
                    }
                }
            } catch (Exception e) {
                logger.severe("RemoveMissingMembersTask failed", e);
            }
        }
    }

    private class PublishNodeMetricsTask implements Runnable {
        @Override
        public void run() {
            for (RaftNode node : nodes.values()) {
                final RaftNodeImpl raftNode = (RaftNodeImpl) node;
                final RaftNodeMetrics metrics = nodeMetrics.get(node.getGroupId());
                assert metrics != null;

                raftNode.execute(new Runnable() {
                    @Override
                    public void run() {
                        RaftState state = raftNode.state();
                        RaftLog log = state.log();
                        metrics.update(state.term(), state.commitIndex(), state.lastApplied(),
                                log.lastLogOrSnapshotTerm(), log.snapshotIndex(),
                                log.lastLogOrSnapshotIndex(), log.availableCapacity());
                    }
                });
            }
        }
    }
}
