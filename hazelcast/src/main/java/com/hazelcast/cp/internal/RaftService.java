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

package com.hazelcast.cp.internal;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.event.CPGroupAvailabilityEvent;
import com.hazelcast.cp.event.CPGroupAvailabilityListener;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.cp.event.impl.CPGroupAvailabilityEventImpl;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.operation.ResetCPMemberOp;
import com.hazelcast.cp.internal.operation.unsafe.UnsafeStateReplicationOp;
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.RaftRole;
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
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.diagnostics.MetricsPlugin;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.GracefulShutdownAwareService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.List;
import java.util.Map;
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
import java.util.function.BiConsumer;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.RaftGroupMembershipManager.MANAGEMENT_TASK_PERIOD_IN_MILLIS;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeImpl.newRaftNode;
import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_DISCRIMINATOR_GROUPID;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_SERVICE_DESTROYED_GROUP_IDS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_SERVICE_MISSING_MEMBERS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_SERVICE_NODES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_METRIC_RAFT_SERVICE_TERMINATED_RAFT_NODE_GROUP_IDS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_PREFIX_RAFT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_PREFIX_RAFT_GROUP;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_PREFIX_RAFT_METADATA;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CP_TAG_NAME;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.SYSTEM_EXECUTOR;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains {@link RaftNode} instances that run the Raft consensus algorithm
 * for the created CP groups. Also implements CP Subsystem management methods.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class RaftService implements ManagedService, SnapshotAwareService<MetadataRaftGroupSnapshot>, GracefulShutdownAwareService,
                                    MembershipAwareService, PreJoinAwareService, RaftNodeLifecycleAwareService,
                                    MigrationAwareService, DynamicMetricsProvider,
                                    EventPublishingService<Object, EventListener> {

    public static final String SERVICE_NAME = "hz:core:raft";

    public static final String EVENT_TOPIC_MEMBERSHIP = "membership";
    public static final String EVENT_TOPIC_AVAILABILITY = "availability";
    public static final String CP_SUBSYSTEM_EXECUTOR = "hz:cpSubsystem";
    static final String CP_SUBSYSTEM_MANAGEMENT_EXECUTOR = "hz:cpSubsystemManagement";
    private static final long REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS = 1;
    private static final int AWAIT_DISCOVERY_STEP_MILLIS = 10;
    private static final long AVAILABILITY_EVENTS_DEDUPLICATION_PERIOD = TimeUnit.MINUTES.toMillis(1);

    private final ReadWriteLock nodeLock = new ReentrantReadWriteLock();
    @Probe(name = CP_METRIC_RAFT_SERVICE_NODES)
    private final ConcurrentMap<CPGroupId, RaftNode> nodes = new ConcurrentHashMap<>();
    private final ConcurrentMap<CPGroupId, RaftNodeMetrics> nodeMetrics = new ConcurrentHashMap<>();
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    @Probe(name = CP_METRIC_RAFT_SERVICE_DESTROYED_GROUP_IDS)
    private final Set<CPGroupId> destroyedGroupIds = newSetFromMap(new ConcurrentHashMap<>());
    @Probe(name = CP_METRIC_RAFT_SERVICE_TERMINATED_RAFT_NODE_GROUP_IDS)
    private final Set<CPGroupId> terminatedRaftNodeGroupIds = newSetFromMap(new ConcurrentHashMap<>());
    private final CPSubsystemConfig config;
    private final RaftInvocationManager invocationManager;
    private final MetadataRaftGroupManager metadataGroupManager;
    @Probe(name = CP_METRIC_RAFT_SERVICE_MISSING_MEMBERS)
    private final ConcurrentMap<CPMemberInfo, Long> missingMembers = new ConcurrentHashMap<>();
    private final int metricsPeriod;
    private final boolean cpSubsystemEnabled;
    private final UnsafeModePartitionState[] unsafeModeStates;
    private final Map<CPGroupAvailabilityEventKey, Long> recentAvailabilityEvents = new ConcurrentHashMap<>();

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        CPSubsystemConfig cpSubsystemConfig = nodeEngine.getConfig().getCPSubsystemConfig();
        this.config = cpSubsystemConfig != null ? new CPSubsystemConfig(cpSubsystemConfig) : new CPSubsystemConfig();
        checkCPSubsystemConfig(config);
        this.cpSubsystemEnabled = config.getCPMemberCount() > 0;
        this.invocationManager = new RaftInvocationManager(nodeEngine, this);
        this.metadataGroupManager = new MetadataRaftGroupManager(this.nodeEngine, this, config);

        if (cpSubsystemEnabled) {
            this.unsafeModeStates = null;
        } else {
            this.unsafeModeStates = new UnsafeModePartitionState[nodeEngine.getPartitionService().getPartitionCount()];
            for (int i = 0; i < unsafeModeStates.length; i++) {
                unsafeModeStates[i] = new UnsafeModePartitionState();
            }
        }

        MetricsRegistry metricsRegistry = this.nodeEngine.getMetricsRegistry();
        metricsRegistry.registerStaticMetrics(this, CP_PREFIX_RAFT);
        metricsRegistry.registerStaticMetrics(metadataGroupManager, CP_PREFIX_RAFT_METADATA);
        metricsRegistry.registerDynamicMetricsProvider(this);
        this.metricsPeriod = nodeEngine.getProperties().getInteger(MetricsPlugin.PERIOD_SECONDS);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        if (!metadataGroupManager.init()) {
            return;
        }

        if (config.getMissingCPMemberAutoRemovalSeconds() > 0) {
            ExecutionService executionService = nodeEngine.getExecutionService();
            executionService.scheduleWithRepetition(CP_SUBSYSTEM_MANAGEMENT_EXECUTOR, new AutoRemoveMissingCPMemberTask(),
                    REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS, REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS, SECONDS);
        }

        MetricsRegistry metricsRegistry = this.nodeEngine.getMetricsRegistry();
        metricsRegistry.scheduleAtFixedRate(new PublishNodeMetricsTask(), metricsPeriod, SECONDS, ProbeLevel.INFO);
    }

    @Override
    public void reset() {
        missingMembers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        if (getCPPersistenceService().isEnabled()) {
            List<Future> futures = new ArrayList<>(nodes.size());
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

    public InternalCompletableFuture<Collection<CPGroupId>> getAllCPGroupIds() {
        return invocationManager.query(getMetadataGroupId(), new GetRaftGroupIdsOp(), LINEARIZABLE);
    }

    public InternalCompletableFuture<Collection<CPGroupId>> getCPGroupIds() {
        return invocationManager.query(getMetadataGroupId(), new GetActiveRaftGroupIdsOp(), LINEARIZABLE);
    }

    public InternalCompletableFuture<CPGroup> getCPGroup(CPGroupId groupId) {
        return invocationManager.query(getMetadataGroupId(), new GetRaftGroupOp(groupId), LINEARIZABLE);
    }

    public InternalCompletableFuture<CPGroup> getCPGroup(String name) {
        return invocationManager.query(getMetadataGroupId(), new GetActiveRaftGroupByNameOp(name), LINEARIZABLE);
    }

    InternalCompletableFuture<Void> resetCPSubsystem() {
        checkState(cpSubsystemEnabled, "CP Subsystem is not enabled!");

        InternalCompletableFuture<Void> future = newCompletableFuture();
        ClusterService clusterService = nodeEngine.getClusterService();
        Collection<Member> members = clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR);

        if (!clusterService.isMaster()) {
            return complete(future, new IllegalStateException("Only master can reset CP Subsystem!"));
        }

        if (config.getCPMemberCount() > members.size() + 1) {
            return complete(future, new IllegalStateException("Not enough cluster members to reset CP Subsystem! "
                    + "Required: " + config.getCPMemberCount() + ", available: " + (members.size() + 1)));
        }

        BiConsumer<Void, Throwable> callback = new BiConsumer<Void, Throwable>() {
            final AtomicInteger latch = new AtomicInteger(members.size());
            volatile Throwable failure;

            @Override
            public void accept(Void aVoid, Throwable throwable) {
                if (throwable == null) {
                    if (latch.decrementAndGet() == 0) {
                        if (failure == null) {
                            future.complete(null);
                        } else {
                            complete(future, failure);
                        }
                    }
                } else {
                    failure = throwable;
                    if (latch.decrementAndGet() == 0) {
                        complete(future, throwable);
                    }
                }
            }
        };

        long seed = newSeed();
        logger.warning("Resetting CP Subsystem with groupId seed: " + seed);
        resetLocal(seed);

        OperationServiceImpl operationService = nodeEngine.getOperationService();
        for (Member member : members) {
            Operation op = new ResetCPMemberOp(seed);
            operationService.<Void>invokeOnTarget(SERVICE_NAME, op, member.getAddress()).whenCompleteAsync(callback);
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

    public void resetLocal(long seed) {
        if (seed == 0L) {
            throw new IllegalArgumentException("Seed cannot be zero!");
        }
        if (seed == metadataGroupManager.getGroupIdSeed()) {
            // we have already seen this seed
            logger.severe("Ignoring reset request. Current groupId seed is already equal to " + seed);
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

        List<InternalCompletableFuture> futures = new ArrayList<>(nodes.size());
        destroyedGroupIds.addAll(nodes.keySet());
        for (RaftNode node : nodes.values()) {
            InternalCompletableFuture f = node.forceSetTerminatedStatus();
            futures.add(f);
        }

        for (InternalCompletableFuture future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                logger.warning(e);
            }
        }

        nodes.clear();

        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(RaftRemoteService.class)) {
            if (serviceInfo.getService() instanceof RaftManagedService) {
                ((RaftManagedService) serviceInfo.getService()).onCPSubsystemRestart();
            }
        }

        nodeMetrics.clear();
        missingMembers.clear();
        invocationManager.reset();
    }

    public InternalCompletableFuture<Void> promoteToCPMember() {
        InternalCompletableFuture<Void> future = newCompletableFuture();

        if (!metadataGroupManager.isDiscoveryCompleted()) {
            return complete(future, new IllegalStateException("CP Subsystem discovery is not completed yet!"));
        }

        if (nodeEngine.getLocalMember().isLiteMember()) {
            return complete(future, new IllegalStateException("Lite members cannot be promoted to CP member!"));
        }

        if (getLocalCPMember() != null) {
            future.complete(null);
            return future;
        }

        MemberImpl localMember = nodeEngine.getLocalMember();
        // Local member may be recovered during restart, for instance via Hot Restart,
        // but Raft state cannot be recovered back.
        // That's why we generate a new UUID while promoting a member to CP.
        // This new UUID generation can be removed when Hot Restart allows to recover Raft state.
        CPMemberInfo member = new CPMemberInfo(newUnsecureUUID(), localMember.getAddress());
        logger.info("Adding new CP member: " + member);

        invocationManager.invoke(getMetadataGroupId(), new AddCPMemberOp(member))
                         .whenCompleteAsync((response, t) -> {
                             if (t == null) {
                                 metadataGroupManager.initPromotedCPMember(member);
                                 future.complete(null);
                             } else {
                                 complete(future, t);
                             }
                         });
        return future;
    }

    private <T> InternalCompletableFuture<T> newCompletableFuture() {
        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        return InternalCompletableFuture.withExecutor(executor);
    }

    public InternalCompletableFuture<Void> removeCPMember(UUID cpMemberUuid) {
        ClusterService clusterService = nodeEngine.getClusterService();
        InternalCompletableFuture<Void> future = newCompletableFuture();

        BiConsumer<Void, Throwable> removeMemberCallback = (response, t) -> {
            if (t == null) {
                future.complete(null);
            } else {
                if (t instanceof CannotRemoveCPMemberException) {
                    t = new IllegalStateException(t.getMessage());
                }
                complete(future, t);
            }
        };

        invocationManager.<Collection<CPMember>>invoke(getMetadataGroupId(), new GetActiveCPMembersOp())
                .whenCompleteAsync((cpMembers, t) -> {
            if (t == null) {
                CPMemberInfo cpMemberToRemove = null;
                for (CPMember cpMember : cpMembers) {
                    if (cpMember.getUuid().equals(cpMemberUuid)) {
                        cpMemberToRemove = (CPMemberInfo) cpMember;
                        break;
                    }
                }
                if (cpMemberToRemove == null) {
                    complete(future, new IllegalArgumentException("No CPMember found with uuid: " + cpMemberUuid));
                    return;
                } else {
                    Member member = clusterService.getMember(cpMemberToRemove.getAddress());
                    if (member != null) {
                        logger.warning("Only unreachable/crashed CP members should be removed. " + member + " is alive but "
                                + cpMemberToRemove + " with the same address is being removed.");
                    }
                }
                invokeTriggerRemoveMember(cpMemberToRemove).whenCompleteAsync(removeMemberCallback);
            } else {
                complete(future, t);
            }
        });

        return future;
    }

    /**
     * this method is idempotent
     */
    public InternalCompletableFuture<Void> forceDestroyCPGroup(String groupName) {
        return invocationManager.invoke(getMetadataGroupId(), new ForceDestroyRaftGroupOp(groupName));
    }

    public InternalCompletableFuture<Collection<CPMember>> getCPMembers() {
        return invocationManager.query(getMetadataGroupId(), new GetActiveCPMembersOp(), LINEARIZABLE);
    }

    public boolean isDiscoveryCompleted() {
        return metadataGroupManager.isDiscoveryCompleted();
    }

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
            long startNanos = Timer.nanos();
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
                remainingTimeNanos -= Timer.nanosElapsed(startNanos);
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
        if (!cpSubsystemEnabled) {
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
        publishGroupAvailabilityEvents(event.getMember());
        updateMissingMembers();
    }

    private void publishGroupAvailabilityEvents(MemberImpl removedMember) {
        ClusterService clusterService = nodeEngine.getClusterService();
        if (clusterService.getClusterVersion().isUnknownOrLessThan(Versions.V4_1)) {
            return;
        }

        // since only the Metadata CP group members keep the CP groups,
        // they will be the ones that keep track of unreachable CP members.
        for (CPGroupId groupId : metadataGroupManager.getActiveGroupIds()) {
            CPGroupSummary group = metadataGroupManager.getGroup(groupId);
            Collection<CPMember> missing = new ArrayList<>();
            boolean availabilityDecreased = false;
            for (CPMember member : group.members()) {
                if (member.getAddress().equals(removedMember.getAddress())) {
                    // Group's availability decreased because of this removed member
                    availabilityDecreased = true;
                    missing.add(member);
                } else if (clusterService.getMember(member.getAddress()) == null) {
                    missing.add(member);
                }
            }

            if (availabilityDecreased) {
                CPGroupAvailabilityEvent e = new CPGroupAvailabilityEventImpl(group.id(), group.members(), missing);
                nodeEngine.getEventService().publishEvent(SERVICE_NAME, EVENT_TOPIC_AVAILABILITY, e,
                        EVENT_TOPIC_AVAILABILITY.hashCode());
            }
        }
    }

    void updateMissingMembers() {
        if (config.getMissingCPMemberAutoRemovalSeconds() == 0 || !metadataGroupManager.isDiscoveryCompleted()
                || (!isStartCompleted() && getCPPersistenceService().getCPMetadataStore().containsLocalMemberFile())) {
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
        return new ArrayList<>(nodes.values());
    }

    public RaftNode getRaftNode(CPGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftNode getOrInitRaftNode(CPGroupId groupId) {
        RaftNode node = nodes.get(groupId);
        if (node == null && isStartCompleted() && isDiscoveryCompleted() && !destroyedGroupIds.contains(groupId)
                && !terminatedRaftNodeGroupIds.contains(groupId)) {
            logger.fine("RaftNode[" + groupId + "] does not exist. Asking to the METADATA CP group...");
            nodeEngine.getExecutionService().execute(CP_SUBSYSTEM_EXECUTOR, new InitializeRaftNodeTask(groupId));
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
        /*
         * WARNING:
         * This method is acquiring a lock.
         * Make sure that you don't call this method from a partition thread.
         */

        assert !(Thread.currentThread() instanceof PartitionOperationThread)
                : "Cannot create RaftNode of " + groupId + " in a partition thread!";

        if (nodes.containsKey(groupId) || !isStartCompleted() || !hasSameSeed(groupId)) {
            return;
        }

        if (getLocalCPMember() == null) {
            logger.warning("Not creating Raft node for " + groupId + " because local CP member is not initialized yet.");
            return;
        }

        nodeLock.readLock().lock();
        try {
            if (destroyedGroupIds.contains(groupId)) {
                logger.warning("Not creating RaftNode[" + groupId + "] since the CP group is already destroyed.");
                return;
            } else if (terminatedRaftNodeGroupIds.contains(groupId)) {
                if (!nodeEngine.isRunning()) {
                    logger.fine("Not creating RaftNode[" + groupId + "] since the local CP member is already terminated.");
                    return;
                }
            }

            int partitionId = getCPGroupPartitionId(groupId);
            RaftIntegration integration = new NodeEngineRaftIntegration(nodeEngine, groupId, localCPMember, partitionId);
            RaftAlgorithmConfig raftAlgorithmConfig = config.getRaftAlgorithmConfig();
            CPPersistenceService persistenceService = getCPPersistenceService();
            RaftStateStore stateStore = persistenceService.createRaftStateStore((RaftGroupId) groupId, null);
            RaftNodeImpl node = newRaftNode(groupId, localCPMember, members, raftAlgorithmConfig, integration, stateStore);

            if (nodes.putIfAbsent(groupId, node) == null) {
                if (destroyedGroupIds.contains(groupId)) {
                    nodes.remove(groupId, node);
                    removeNodeMetrics(groupId);
                    logger.warning("Not creating RaftNode[" + groupId + "] since the CP group is already destroyed.");
                    return;
                }

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
        RaftIntegration integration = new NodeEngineRaftIntegration(nodeEngine, groupId, restoredState.localEndpoint(),
                partitionId);
        RaftAlgorithmConfig raftAlgorithmConfig = config.getRaftAlgorithmConfig();
        RaftStateStore stateStore = getCPPersistenceService().createRaftStateStore(groupId, logFileStructure);
        RaftNodeImpl node = RaftNodeImpl.restoreRaftNode(groupId, restoredState, raftAlgorithmConfig, integration, stateStore);

        // no need to lock here...
        RaftNode prev = nodes.putIfAbsent(groupId, node);
        checkState(prev == null, "Could not restore " + groupId + " because its Raft node already exists!");

        node.start();
        logger.info("RaftNode[" + groupId + "] is restored.");
        return node;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor rootDescriptor = descriptor.withPrefix(CP_PREFIX_RAFT_GROUP);
        for (Entry<CPGroupId, RaftNodeMetrics> entry : nodeMetrics.entrySet()) {
            CPGroupId groupId = entry.getKey();
            RaftRole role = entry.getValue().role;
            MetricDescriptor groupDescriptor = rootDescriptor
                    .copy()
                    .withDiscriminator(CP_DISCRIMINATOR_GROUPID, String.valueOf(groupId.getId()))
                    .withTag(CP_TAG_NAME, groupId.getName())
                    .withTag("role", role != null ? role.toString() : "NONE");
            context.collect(groupDescriptor, entry.getValue());
        }
    }

    private void removeNodeMetrics(CPGroupId groupId) {
        nodeMetrics.remove(groupId);
    }

    private boolean hasSameSeed(CPGroupId groupId) {
        return getMetadataGroupId().getSeed() == ((RaftGroupId) groupId).getSeed();
    }

    public boolean updateInvocationManagerMembers(long groupIdSeed, long membersCommitIndex,
            Collection<? extends CPMember> members) {
        return invocationManager.getRaftInvocationContext().setMembers(groupIdSeed, membersCommitIndex, members);
    }

    public void terminateRaftNode(CPGroupId groupId, boolean groupDestroyed) {
        if (destroyedGroupIds.contains(groupId) || !hasSameSeed(groupId)) {
            return;
        }

        assert !(Thread.currentThread() instanceof PartitionOperationThread)
                : "Cannot terminate RaftNode of " + groupId + " in a partition thread!";

        nodeLock.readLock().lock();
        try {
            if (destroyedGroupIds.contains(groupId)) {
                return;
            }

            if (groupDestroyed) {
                destroyedGroupIds.add(groupId);
            }

            terminatedRaftNodeGroupIds.add(groupId);
            RaftNode node = nodes.get(groupId);
            CPPersistenceService persistenceService = getCPPersistenceService();
            if (node != null) {
                destroyRaftNode(node, groupDestroyed);
                logger.info("RaftNode[" + groupId + "] is destroyed.");
            } else if (groupDestroyed && persistenceService.isEnabled()) {
                persistenceService.removeRaftStateStore((RaftGroupId) groupId);
                logger.info("RaftStateStore of RaftNode[" + groupId + "] is deleted.");
            }
        } finally {
            nodeLock.readLock().unlock();
        }
    }

    public void stepDownRaftNode(CPGroupId groupId) {
        if (terminatedRaftNodeGroupIds.contains(groupId) || !hasSameSeed(groupId)) {
            return;
        }

        assert !(Thread.currentThread() instanceof PartitionOperationThread)
                : "Cannot step down RaftNode of " + groupId + " in a partition thread!";

        nodeLock.readLock().lock();
        try {
            if (terminatedRaftNodeGroupIds.contains(groupId)) {
                return;
            }

            CPPersistenceService persistenceService = getCPPersistenceService();
            RaftNode node = nodes.get(groupId);
            if (node != null && node.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
                terminatedRaftNodeGroupIds.add(groupId);
                destroyRaftNode(node, true);
                logger.fine("RaftNode[" + groupId + "] has stepped down.");
            } else if (node == null && persistenceService.isEnabled()) {
                persistenceService.removeRaftStateStore((RaftGroupId) groupId);
                logger.info("RaftStateStore of RaftNode[" + groupId + "] is deleted.");
            }
        } finally {
            nodeLock.readLock().unlock();
        }
    }

    private void destroyRaftNode(RaftNode node, boolean removeRaftStateStore) {
        RaftGroupId groupId = (RaftGroupId) node.getGroupId();
        node.forceSetTerminatedStatus().whenCompleteAsync((v, t) -> {
            nodes.remove(groupId, node);
            removeNodeMetrics(groupId);
            CPPersistenceService persistenceService = getCPPersistenceService();
            try {
                if (removeRaftStateStore && persistenceService.isEnabled()) {
                    persistenceService.removeRaftStateStore(groupId);
                    logger.info("RaftStateStore of RaftNode[" + groupId + "] is deleted.");
                }
            } catch (Exception e) {
                logger.severe("Deletion of RaftStateStore of RaftNode[" + groupId + "] failed.", e);
            }
        });
    }

    public RaftGroupId createRaftGroupForProxy(String name) {
        String groupName = getGroupNameForProxy(name);
        if (cpSubsystemEnabled) {
            try {
                CPGroupSummary group = getGroupSummaryForProxy(groupName).joinInternal();
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
        } else {
            return createPartitionBasedRaftGroupId(name, groupName);
        }
    }

    private RaftGroupId createPartitionBasedRaftGroupId(String name, String groupName) {
        if (DEFAULT_GROUP_NAME.equals(groupName)) {
            // In unsafe mode, if there's no group specified,
            // we will use proxy name as group name.
            groupName = name;
        }
        Data key = nodeEngine.getSerializationService().toData(groupName);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        return new RaftGroupId(groupName, 0, partitionId);
    }

    public InternalCompletableFuture<CPGroupId> createRaftGroupForProxyAsync(String name) {
        String groupName = getGroupNameForProxy(name);
        if (cpSubsystemEnabled) {
            InternalCompletableFuture<CPGroupId> future = newCompletableFuture();
            InternalCompletableFuture<CPGroupSummary> groupIdFuture = getGroupSummaryForProxy(groupName);
            groupIdFuture.whenCompleteAsync((response, throwable) -> {
                if (throwable == null) {
                    if (response != null) {
                        future.complete(response.id());
                    } else {
                        invocationManager.createRaftGroup(groupName).whenCompleteAsync((r, t) -> {
                            complete(future, r, t);
                        });
                    }
                } else {
                    complete(future, throwable);
                }
            });
            return future;
        } else {
            return newCompletedFuture(createPartitionBasedRaftGroupId(name, groupName));
        }
    }

    private InternalCompletableFuture<CPGroupSummary> getGroupSummaryForProxy(String groupName) {
        return invocationManager.query(getMetadataGroupId(), new GetActiveRaftGroupByNameOp(groupName), LINEARIZABLE);
    }

    private InternalCompletableFuture<Void> invokeTriggerRemoveMember(CPMemberInfo member) {
        return invocationManager.invoke(getMetadataGroupId(), new RemoveCPMemberOp(member));
    }

    private static <T> InternalCompletableFuture<T> complete(InternalCompletableFuture<T> future, Throwable t) {
        future.completeExceptionally(t);
        return future;
    }

    private static <T> void complete(InternalCompletableFuture<T> future,
                                                      T value,
                                                      Throwable t) {
        if (t == null) {
            future.complete(value);
        } else {
            future.completeExceptionally(t);
        }
    }

    public static String withoutDefaultGroupName(String name) {
        name = name.trim();
        int i = name.indexOf("@");
        if (i == -1) {
            return name;
        }

        checkTrue(name.indexOf("@", i + 1) == -1, "Custom group name must be specified at most once");
        String groupName = name.substring(i + 1).trim();
        if (equalsIgnoreCase(groupName, DEFAULT_GROUP_NAME)) {
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
        checkFalse(equalsIgnoreCase(groupName, METADATA_CP_GROUP_NAME),
                "CP data structures cannot run on the METADATA CP group!");
        return equalsIgnoreCase(groupName, DEFAULT_GROUP_NAME) ? DEFAULT_GROUP_NAME : groupName;
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

    public boolean isCpSubsystemEnabled() {
        return cpSubsystemEnabled;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    public void handleActiveCPMembers(RaftGroupId receivedMetadataGroupId, long membersCommitIndex,
                                      Collection<CPMemberInfo> members) {
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

        if (updateInvocationManagerMembers(receivedMetadataGroupId.getSeed(), membersCommitIndex, members)) {
            if (logger.isFineEnabled()) {
                logger.fine("Handled new active CP members list: " + members + ", members commit index: " + membersCommitIndex
                        + ", METADATA group id seed: " + receivedMetadataGroupId.getSeed());
            }
        }

        RaftGroupId metadataGroupId = getMetadataGroupId();
        if (receivedMetadataGroupId.getSeed() < metadataGroupId.getSeed() || metadataGroupId.equals(receivedMetadataGroupId)) {
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
                members = new ArrayList<>(members);
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
    public void onRaftNodeTerminated(CPGroupId groupId) {
        nodeEngine.getExecutionService().execute(CP_SUBSYSTEM_EXECUTOR, () -> terminateRaftNode(groupId, false));
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
        nodeEngine.getExecutionService().execute(CP_SUBSYSTEM_EXECUTOR, () -> stepDownRaftNode(groupId));
    }

    public Collection<CPGroupId> getLeadedGroups() {
        Collection<CPGroupId> groupIds = new ArrayList<>();
        RaftEndpoint localEndpoint = getLocalCPEndpoint();
        for (RaftNode raftNode : nodes.values()) {
            if (CPGroup.METADATA_CP_GROUP_NAME.equals(raftNode.getGroupId().getName())) {
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

    public InternalCompletableFuture transferLeadership(CPGroupId groupId, CPMemberInfo destination) {
        RaftNode raftNode = getRaftNode(groupId);
        if (raftNode == null) {
            throw new IllegalStateException("RaftNode does not exist for group: " + groupId);
        }
        return raftNode.transferLeadership(destination.toRaftEndpoint());
    }

    public int getCPGroupPartitionId(CPGroupId groupId) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return getCPGroupPartitionId(groupId, partitionCount);
    }

    public static int getCPGroupPartitionId(CPGroupId groupId, int partitionCount) {
        assert groupId.getId() >= 0 : "Invalid groupId: " + groupId;
        return (int) (groupId.getId() % partitionCount);
    }

    public long nextUnsafeModeCommitIndex(CPGroupId groupId) {
        assert !cpSubsystemEnabled;
        int partitionId = getCPGroupPartitionId(groupId);
        UnsafeModePartitionState unsafeModeState = unsafeModeStates[partitionId];
        return unsafeModeState.nextCommitIndex();
    }

    public void registerUnsafeWaitingOperation(CPGroupId groupId, long commitIndex, Operation op) {
        assert !cpSubsystemEnabled;
        int partitionId = getCPGroupPartitionId(groupId);
        UnsafeModePartitionState unsafeModeState = unsafeModeStates[partitionId];
        if (!unsafeModeState.registerWaitingOp(commitIndex, op)) {
            throw new IllegalArgumentException("Cannot register " + op + " with index " + commitIndex);
        }
    }

    /**
     * Completes all futures registered with {@code indices}
     * in the CP group associated with {@code groupId}.
     *
     * @return {@code true} if the CP group exists, {@code false} otherwise.
     */
    public boolean completeFutures(CPGroupId groupId, Collection<Long> indices, Object result) {
        if (cpSubsystemEnabled) {
            RaftNodeImpl raftNode = (RaftNodeImpl) getRaftNode(groupId);
            if (raftNode == null) {
                return false;
            }

            for (Long index : indices) {
                raftNode.completeFuture(index, result);
            }
        } else {
            int partitionId = getCPGroupPartitionId(groupId);
            UnsafeModePartitionState unsafeModeState = unsafeModeStates[partitionId];
            for (Long index : indices) {
                Operation op = unsafeModeState.removeWaitingOp(index);
                sendOperationResponse(op, result);
            }
        }
        return true;
    }

    /**
     * Completes all futures registered with {@code indices}
     * in the CP group associated with {@code groupId}.
     *
     * @return {@code true} if the CP group exists, {@code false} otherwise.
     */
    public boolean completeFutures(CPGroupId groupId, Collection<Entry<Long, Object>> results) {
        if (cpSubsystemEnabled) {
            RaftNodeImpl raftNode = (RaftNodeImpl) getRaftNode(groupId);
            if (raftNode == null) {
                return false;
            }

            for (Entry<Long, Object> result : results) {
                raftNode.completeFuture(result.getKey(), result.getValue());

            }
        } else {
            int partitionId = getCPGroupPartitionId(groupId);
            UnsafeModePartitionState unsafeModeState = unsafeModeStates[partitionId];
            for (Entry<Long, Object> result : results) {
                Operation op = unsafeModeState.removeWaitingOp(result.getKey());
                sendOperationResponse(op, result.getValue());
            }
        }
        return true;
    }

    private void sendOperationResponse(Operation op, Object result) {
        if (op != null) {
            try {
                op.sendResponse(result);
            } catch (ResponseAlreadySentException e) {
                op.logError(e);
            }
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (cpSubsystemEnabled) {
            return null;
        }
        if (event.getReplicaIndex() > getBackupCount()) {
            return null;
        }
        UnsafeModePartitionState state = unsafeModeStates[event.getPartitionId()];
        return state.commitIndex() == 0 ? null : new UnsafeStateReplicationOp(state);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (cpSubsystemEnabled) {
            return;
        }

        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            UnsafeModePartitionState state = unsafeModeStates[event.getPartitionId()];
            if (event.getCurrentReplicaIndex() == 0) {
                // Waiting operations are registered only on primary.
                Object ex = new PartitionMigratingException("Partition " + event.getPartitionId() + " is migrating!");
                for (Operation op : state.getWaitingOps()) {
                    op.sendResponse(ex);
                }
            }
            int thresholdReplicaIndex = event.getNewReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > getBackupCount()) {
                state.reset();
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (cpSubsystemEnabled) {
            return;
        }
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            int thresholdReplicaIndex = event.getCurrentReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > getBackupCount()) {
                unsafeModeStates[event.getPartitionId()].reset();
            }
        }
    }

    private int getBackupCount() {
        return 1;
    }

    public void applyUnsafeModeState(int partitionId, UnsafeModePartitionState state) {
        assert !cpSubsystemEnabled;
        unsafeModeStates[partitionId].apply(state);
    }

    public UUID registerMembershipListener(CPMembershipListener listener) {
        return nodeEngine.getEventService().registerListener(SERVICE_NAME, EVENT_TOPIC_MEMBERSHIP, listener).getId();
    }

    public boolean removeMembershipListener(UUID id) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, EVENT_TOPIC_MEMBERSHIP, id);
    }

    public UUID registerAvailabilityListener(CPGroupAvailabilityListener listener) {
        return nodeEngine.getEventService().registerListener(SERVICE_NAME, EVENT_TOPIC_AVAILABILITY, listener).getId();
    }

    public boolean removeAvailabilityListener(UUID id) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, EVENT_TOPIC_AVAILABILITY, id);
    }

    @Override
    public void dispatchEvent(Object e, EventListener l) {
        long now = Clock.currentTimeMillis();
        recentAvailabilityEvents.values().removeIf(expirationTime -> expirationTime < now);

        if (e instanceof CPMembershipEvent) {
            CPMembershipEvent event = (CPMembershipEvent) e;
            CPMembershipListener listener = (CPMembershipListener) l;
            switch (event.getType()) {
                case ADDED:
                    listener.memberAdded(event);
                    break;
                case REMOVED:
                    listener.memberRemoved(event);
                    break;
                default:
                    throw new IllegalArgumentException("Unhandled event: " + event);
            }
            return;
        }
        if (e instanceof CPGroupAvailabilityEvent) {
            CPGroupAvailabilityEvent event = (CPGroupAvailabilityEvent) e;
            if (recentAvailabilityEvents.putIfAbsent(new CPGroupAvailabilityEventKey(event, l),
                    now + AVAILABILITY_EVENTS_DEDUPLICATION_PERIOD) != null) {
                return;
            }
            CPGroupAvailabilityListener listener = (CPGroupAvailabilityListener) l;
            if (event.isMajorityAvailable()) {
                listener.availabilityDecreased(event);
            } else {
                listener.majorityLost(event);
            }
            return;
        }
        throw new IllegalArgumentException("Unhandled event: " + e);
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
            InternalCompletableFuture<CPGroupSummary> f = invocationManager.query(getMetadataGroupId(), op, LEADER_LOCAL);
            f.whenCompleteAsync((group, throwable) -> {
                if (throwable == null) {
                    if (group != null) {
                        if (group.members().contains(getLocalCPMember())) {
                            createRaftNode(groupId, group.initialMembers());
                        } else {
                            // I can be the member that is just added to the raft group...
                            queryInitialMembersFromTargetRaftGroup();
                        }
                    } else if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the METADATA CP group");
                    }
                } else {
                    if (throwable instanceof CPGroupDestroyedException) {
                        CPGroupId destroyedGroupId = ((CPGroupDestroyedException) throwable).getGroupId();
                        terminateRaftNode(destroyedGroupId, true);
                    }

                    if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the METADATA CP group", throwable);
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
            InternalCompletableFuture<Collection<RaftEndpoint>> f = invocationManager.query(groupId, op, LEADER_LOCAL);
            f.whenCompleteAsync((initialMembers, t) -> {
                if (t == null) {
                    createRaftNode(groupId, initialMembers);
                } else {
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

    private class PublishNodeMetricsTask implements Runnable {
        @Override
        public void run() {
            for (RaftNode node : nodes.values()) {
                final RaftNodeImpl raftNode = (RaftNodeImpl) node;

                raftNode.execute(() -> {
                    RaftState state = raftNode.state();
                    RaftLog log = state.log();
                    RaftNodeMetrics metrics = new RaftNodeMetrics(state.role(), state.memberCount(), state.term(),
                            state.commitIndex(), state.lastApplied(), log.lastLogOrSnapshotTerm(), log.snapshotIndex(),
                            log.lastLogOrSnapshotIndex(), log.availableCapacity());
                    nodeMetrics.put(node.getGroupId(), metrics);
                });
            }
        }
    }
}
