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

package com.hazelcast.cp.internal;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPGroupsSnapshot;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.event.CPGroupAvailabilityEvent;
import com.hazelcast.cp.event.CPGroupAvailabilityListener;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.cp.event.impl.CPGroupAvailabilityEventImpl;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.operation.GetCPObjectInfosOp;
import com.hazelcast.cp.internal.operation.ResetCPMemberOp;
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
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
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
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.diagnostics.MetricsPlugin;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
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
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;

import javax.annotation.Nullable;
import java.util.AbstractMap;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cp.CPGroup.DEFAULT_GROUP_NAME;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.RaftGroupMembershipManager.MANAGEMENT_TASK_PERIOD_IN_MILLIS;
import static com.hazelcast.cp.internal.RaftServiceUtil.CP_AUTO_STEP_DOWN_WHEN_LEADER_ATTRIBUTE;
import static com.hazelcast.cp.internal.kubernetes.CPKubernetesUtil.isKubernetesContext;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeImpl.newRaftNode;
import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_DISCRIMINATOR_GROUPID;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_SERVICE_DESTROYED_GROUP_IDS;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_SERVICE_MISSING_MEMBERS;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_SERVICE_NODES;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_METRIC_RAFT_SERVICE_TERMINATED_RAFT_NODE_GROUP_IDS;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_PREFIX_RAFT;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_PREFIX_RAFT_GROUP;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_PREFIX_RAFT_METADATA;
import static com.hazelcast.internal.metrics.CPMetricDescriptorConstants.CP_TAG_NAME;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.SYSTEM_EXECUTOR;
import static java.lang.Boolean.parseBoolean;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains {@link RaftNode} instances that run the Raft consensus algorithm
 * for the created CP groups. Also implements CP Subsystem management methods.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class RaftService implements ManagedService, SnapshotAwareService<MetadataRaftGroupSnapshot>, GracefulShutdownAwareService,
        MembershipAwareService, PreJoinAwareService<RaftServicePreJoinOp>,
        RaftNodeLifecycleAwareService, DynamicMetricsProvider,
        EventPublishingService<Object, EventListener> {

    public static final String SERVICE_NAME = RaftServiceUtil.SERVICE_NAME;

    public static final String EVENT_TOPIC_MEMBERSHIP = "membership";
    public static final String EVENT_TOPIC_AVAILABILITY = "availability";
    public static final String CP_SUBSYSTEM_EXECUTOR = RaftServiceUtil.CP_SUBSYSTEM_EXECUTOR;
    static final String CP_SUBSYSTEM_MANAGEMENT_EXECUTOR = "hz:cpSubsystemManagement";
    private static final long REMOVE_MISSING_MEMBER_TASK_PERIOD_SECONDS = 1;
    private static final int AWAIT_DISCOVERY_STEP_MILLIS = 10;
    private static final long AVAILABILITY_EVENTS_DEDUPLICATION_PERIOD = TimeUnit.MINUTES.toMillis(1);
    private static final int TRY_COUNT = 10;

    protected final ConcurrentMap<CPGroupId, RaftNode> nodes = new ConcurrentHashMap<>();
    protected final Executor internalAsyncExecutor;
    protected final NodeEngineImpl nodeEngine;
    protected final ILogger logger;
    protected final CPSubsystemConfig config;
    protected final CPGroupViewTracker groupViewTracker;
    protected final RaftInvocationManager invocationManager;
    protected final MetadataRaftGroupManager metadataGroupManager;
    @Probe(name = CP_METRIC_RAFT_SERVICE_MISSING_MEMBERS)
    protected final ConcurrentMap<CPMemberInfo, Long> missingMembers = new ConcurrentHashMap<>();

    private final ReadWriteLock nodeLock = new ReentrantReadWriteLock();
    @Probe(name = CP_METRIC_RAFT_SERVICE_NODES)
    private final ConcurrentMap<CPGroupId, RaftNodeMetrics> nodeMetrics = new ConcurrentHashMap<>();
    @Probe(name = CP_METRIC_RAFT_SERVICE_DESTROYED_GROUP_IDS)
    private final Set<CPGroupId> destroyedGroupIds = ConcurrentHashMap.newKeySet();
    @Probe(name = CP_METRIC_RAFT_SERVICE_TERMINATED_RAFT_NODE_GROUP_IDS)
    private final Set<CPGroupId> terminatedRaftNodeGroupIds = ConcurrentHashMap.newKeySet();
    private final int metricsPeriod;
    private final boolean cpSubsystemEnabled;
    private final Map<CPGroupAvailabilityEventKey, Long> recentAvailabilityEvents = new ConcurrentHashMap<>();
    private final boolean kubernetesContext;

    @SuppressWarnings("ExecutableStatementCount")
    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        CPSubsystemConfig cpSubsystemConfig = nodeEngine.getConfig().getCPSubsystemConfig();
        this.config = cpSubsystemConfig != null ? new CPSubsystemConfig(cpSubsystemConfig) : new CPSubsystemConfig();
        checkCPSubsystemConfig(config);
        this.cpSubsystemEnabled = config.getCPMemberCount() > 0;
        this.invocationManager = createInvocationManager();
        this.metadataGroupManager = createMetadataGroupManager(config);
        this.internalAsyncExecutor = nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
        this.kubernetesContext = isKubernetesContext(nodeEngine.getConfig());
        this.groupViewTracker = new CPGroupViewTracker(nodeEngine, this);
        MetricsRegistry metricsRegistry = this.nodeEngine.getMetricsRegistry();
        metricsRegistry.registerStaticMetrics(this, CP_PREFIX_RAFT);
        metricsRegistry.registerStaticMetrics(metadataGroupManager, CP_PREFIX_RAFT_METADATA);
        metricsRegistry.registerDynamicMetricsProvider(this);
        this.metricsPeriod = nodeEngine.getProperties().getInteger(MetricsPlugin.PERIOD_SECONDS);
    }

    protected MetadataRaftGroupManager createMetadataGroupManager(CPSubsystemConfig config) {
        return new MetadataRaftGroupManager(this.nodeEngine, this, config,
                new RaftGroupMembershipManager(nodeEngine, this), new NoOpMembershipPolicy());
    }

    protected RaftInvocationManager createInvocationManager() {
        return new RaftInvocationManager(nodeEngine, this);
    }

    /**
     * Returns true if the given group belongs to an older CP-subsystem
     * generation than the local one, i.e. it was created before the CP
     * subsystem was reset to a new generation and is therefore no longer
     * valid. The comparison is based on the group id seed, which is bumped
     * whenever the CP subsystem is reset, so this check is stateless and
     * deterministic: it can be evaluated even after the local Raft state has
     * been wiped and before any Raft node for the group exists.
     * <p>
     * This differs from {@link #isRaftGroupDestroyed(CPGroupId)} in what it
     * models and how it is evaluated:
     * <ul>
     * <li>{@link #isRaftGroupDestroyed(CPGroupId)} reports that the group was
     * explicitly destroyed and reflects only what this member has learned
     * about it, whereas a stale group is identified purely by its seed and is
     * treated as invalid by every member of the new generation.</li>
     * <li>Both checks make the caller fail the operation with a
     * {@link com.hazelcast.cp.exception.CPGroupDestroyedException}, see
     * {@link com.hazelcast.cp.internal.operation.RaftReplicateOp} and
     * {@link com.hazelcast.cp.internal.operation.RaftQueryOp}. A group that
     * is neither stale nor known to be destroyed is treated as not-yet-
     * discovered instead of destroyed.</li>
     * </ul>
     */
    public boolean isStaleGroup(CPGroupId groupId) {
        return groupId instanceof RaftGroupId raftGroupId
                && raftGroupId.getSeed() < getMetadataGroupId().getSeed();
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

    public CompletableFuture<Collection<String>> getObjectNames(
            CPGroupId groupId,
            String serviceName,
            boolean returnTombstone) {

        RaftNode raftNode = getRaftNode(groupId);
        if (raftNode != null) {
            RaftEndpoint endpoint = raftNode.getLeader();
            if (endpoint == null) {
                throw new NotLeaderException(groupId, raftNode.getLocalMember(), null);
            }
            CPMember cpMember = invocationManager
                    .getRaftInvocationContext()
                    .getCPMember(endpoint.getUuid());

            GetCPObjectInfosOp op = new GetCPObjectInfosOp(groupId, serviceName, returnTombstone);
            return nodeEngine.getOperationService()
                    .createInvocationBuilder(null, op, cpMember.getAddress())
                    .setTryCount(TRY_COUNT)
                    .<Collection<String>>invoke()
                    .toCompletableFuture();
        } else {
            InternalCompletableFuture<CPGroup> cpGroup = getCPGroup(groupId);
            return cpGroup.thenApplyAsync(group -> {
                Collection<CPMember> members = group.members();
                for (CPMember cpMember : members) {
                    GetCPObjectInfosOp op = new GetCPObjectInfosOp(groupId, serviceName, returnTombstone);
                    try {
                        //noinspection unchecked
                        return (Collection<String>) nodeEngine.getOperationService()
                                .invokeOnTarget(null, op, cpMember.getAddress())
                                .get();
                    } catch (ExecutionException e) {
                        if (!(e.getCause() instanceof NotLeaderException)) {
                            throw new HazelcastException(e);
                        }
                    } catch (InterruptedException e) {
                        throw new HazelcastException(e);
                    }
                }
                throw new HazelcastException("Could not retrieve CPObjectInfos");
            }, nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR));
        }
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

        BiConsumer<Void, Throwable> callback = new BiConsumer<>() {
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
            operationService.<Void>invokeOnTarget(SERVICE_NAME, op, member.getAddress())
                    .whenCompleteAsync(callback, internalAsyncExecutor);
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
        groupViewTracker.reset();
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
        CPMemberInfo member = new CPMemberInfo(newUnsecureUUID(), localMember.getAddress(), autoStepDownForPromotedMember());
        logger.info("Adding new CP member: " + member);

        invocationManager.invoke(getMetadataGroupId(), new AddCPMemberOp(member))
                .whenCompleteAsync((response, t) -> {
                    if (t == null) {
                        metadataGroupManager.initPromotedCPMember(member);
                        future.complete(null);
                    } else {
                        complete(future, t);
                    }
                }, internalAsyncExecutor);
        return future;
    }

    protected boolean autoStepDownForPromotedMember() {
        return false;
    }

    private <T> InternalCompletableFuture<T> newCompletableFuture() {
        ManagedExecutorService executor = nodeEngine.getExecutionService().getExecutor(SYSTEM_EXECUTOR);
        return InternalCompletableFuture.withExecutor(executor);
    }

    public InternalCompletableFuture<Void> removeCPMember(UUID cpMemberUuid) {
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
                            Member member = getClusterMember(cpMemberToRemove);
                            if (member != null) {
                                logger.warning("Only unreachable/crashed CP members should be removed. "
                                        + member + " is alive but "
                                        + cpMemberToRemove + " with the same address is being removed.");
                            }
                            getGroupViewTracker().onCPMemberRemoved(cpMemberToRemove);
                        }
                        invokeTriggerRemoveMember(cpMemberToRemove)
                                .whenCompleteAsync(removeMemberCallback, internalAsyncExecutor);
                    } else {
                        complete(future, t);
                    }
                }, internalAsyncExecutor);
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

    protected boolean shouldSkipGracefulRemoval() {
        return false;
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        CPMemberInfo localMember = getLocalCPMember();
        if (localMember == null) {
            return true;
        }
        if (shouldSkipGracefulRemoval()) {
            return true;
        }
        logger.fine("Triggering remove member procedure for %s", localMember);
        publishGroupAvailabilityEventsForGracefulShutdown(nodeEngine.getLocalMember());
        if (ensureCPMemberRemoved(localMember, unit.toNanos(timeout))) {
            return true;
        }
        logger.fine("Remove member procedure NOT completed for %s in %s ms.", localMember, unit.toMillis(timeout));
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
    public RaftServicePreJoinOp getPreJoinOperation() {
        if (!cpSubsystemEnabled) {
            return null;
        }
        if (!nodeEngine.getClusterService().isMaster()) {
            return null;
        }
        boolean discoveryCompleted = metadataGroupManager.isDiscoveryCompleted();
        RaftGroupId metadataGroupId = metadataGroupManager.getMetadataGroupId();
        return createPreJoinOp(discoveryCompleted, metadataGroupId);
    }

    private RaftServicePreJoinOp createPreJoinOp(boolean discoveryCompleted, RaftGroupId metadataGroupId) {
        return new RaftServicePreJoinOp(discoveryCompleted, metadataGroupId,
                getGroupViewTracker().createSnapshotView(false));
    }

    public void receivePreJoinSnapshot(CPGroupsSnapshot snapshot) {
        getGroupViewTracker().receivePreJoinOp(snapshot);
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        metadataGroupManager.broadcastActiveCPMembers();
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        publishGroupAvailabilityEvents(event.getMember());
        addToMissingMembers(event.getMember());
    }


    private void publishGroupAvailabilityEvents(MemberImpl removedMember) {
        publishGroupAvailabilityEvents(removedMember, false);
    }

    private void publishGroupAvailabilityEventsForGracefulShutdown(MemberImpl removedMember) {
        publishGroupAvailabilityEvents(removedMember, true);
    }

    private void publishGroupAvailabilityEvents(MemberImpl removedMember, boolean isShutdown) {
        ClusterService clusterService = nodeEngine.getClusterService();
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
                CPGroupAvailabilityEvent e = new CPGroupAvailabilityEventImpl(group.id(), group.members(), missing, isShutdown);
                nodeEngine.getEventService().publishEvent(SERVICE_NAME, EVENT_TOPIC_AVAILABILITY, e,
                        EVENT_TOPIC_AVAILABILITY.hashCode());
            }
        }
    }

    // The node should be removed from missing CP members only if it has the same Address and CP UUID.
    public void removeFromMissingMembers(List<MemberInfo> membersInfo) {
        if (skipUpdateMissingMembers()) {
            return;
        }

        for (MemberInfo memberInfo : membersInfo) {
            if (memberInfo.getCPMemberUUID() != null) {
                boolean autoStepsDownWhenLeader =
                        memberInfo.getAttributes() != null
                                && parseBoolean(memberInfo.getAttributes()
                                .getOrDefault(CP_AUTO_STEP_DOWN_WHEN_LEADER_ATTRIBUTE, "false"));
                CPMemberInfo cpMember = new CPMemberInfo(memberInfo.getCPMemberUUID(), memberInfo.getAddress(),
                        autoStepsDownWhenLeader);
                if (missingMembers.remove(cpMember) != null) {
                    logger.info(cpMember
                            + " rejoins the CP Subsystem and will be not auto-removed.");
                }
            }
        }
    }

    void addToMissingMembers(MemberImpl member) {
        // since only the Metadata CP group members keep the active CP member list,
        // they will be the ones that keep track of missing CP members.
        Collection<CPMemberInfo> activeMembers = metadataGroupManager.getActiveMembers();
        updateMissingMembers(activeMembers, address -> member.getAddress().equals(address));
    }

    void updateMissingMembers(Collection<CPMemberInfo> activeMembers) {
        ClusterService clusterService = nodeEngine.getClusterService();
        updateMissingMembers(activeMembers, address -> clusterService.getMember(address) == null);
    }

    private void updateMissingMembers(Collection<CPMemberInfo> activeMembers, Predicate<Address> addressPredicate) {
        if (skipUpdateMissingMembers()) {
            return;
        }

        missingMembers.keySet().retainAll(activeMembers);

        for (CPMemberInfo cpMember : activeMembers) {
            if (addressPredicate.test(cpMember.getAddress())) {
                if (missingMembers.putIfAbsent(cpMember, Clock.currentTimeMillis()) == null) {
                    logger.warning(cpMember + " is not present in the cluster. It will be auto-removed from the "
                            + "CP Subsystem after " + config.getMissingCPMemberAutoRemovalSeconds() + " seconds.");
                }
            }
        }
    }

    boolean skipUpdateMissingMembers() {
        return config.getMissingCPMemberAutoRemovalSeconds() == 0 || config.isPersistenceEnabled()
                || !metadataGroupManager.isDiscoveryCompleted() || !isStartCompleted();
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

    public void handleSnapshotRequest(CPGroupId groupId, InstallSnapshotRequest request, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, request, target);
        if (node != null) {
            node.handleInstallSnapshotRequest(request);
        }
    }

    public void handleSnapshotResponse(CPGroupId groupId, InstallSnapshotResponse response, RaftEndpoint target) {
        RaftNode node = getOrInitRaftNodeIfTargetLocalCPMember(groupId, response, target);
        if (node != null) {
            node.handleInstallSnapshotResponse(response);
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
            logger.fine("RaftNode[%s] does not exist. Asking to the METADATA CP group...", groupId);
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

    public boolean isStartCompleted() {
        return nodeEngine.getNode().getNodeExtension().isStartCompleted();
    }

    public boolean isRaftGroupDestroyed(CPGroupId groupId) {
        return destroyedGroupIds.contains(groupId);
    }

    public boolean isRaftGroupDestroyedOrTerminated(CPGroupId groupId) {
        return destroyedGroupIds.contains(groupId) || terminatedRaftNodeGroupIds.contains(groupId);
    }

    public CPSubsystemConfig getConfig() {
        return config;
    }

    @Nullable
    public CPMemberInfo getLocalCPMember() {
        return metadataGroupManager.getLocalCPMember();
    }

    @Nullable
    public RaftEndpoint getLocalCPEndpoint() {
        CPMemberInfo localCPMember = getLocalCPMember();
        return localCPMember != null ? localCPMember.toRaftEndpoint() : null;
    }

    protected RaftIntegration createRaftIntegration(CPGroupId groupId, RaftEndpoint localCPMember, int partitionId) {
        return new NodeEngineRaftIntegration(nodeEngine, groupId, localCPMember, partitionId);
    }

    protected RaftNodeImpl buildRaftNode(CPGroupId groupId, RaftEndpoint localCPMember, Collection<RaftEndpoint> members,
                                         RaftAlgorithmConfig raftAlgorithmConfig, RaftIntegration integration,
                                         RaftStateStore stateStore) {
        return newRaftNode(groupId, localCPMember, members, raftAlgorithmConfig, integration, stateStore);
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
                    logger.fine("Not creating RaftNode[%s] since the local CP member is already terminated.", groupId);
                    return;
                }
            }

            int partitionId = getCPGroupPartitionId(groupId);
            RaftIntegration integration = createRaftIntegration(groupId, localCPMember, partitionId);
            RaftAlgorithmConfig raftAlgorithmConfig = config.getRaftAlgorithmConfig();
            CPPersistenceService persistenceService = getCPPersistenceService();
            RaftStateStore stateStore = persistenceService.createRaftStateStore((RaftGroupId) groupId, null);
            RaftNodeImpl node = buildRaftNode(groupId, localCPMember, members, raftAlgorithmConfig,
                    integration, stateStore);

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
                logger.fine("RaftNode[%s] has stepped down.", groupId);
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
            getGroupViewTracker().onCPGroupDestroyed(groupId);
            CPPersistenceService persistenceService = getCPPersistenceService();
            try {
                if (removeRaftStateStore && persistenceService.isEnabled()) {
                    persistenceService.removeRaftStateStore(groupId);
                    logger.info("RaftStateStore of RaftNode[" + groupId + "] is deleted.");
                }
            } catch (Exception e) {
                logger.severe("Deletion of RaftStateStore of RaftNode[" + groupId + "] failed.", e);
            }
        }, internalAsyncExecutor);
    }

    protected void onDestroyRaftNode(RaftGroupId groupId) {
    }

    public RaftGroupId createRaftGroupForProxy(String name) {
        if (!cpSubsystemEnabled) {
            throw new HazelcastException("CP Subsystem is not enabled!");
        }

        String groupName = getGroupNameForProxy(name);
        try {
            CPGroupSummary group = getGroupSummaryForProxy(groupName).joinInternal();
            if (group != null) {
                return (RaftGroupId) group.id();
            }
            return invocationManager.createRaftGroup(groupName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Could not create CP group: " + groupName, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Could not create CP group: " + groupName, e);
        }
    }

    public InternalCompletableFuture<CPGroupId> createRaftGroupForProxyAsync(String name) {
        if (!cpSubsystemEnabled) {
            throw new HazelcastException("CP Subsystem is not enabled!");
        }

        String groupName = getGroupNameForProxy(name);
        InternalCompletableFuture<CPGroupId> future = newCompletableFuture();
        InternalCompletableFuture<CPGroupSummary> groupIdFuture = getGroupSummaryForProxy(groupName);
        groupIdFuture.whenCompleteAsync((response, throwable) -> {
            if (throwable == null) {
                if (response != null) {
                    future.complete(response.id());
                } else {
                    invocationManager.createRaftGroup(groupName).whenCompleteAsync((r, t) ->
                            complete(future, r, t), internalAsyncExecutor);
                }
            } else {
                complete(future, throwable);
            }
        }, internalAsyncExecutor);
        return future;
    }

    private InternalCompletableFuture<CPGroupSummary> getGroupSummaryForProxy(String groupName) {
        return invocationManager.query(getMetadataGroupId(), new GetActiveRaftGroupByNameOp(groupName), LINEARIZABLE);
    }

    private InternalCompletableFuture<Void> invokeTriggerRemoveMember(CPMemberInfo member) {
        return invocationManager.invoke(getMetadataGroupId(), new RemoveCPMemberOp(member));
    }

    static <T> InternalCompletableFuture<T> complete(InternalCompletableFuture<T> future, Throwable t) {
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
        checkTrue(!groupName.isEmpty(), "Custom CP group name cannot be empty string");
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
        checkTrue(!objectName.isEmpty(), "Object name cannot be empty string");
        return objectName;
    }

    public RaftGroupId getMetadataGroupId() {
        return metadataGroupManager.getMetadataGroupId();
    }

    public boolean isCpSubsystemEnabled() {
        return cpSubsystemEnabled;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:nestedifdepth"})
    public void handleActiveCPMembers(RaftGroupId receivedMetadataGroupId, long membersCommitIndex,
                                      Collection<CPMemberInfo> members) {
        if (!metadataGroupManager.isDiscoveryCompleted()) {
            if (kubernetesContext && getCPPersistenceService().isEnabled()) {
                // Under k8s there can be a substantial delay in when pods are created - it's possible to have this semantics
                // outside k8s but for now we've restricted the following specifically to when operating within k8s.
                //
                // Reasoning.
                // =========
                // We have a bootstrap sequence when using persistence on a restart which broadcasts our new IP should
                // our IP have changed between our last execution. Under k8s this will almost always be the case. See
                // PublishLocalCPMemberOp which carries the CPMember with the updated IP.
                //
                // During the bootstrap sequence a member will broadcast its IP change until one of the following is true:
                //
                //   1. We managed to verify ourselves on the METADATA GP Group; or
                //   2. We timed out in the bootstrapping phase
                //
                // Let's discard (2) as that's a case that covers a much broader category of issue. For (1) the verification is
                // the successful invocation of VerifyRestartCPMemberOp. The successful invocation implies majority on the
                // METADATA CP Group. So, assuming a 3-member CP Group and a podManagementPolicy like OrderedReady you can have
                // the following on a cluster wide startup (post previous start-pause):
                //
                //  1. pod-0 (starts at time 1): starts broadcasting IP change; starts attempting to verify itself on
                //     METADATA CP Group
                //  2. pod-1 (starts at time 2): starts broadcasting IP change; starts attempting to verify itself on
                //     METADATA CP Group
                //  3. (at time 3) pod-0 + pod-1 form majority on METADATA; stop broadcasting their respective IP changes
                //  3. pod-2 (starts at time 4): starts broadcasting IP change; starts attempting to verify itself on
                //     METADATA CP Group, however it will not update the new IP coordinates as coordinated by METADATA leader as
                //     discovery is not complete.
                //
                //  Note. It's still possible that the reliance on the leader of METADATA broadcasting this information is an
                //  issue in which case the broadcasting of the updated IPs in the bootstrapping phase as described earlier needs
                //  to be revisited to remove condition (1) for when ceasing to broadcast our IP change. This is because that
                //  operation has no invariant on who publishes the IP change -- each member does this.
                if (members != null) {
                    if (logger.isFineEnabled()) {
                        logger.fine("CP restore...(k8s) updating active member invocation contexts: %s", members);
                    }
                    // Update our invocation contexts for the member UUIDs and their respective IP that form the METADATA CP
                    // group. Note: for k8s currently we only support deployments whose number of members is the same as the CP
                    // group size. The below logic mirrors that of PublishLocalCPMemberOp but for each of the active CP members.
                    for (CPMember member : members) {
                        invocationManager.getRaftInvocationContext().updateMember(member);
                    }
                }
            } else {
                if (logger.isFineEnabled()) {
                    logger.fine("Ignoring received active CP members: %s since discovery is in progress.", members);
                }
            }
            return;
        }

        checkNotNull(members);
        checkFalse(members.isEmpty(), "Active CP members list cannot be empty");
        if (members.size() == 1) {
            logger.fine("There is one active CP member left: %s", members);
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
            RaftEndpoint leader = raftNode.getLeader();
            if (leader != null && leader.equals(localEndpoint)) {
                groupIds.add(raftNode.getGroupId());
            }
        }
        return groupIds;
    }

    public Map.Entry<Integer, Collection<CPGroupId>> getEnrichedLeadershipInfo() {
        Collection<CPGroupId> groups = getLeadedGroups();
        return new AbstractMap.SimpleImmutableEntry<>(0, groups);
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

    /**
     * Completes all futures registered with {@code indices}
     * in the CP group associated with {@code groupId}.
     *
     * @return {@code true} if the CP group exists, {@code false} otherwise.
     */
    public boolean completeFutures(CPGroupId groupId, Collection<Long> indices, Object result) {
        RaftNodeImpl raftNode = (RaftNodeImpl) getRaftNode(groupId);
        if (raftNode == null) {
            return false;
        }

        for (Long index : indices) {
            raftNode.completeFuture(index, result);
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
        RaftNodeImpl raftNode = (RaftNodeImpl) getRaftNode(groupId);
        if (raftNode == null) {
            return false;
        }

        for (Entry<Long, Object> result : results) {
            raftNode.completeFuture(result.getKey(), result.getValue());

        }
        return true;
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

        if (e instanceof CPMembershipEvent event) {
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
        if (e instanceof CPGroupAvailabilityEvent event) {
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

    public CPGroupViewTracker getGroupViewTracker() {
        return groupViewTracker;
    }

    public CPGroupsSnapshot currentGroupsSnapshot(boolean uuidMapping) {
        return CPGroupsSnapshot.EMPTY;
    }

    /**
     * Fetches an AP cluster {@link Member} object associated with the passed
     * {@link CPMember} object.
     *
     * @param cpMember The {@link CPMember} info to use to find the AP object
     * @return the {@link Member} object if found, else an exception is raised
     */
    public Member getClusterMember(CPMember cpMember) {
        // CPMember.uuid can be different from Member.uuid
        // During split-brain merge, Member.uuid changes but CPMember.uuid remains the same.
        return nodeEngine.getClusterService().getMember(cpMember.getAddress());
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
                        logger.fine("Cannot get initial members of %s from the METADATA CP group", groupId);
                    }
                } else {
                    if (throwable instanceof CPGroupDestroyedException exception) {
                        CPGroupId destroyedGroupId = exception.getGroupId();
                        terminateRaftNode(destroyedGroupId, true);
                    }

                    if (logger.isFineEnabled()) {
                        logger.fine("Cannot get initial members of " + groupId + " from the METADATA CP group", throwable);
                    }
                }
            }, internalAsyncExecutor);
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
            }, internalAsyncExecutor);
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
                            log.lastLogOrSnapshotIndex(), log.availableCapacity(), state.getLeadershipStats(),
                            raftNode.getLastSnapshotBuildDurationMs(), raftNode.getSnapshotBuildCount(),
                            raftNode.getChunkedSnapshotInstaller().getLastSnapshotTransferDurationMs(),
                            raftNode.getChunkedSnapshotInstaller().getSnapshotTransferCount());
                    nodeMetrics.put(node.getGroupId(), metrics);
                });
            }
        }
    }
}
