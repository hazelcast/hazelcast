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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterStateListener;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOp;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.PartitionAwareService;
import com.hazelcast.internal.partition.PartitionEventListener;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.PartitionServiceProxy;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.ReadonlyInternalPartition;
import com.hazelcast.internal.partition.operation.AssignPartitions;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.internal.partition.operation.PartitionStateCheckOperation;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;
import com.hazelcast.internal.partition.operation.ShutdownRequestOperation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.scheduler.CoalescingDelayedTrigger;
import com.hazelcast.internal.util.scheduler.ScheduledEntry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.PartitionEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_SERVICE_MAX_BACKUP_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_SERVICE_MIGRATION_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_PREFIX;
import static com.hazelcast.internal.partition.PartitionStampUtil.calculateStamp;
import static com.hazelcast.internal.partition.impl.MigrationManager.applyMigration;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The {@link InternalPartitionService} implementation.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class InternalPartitionServiceImpl implements InternalPartitionService,
        EventPublishingService<PartitionEvent, PartitionEventListener<PartitionEvent>>,
        PartitionAwareService, ClusterStateListener {

    private static final int PARTITION_OWNERSHIP_WAIT_MILLIS = 10;
    private static final int SAFE_SHUTDOWN_MAX_AWAIT_STEP_MILLIS = 1000;
    private static final long FETCH_PARTITION_STATE_SECONDS = 5;
    private static final long TRIGGER_MASTER_DELAY_MILLIS = 1000;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final int partitionCount;

    private final long partitionMigrationTimeout;

    private final PartitionServiceProxy proxy;
    private final Lock partitionServiceLock = new ReentrantLock();

    private final PartitionStateManager partitionStateManager;
    private final MigrationManager migrationManager;
    private final PartitionReplicaManager replicaManager;
    private final PartitionReplicaStateChecker partitionReplicaStateChecker;
    private final PartitionEventManager partitionEventManager;

    /** Determines if a {@link AssignPartitions} is being sent to the master, used to limit partition assignment requests. */
    private final AtomicBoolean masterTriggered = new AtomicBoolean(false);
    private final CoalescingDelayedTrigger masterTrigger;

    private final AtomicReference<CountDownLatch> shutdownLatchRef = new AtomicReference<>();

    private volatile Address latestMaster;

    /** Whether the master should fetch the partition tables from other nodes, can happen when node becomes new master. */
    private volatile boolean shouldFetchPartitionTables;

    public InternalPartitionServiceImpl(Node node) {
        HazelcastProperties properties = node.getProperties();
        this.partitionCount = properties.getInteger(ClusterProperty.PARTITION_COUNT);
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.logger = node.getLogger(InternalPartitionService.class);

        partitionStateManager = new PartitionStateManager(node, this);
        migrationManager = new MigrationManager(node, this, partitionServiceLock);
        replicaManager = new PartitionReplicaManager(node, this);

        partitionReplicaStateChecker = new PartitionReplicaStateChecker(node, this);
        partitionEventManager = new PartitionEventManager(node);

        masterTrigger = new CoalescingDelayedTrigger(nodeEngine.getExecutionService(), TRIGGER_MASTER_DELAY_MILLIS,
                2 * TRIGGER_MASTER_DELAY_MILLIS, this::resetMasterTriggeredFlag);

        partitionMigrationTimeout = properties.getMillis(ClusterProperty.PARTITION_MIGRATION_TIMEOUT);

        proxy = new PartitionServiceProxy(nodeEngine, this);

        MetricsRegistry metricsRegistry = nodeEngine.getMetricsRegistry();
        metricsRegistry.registerStaticMetrics(this, PARTITIONS_PREFIX);
        metricsRegistry.registerStaticMetrics(partitionStateManager, PARTITIONS_PREFIX);
        metricsRegistry.registerStaticMetrics(migrationManager, PARTITIONS_PREFIX);
        metricsRegistry.registerStaticMetrics(replicaManager, PARTITIONS_PREFIX);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        int partitionTableSendInterval = node.getProperties().getSeconds(ClusterProperty.PARTITION_TABLE_SEND_INTERVAL);
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new PublishPartitionRuntimeStateTask(node, this),
                partitionTableSendInterval, partitionTableSendInterval, SECONDS);

        migrationManager.start();
        replicaManager.scheduleReplicaVersionSync(executionService);
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        if (!partitionStateManager.isInitialized()) {
            firstArrangement();
        }
        final InternalPartition partition = partitionStateManager.getPartitionImpl(partitionId);
        if (partition.getOwnerReplicaOrNull() == null && !node.isMaster()) {
            if (!isClusterFormedByOnlyLiteMembers()) {
                triggerMasterToAssignPartitions();
            }
        }
        return partition.getOwnerOrNull();
    }

    @Override
    public Address getPartitionOwnerOrWait(int partitionId) {
        Address owner;
        while ((owner = getPartitionOwner(partitionId)) == null) {
            if (!nodeEngine.isRunning()) {
                throw new HazelcastInstanceNotActiveException();
            }

            ClusterState clusterState = node.getClusterService().getClusterState();
            if (!clusterState.isMigrationAllowed()) {
                throw new IllegalStateException("Partitions can't be assigned since cluster-state: " + clusterState);
            }
            if (isClusterFormedByOnlyLiteMembers()) {
                throw new NoDataMemberInClusterException(
                        "Partitions can't be assigned since all nodes in the cluster are lite members");
            }

            try {
                Thread.sleep(PARTITION_OWNERSHIP_WAIT_MILLIS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                throw ExceptionUtil.rethrow(e);
            }
        }
        return owner;
    }

    @Override
    public PartitionRuntimeState firstArrangement() {
        if (!isLocalMemberMaster()) {
            triggerMasterToAssignPartitions();
            return null;
        }

        try {
            if (!partitionServiceLock.tryLock(PARTITION_OWNERSHIP_WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
                return null;
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return null;
        }
        try {
            if (!partitionStateManager.isInitialized()) {
                Set<Member> excludedMembers = migrationManager.getShutdownRequestedMembers();
                if (partitionStateManager.initializePartitionAssignments(excludedMembers)) {
                    publishPartitionRuntimeState();
                }
            }

            return createPartitionStateInternal();
        } finally {
            partitionServiceLock.unlock();
        }
    }

    /** Sends a {@link AssignPartitions} to the master to assign partitions. */
    private void triggerMasterToAssignPartitions() {
        if (!shouldTriggerMasterToAssignPartitions()) {
            return;
        }

        ClusterServiceImpl clusterService = node.getClusterService();

        ClusterState clusterState = clusterService.getClusterState();
        if (!clusterState.isMigrationAllowed()) {
            logger.warning("Partitions can't be assigned since cluster-state=" + clusterState);
            return;
        }

        final Address masterAddress = latestMaster;
        if (masterAddress == null || masterAddress.equals(node.getThisAddress())) {
            return;
        }

        if (masterTriggered.compareAndSet(false, true)) {
            OperationServiceImpl operationService = nodeEngine.getOperationService();
            InvocationFuture<PartitionRuntimeState> future =
                    operationService.invokeOnTarget(SERVICE_NAME, new AssignPartitions(), masterAddress);
            future.whenCompleteAsync((partitionState, throwable) -> {
                                if (throwable == null) {
                                    resetMasterTriggeredFlag();
                                    if (partitionState != null) {
                                        partitionState.setMaster(masterAddress);
                                        processPartitionRuntimeState(partitionState);
                                    }
                                } else {
                                    resetMasterTriggeredFlag();
                                    logger.severe(throwable);
                                }
                            });

            masterTrigger.executeWithDelay();
        }
    }

    private boolean shouldTriggerMasterToAssignPartitions() {
        ClusterServiceImpl clusterService = node.getClusterService();
        return !partitionStateManager.isInitialized() && clusterService.isJoined() && node.getNodeExtension().isStartCompleted();
    }

    private void resetMasterTriggeredFlag() {
        masterTriggered.set(false);
    }

    private boolean isClusterFormedByOnlyLiteMembers() {
        final ClusterServiceImpl clusterService = node.getClusterService();
        return clusterService.getMembers(DATA_MEMBER_SELECTOR).isEmpty();
    }

    /**
     * Sets the initial partition table and state version. If any partition has a replica, the partition state manager is
     * set to initialized, otherwise {@link PartitionStateManager#isInitialized()} stays uninitialized but the current state
     * will be updated nevertheless.
     * This method acquires the partition service lock.
     *
     * @param partitionTable the initial partition table
     * @throws IllegalStateException if the partition manager has already been initialized
     */
    public void setInitialState(PartitionTableView partitionTable) {
        partitionServiceLock.lock();
        try {
            partitionStateManager.setInitialState(partitionTable);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public int getMemberGroupsSize() {
        return partitionStateManager.getMemberGroupsSize();
    }

    @Probe(name = PARTITIONS_METRIC_PARTITION_SERVICE_MAX_BACKUP_COUNT)
    @Override
    public int getMaxAllowedBackupCount() {
        return max(min(getMemberGroupsSize() - 1, InternalPartition.MAX_BACKUP_COUNT), 0);
    }

    public void updateMemberGroupSize() {
        partitionStateManager.updateMemberGroupsSize();
    }

    @Override
    public void memberAdded(Member member) {
        logger.fine("Adding " + member);
        partitionServiceLock.lock();
        try {
            latestMaster = node.getClusterService().getMasterAddress();
            if (!member.localMember()) {
                partitionStateManager.updateMemberGroupsSize();
            }
            if (isLocalMemberMaster()) {
                if (partitionStateManager.isInitialized()) {
                    migrationManager.triggerControlTask();
                }
            }
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public void memberRemoved(Member... members) {
        if (members.length == 0) {
            return;
        }
        logger.fine("Removing " + Arrays.toString(members));
        partitionServiceLock.lock();
        try {
            ClusterState clusterState = node.getClusterService().getClusterState();
            for (Member member : members) {
                migrationManager.onMemberRemove(member);
                replicaManager.cancelReplicaSyncRequestsTo(member);

                Address formerMaster = latestMaster;
                latestMaster = node.getClusterService().getMasterAddress();

                if (clusterState.isMigrationAllowed() || clusterState.isPartitionPromotionAllowed()) {
                    partitionStateManager.updateMemberGroupsSize();

                    boolean isThisNodeNewMaster = node.isMaster() && !node.getThisAddress().equals(formerMaster);
                    if (isThisNodeNewMaster) {
                        assert !shouldFetchPartitionTables;
                        shouldFetchPartitionTables = true;
                    }
                    // keep partition table snapshot as member leaves, unless
                    // no partitions were assigned to it (member left with graceful shutdown)
                    if (!partitionStateManager.isAbsentInPartitionTable(member)) {
                        partitionStateManager.storeSnapshot(member.getUuid());
                    }
                }
            }
            // the following call should be made outside the loop, otherwise if node.isMaster() == true
            // current node might store partition assignment snapshot for some members but not the others,
            // since migrationManager.triggerControlTaskWithDelay() might remove unknown members from
            // the partition table before the above logic is executed for another removed member.
            if (node.isMaster()
                    && (clusterState.isMigrationAllowed() || clusterState.isPartitionPromotionAllowed())) {
                migrationManager.triggerControlTaskWithDelay();
            }
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        if (!newState.isMigrationAllowed()) {
            return;
        }
        if (!partitionStateManager.isInitialized()) {
            return;
        }
        if (!isLocalMemberMaster()) {
            return;
        }

        partitionServiceLock.lock();
        try {
            if (partitionStateManager.isInitialized()
                    && migrationManager.shouldTriggerRepartitioningWhenClusterStateAllowsMigration()) {
                migrationManager.triggerControlTask();
            }
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public PartitionRuntimeState createPartitionState() {
        if (!isFetchMostRecentPartitionTableTaskRequired()) {
            return createPartitionStateInternal();
        }
        return null;
    }

    /**
     * Returns a copy of the partition table or {@code null} if not initialized. This method will acquire the partition service
     * lock.
     */
    public PartitionRuntimeState createPartitionStateInternal() {
        partitionServiceLock.lock();
        try {
            if (!partitionStateManager.isInitialized()) {
                return null;
            }

            List<MigrationInfo> completedMigrations = migrationManager.getCompletedMigrationsCopy();
            InternalPartition[] partitions = partitionStateManager.getPartitions();

            PartitionRuntimeState state;

            long stamp = partitionStateManager.getStamp();
            assert calculateStamp(partitions) == stamp : "Invalid partition stamp! Expected: "
                    + calculateStamp(partitions) + ", Actual: " + stamp;
            state = new PartitionRuntimeState(partitions, completedMigrations, stamp);
            // Create copy of active migrations set.
            // Because original set can be updated concurrently behind the scenes.
            Collection<MigrationInfo> activeMigrations = new ArrayList<>(migrationManager.getActiveMigrations());
            state.setActiveMigrations(activeMigrations);
            return state;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    /**
     * Creates a transient {@link PartitionRuntimeState} to commit promotions by applying the {@code migrationInfos}.
     * The partition table version is incremented by number of promotions.
     * This method will acquire the partition service lock.
     *
     * @param migrationInfos the promotions to be executed on the destination
     * @return the partition table with the executed migrations or {@code null} if the partitions are not initialized (assigned)
     */
    PartitionRuntimeState createPromotionCommitPartitionState(Collection<MigrationInfo> migrationInfos) {
        partitionServiceLock.lock();
        try {
            if (!partitionStateManager.isInitialized()) {
                return null;
            }

            List<MigrationInfo> completedMigrations = migrationManager.getCompletedMigrationsCopy();
            InternalPartition[] partitions = partitionStateManager.getPartitionsCopy(false);

            for (MigrationInfo migrationInfo : migrationInfos) {
                int partitionId = migrationInfo.getPartitionId();
                InternalPartitionImpl partition = (InternalPartitionImpl) partitions[partitionId];
                applyMigration(partition, migrationInfo);
                migrationInfo.setStatus(MigrationStatus.SUCCESS);
            }

            long stamp = calculateStamp(partitions);
            return new PartitionRuntimeState(partitions, completedMigrations, stamp);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    /**
     * Called on the master node to publish the current partition state to all cluster nodes. It will not publish the partition
     * state if the partitions have not yet been initialized, there is ongoing repartitioning or a node is joining the cluster.
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    void publishPartitionRuntimeState() {
        if (!partitionStateManager.isInitialized()) {
            // do not send partition state until initialized!
            return;
        }

        if (!isLocalMemberMaster()) {
            return;
        }

        if (!areMigrationTasksAllowed()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }

        PartitionRuntimeState partitionState = createPartitionStateInternal();
        if (partitionState == null) {
            return;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Publishing partition state, stamp: " + partitionState.getStamp());
        }

        PartitionStateOperation op = new PartitionStateOperation(partitionState, false);
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Member> members = node.clusterService.getMembers();
        for (Member member : members) {
            if (!member.localMember()) {
                try {
                    operationService.send(op, member.getAddress());
                } catch (Exception e) {
                    logger.finest(e);
                }
            }
        }
    }

    void sendPartitionRuntimeState(Address target) {
        if (!isLocalMemberMaster()) {
            return;
        }
        assert partitionStateManager.isInitialized();

        PartitionRuntimeState partitionState = createPartitionStateInternal();
        assert partitionState != null;

        if (logger.isFineEnabled()) {
            logger.fine("Sending partition state, stamp: " + partitionState.getStamp() + ", to " + target);
        }

        OperationService operationService = nodeEngine.getOperationService();
        PartitionStateOperation op = new PartitionStateOperation(partitionState, true);
        operationService.invokeOnTarget(SERVICE_NAME, op, target);
    }

    void checkClusterPartitionRuntimeStates() {
        if (!partitionStateManager.isInitialized()) {
            return;
        }

        if (!isLocalMemberMaster()) {
            return;
        }

        if (!areMigrationTasksAllowed()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }

        long stamp = getPartitionStateStamp();
        if (logger.isFineEnabled()) {
            logger.fine("Checking partition state, stamp: " + stamp);
        }

        OperationService operationService = nodeEngine.getOperationService();
        Collection<Member> members = node.clusterService.getMembers();
        for (Member member : members) {
            if (!member.localMember()) {
                PartitionStateCheckOperation op = new PartitionStateCheckOperation(stamp);
                InvocationFuture<Boolean> future = operationService.invokeOnTarget(SERVICE_NAME, op, member.getAddress());
                future.whenCompleteAsync((response, throwable) -> {
                    if (throwable == null) {
                        if (!Boolean.TRUE.equals(response)) {
                            logger.fine(member + " has a stale partition state. Will send the most recent partition state now.");
                            sendPartitionRuntimeState(member.getAddress());
                        }
                    } else {
                        logger.fine("Failure while checking partition state on " + member, throwable);
                        sendPartitionRuntimeState(member.getAddress());
                    }
                });
            }
        }
    }

    /**
     * Sets the {@code partitionState} if the node is started and the state is sent by the master known by this node.
     *
     * @param partitionState the new partition state
     * @return {@code true} if the partition state was applied
     */
    public boolean processPartitionRuntimeState(PartitionRuntimeState partitionState) {
        if (!node.getNodeExtension().isStartCompleted()) {
            logger.warning("Ignoring received partition table, startup is not completed yet. Sender: "
                    + partitionState.getMaster());
            return false;
        }

        return applyPartitionRuntimeState(partitionState);
    }

    public boolean applyPartitionRuntimeState(PartitionRuntimeState partitionState) {
        Address sender = partitionState.getMaster();
        if (!validateSenderIsMaster(sender, "partition table update")) {
            return false;
        }

        assert calculateStamp(partitionState.getPartitions()) == partitionState.getStamp()
                : "Invalid partition stamp! Expected: " + calculateStamp(partitionState.getPartitions())
                + ", Actual: " + partitionState.getStamp();
        return applyNewPartitionTable(partitionState.getPartitions(), partitionState.getCompletedMigrations(), sender);
    }

    private boolean validateSenderIsMaster(Address sender, String messageType) {
        Address thisAddress = node.getThisAddress();
        if (thisAddress.equals(latestMaster) && !thisAddress.equals(sender)) {
            logger.warning("This is the master node and received " + messageType + " from " + sender
                    + ". Ignoring incoming state! ");
            return false;
        } else {
            if (!isMemberMaster(sender)) {
                if (node.clusterService.getMember(sender) == null) {
                    logger.severe("Received " + messageType + " from an unknown member!"
                            + " => Sender: " + sender + "! ");
                } else {
                    logger.warning("Received " + messageType + ", but its sender doesn't seem to be master!"
                            + " => Sender: " + sender + "! (Ignore if master node has changed recently.)");
                }
                return false;
            }
        }
        return true;
    }

    /**
     * Applies the {@code partitionState} sent by the {@code sender} if the new state is newer than the current one
     * and finalizes the migrations.
     * This method does not validate the sender. It is caller method's responsibility.
     * This method will acquire the partition service lock.
     *
     * @param partitions the new partition table
     * @param completedMigrations latest completed migrations
     * @param sender         the sender of the new partition state
     * @return {@code true} if the partition state version is higher than the current one and was applied or
     * if the partition state version is same as the current one
     */
    private boolean applyNewPartitionTable(InternalPartition[] partitions, Collection<MigrationInfo> completedMigrations,
            Address sender) {
        partitionServiceLock.lock();
        try {
            requestMemberListUpdateIfUnknownMembersFound(sender, partitions);
            return updatePartitionsAndFinalizeMigrations(partitions, completedMigrations, sender);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    private void requestMemberListUpdateIfUnknownMembersFound(Address sender, InternalPartition[] partitions) {
        ClusterServiceImpl clusterService = node.clusterService;
        ClusterState clusterState = clusterService.getClusterState();
        Set<PartitionReplica> unknownReplicas = new HashSet<>();

        for (InternalPartition partition : partitions) {
            for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                PartitionReplica replica = partition.getReplica(index);
                if (replica == null) {
                    continue;
                }
                if (node.clusterService.getMember(replica.address(), replica.uuid()) == null
                        && (clusterState.isJoinAllowed()
                        || !clusterService.isMissingMember(replica.address(), replica.uuid()))) {
                        unknownReplicas.add(replica);
                }
            }
        }

        if (!unknownReplicas.isEmpty()) {
            if (logger.isWarningEnabled()) {
                StringBuilder s = new StringBuilder("Following unknown addresses are found in partition table")
                        .append(" sent from master[").append(sender).append("].")
                        .append(" (Probably they have recently joined or left the cluster.)")
                        .append(" {");
                for (PartitionReplica replica : unknownReplicas) {
                    s.append("\n\t").append(replica);
                }
                s.append("\n}");
                logger.warning(s.toString());
            }

            Address masterAddress = node.getClusterService().getMasterAddress();
            // If node is shutting down, master can be null.
            if (masterAddress != null && !masterAddress.equals(node.getThisAddress())) {
                // unknown addresses found in partition table, request a new member-list from master
                nodeEngine.getOperationService().send(new TriggerMemberListPublishOp(), masterAddress);
            }
        }
    }

    /**
     * Updates all partitions and version, updates (adds and retains) the completed migrations and finalizes the active
     * migration if it is equal to any completed.
     *
     * @param partitions          new partition table
     * @param completedMigrations completed migrations
     * @param sender              the sender of the new partition state
     * @see MigrationManager#scheduleActiveMigrationFinalization(MigrationInfo)
     * @return whether or not the new partition table is accepted
     */
    private boolean updatePartitionsAndFinalizeMigrations(InternalPartition[] partitions,
            Collection<MigrationInfo> completedMigrations, Address sender) {

        boolean applied = false;
        boolean accepted = false;

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartition newPartition = partitions[partitionId];

            InternalPartitionImpl currentPartition = partitionStateManager.getPartitionImpl(partitionId);
            int currentVersion = currentPartition.version();
            int newVersion = newPartition.version();

            if (newVersion < currentVersion) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Already applied partition update. partitionId=" + partitionId
                            + ", local version: " + currentVersion
                            + ", master version: " + newVersion + ", master: " + sender);
                }
                continue;
            } else if (newVersion == currentVersion) {
                if (!currentPartition.equals(newPartition)) {
                     throw new IllegalStateException("Partition updates are diverged! Local: " + currentPartition
                             + ", Received: " + newPartition);
                }
                if (logger.isFinestEnabled()) {
                    logger.finest("Already applied partition update. partitionId=" + partitionId
                            + ", version: " + currentVersion + ", master: " + sender);
                }
                accepted = true;
                continue;
            }

            applied = true;
            accepted = true;
            currentPartition.setReplicasAndVersion(newPartition);
        }

        for (MigrationInfo migration : completedMigrations) {
            boolean added = migrationManager.addCompletedMigration(migration);
            if (added) {
                migrationManager.scheduleActiveMigrationFinalization(migration);
            }
        }

        // Manually trigger partition stamp calculation.
        // Because partition versions are explicitly set to master's versions
        // while applying the partition table updates.
        partitionStateManager.updateStamp();

        if (logger.isFineEnabled()) {
            if (applied) {
                logger.fine("Applied partition state update with stamp: " + calculateStamp(partitions)
                        + ", Local stamp is: " + partitionStateManager.getStamp());
            } else {
                logger.fine("Already applied partition state update with stamp: " + calculateStamp(partitions)
                        + ", Local stamp is: " + partitionStateManager.getStamp());
            }
        }
        migrationManager.retainCompletedMigrations(completedMigrations);

        if (!partitionStateManager.setInitialized()) {
            node.getNodeExtension().onPartitionStateChange();
        }
        return accepted;
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public boolean applyCompletedMigrations(Collection<MigrationInfo> migrations, Address sender) {
        if (!validateSenderIsMaster(sender, "completed migrations")) {
            return false;
        }
        partitionServiceLock.lock();
        try {
            if (!partitionStateManager.isInitialized()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot apply completed migrations until partition table is initialized. "
                            + "Completed migrations: " + migrations);
                }
                return false;
            }

            if (isLocalMemberMaster()) {
                return true;
            }

            boolean appliedAllMigrations = true;
            for (MigrationInfo migration : migrations) {
                InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migration.getPartitionId());
                int currentVersion = partition.version();

                if (migration.getFinalPartitionVersion() <= currentVersion) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Already applied " + migration + ". Local version: " + currentVersion
                                + ", Commit version: " + migration.getFinalPartitionVersion() + " Master: " + sender);
                    }
                    continue;
                }

                if (migration.getInitialPartitionVersion() != currentVersion) {
                    logger.warning("Cannot apply migration commit: " + migration + ". Expected version: "
                            + migration.getInitialPartitionVersion() + ", current version: " + currentVersion
                            + ", final version: " + migration.getFinalPartitionVersion() + ", Master: " + sender);
                    appliedAllMigrations = false;
                    break;
                }

                boolean added = migrationManager.addCompletedMigration(migration);
                assert added : "Migration: " + migration;

                if (migration.getStatus() == MigrationStatus.SUCCESS) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Applying completed migration " + migration);
                    }
                    applyMigration(partition, migration);
                } else {
                    // migration failed...
                    int increment = migration.getPartitionVersionIncrement();
                    partitionStateManager.incrementPartitionVersion(partition.getPartitionId(), increment);
                }
                migrationManager.scheduleActiveMigrationFinalization(migration);
            }

            if (logger.isFineEnabled()) {
                logger.fine("Applied completed migrations with partition state stamp: " + partitionStateManager.getStamp());
            }

            migrationManager.retainCompletedMigrations(migrations);
            node.getNodeExtension().onPartitionStateChange();

            return appliedAllMigrations;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public IPartition[] getPartitions() {
        IPartition[] result = new IPartition[partitionCount];
        System.arraycopy(partitionStateManager.getPartitions(), 0, result, 0, partitionCount);
        return result;
    }

    @Override
    public InternalPartition[] getInternalPartitions() {
        return partitionStateManager.getPartitions();
    }

    @Override
    public InternalPartition getPartition(int partitionId) {
        return getPartition(partitionId, true);
    }

    @Override
    public InternalPartition getPartition(int partitionId, boolean triggerOwnerAssignment) {
        InternalPartitionImpl p = partitionStateManager.getPartitionImpl(partitionId);
        if (triggerOwnerAssignment && p.getOwnerReplicaOrNull() == null) {
            // probably ownerships are not set yet.
            // force it.
            getPartitionOwner(partitionId);
        }
        return p;
    }

    @Override
    public boolean onShutdown(long timeout, TimeUnit unit) {
        if (!node.getClusterService().isJoined()) {
            return true;
        }

        if (node.isLiteMember()) {
            return true;
        }

        CountDownLatch latch = getShutdownLatch();
        OperationServiceImpl operationService = nodeEngine.getOperationService();

        long timeoutMillis = unit.toMillis(timeout);
        long awaitStep = Math.min(SAFE_SHUTDOWN_MAX_AWAIT_STEP_MILLIS, timeoutMillis);
        try {
            do {
                Address masterAddress = nodeEngine.getMasterAddress();
                if (masterAddress == null) {
                    logger.warning("Safe shutdown failed, master member is not known!");
                    return false;
                }

                if (node.getThisAddress().equals(masterAddress)) {
                    onShutdownRequest(node.getLocalMember());
                } else {
                    UUID memberUuid = node.getLocalMember().getUuid();
                    operationService.send(new ShutdownRequestOperation(memberUuid), masterAddress);
                }
                if (latch.await(awaitStep, TimeUnit.MILLISECONDS)) {
                    return true;
                }
                timeoutMillis -= awaitStep;
            } while (timeoutMillis > 0);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            logger.info("Safe shutdown is interrupted!");
        }
        return false;
    }

    private CountDownLatch getShutdownLatch() {
        CountDownLatch latch = shutdownLatchRef.get();
        if (latch == null) {
            latch = new CountDownLatch(1);
            if (!shutdownLatchRef.compareAndSet(null, latch)) {
                latch = shutdownLatchRef.get();
            }
        }
        return latch;
    }

    public void onShutdownRequest(Member member) {
        if (partitionServiceLock.tryLock()) {
            try {
                migrationManager.onShutdownRequest(member);
            } finally {
                partitionServiceLock.unlock();
            }
        }
    }

    public void onShutdownResponse() {
        CountDownLatch latch = shutdownLatchRef.get();
        assert latch != null;
        latch.countDown();
    }

    @Override
    public boolean isMemberStateSafe() {
        return partitionReplicaStateChecker.getPartitionServiceState() == PartitionServiceState.SAFE;
    }

    @Override
    public boolean hasOnGoingMigration() {
        return hasOnGoingMigrationLocal()
                || (!isLocalMemberMaster() && partitionReplicaStateChecker.hasOnGoingMigrationMaster(Level.FINEST));
    }

    @Override
    public boolean hasOnGoingMigrationLocal() {
        return migrationManager.hasOnGoingMigration();
    }

    @Override
    public final int getPartitionId(@Nonnull Data key) {
        return HashUtil.hashToIndex(key.getPartitionHash(), partitionCount);
    }

    @Override
    public final int getPartitionId(@Nonnull Object key) {
        return getPartitionId(nodeEngine.toData(key));
    }

    @Override
    public final int getPartitionCount() {
        return partitionCount;
    }

    public long getPartitionMigrationTimeout() {
        return partitionMigrationTimeout;
    }

    @Override
    public PartitionReplicaVersionManager getPartitionReplicaVersionManager() {
        return replicaManager;
    }

    @Override
    public Map<Address, List<Integer>> getMemberPartitionsMap() {
        Collection<Member> dataMembers = node.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        int dataMembersSize = dataMembers.size();
        int partitionsPerMember = (dataMembersSize > 0 ? (int) ceil((float) partitionCount / dataMembersSize) : 0);

        Map<Address, List<Integer>> memberPartitions = createHashMap(dataMembersSize);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Address owner = getPartitionOwnerOrWait(partitionId);
            List<Integer> ownedPartitions =
                    memberPartitions.computeIfAbsent(owner, k -> new ArrayList<>(partitionsPerMember));
            ownedPartitions.add(partitionId);
        }
        return memberPartitions;
    }

    @Override
    public List<Integer> getMemberPartitions(Address target) {
        List<Integer> ownedPartitions = new LinkedList<>();
        for (int i = 0; i < partitionCount; i++) {
            final Address owner = getPartitionOwner(i);
            if (target.equals(owner)) {
                ownedPartitions.add(i);
            }
        }
        return ownedPartitions;
    }

    @Override
    public List<Integer> getMemberPartitionsIfAssigned(Address target) {
        if (!partitionStateManager.isInitialized()) {
            return Collections.emptyList();
        }
        return getMemberPartitions(target);
    }

    @Override
    public void reset() {
        partitionServiceLock.lock();
        try {
            shouldFetchPartitionTables = false;
            replicaManager.reset();
            partitionStateManager.reset();
            migrationManager.reset();
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public void pauseMigration() {
        migrationManager.pauseMigration();
    }

    @Override
    public void resumeMigration() {
        migrationManager.resumeMigration();
    }

    public boolean areMigrationTasksAllowed() {
        return migrationManager.areMigrationTasksAllowed();
    }

    @Override
    public void shutdown(boolean terminate) {
        logger.finest("Shutting down the partition service");
        migrationManager.stop();
        reset();
    }

    @Probe(name = PARTITIONS_METRIC_PARTITION_SERVICE_MIGRATION_QUEUE_SIZE)
    @Override
    public long getMigrationQueueSize() {
        return migrationManager.getMigrationQueueSize();
    }

    @Override
    public PartitionServiceProxy getPartitionServiceProxy() {
        return proxy;
    }

    @Override
    public UUID addMigrationListener(MigrationListener listener) {
        return partitionEventManager.addMigrationListener(listener);
    }

    @Override
    public CompletableFuture<UUID> addMigrationListenerAsync(MigrationListener migrationListener) {
        return partitionEventManager.addMigrationListenerAsync(migrationListener);
    }

    @Override
    public UUID addLocalMigrationListener(MigrationListener migrationListener) {
        return partitionEventManager.addLocalMigrationListener(migrationListener);
    }

    @Override
    public boolean removeMigrationListener(UUID registrationId) {
        return partitionEventManager.removeMigrationListener(registrationId);
    }

    @Override
    public CompletableFuture<Boolean> removeMigrationListenerAsync(UUID registrationId) {
        return partitionEventManager.removeMigrationListenerAsync(registrationId);
    }

    @Override
    public UUID addPartitionLostListener(PartitionLostListener listener) {
        return partitionEventManager.addPartitionLostListener(listener);
    }

    @Override
    public CompletableFuture<UUID> addPartitionLostListenerAsync(PartitionLostListener listener) {
        return partitionEventManager.addPartitionLostListenerAsync(listener);
    }

    @Override
    public UUID addLocalPartitionLostListener(PartitionLostListener listener) {
        return partitionEventManager.addLocalPartitionLostListener(listener);
    }

    @Override
    public boolean removePartitionLostListener(UUID registrationId) {
        return partitionEventManager.removePartitionLostListener(registrationId);
    }

    @Override
    public CompletableFuture<Boolean> removePartitionLostListenerAsync(UUID registrationId) {
        return partitionEventManager.removePartitionLostListenerAsync(registrationId);
    }

    @Override
    public void dispatchEvent(PartitionEvent event, PartitionEventListener<PartitionEvent> partitionEventListener) {
        partitionEventListener.onEvent(event);
    }

    @Override
    public boolean isPartitionOwner(int partitionId) {
        InternalPartition partition = partitionStateManager.getPartitionImpl(partitionId);
        return partition.isLocal();
    }

    @Override
    public long getPartitionStateStamp() {
        return partitionStateManager.getStamp();
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent event) {
        partitionEventManager.onPartitionLost(event);
    }

    public void setMigrationInterceptor(MigrationInterceptor listener) {
        migrationManager.setMigrationInterceptor(listener);
    }

    public MigrationInterceptor getMigrationInterceptor() {
        return migrationManager.getMigrationInterceptor();
    }

    public void resetMigrationInterceptor() {
        migrationManager.resetMigrationInterceptor();
    }

    /**
     * @return copy of ongoing replica-sync operations
     */
    public List<ReplicaFragmentSyncInfo> getOngoingReplicaSyncRequests() {
        return replicaManager.getOngoingReplicaSyncRequests();
    }

    /**
     * @return copy of scheduled replica-sync requests
     */
    public List<ScheduledEntry<ReplicaFragmentSyncInfo, Void>> getScheduledReplicaSyncRequests() {
        return replicaManager.getScheduledReplicaSyncRequests();
    }

    public PartitionStateManager getPartitionStateManager() {
        return partitionStateManager;
    }

    public MigrationManager getMigrationManager() {
        return migrationManager;
    }

    public PartitionReplicaManager getReplicaManager() {
        return replicaManager;
    }

    @Override
    public PartitionReplicaStateChecker getPartitionReplicaStateChecker() {
        return partitionReplicaStateChecker;
    }

    public PartitionEventManager getPartitionEventManager() {
        return partitionEventManager;
    }

    boolean isFetchMostRecentPartitionTableTaskRequired() {
        return shouldFetchPartitionTables;
    }

    boolean scheduleFetchMostRecentPartitionTableTaskIfRequired() {
       partitionServiceLock.lock();
       try {
           if (shouldFetchPartitionTables) {
               migrationManager.schedule(new FetchMostRecentPartitionTableTask());
               return true;
           }

           return false;
       } finally {
           partitionServiceLock.unlock();
       }
    }

    public void replaceMember(Member oldMember, Member newMember) {
        partitionServiceLock.lock();
        try {
            partitionStateManager.replaceMember(oldMember, newMember);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public PartitionTableView createPartitionTableView() {
        partitionServiceLock.lock();
        try {
            return partitionStateManager.getPartitionTable();
        } finally {
            partitionServiceLock.unlock();
        }
    }

    public PartitionTableView getLeftMemberSnapshot(UUID uuid) {
        return partitionStateManager.getSnapshot(uuid);
    }

    /**
     * Returns true only if local member is the last known master by
     * {@code InternalPartitionServiceImpl}.
     * <p>
     * Last known master is updated under partition service lock,
     * when the former master leaves the cluster.
     * <p>
     * This method should be used instead of {@code node.isMaster()}
     * when the logic relies on being master.
     */
    public boolean isLocalMemberMaster() {
        return isMemberMaster(node.getThisAddress());
    }

    /**
     * Returns true only if the member is the last known master by
     * {@code InternalPartitionServiceImpl} and {@code ClusterServiceImpl}.
     * <p>
     * Last known master is updated under partition service lock,
     * when the former master leaves the cluster.
     */
    public boolean isMemberMaster(Address address) {
        if (address == null) {
            return false;
        }
        Address master = latestMaster;
        ClusterServiceImpl clusterService = node.getClusterService();

        // If this is the only node, we can rely on ClusterService only.
        if (master == null && clusterService.getSize() == 1) {
            master = clusterService.getMasterAddress();
        }

        // address should be the known master by both PartitionService and ClusterService.
        return address.equals(master) && address.equals(clusterService.getMasterAddress());
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean commitMigrationOnDestination(MigrationInfo migration, Address sender) {
        partitionServiceLock.lock();
        try {
            if (!validateSenderIsMaster(sender, "migration commit")) {
                return false;
            }

            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migration.getPartitionId());
            int currentVersion = partition.version();
            int initialVersion = migration.getInitialPartitionVersion();
            int finalVersion = migration.getFinalPartitionVersion();

            if (finalVersion == currentVersion) {
                if (logger.isFineEnabled()) {
                    logger.fine("Already applied migration commit. Version: " + currentVersion + ", Master: " + sender);
                }
                return true;
            }
            if (finalVersion < currentVersion) {
                if (logger.isFineEnabled()) {
                    logger.fine("Already applied migration commit. Local version: " + currentVersion
                            + ", Master version: " + finalVersion + " Master: " + sender);
                }
                return false;
            }
            if (initialVersion != currentVersion) {
                throw new IllegalStateException("Invalid migration commit! Expected version: " + initialVersion
                        + ", current version: " + currentVersion + ", Master: " + sender);
            }

            MigrationInfo activeMigration = migrationManager.getActiveMigration(migration.getPartitionId());
            assert migration.equals(activeMigration) : "Committed migration: " + migration
                    + ", Active migration: " + activeMigration;

            boolean added = migrationManager.addCompletedMigration(migration);
            assert added : "Could not add completed migration on destination: " + migration;

            applyMigration(partition, migration);
            assert partition.version() == finalVersion
                    : "Current: " + partition.version() + ", Expected: " + finalVersion;

            activeMigration.setStatus(migration.getStatus());
            migrationManager.finalizeMigration(migration);
            if (logger.isFineEnabled()) {
                logger.fine("Committed " + migration + " on destination with partition version: " + finalVersion);
            }
            return true;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    /**
     * Invoked on a node when it becomes master. It will receive partition states from all members and consolidate them into one.
     * It guarantees the monotonicity of the partition table.
     * <ul>
     * <li>Fetch partition tables from all cluster members</li>
     * <li>Pick the most up to date partition table and apply it to local</li>
     * <li>Complete the pending migration, if present</li>
     * <li>Send the new partition table to all cluster members</li>
     * </ul>
     */
    private class FetchMostRecentPartitionTableTask implements MigrationRunnable {

        private final Address thisAddress = node.getThisAddress();

        private InternalPartition[] latestPartitions;

        private boolean initialized = partitionStateManager.isInitialized();

        public void run() {
            ClusterState clusterState = node.getClusterService().getClusterState();
            if (!clusterState.isMigrationAllowed() && !clusterState.isPartitionPromotionAllowed()) {
                // If migrations and promotions are not allowed, partition table cannot be modified and we should have
                // the most recent partition table already. Because cluster state cannot be changed
                // when our partition table is stale.
                logger.fine("No need to fetch the latest partition table. "
                        + "Cluster state does not allow to modify partition table.");
                shouldFetchPartitionTables = false;
                return;
            }

            syncWithPartitionThreads();

            logger.info("Fetching partition tables from cluster to determine the most recent one... "
                    + "Local stamp: " + partitionStateManager.getStamp());
            latestPartitions = partitionStateManager.getPartitionsCopy(true);

            Collection<MigrationInfo> allCompletedMigrations = new HashSet<>();
            Collection<MigrationInfo> allActiveMigrations = new HashSet<>();

            collectAndProcessResults(allCompletedMigrations, allActiveMigrations);

            logger.info("Most recent partition table is determined.");

            processNewState(allCompletedMigrations, allActiveMigrations);
            publishPartitionRuntimeState();
        }

        private Future<PartitionRuntimeState> fetchPartitionState(Member m) {
            return nodeEngine.getOperationService()
                    .invokeOnTarget(SERVICE_NAME, new FetchPartitionStateOperation(), m.getAddress());
        }

        /** Collects all completed and active migrations and sets the partition state to the latest version. */
        private void collectAndProcessResults(Collection<MigrationInfo> allCompletedMigrations,
                Collection<MigrationInfo> allActiveMigrations) {

            Collection<Member> members = node.clusterService.getMembers(NON_LOCAL_MEMBER_SELECTOR);
            Map<Member, Future<PartitionRuntimeState>> futures = new HashMap<>();
            for (Member member : members) {
                Future<PartitionRuntimeState> future = fetchPartitionState(member);
                futures.put(member, future);
            }

            while (!futures.isEmpty()) {
                Iterator<Map.Entry<Member, Future<PartitionRuntimeState>>> iter = futures.entrySet().iterator();
                while (iter.hasNext()) {
                    PartitionRuntimeState state = collectNextPartitionState(iter);
                    if (state == null) {
                        // state can be null, if not initialized or operation is retried
                        continue;
                    }

                    for (InternalPartition partition : state.getPartitions()) {
                        int partitionId = partition.getPartitionId();
                        InternalPartition latestPartition = latestPartitions[partitionId];
                        if (latestPartition.version() < partition.version()) {
                            latestPartitions[partitionId] = partition;
                            initialized = true;
                        } else {
                            if (latestPartition.version() == partition.version()) {
                                if (!latestPartition.equals(partition)) {
                                    logger.warning("Issue while determining latest partition... Latest: " + latestPartition
                                        + ", Received: " + partition);
                                }
                            }
                        }
                    }

                    allCompletedMigrations.addAll(state.getCompletedMigrations());

                    if (state.getActiveMigrations() != null) {
                        allActiveMigrations.addAll(state.getActiveMigrations());
                    }
                }
            }
        }

        /**
         * Fetches known partition state from next member and returns null if target member is left
         * and/or not a member of this cluster anymore.
         * If future timeouts, then fetch operation is retried until we learn target member's partition state
         * or it leaves the cluster.
         */
        private PartitionRuntimeState collectNextPartitionState(Iterator<Map.Entry<Member, Future<PartitionRuntimeState>>> iter) {
            Map.Entry<Member, Future<PartitionRuntimeState>> next = iter.next();
            Member member = next.getKey();
            Future<PartitionRuntimeState> future = next.getValue();
            boolean collectedState = true;

            try {
                PartitionRuntimeState state = future.get(FETCH_PARTITION_STATE_SECONDS, SECONDS);
                if (state == null) {
                    logger.fine("Received NULL partition state from " + member);
                } else {
                    logger.fine("Received partition state version from " + member);
                }
                return state;
            } catch (InterruptedException e) {
                logger.fine("FetchMostRecentPartitionTableTask is interrupted.");
                Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
                collectedState = false;
                // Fetch partition state operation is idempotent.
                // We will retry it until it we learn the partition state or the member leaves the cluster.
                // We can't just rely on invocation retries, because if connection is dropped while
                // our operation is on the wire, invocation won't get any response and will eventually timeout.
                next.setValue(fetchPartitionState(member));
            } catch (Exception e) {
                Level level = Level.SEVERE;
                if ((e instanceof MemberLeftException) || (e.getCause() instanceof TargetNotMemberException)) {
                    level = Level.FINE;
                }
                logger.log(level, "Failed to fetch partition table from " + member, e);
            } finally {
                if (collectedState) {
                    iter.remove();
                }
            }
            return null;
        }

        /**
         * Applies a partition state and marks all migrations (including local) as complete, when a newer state is received.
         * The method will acquire the partition state lock.
         *
         * @param allCompletedMigrations received completed migrations from other nodes
         * @param allActiveMigrations    received active migrations from other nodes
         */
        private void processNewState(Collection<MigrationInfo> allCompletedMigrations,
                Collection<MigrationInfo> allActiveMigrations) {

            partitionServiceLock.lock();
            try {
                processMigrations(allCompletedMigrations, allActiveMigrations);
                if (initialized) {
                    for (MigrationInfo migration : allCompletedMigrations) {
                        if (migration.getStatus() == MigrationStatus.FAILED) {
                            // Apply version increment of failed active migrations.
                            int partitionId = migration.getPartitionId();
                            InternalPartition partition = latestPartitions[partitionId];
                            latestPartitions[partitionId] = new ReadonlyInternalPartition(partition.getReplicasCopy(),
                                    partitionId, migration.getFinalPartitionVersion());
                        }
                    }
                    logger.info("Applying the most recent of partition state...");
                    applyNewPartitionTable(latestPartitions, allCompletedMigrations, thisAddress);
                }
                shouldFetchPartitionTables = false;
            } catch (Throwable rethrowed) {
                String lineSeparator = System.lineSeparator();

                StringBuilder sb = new StringBuilder()
                        .append("latestPartitions:").append(lineSeparator)
                        .append(StringUtil.toString(latestPartitions)).append(lineSeparator)
                        .append("allCompletedMigrations:").append(lineSeparator)
                        .append(StringUtil.toString(allCompletedMigrations)).append(lineSeparator)
                        .append("allActiveMigrations:").append(lineSeparator)
                        .append(StringUtil.toString((allActiveMigrations))).append(lineSeparator)
                        .append(rethrowed);

                logger.warning(sb.toString());
                throw rethrowed;
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /** Moves all migrations to completed (including local) and marks active migrations as {@link MigrationStatus#FAILED}. */
        private void processMigrations(Collection<MigrationInfo> allCompletedMigrations,
                                       Collection<MigrationInfo> allActiveMigrations) {
            allCompletedMigrations.addAll(migrationManager.getCompletedMigrationsCopy());
            allActiveMigrations.addAll(migrationManager.getActiveMigrations());

            for (MigrationInfo migration : allActiveMigrations) {
                if (allCompletedMigrations.add(migration)) {
                    logger.info("Marking active migration " + migration + " as " + MigrationStatus.FAILED);
                    migration.setStatus(MigrationStatus.FAILED);
                    migration.setPartitionVersionIncrement(migration.getPartitionVersionIncrement() + 1);
                }
            }
        }

        private void syncWithPartitionThreads() {
            OperationExecutor opExecutor = nodeEngine.getOperationService().getOperationExecutor();
            CountDownLatch latch = new CountDownLatch(opExecutor.getPartitionThreadCount());
            opExecutor.executeOnPartitionThreads(new PartitionThreadBarrierTask(latch));
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.warning(e);
                Thread.currentThread().interrupt();
            }
        }

        /**
         * PartitionThreadBarrierTask is executed on all partition operation threads
         * and to ensure all pending/running migration operations are completed.
         */
        private final class PartitionThreadBarrierTask implements Runnable, UrgentSystemOperation {
            private final CountDownLatch latch;

            private PartitionThreadBarrierTask(CountDownLatch latch) {
                this.latch = latch;
            }

            @Override
            public void run() {
                latch.countDown();
            }
        }
    }

    @Override
    public String toString() {
        return "InternalPartitionService {"
                + "stamp: " + getPartitionStateStamp() + ", migrationQ: " + getMigrationQueueSize() + "}";
    }
}
