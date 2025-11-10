/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.PartitionStateVersionMismatchException;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.impl.MigrationInterceptor.MigrationParticipant;
import com.hazelcast.internal.partition.impl.MigrationPlanner.MigrationDecisionCallback;
import com.hazelcast.internal.partition.operation.DemoteResponseOperation;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.partition.operation.MigrationCommitOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.internal.partition.operation.PromotionCommitOperation;
import com.hazelcast.internal.partition.operation.PublishCompletedMigrationsOperation;
import com.hazelcast.internal.partition.operation.ShutdownResponseOperation;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.internal.util.collection.IntHashSet;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.internal.util.scheduler.CoalescingDelayedTrigger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_MIGRATION_MANAGER_MIGRATION_ACTIVE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_PREFIX;
import static com.hazelcast.internal.metrics.ProbeUnit.BOOLEAN;
import static com.hazelcast.internal.partition.IPartitionService.SERVICE_NAME;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_CHUNKED_MIGRATION_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_FRAGMENTED_MIGRATION_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_MIGRATION_INTERVAL;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_MIGRATION_TIMEOUT;

/**
 * Maintains migration system state and manages migration operations performed within the cluster.
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling",
        "checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class MigrationManagerImpl implements MigrationManager {

    private static final int MIGRATION_PAUSE_DURATION_SECONDS_ON_MIGRATION_FAILURE = 3;
    private static final int PUBLISH_COMPLETED_MIGRATIONS_BATCH_SIZE = 10;
    private static final long CHECK_CLUSTER_PARTITION_RUNTIME_STATES_SYNC_TIMEOUT_SECONDS = 2;

    private static final int COMMIT_SUCCESS = 1;
    private static final int COMMIT_RETRY = 0;
    private static final int COMMIT_FAILURE = -1;
    final long partitionMigrationInterval;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;
    private final PartitionStateManager partitionStateManager;
    private final MigrationQueue migrationQueue = new MigrationQueue();
    private final MigrationThread migrationThread;
    private final AtomicBoolean migrationTasksAllowed = new AtomicBoolean(true);
    private final long partitionMigrationTimeout;
    private final CoalescingDelayedTrigger delayedResumeMigrationTrigger;
    private final Set<Member> shutdownRequestedMembers = new HashSet<>();
    private final Set<Member> demoteRequestedMembers = new HashSet<>();

    // updates will be done under lock, but reads will be multithreaded.
    private final ConcurrentMap<Integer, MigrationInfo> activeMigrations = new ConcurrentHashMap<>();
    // both reads and updates will be done under lock!
    private final LinkedHashSet<MigrationInfo> completedMigrations = new LinkedHashSet<>();
    private final AtomicBoolean promotionPermit = new AtomicBoolean();
    private final MigrationStats stats = new MigrationStats();
    private volatile MigrationInterceptor migrationInterceptor = new MigrationInterceptor.NopMigrationInterceptor();
    private final Lock partitionServiceLock;
    private final MigrationPlanner migrationPlanner;
    private final boolean fragmentedMigrationEnabled;
    private final boolean chunkedMigrationEnabled;
    private final int maxTotalChunkedDataInBytes;
    private final long memberHeartbeatTimeoutMillis;
    private boolean triggerRepartitioningWhenClusterStateAllowsMigration;
    private final int maxParallelMigrations;
    private final AtomicInteger migrationCount = new AtomicInteger();
    private final Set<MigrationInfo> finalizingMigrationsRegistry = ConcurrentHashMap.newKeySet();
    private final Executor asyncExecutor;

    /**
     * the positive number of seconds to delay triggering rebalancing
     * or 0 when no delay should be applied
     */
    private final int autoRebalanceDelaySeconds;
    private volatile boolean delayNextRepartitioningExecution;
    private volatile ScheduledFuture<Void> scheduledControlTaskFuture;

    @SuppressWarnings("checkstyle:executablestatementcount")
    MigrationManagerImpl(Node node, InternalPartitionServiceImpl service, Lock partitionServiceLock) {
        this.node = node;
        this.nodeEngine = node.getNodeEngine();
        this.partitionService = service;
        this.logger = node.getLogger(getClass());
        this.partitionServiceLock = partitionServiceLock;
        migrationPlanner = new MigrationPlanner(node.getLogger(MigrationPlanner.class));
        HazelcastProperties properties = node.getProperties();
        partitionMigrationInterval = properties.getPositiveMillisOrDefault(PARTITION_MIGRATION_INTERVAL, 0);
        partitionMigrationTimeout = properties.getMillis(PARTITION_MIGRATION_TIMEOUT);
        fragmentedMigrationEnabled = properties.getBoolean(PARTITION_FRAGMENTED_MIGRATION_ENABLED);
        chunkedMigrationEnabled = properties.getBoolean(PARTITION_CHUNKED_MIGRATION_ENABLED);
        maxTotalChunkedDataInBytes = (int) MEGABYTES.toBytes(properties.getInteger(PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB));
        maxParallelMigrations = properties.getInteger(ClusterProperty.PARTITION_MAX_PARALLEL_MIGRATIONS);
        partitionStateManager = partitionService.getPartitionStateManager();
        ILogger migrationThreadLogger = node.getLogger(MigrationThread.class);
        String hzName = nodeEngine.getHazelcastInstance().getName();
        migrationThread = new MigrationThread(this, hzName, migrationThreadLogger, migrationQueue);
        long migrationPauseDelayMs = TimeUnit.SECONDS.toMillis(MIGRATION_PAUSE_DURATION_SECONDS_ON_MIGRATION_FAILURE);
        ExecutionService executionService = nodeEngine.getExecutionService();
        delayedResumeMigrationTrigger = new CoalescingDelayedTrigger(
                executionService, migrationPauseDelayMs, 2 * migrationPauseDelayMs, this::resumeMigration);
        this.memberHeartbeatTimeoutMillis = properties.getMillis(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS);
        nodeEngine.getMetricsRegistry().registerStaticMetrics(stats, PARTITIONS_PREFIX);
        this.autoRebalanceDelaySeconds =
                node.getConfig().getPersistenceConfig().isEnabled()
                        ? node.getConfig().getPersistenceConfig().getRebalanceDelaySeconds()
                        : PersistenceConfig.DEFAULT_REBALANCE_DELAY;
        this.asyncExecutor = node.getNodeEngine().getExecutionService().getExecutor(ASYNC_EXECUTOR);
    }

    @Override
    public long getPartitionMigrationInterval() {
        return partitionMigrationInterval;
    }
    @Probe(name = MIGRATION_METRIC_MIGRATION_MANAGER_MIGRATION_ACTIVE, unit = BOOLEAN)
    private int migrationActiveProbe() {
        return migrationTasksAllowed.get() ? 1 : 0;
    }

    @Override
    public void pauseMigration() {
        migrationTasksAllowed.set(false);
    }

    @Override
    public void resumeMigration() {
        migrationTasksAllowed.set(true);
    }

    private void resumeMigrationEventually() {
        delayedResumeMigrationTrigger.executeWithDelay();
    }

    @Override
    public boolean areMigrationTasksAllowed() {
        return migrationTasksAllowed.get();
    }

    @Override
    public void finalizeMigration(MigrationInfo migrationInfo) {
        try {
            PartitionReplica localReplica = PartitionReplica.from(node.getLocalMember());
            int partitionId = migrationInfo.getPartitionId();

            boolean source = localReplica.equals(migrationInfo.getSource());
            boolean destination = localReplica.equals(migrationInfo.getDestination());

            assert migrationInfo.getStatus() == MigrationStatus.SUCCESS
                    || migrationInfo.getStatus() == MigrationStatus.FAILED : "Invalid migration: " + migrationInfo;

            if (source || destination) {
                boolean success = migrationInfo.getStatus() == MigrationStatus.SUCCESS;

                MigrationParticipant participant = source ? MigrationParticipant.SOURCE : MigrationParticipant.DESTINATION;
                if (success) {
                    migrationInterceptor.onMigrationCommit(participant, migrationInfo);
                } else {
                    migrationInterceptor.onMigrationRollback(participant, migrationInfo);
                }

                MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                FinalizeMigrationOperation op = new FinalizeMigrationOperation(migrationInfo, endpoint, success);
                op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setValidateTarget(false).setService(partitionService);
                registerFinalizingMigration(migrationInfo);
                OperationServiceImpl operationService = nodeEngine.getOperationService();
                if (logger.isFineEnabled()) {
                    logger.fine("Finalizing %s", migrationInfo);
                }
                if (operationService.isRunAllowed(op)) {
                    // When migration finalization is triggered by subsequent migrations
                    // on partition thread, finalization may run directly on calling thread.
                    // Otherwise, if previously completed migration and newly submitted migration
                    // are on the same partition, then finalizing previous migration later will
                    // break the order, hence safety.
                    operationService.run(op);
                } else {
                    operationService.execute(op);
                }
                removeActiveMigration(migrationInfo);
            } else {
                PartitionReplica partitionOwner = partitionStateManager.getPartitionImpl(partitionId).getOwnerReplicaOrNull();
                if (localReplica.equals(partitionOwner)) {
                    // This is the primary owner of the partition and the migration was a backup replica migration.
                    // In this case, primary owner has nothing to do anymore,
                    // just remove the active migration and clear the migrating flag.
                    removeActiveMigration(migrationInfo);
                    partitionStateManager.clearMigratingFlag(partitionId);
                } else {
                    logger.severe("Failed to finalize migration because " + localReplica
                            + " is not a participant of the migration: " + migrationInfo);
                }
            }
        } catch (Exception e) {
            logger.warning(e);
        }
    }

    private void registerFinalizingMigration(MigrationInfo migration) {
        finalizingMigrationsRegistry.add(migration);
    }

    @Override
    public boolean isChunkedMigrationEnabled() {
        return chunkedMigrationEnabled;
    }

    @Override
    public int getMaxTotalChunkedDataInBytes() {
        return maxTotalChunkedDataInBytes;
    }

    @Override
    public boolean removeFinalizingMigration(MigrationInfo migration) {
        return finalizingMigrationsRegistry.remove(migration);
    }

    @Override
    public boolean isFinalizingMigrationRegistered(int partitionId) {
        return finalizingMigrationsRegistry.stream().anyMatch(m -> partitionId == m.getPartitionId());
    }

    @Override
    public MigrationInfo addActiveMigration(MigrationInfo migrationInfo) {
        return activeMigrations.putIfAbsent(migrationInfo.getPartitionId(), migrationInfo);
    }

    @Override
    public MigrationInfo getActiveMigration(int partitionId) {
        return activeMigrations.get(partitionId);
    }

    @Override
    public Collection<MigrationInfo> getActiveMigrations() {
        return Collections.unmodifiableCollection(activeMigrations.values());
    }

    /**
     * Removes the current {@code activeMigration} if the {@code migration} is the same
     * and returns {@code true} if removed.
     *
     */
    private boolean removeActiveMigration(MigrationInfo migration) {
        MigrationInfo activeMigration =
                activeMigrations.computeIfPresent(migration.getPartitionId(),
                        (k, currentMigration) -> currentMigration.equals(migration) ? null : currentMigration);

        if (activeMigration != null) {
            logger.warning("Active migration could not be removed! "
                    + "Current migration=" + migration + ", active migration=" + activeMigration);
            return false;
        }
        return true;
    }

    @Override
    public boolean acquirePromotionPermit() {
        return promotionPermit.compareAndSet(false, true);
    }

    @Override
    public void releasePromotionPermit() {
        promotionPermit.set(false);
    }

    @Override
    public void scheduleActiveMigrationFinalization(final MigrationInfo migrationInfo) {
        partitionServiceLock.lock();
        try {
            MigrationInfo activeMigrationInfo = getActiveMigration(migrationInfo.getPartitionId());
            if (migrationInfo.equals(activeMigrationInfo)) {
                activeMigrationInfo.setStatus(migrationInfo.getStatus());
                if (logger.isFineEnabled()) {
                    logger.fine("Scheduled finalization of %s", activeMigrationInfo);
                }
                finalizeMigration(activeMigrationInfo);
                return;
            }

            PartitionReplica source = migrationInfo.getSource();
            if (source != null && migrationInfo.getSourceCurrentReplicaIndex() > 0
                    && source.isIdentical(node.getLocalMember())) {
                // This is former backup replica owner.
                // Former backup owner does not participate in migration transaction, data always copied
                // from the primary replica. Former backup replica is not notified about this migration
                // until the migration is committed on destination. Active migration is not set
                // for this migration.

                // This path can be executed multiple times,
                // when a periodic update (latest completed migrations or the whole partition table) is received
                // and a new migration request is submitted concurrently.
                // That's why, migration should be validated by partition table, to determine whether
                // this migration finalization is already processed or not.
                InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());

                if (migrationInfo.getStatus() == MigrationStatus.SUCCESS
                        && migrationInfo.getSourceNewReplicaIndex() != partition.getReplicaIndex(source)) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("Already finalized %s on former backup replica. -> %s", migrationInfo, partition);
                    }
                    return;
                }
                if (logger.isFineEnabled()) {
                    logger.fine("Scheduled finalization of %s on former backup replica.", migrationInfo);
                }
                finalizeMigration(migrationInfo);
            }
        } finally {
            partitionServiceLock.unlock();
        }
    }

    /**
     * Sends a {@link MigrationCommitOperation} to the destination and returns {@code true} if the new partition state
     * was applied on the destination.
     */
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    private CompletionStage<Boolean> commitMigrationToDestinationAsync(final MigrationInfo migration) {
        PartitionReplica destination = migration.getDestination();

        if (destination.isIdentical(node.getLocalMember())) {
            if (logger.isFinestEnabled()) {
                logger.finest("Shortcutting migration commit, since destination is master. -> %s", migration);
            }
            return CompletableFuture.completedFuture(Boolean.TRUE);
        }

        Member member = node.getClusterService().getMember(destination.address(), destination.uuid());
        if (member == null) {
            logger.warning("Cannot commit " + migration + ". Destination " + destination + " is not a member anymore");
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }

        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Sending migration commit operation to %s for %s", destination, migration);
            }
            migration.setStatus(MigrationStatus.SUCCESS);
            UUID destinationUuid = member.getUuid();

            MigrationCommitOperation operation = new MigrationCommitOperation(migration, destinationUuid);
            InvocationFuture<Boolean> future = nodeEngine.getOperationService()
                    .createInvocationBuilder(SERVICE_NAME, operation, destination.address())
                    .setTryCount(Integer.MAX_VALUE)
                    .setCallTimeout(memberHeartbeatTimeoutMillis).invoke();

            return future.handleAsync((done, t) -> {
                // Inspect commit result;
                // - if there's an exception, either retry or fail
                // - if result is true then success, otherwise failure
                logger.fine("Migration commit response received -> " + migration + ", success: " + done + ", failure: " + t);
                if (t != null) {
                    logMigrationCommitFailure(migration, t);
                    if (t instanceof OperationTimeoutException || t.getCause() instanceof OperationTimeoutException) {
                        return COMMIT_RETRY;
                    }
                    return COMMIT_FAILURE;
                }
                return done ? COMMIT_SUCCESS : COMMIT_FAILURE;
            }, asyncExecutor).thenComposeAsync(result -> {
                switch (result) {
                    case COMMIT_SUCCESS:
                        return CompletableFuture.completedFuture(true);
                    case COMMIT_FAILURE:
                        return CompletableFuture.completedFuture(false);
                    case COMMIT_RETRY:
                        logger.fine("Retrying migration commit for -> %s", migration);
                        return commitMigrationToDestinationAsync(migration);
                    default:
                        throw new IllegalArgumentException("Unknown migration commit result: " + result);
                }
            }, asyncExecutor).handleAsync((result, t) -> {
                if (t != null) {
                    logMigrationCommitFailure(migration, t);
                    return false;
                }
                if (logger.isFineEnabled()) {
                    logger.fine("Migration commit result " + result + " from " + destination + " for " + migration);
                }
                return result;
            }, asyncExecutor);

        } catch (Throwable t) {
            logMigrationCommitFailure(migration, t);
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }
    }

    private void logMigrationCommitFailure(MigrationInfo migration, Throwable t) {
        boolean memberLeft = t instanceof MemberLeftException
                || t.getCause() instanceof TargetNotMemberException
                || t.getCause() instanceof HazelcastInstanceNotActiveException;

        PartitionReplica destination = migration.getDestination();
        if (memberLeft) {
            if (destination.isIdentical(node.getLocalMember())) {
                logger.fine("Migration commit failed for %s since this node is shutting down.", migration);
                return;
            }
            logger.warning("Migration commit failed for " + migration
                    + " since destination " + destination + " left the cluster");
        } else {
            logger.severe("Migration commit to " + destination + " failed for " + migration, t);
        }
    }

    @Override
    public boolean addCompletedMigration(MigrationInfo migrationInfo) {
        if (migrationInfo.getStatus() != MigrationStatus.SUCCESS
                && migrationInfo.getStatus() != MigrationStatus.FAILED) {
            throw new IllegalArgumentException("Migration doesn't seem completed: " + migrationInfo);
        }

        if (migrationInfo.getInitialPartitionVersion() <= 0 || migrationInfo.getPartitionVersionIncrement() <= 0) {
            throw new IllegalArgumentException("Partition state versions are not set: " + migrationInfo);
        }

        partitionServiceLock.lock();
        try {
            boolean added = completedMigrations.add(migrationInfo);
            if (added) {
                stats.incrementCompletedMigrations();
            }
            return added;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public void retainCompletedMigrations(Collection<MigrationInfo> migrations) {
        partitionServiceLock.lock();
        try {
            completedMigrations.retainAll(migrations);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    /**
     * Evicts completed migrations from the list
     *
     * @param migrations completed migrations to evict
     */
    private void evictCompletedMigrations(Collection<MigrationInfo> migrations) {
        partitionServiceLock.lock();
        try {
            completedMigrations.removeAll(migrations);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public void triggerControlTask() {
        migrationQueue.clear();
        migrationThread.abortMigrationTask();
        if (stats.getRemainingMigrations() > 0) {
            // triggered control task before current migrations are completed
            migrationQueue.add(new PublishCompletedMigrationsTask());
        }
        if (!node.getClusterService().isJoined()) {
            logger.fine("Node is not joined, will not trigger ControlTask");
            return;
        }
        if (!partitionService.isLocalMemberMaster()) {
            logger.fine("Node is not master, will not trigger ControlTask");
            return;
        }
        migrationQueue.add(new ControlTask());
        if (logger.isFinestEnabled()) {
            logger.finest("Migration queue is cleared and control task is scheduled");
        }
    }

    @Override
    public void triggerControlTaskWithDelay() {
        if (autoRebalanceDelaySeconds > 0) {
            delayNextRepartitioningExecution = true;
        }
        triggerControlTask();
    }

    @Override
    public MigrationInterceptor getMigrationInterceptor() {
        return migrationInterceptor;
    }

    @Override
    public void setMigrationInterceptor(MigrationInterceptor interceptor) {
        Preconditions.checkNotNull(interceptor);
        migrationInterceptor = interceptor;
    }

    @Override
    public void resetMigrationInterceptor() {
        migrationInterceptor = new MigrationInterceptor.NopMigrationInterceptor();
    }

    @Override
    public boolean onDemoteRequest(Member member) {
        if (this.node.getClusterService().getSize(DATA_MEMBER_SELECTOR) - getDataDisownRequestedMembers().size() <= 1) {
            logger.info("Demote is not possible because there would be no data member left after demotion");
            return false;
        }
        if (!partitionStateManager.isInitialized()) {
            if (demoteRequestedMembers.add(member)) {
                logger.info("Demote request of " + member + " is handled");
            }
            sendDemoteResponseOperation(member);
            return true;
        }
        ClusterState clusterState = node.getClusterService().getClusterState();
        if (!clusterState.isMigrationAllowed() && clusterState != ClusterState.IN_TRANSITION) {
            logger.info("Demote is not possible in cluster state " + clusterState);
            return false;
        }
        if (demoteRequestedMembers.add(member)) {
            logger.info("Demote request of " + member + " is handled");
            triggerControlTask();
        }
        return true;
    }

    @Override
    public void onShutdownRequest(Member member) {
        if (!partitionStateManager.isInitialized()) {
            sendShutdownResponseOperation(member);
            return;
        }
        ClusterState clusterState = node.getClusterService().getClusterState();
        if (!clusterState.isMigrationAllowed() && clusterState != ClusterState.IN_TRANSITION) {
            sendShutdownResponseOperation(member);
            return;
        }
        if (shutdownRequestedMembers.add(member)) {
            logger.info("Shutdown request of " + member + " is handled");
            triggerControlTask();
        }
    }

    @Override
    public void onMemberRemove(Member member) {
        shutdownRequestedMembers.remove(member);
        demoteRequestedMembers.remove(member);
    }

    @Override
    public void schedule(MigrationRunnable runnable) {
        migrationQueue.add(runnable);
    }

    @Override
    public List<MigrationInfo> getCompletedMigrationsCopy() {
        partitionServiceLock.lock();
        try {
            return new ArrayList<>(completedMigrations);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    /**
     * Returns a copy of the list of completed migrations for the specific partition.
     * Runs under the partition service lock.
     */
    private List<MigrationInfo> getCompletedMigrations(int partitionId) {
        partitionServiceLock.lock();
        try {
            List<MigrationInfo> migrations = new LinkedList<>();
            for (MigrationInfo migration : completedMigrations) {
                if (partitionId == migration.getPartitionId()) {
                    migrations.add(migration);
                }
            }
            return migrations;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    @Override
    public boolean hasOnGoingMigration() {
        return !activeMigrations.isEmpty() || getMigrationQueueSize() > 0;
    }

    @Override
    public int getMigrationQueueSize() {
        int migrations = migrationCount.get();
        return migrations + migrationQueue.migrationTaskCount();
    }

    @Override
    public void reset() {
        try {
            if (scheduledControlTaskFuture != null) {
                scheduledControlTaskFuture.cancel(true);
            }
        } catch (Throwable t) {
            logger.fine("Cancelling a scheduled control task threw an exception", t);
        }
        migrationQueue.clear();
        migrationCount.set(0);
        activeMigrations.clear();
        completedMigrations.clear();
        shutdownRequestedMembers.clear();
        demoteRequestedMembers.clear();
        migrationTasksAllowed.set(true);
    }

    @Override
    public void start() {
        migrationThread.start();
    }

    @Override
    public void stop() {
        migrationThread.stopNow();
    }

    @Override
    public void scheduleMigration(MigrationInfo migrationInfo) {
        migrationQueue.add(() -> new AsyncMigrationTask(migrationInfo).run().toCompletableFuture().join());
    }

    /**
     * Mutates the partition state and applies the migration.
     */
    static void applyMigration(InternalPartitionImpl partition, MigrationInfo migrationInfo) {
        final PartitionReplica[] members = partition.getReplicasCopy();
        if (migrationInfo.getSourceCurrentReplicaIndex() > -1) {
            members[migrationInfo.getSourceCurrentReplicaIndex()] = null;
        }
        if (migrationInfo.getDestinationCurrentReplicaIndex() > -1) {
            members[migrationInfo.getDestinationCurrentReplicaIndex()] = null;
        }
        members[migrationInfo.getDestinationNewReplicaIndex()] = migrationInfo.getDestination();
        if (migrationInfo.getSourceNewReplicaIndex() > -1) {
            members[migrationInfo.getSourceNewReplicaIndex()] = migrationInfo.getSource();
        }
        partition.setReplicas(members);
    }

    @Override
    public Set<Member> getDataDisownRequestedMembers() {
        Set<Member> result = new HashSet<>(shutdownRequestedMembers);
        result.addAll(demoteRequestedMembers);
        return result;
    }

    /**
     * Sends a {@link DemoteResponseOperation} to the {@code address} or takes a shortcut if demote is local.
     */
    private void sendDemoteResponseOperation(Member member) {
        if (node.getThisAddress().equals(member.getAddress())) {
            partitionService.onDemoteResponse();
        } else {
            nodeEngine.getOperationService().send(new DemoteResponseOperation(member.getUuid()), member.getAddress());
        }
    }

    /**
     * Sends a {@link ShutdownResponseOperation} to the {@code address} or takes a shortcut if shutdown is local.
     */
    private void sendShutdownResponseOperation(Member member) {
        if (node.getThisAddress().equals(member.getAddress())) {
            assert !node.isRunning() : "Node state: " + node.getState();
            partitionService.onShutdownResponse();
        } else {
            nodeEngine.getOperationService().send(new ShutdownResponseOperation(member.getUuid()), member.getAddress());
        }
    }

    @Override
    public boolean shouldTriggerRepartitioningWhenClusterStateAllowsMigration() {
        return triggerRepartitioningWhenClusterStateAllowsMigration;
    }

    private void publishCompletedMigrations() {
        if (!partitionService.isLocalMemberMaster()) {
            return;
        }

        assert partitionStateManager.isInitialized();

        final List<MigrationInfo> migrations = getCompletedMigrationsCopy();
        if (logger.isFineEnabled()) {
            logger.fine("Publishing completed migrations [%s]: %s", migrations.size(), migrations);
        }

        OperationService operationService = nodeEngine.getOperationService();
        ClusterServiceImpl clusterService = node.clusterService;
        final Collection<Member> members = clusterService.getMembers();
        final AtomicInteger latch = new AtomicInteger(members.size() - 1);

        for (Member member : members) {
            if ((member.localMember())) {
                continue;
            }
            Operation operation = new PublishCompletedMigrationsOperation(migrations);
            InternalCompletableFuture<Boolean> f
                    = operationService.invokeOnTarget(SERVICE_NAME, operation, member.getAddress());

            f.whenCompleteAsync((response, t) -> {
                if (t == null) {
                    if (!Boolean.TRUE.equals(response)) {
                        logger.fine(member + " rejected completed migrations with response " + response);
                        partitionService.sendPartitionRuntimeState(member.getAddress());
                        return;
                    }

                    if (latch.decrementAndGet() == 0) {
                        logger.fine("Evicting %s completed migrations.", migrations.size());
                        evictCompletedMigrations(migrations);
                    }
                } else {
                    logger.fine("Failure while publishing completed migrations to " + member, t);
                    partitionService.sendPartitionRuntimeState(member.getAddress());
                }
            }, asyncExecutor);
        }
    }

    @Override
    public MigrationStats getStats() {
        return stats;
    }

    /**
     * Invoked on the master node. Rearranges the partition table if there is no recent activity in the cluster after
     * this task has been scheduled, schedules migrations and syncs the partition state.
     * Also schedules a {@link ProcessShutdownRequestsTask}. Acquires partition service lock.
     */
    class RedoPartitioningTask implements MigrationRunnable {
        @Override
        public void run() {
            if (!partitionService.isLocalMemberMaster()) {
                return;
            }
            partitionServiceLock.lock();
            try {
                triggerRepartitioningWhenClusterStateAllowsMigration
                        = !node.getClusterService().getClusterState().isMigrationAllowed();
                if (triggerRepartitioningWhenClusterStateAllowsMigration) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Migrations are not allowed yet, %s",
                                "repartitioning will be triggered when cluster state allows migrations.");
                    }
                    assignCompletelyLostPartitions();
                    return;
                }

                PartitionReplica[][] newState = repartition();
                if (newState == null) {
                    return;
                }
                processNewPartitionState(newState);
                migrationQueue.add(new ProcessShutdownRequestsTask());
                migrationQueue.add(new ProcessDemoteRequestsTask());
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /**
         * Rearranges the partition table if the cluster is stable, returns the new partition table and schedules a
         * {@link ProcessShutdownRequestsTask} if the repartitioning failed.
         *
         * @return the new partition table or {@code null} if the cluster is not stable or the repartitioning failed
         */
        private PartitionReplica[][] repartition() {
            if (!migrationsTasksAllowed()) {
                return null;
            }

            PartitionReplica[][] newState = null;
            if (node.getNodeExtension().getInternalHotRestartService().isEnabled()) {
                // check partition table snapshots when persistence is enabled
                newState = checkSnapshots();
            }
            if (newState != null) {
                logger.info("Identified a snapshot of left member for repartition");
            } else {
                newState = partitionStateManager.repartition(getDataDisownRequestedMembers(), null);
            }
            if (newState == null) {
                migrationQueue.add(new ProcessShutdownRequestsTask());
                return null;
            }

            if (!migrationsTasksAllowed()) {
                return null;
            }
            return newState;
        }

        PartitionReplica[][] checkSnapshots() {
            Collection<Member> currentMembers = node.getClusterService().getMembers(DATA_MEMBER_SELECTOR);

            Set<UUID> currentReplicas = new HashSet<>();
            currentMembers.forEach(member -> currentReplicas.add(member.getUuid()));
            getDataDisownRequestedMembers().forEach(member -> currentReplicas.remove(member.getUuid()));

            Map<UUID, Address> currentAddressMapping = new HashMap<>();
            currentMembers.forEach(member -> currentAddressMapping.put(member.getUuid(), member.getAddress()));

            Set<PartitionTableView> candidates = new TreeSet<>(
                    new PartitionTableViewDistanceComparator(partitionStateManager.getPartitionTable()));

            for (PartitionTableView partitionTableView : partitionStateManager.snapshots()) {
                if (partitionTableView.getMemberUuids().equals(currentReplicas)) {
                    candidates.add(partitionTableView);
                }
            }
            if (candidates.isEmpty()) {
                return null;
            }
            // find the least distant
            return candidates.iterator().next().toArray(currentAddressMapping);
        }

        /**
         * Assigns new owners to completely lost partitions (which do not have owners for any replica)
         * when cluster state does not allow migrations/repartitioning but allows promotions.
         */
        private void assignCompletelyLostPartitions() {
            if (!node.getClusterService().getClusterState().isPartitionPromotionAllowed()) {
                // Migrations and partition promotions are not allowed. We cannot modify partition table.
                return;
            }

            logger.fine("Cluster state doesn't allow repartitioning. RedoPartitioningTask will only assign lost partitions.");
            InternalPartition[] partitions = partitionStateManager.getPartitions();
            PartitionIdSet partitionIds = Arrays.stream(partitions)
                    .filter(p -> InternalPartition.replicaIndices().allMatch(i -> p.getReplica(i) == null))
                    .map(InternalPartition::getPartitionId)
                    .collect(Collectors.toCollection(() -> new PartitionIdSet(partitions.length)));

            if (!partitionIds.isEmpty()) {
                PartitionReplica[][] state = partitionStateManager.repartition(getDataDisownRequestedMembers(), partitionIds);
                if (state != null) {
                    logger.warning("Assigning new owners for " + partitionIds.size()
                            + " LOST partitions, when migration is not allowed!");

                    int replicaUpdateCount = (int) partitionIds.stream()
                            .flatMap(partitionId -> Arrays.stream(state[partitionId]).filter(Objects::nonNull)).count();
                    MigrationStateImpl[] states = {new MigrationStateImpl(Clock.currentTimeMillis(), replicaUpdateCount, 0, 0L)};

                    PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
                    partitionEventManager.sendMigrationProcessStartedEvent(states[0]);

                    partitionIds.intIterator().forEachRemaining((IntConsumer) partitionId -> {
                        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
                        PartitionReplica[] replicas = state[partitionId];
                        partition.setReplicas(replicas);

                        InternalPartition.replicaIndices()
                                .filter(i -> replicas[i] != null)
                                .forEach(i -> {
                                    MigrationInfo migration = new MigrationInfo(partitionId, null, replicas[i], -1, -1, -1, i)
                                            .setStatus(MigrationStatus.SUCCESS);
                                    states[0] = states[0].onComplete(0L);
                                    partitionEventManager.sendMigrationEvent(states[0], migration, 0L);
                                });
                    });
                    partitionEventManager.sendMigrationProcessCompletedEvent(states[0]);
                    node.getNodeExtension().onPartitionStateChange();
                } else {
                    logger.warning("Unable to assign LOST partitions");
                }
            }
        }

        /**
         * Processes the new partition state by planning and scheduling migrations.
         */
        private void processNewPartitionState(PartitionReplica[][] newState) {
            int migrationCount = 0;
            // List of migration queues per-partition
            List<Queue<MigrationInfo>> partitionMigrationQueues = new ArrayList<>(newState.length);
            Int2ObjectHashMap<PartitionReplica> lostPartitions = new Int2ObjectHashMap<>();

            for (int partitionId = 0; partitionId < newState.length; partitionId++) {
                InternalPartitionImpl currentPartition = partitionStateManager.getPartitionImpl(partitionId);
                PartitionReplica[] currentReplicas = currentPartition.replicas();
                PartitionReplica[] newReplicas = newState[partitionId];

                MigrationCollector migrationCollector = new MigrationCollector(currentPartition);
                if (logger.isFinestEnabled()) {
                    logger.finest("Planning migrations for partitionId=" + partitionId
                            + ". Current replicas: " + Arrays.toString(currentReplicas)
                            + ", New replicas: " + Arrays.toString(newReplicas));
                }
                migrationPlanner.planMigrations(partitionId, currentReplicas, newReplicas, migrationCollector);
                migrationPlanner.prioritizeCopiesAndShiftUps(migrationCollector.migrations);
                if (migrationCollector.lostPartitionDestination != null) {
                    lostPartitions.put(partitionId, migrationCollector.lostPartitionDestination);
                }
                if (!migrationCollector.migrations.isEmpty()) {
                    partitionMigrationQueues.add(migrationCollector.migrations);
                    migrationCount += migrationCollector.migrations.size();
                }
            }

            stats.markNewRepartition(migrationCount);
            if (migrationCount > 0) {
                partitionService.getPartitionEventManager().sendMigrationProcessStartedEvent(stats.toMigrationState());
            }

            if (!lostPartitions.isEmpty()) {
                logger.warning("Assigning new owners for " + lostPartitions.size() + " LOST partitions!");
                lostPartitions.forEach((partitionId, destination) -> {
                    InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
                    assignLostPartitionOwner(partition, destination);
                });
                node.getNodeExtension().onPartitionStateChange();
            }

            partitionService.publishPartitionRuntimeState();

            if (migrationCount > 0) {
                scheduleMigrations(partitionMigrationQueues);
                // Schedule a task to publish completed migrations after all migrations tasks are completed.
                schedule(new PublishCompletedMigrationsTask());
            }
            logMigrationStatistics(migrationCount);
        }

        /**
         * Schedules all migrations.
         */
        private void scheduleMigrations(List<Queue<MigrationInfo>> partitionMigrationQueues) {
            schedule(new MigrationPlanTask(partitionMigrationQueues));
        }

        private void logMigrationStatistics(int migrationCount) {
            if (migrationCount > 0) {
                logger.info("Repartitioning cluster data. Migration tasks count: " + migrationCount);
            } else {
                logger.info("Partition balance is ok, no need to repartition.");
            }
        }

        private void assignLostPartitionOwner(InternalPartitionImpl partition, PartitionReplica newOwner) {
            partition.setReplica(0, newOwner);
            stats.incrementCompletedMigrations();
            MigrationInfo migrationInfo = new MigrationInfo(partition.getPartitionId(), null, newOwner, -1, -1, -1, 0);
            migrationInfo.setStatus(MigrationStatus.SUCCESS);
            partitionService.getPartitionEventManager().sendMigrationEvent(stats.toMigrationState(), migrationInfo, 0L);
        }

        /**
         * Returns {@code true} if there are no migrations in the migration queue, no new node is joining, there is no
         * ongoing repartitioning,
         * otherwise triggers the control task.
         */
        private boolean migrationsTasksAllowed() {
            boolean migrationTasksAllowed = areMigrationTasksAllowed();
            boolean hasMigrationTasks = migrationQueue.migrationTaskCount() > 1;
            if (migrationTasksAllowed && !hasMigrationTasks) {
                return true;
            }
            triggerControlTask();
            return false;
        }

        private class MigrationCollector implements MigrationDecisionCallback {
            private final InternalPartitionImpl partition;
            private final LinkedList<MigrationInfo> migrations = new LinkedList<>();
            private PartitionReplica lostPartitionDestination;

            MigrationCollector(InternalPartitionImpl partition) {
                this.partition = partition;
            }

            @Override
            public void migrate(PartitionReplica source,
                                int sourceCurrentReplicaIndex,
                                int sourceNewReplicaIndex,
                                PartitionReplica destination,
                                int destinationCurrentReplicaIndex,
                                int destinationNewReplicaIndex) {

                int partitionId = partition.getPartitionId();
                if (logger.isFineEnabled()) {
                    logger.fine("Planned migration -> partitionId=" + partitionId
                            + ", source=" + source + ", sourceCurrentReplicaIndex=" + sourceCurrentReplicaIndex
                            + ", sourceNewReplicaIndex=" + sourceNewReplicaIndex + ", destination=" + destination
                            + ", destinationCurrentReplicaIndex=" + destinationCurrentReplicaIndex
                            + ", destinationNewReplicaIndex=" + destinationNewReplicaIndex);
                }

                if (source == null && destinationCurrentReplicaIndex == -1 && destinationNewReplicaIndex == 0) {
                    assert destination != null : "partitionId=" + partitionId + " destination is null";
                    assert sourceCurrentReplicaIndex == -1
                            : "partitionId=" + partitionId + " invalid index: " + sourceCurrentReplicaIndex;
                    assert sourceNewReplicaIndex == -1
                            : "partitionId=" + partitionId + " invalid index: " + sourceNewReplicaIndex;

                    assert lostPartitionDestination == null : "Current: " + lostPartitionDestination + ", New: " + destination;
                    lostPartitionDestination = destination;

                } else if (destination == null && sourceNewReplicaIndex == -1) {
                    assert source != null : "partitionId=" + partitionId + " source is null";
                    assert sourceCurrentReplicaIndex != -1
                            : "partitionId=" + partitionId + " invalid index: " + sourceCurrentReplicaIndex;
                    assert sourceCurrentReplicaIndex != 0
                            : "partitionId=" + partitionId + " invalid index: " + sourceCurrentReplicaIndex;
                    PartitionReplica currentSource = partition.getReplica(sourceCurrentReplicaIndex);
                    assert source.equals(currentSource)
                            : "partitionId=" + partitionId + " current source="
                            + source + " is different than expected source=" + source;

                    partition.setReplica(sourceCurrentReplicaIndex, null);
                } else {
                    MigrationInfo migration = new MigrationInfo(partitionId, source, destination,
                            sourceCurrentReplicaIndex, sourceNewReplicaIndex,
                            destinationCurrentReplicaIndex, destinationNewReplicaIndex);
                    migrations.add(migration);
                }
            }
        }
    }

    class MigrationPlanTask implements MigrationRunnable {
        /**
         * List of migration queues per-partition
         */
        private final List<Queue<MigrationInfo>> partitionMigrationQueues;
        /**
         * Queue for completed migrations.
         * It will be processed concurrently while migrations are running.
         */
        private final BlockingQueue<MigrationInfo> completed;
        /**
         * Set of currently migrating partition IDs.
         * It's illegal to have concurrent migrations on the same partition.
         */
        private final IntHashSet migratingPartitions;

        /**
         * Map of endpoint -> migration-count.
         * Only {@link #maxParallelMigrations} number of migrations are allowed on a single member.
         */
        private final Map<Address, Integer> endpoint2MigrationCount = new HashMap<>();
        private int ongoingMigrationCount;
        private boolean failed;
        private volatile boolean aborted;

        MigrationPlanTask(List<Queue<MigrationInfo>> partitionMigrationQueues) {
            this.partitionMigrationQueues = partitionMigrationQueues;
            this.completed = new ArrayBlockingQueue<>(partitionMigrationQueues.size());
            this.migratingPartitions
                    = new IntHashSet(partitionMigrationQueues
                    .stream().mapToInt(Collection::size).sum(), -1);
        }

        @Override
        public void run() {
            migrationCount.set(partitionMigrationQueues
                    .stream().mapToInt(Collection::size).sum());

            while (true) {
                MigrationInfo migration = next();
                if (migration == null) {
                    break;
                }

                if (failed || aborted) {
                    break;
                }

                onStart(migration);

                try {
                    CompletionStage<Boolean> f = new AsyncMigrationTask(migration).run();
                    f.thenRunAsync(() -> {
                        logger.fine("AsyncMigrationTask completed: %s", migration);
                        boolean offered = completed.offer(migration);
                        assert offered : "Failed to offer completed migration: " + migration;
                    }, CALLER_RUNS);
                } catch (Throwable e) {
                    logger.warning("AsyncMigrationTask failed: " + migration, e);
                    boolean offered = completed.offer(migration);
                    assert offered : "Failed to offer completed migration: " + migration;
                }

                if (!migrationDelay()) {
                    break;
                }
            }

            waitOngoingMigrations();

            if (failed || aborted) {
                logger.info("Rebalance process was " + (failed ? "failed" : "aborted")
                        + ". Ignoring remaining migrations. Will recalculate the new migration plan. ("
                        + stats.formatToString(logger.isFineEnabled()) + ")");
                migrationCount.set(0);
                partitionMigrationQueues.clear();
            } else {
                logger.info("All migration tasks have been completed. (" + stats.formatToString(logger.isFineEnabled()) + ")");
            }
        }

        private void onStart(MigrationInfo migration) {
            boolean added = migratingPartitions.add(migration.getPartitionId());
            assert added : "Couldn't add partitionId to migrating partitions set: " + migration;

            BiFunction<Address, Integer, Integer> inc = (address, current) -> current != null ? current + 1 : 1;

            int count = endpoint2MigrationCount.compute(migration.getDestinationAddress(), inc);
            assert count > 0 && count <= maxParallelMigrations : "Count: " + count + " -> " + migration;

            count = endpoint2MigrationCount.compute(sourceAddress(migration), inc);
            assert count > 0 && count <= maxParallelMigrations : "Count: " + count + " -> " + migration;

            ongoingMigrationCount++;
            migrationCount.decrementAndGet();
        }

        private void onComplete(MigrationInfo migration) {
            boolean removed = migratingPartitions.remove(migration.getPartitionId());
            assert removed : "Couldn't remove partitionId from migrating partitions set: " + migration;

            BiFunction<Address, Integer, Integer> dec = (address, current) -> current != null ? current - 1 : -1;

            long count = endpoint2MigrationCount.compute(migration.getDestinationAddress(), dec);
            assert count >= 0 && count < maxParallelMigrations : "Count: " + count + " -> " + migration;

            count = endpoint2MigrationCount.compute(sourceAddress(migration), dec);
            assert count >= 0 && count < maxParallelMigrations : "Count: " + count + " -> " + migration;

            if (migration.getStatus() != MigrationStatus.SUCCESS) {
                failed = true;
            }

            ongoingMigrationCount--;
        }

        private boolean processCompleted() {
            boolean ok = false;
            MigrationInfo migration;
            while ((migration = completed.poll()) != null) {
                onComplete(migration);
                ok = true;
            }
            return ok;
        }

        private MigrationInfo next() {
            MigrationInfo m;
            while ((m = next0()) == null) {
                if (partitionMigrationQueues.isEmpty()) {
                    break;
                }

                if (!processCompleted()) {
                    try {
                        MigrationInfo migration = completed.take();
                        onComplete(migration);
                    } catch (InterruptedException e) {
                        onInterrupted(e);
                        break;
                    }
                }

                if (failed || aborted) {
                    break;
                }
            }
            return m;
        }

        private MigrationInfo next0() {
            Iterator<Queue<MigrationInfo>> iter = partitionMigrationQueues.iterator();
            while (iter.hasNext()) {
                Queue<MigrationInfo> q = iter.next();
                if (q.isEmpty()) {
                    iter.remove();
                    continue;
                }

                if (!select(q.peek())) {
                    continue;
                }

                return q.poll();
            }
            return null;
        }

        private boolean select(MigrationInfo m) {
            if (m == null) {
                return true;
            }

            if (migratingPartitions.contains(m.getPartitionId())) {
                return false;
            }
            if (endpoint2MigrationCount.getOrDefault(m.getDestinationAddress(), 0) == maxParallelMigrations) {
                return false;
            }
            return endpoint2MigrationCount.getOrDefault(sourceAddress(m), 0) < maxParallelMigrations;
        }

        private Address sourceAddress(MigrationInfo m) {
            if (m.getSourceCurrentReplicaIndex() == 0) {
                return m.getSourceAddress();
            }
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(m.getPartitionId());
            return partition.getOwnerOrNull();
        }

        private boolean migrationDelay() {
            if (partitionMigrationInterval > 0) {
                try {
                    Thread.sleep(partitionMigrationInterval);
                } catch (InterruptedException e) {
                    onInterrupted(e);
                    return false;
                }
            }
            return true;
        }

        private void waitOngoingMigrations() {
            boolean interrupted = false;
            while (ongoingMigrationCount > 0) {
                try {
                    MigrationInfo migration = completed.take();
                    onComplete(migration);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        private void onInterrupted(InterruptedException e) {
            logger.info("MigrationProcessTask is interrupted! Ignoring remaining migrations...", e);
            Thread.currentThread().interrupt();
            abort();
        }

        void abort() {
            aborted = true;
        }
    }

    /**
     * Invoked on the master node to migrate a partition (excluding promotions).
     * It will execute the {@link MigrationRequestOperation} on the partition owner.
     */
    private class AsyncMigrationTask {
        private final MigrationInfo migration;

        AsyncMigrationTask(MigrationInfo migration) {
            this.migration = migration;
            migration.setMaster(node.getThisAddress());
        }

        CompletionStage<Boolean> run() {
            if (!partitionService.isLocalMemberMaster()) {
                return CompletableFuture.completedFuture(Boolean.FALSE);
            }

            if (migration.getSource() == null
                    && migration.getDestinationCurrentReplicaIndex() > 0
                    && migration.getDestinationNewReplicaIndex() == 0) {

                throw new IllegalStateException("Promotion migrations must be handled by "
                        + RepairPartitionTableTask.class.getSimpleName() + " -> " + migration);
            }

            Member partitionOwner = checkMigrationParticipantsAndGetPartitionOwner();
            if (partitionOwner == null) {
                return CompletableFuture.completedFuture(Boolean.FALSE);
            }

            return executeMigrateOperation(partitionOwner);
        }

        private void beforeMigration() {
            migration.setInitialPartitionVersion(partitionStateManager.getPartitionVersion(migration.getPartitionId()));
            migrationInterceptor.onMigrationStart(MigrationParticipant.MASTER, migration);
            if (logger.isFineEnabled()) {
                logger.fine("Starting Migration: %s", migration);
            }
        }

        /**
         * Checks if the partition owner is not {@code null}, the source and destinations are still members and returns the owner.
         * Returns {@code null} and reschedules the {@link ControlTask} if the checks failed.
         */
        private Member checkMigrationParticipantsAndGetPartitionOwner() {
            Member partitionOwner = getPartitionOwner();
            if (partitionOwner == null) {
                logger.fine("Partition owner is null. Ignoring %s", migration);
                triggerRepartitioningAfterMigrationFailure();
                return null;
            }
            if (migration.getSource() != null) {
                PartitionReplica source = migration.getSource();
                if (node.getClusterService().getMember(source.address(), source.uuid()) == null) {
                    logger.fine("Source is not a member anymore. Ignoring %s", migration);
                    triggerRepartitioningAfterMigrationFailure();
                    return null;
                }
            }
            PartitionReplica destination = migration.getDestination();
            if (node.getClusterService().getMember(destination.address(), destination.uuid()) == null) {
                logger.fine("Destination is not a member anymore. Ignoring %s", migration);
                triggerRepartitioningAfterMigrationFailure();
                return null;
            }
            return partitionOwner;
        }

        /**
         * Returns the partition owner or {@code null} if it is not set.
         */
        private Member getPartitionOwner() {
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migration.getPartitionId());
            PartitionReplica owner = partition.getOwnerReplicaOrNull();
            if (owner == null) {
                logger.warning("Skipping migration, since partition owner doesn't exist! -> " + migration + ", " + partition);
                return null;
            }
            return node.getClusterService().getMember(owner.address(), owner.uuid());
        }

        /**
         * Sends a {@link MigrationRequestOperation} to the {@code fromMember} and returns the migration result if the
         * migration was successful.
         */
        @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
        private CompletionStage<Boolean> executeMigrateOperation(Member fromMember) {
            long start = Timer.nanos();
            CompletableFuture<Boolean> future;
            try {
                beforeMigration();

                List<MigrationInfo> completedMigrations = getCompletedMigrations(migration.getPartitionId());
                Operation op = new MigrationRequestOperation(migration, completedMigrations, 0,
                        fragmentedMigrationEnabled, isChunkedMigrationEnabled(), maxTotalChunkedDataInBytes);
                future = nodeEngine.getOperationService()
                        .createInvocationBuilder(SERVICE_NAME, op, fromMember.getAddress())
                        .setCallTimeout(partitionMigrationTimeout)
                        .invoke();
            } catch (Throwable t) {
                logger.warning("Error during " + migration, t);
                future = InternalCompletableFuture.completedExceptionally(t);
            }

            return future.handleAsync((done, t) -> {
                stats.recordMigrationOperationTime();
                logger.fine("Migration operation response received -> " + migration + ", success: " + done + ", failure: " + t);

                if (t != null) {
                    Level level = nodeEngine.isRunning() ? Level.WARNING : Level.FINE;
                    if (t instanceof ExecutionException && t.getCause() instanceof PartitionStateVersionMismatchException) {
                        level = Level.FINE;
                    }
                    if (logger.isLoggable(level)) {
                        logger.log(level, "Failed migration from " + fromMember + " for " + migration, t);
                    }
                    return Boolean.FALSE;
                }
                return done;
            }, asyncExecutor).thenComposeAsync(result -> {
                if (result) {
                    if (logger.isFineEnabled()) {
                        logger.fine("Finished Migration: %s", migration);
                    }
                    return migrationOperationSucceeded();
                } else {
                    Level level = nodeEngine.isRunning() ? Level.WARNING : Level.FINE;
                    if (logger.isLoggable(level)) {
                        logger.log(level, "Migration failed: " + migration);
                    }
                    migrationOperationFailed(fromMember);
                    return CompletableFuture.completedFuture(false);
                }
            }, asyncExecutor).handleAsync((result, t) -> {
                stats.recordMigrationTaskTime();

                partitionService.getPartitionEventManager().sendMigrationEvent(stats.toMigrationState(), migration,
                        TimeUnit.NANOSECONDS.toMillis(Timer.nanosElapsed(start)));

                if (t != null) {
                    Level level = nodeEngine.isRunning() ? Level.WARNING : Level.FINE;
                    logger.log(level, "Error during " + migration, t);
                    return false;
                }
                return result;
            }, asyncExecutor);
        }

        /**
         * Called on the master node to complete the migration and notify the migration listeners that the migration completed.
         * It will :
         * <ul>
         * <li>set the migration status</li>
         * <li>update the completed migration list</li>
         * <li>schedule the migration for finalization</li>
         * <li>update the local partition state version</li>
         * <li>sync the partition state with cluster members</li>
         * <li>triggers the {@link ControlTask}</li>
         * <li>publishes a {@link ReplicaMigrationEvent}</li>
         * </ul>
         * <p>
         * Acquires the partition state lock.
         */
        private void migrationOperationFailed(Member partitionOwner) {
            migration.setStatus(MigrationStatus.FAILED);
            migrationInterceptor.onMigrationComplete(MigrationParticipant.MASTER, migration, false);
            partitionServiceLock.lock();
            try {
                migrationInterceptor.onMigrationRollback(MigrationParticipant.MASTER, migration);
                scheduleActiveMigrationFinalization(migration);
                int delta = migration.getPartitionVersionIncrement() + 1;
                partitionStateManager.incrementPartitionVersion(migration.getPartitionId(), delta);
                migration.setPartitionVersionIncrement(delta);
                node.getNodeExtension().onPartitionStateChange();
                addCompletedMigration(migration);

                if (!partitionOwner.localMember()) {
                    partitionService.sendPartitionRuntimeState(partitionOwner.getAddress());
                }
                if (!migration.getDestination().isIdentical(node.getLocalMember())) {
                    partitionService.sendPartitionRuntimeState(migration.getDestination().address());
                }

                triggerRepartitioningAfterMigrationFailure();
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /**
         * Waits for some time and rerun the {@link ControlTask}.
         */
        private void triggerRepartitioningAfterMigrationFailure() {
            // Migration failed.
            // Pause migration process for a small amount of time, if a migration attempt is failed.
            // Otherwise, migration failures can do a busy spin until migration problem is resolved.
            // Migration can fail either a node's just joined and not completed start yet or it's just left the cluster.
            // Re-execute RedoPartitioningTask when all other migration tasks are done,
            // an imbalance may occur because of this failure.
            partitionServiceLock.lock();
            try {
                pauseMigration();
                triggerControlTask();
                resumeMigrationEventually();
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /**
         * Called on the master node to complete the migration and notify the migration listeners that the migration completed.
         * It will :
         * <ul>
         * <li>commit the migration on the destination</li>
         * <li>set the migration status</li>
         * <li>update the local partition state</li>
         * <li>schedule the migration for finalization</li>
         * <li>sync the partition state with cluster members</li>
         * <li>update the completed migration list</li>
         * <li>publishes a {@link ReplicaMigrationEvent}</li>
         * </ul>
         * <p>
         * Triggers the {@link ControlTask} if the migration failed. Acquires the partition state lock to process the result
         * of the migration commit.
         */
        private CompletionStage<Boolean> migrationOperationSucceeded() {
            migrationInterceptor.onMigrationComplete(MigrationParticipant.MASTER, migration, true);

            CompletionStage<Boolean> f = commitMigrationToDestinationAsync(migration);
            f = f.thenApplyAsync(commitSuccessful -> {
                stats.recordDestinationCommitTime();

                partitionServiceLock.lock();
                try {
                    InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migration.getPartitionId());
                    assert migration.getInitialPartitionVersion() == partition.version()
                            : "Migration initial version: " + migration.getInitialPartitionVersion()
                            + ", Partition version: " + partition.version();

                    if (commitSuccessful) {
                        migration.setStatus(MigrationStatus.SUCCESS);
                        migrationInterceptor.onMigrationCommit(MigrationParticipant.MASTER, migration);

                        // updates partition table after successful commit
                        applyMigration(partition, migration);
                    } else {
                        migration.setStatus(MigrationStatus.FAILED);
                        migrationInterceptor.onMigrationRollback(MigrationParticipant.MASTER, migration);

                        // Increasing partition version +1 more.
                        // This is to avoid conflict on commit failure; which commit runs on destination
                        // but invocation fails and migration is not applied to the partition table.
                        // Normally this is not expected since a commit invocation retries when operation
                        // timeouts. Still this is safer...
                        int delta = migration.getPartitionVersionIncrement() + 1;
                        migration.setPartitionVersionIncrement(delta);
                        partitionStateManager.incrementPartitionVersion(partition.getPartitionId(), delta);

                        if (!migration.getDestination().isIdentical(node.getLocalMember())) {
                            partitionService.sendPartitionRuntimeState(migration.getDestination().address());
                        }
                        triggerRepartitioningAfterMigrationFailure();
                    }

                    assert migration.getFinalPartitionVersion() == partition.version()
                            : "Migration final version: " + migration.getFinalPartitionVersion()
                            + ", Partition version: " + partition.version();

                    addCompletedMigration(migration);
                    scheduleActiveMigrationFinalization(migration);
                    node.getNodeExtension().onPartitionStateChange();

                    if (completedMigrations.size() >= PUBLISH_COMPLETED_MIGRATIONS_BATCH_SIZE) {
                        publishCompletedMigrations();
                    }
                } finally {
                    partitionServiceLock.unlock();
                }
                return commitSuccessful;
            }, asyncExecutor);
            return f;
        }
    }

    /**
     * Checks if the partition table needs repairing once the partitions have been initialized (assigned).
     * This means that it will:
     * <li>Remove unknown addresses from the partition table</li>
     * <li>Promote the partition replicas if necessary (the partition owner is missing)</li>
     * </ul>
     * If the promotions are successful, schedules the {@link RedoPartitioningTask}. If the process was not successful
     * it will trigger a {@link ControlTask} to restart the partition table repair process.
     * <p>
     * Invoked on the master node. Acquires partition service lock when scheduling the tasks on the migration queue.
     */
    private class RepairPartitionTableTask implements MigrationRunnable {
        @Override
        public void run() {
            if (!partitionStateManager.isInitialized()) {
                return;
            }
            ClusterState clusterState = node.getClusterService().getClusterState();
            if (!clusterState.isMigrationAllowed() && !clusterState.isPartitionPromotionAllowed()) {
                // If migrations and promotions are not allowed, partition table cannot be modified and we should have
                // the most recent partition table already. Because cluster state cannot be changed
                // when our partition table is stale.
                logger.fine("Will not repair partition table at the moment. %s",
                        "Cluster state does not allow to modify partition table.");
                return;
            }

            // Schedule a control task after configured delay.
            // Since backups are not promoted, if a member crashed then
            // its previously owned partitions are not available.
            // Recovery however will be faster as no partition migrations will occur
            // when crashed member rejoins.
            if (delayNextRepartitioningExecution) {
                logger.fine("Delaying next repartitioning execution");
                delayNextRepartitioningExecution = false;
                ExecutionService executionService = nodeEngine.getExecutionService();
                scheduledControlTaskFuture = (ScheduledFuture<Void>) executionService.schedule(
                        MigrationManagerImpl.this::triggerControlTask,
                        autoRebalanceDelaySeconds, TimeUnit.SECONDS);
                return;
            }

            long promotionsStart = Timer.nanos();

            Map<PartitionReplica, Collection<MigrationInfo>> promotions = removeUnknownMembersAndCollectPromotions();
            boolean success = promoteBackupsForMissingOwners(promotions);

            long promotionsElapsedMillis = Timer.millisElapsed(promotionsStart);
            int promotionsCount = promotions.values().stream().mapToInt(Collection::size).sum();

            if (!promotions.isEmpty()) {
                if (success) {
                    logger.info(String.format("Successfully promoted %d backups on %d members in %d ms",
                            promotionsCount, promotions.size(), promotionsElapsedMillis));
                } else {
                    logger.info(String.format("Tried to promote %d backups on %d members but some failed in %d ms",
                            promotionsCount, promotions.size(), promotionsElapsedMillis));
                }
            }

            partitionServiceLock.lock();
            try {
                if (success) {
                    logger.finest("RedoPartitioningTask scheduled");
                    migrationQueue.add(new RedoPartitioningTask());
                } else {
                    triggerControlTask();
                }
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /**
         * Removes members from the partition table which are not registered as cluster data members and checks
         * if any partitions need promotion (partition owners are missing).
         * Invoked on the master node. Acquires partition service lock.
         *
         * @return promotions that need to be sent, grouped by target replica
         */
        private Map<PartitionReplica, Collection<MigrationInfo>> removeUnknownMembersAndCollectPromotions() {
            partitionServiceLock.lock();
            try {
                partitionStateManager.removeUnknownAndLiteMembers();

                Map<PartitionReplica, Collection<MigrationInfo>> promotions = new HashMap<>();
                for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                    MigrationInfo migration = createPromotionMigrationIfOwnerIsNull(partitionId);
                    if (migration == null) {
                        continue;
                    }
                    Collection<MigrationInfo> migrations =
                            promotions.computeIfAbsent(migration.getDestination(), k -> new ArrayList<>());
                    migrations.add(migration);
                }
                return promotions;
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /**
         * Sends promotions to the destinations and commits if the destinations successfully process these promotions.
         * Called on the master node.
         *
         * @param promotions the promotions that need to be sent, grouped by target replica
         * @return if all promotions were successful
         */
        private boolean promoteBackupsForMissingOwners(Map<PartitionReplica, Collection<MigrationInfo>> promotions) {
            boolean allSucceeded = true;
            for (Map.Entry<PartitionReplica, Collection<MigrationInfo>> entry : promotions.entrySet()) {
                PartitionReplica destination = entry.getKey();
                Collection<MigrationInfo> migrations = entry.getValue();
                allSucceeded &= commitPromotionMigrations(destination, migrations);
            }
            return allSucceeded;
        }

        /**
         * Sends promotions to the destination and commits the {@code migrations} if successful. Called on the master node.
         *
         * @param destination the promotion destination
         * @param migrations  the promotion migrations
         * @return if the promotions were successful
         */
        private boolean commitPromotionMigrations(PartitionReplica destination, Collection<MigrationInfo> migrations) {
            migrationInterceptor.onPromotionStart(MigrationParticipant.MASTER, migrations);
            boolean success = commitPromotionsToDestination(destination, migrations);
            boolean local = destination.isIdentical(node.getLocalMember());
            if (!local) {
                processPromotionCommitResult(destination, migrations, success);
            }
            migrationInterceptor.onPromotionComplete(MigrationParticipant.MASTER, migrations, success);
            partitionService.publishPartitionRuntimeState();
            return success;
        }

        /**
         * Applies the {@code migrations} to the local partition table if {@code success} is {@code true}.
         * In any case it will increase the partition state version.
         * Called on the master node. This method will acquire the partition service lock.
         *
         * @param destination the promotion destination
         * @param migrations  the promotions for the destination
         * @param success     if the {@link PromotionCommitOperation} were successfully processed by the {@code destination}
         */
        private void processPromotionCommitResult(PartitionReplica destination, Collection<MigrationInfo> migrations,
                                                  boolean success) {
            partitionServiceLock.lock();
            try {
                if (!partitionStateManager.isInitialized()) {
                    // node reset/terminated while running task
                    return;
                }
                if (success) {
                    for (MigrationInfo migration : migrations) {
                        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migration.getPartitionId());

                        assert partition.getOwnerReplicaOrNull() == null : "Owner should be null: " + partition;
                        assert destination.equals(partition.getReplica(migration.getDestinationCurrentReplicaIndex()))
                                : "Invalid replica! Destination: " + destination + ", index: "
                                + migration.getDestinationCurrentReplicaIndex() + ", " + partition;
                        // single partition replica swap, increments partition state version by 2
                        partition.swapReplicas(0, migration.getDestinationCurrentReplicaIndex());
                    }
                } else {
                    // Increasing partition version +1 more.
                    // This is to avoid conflict on commit failure; which commit runs on destination
                    // but invocation fails and migration is not applied to the partition table.
                    // Normally this is not expected since a commit invocation retries when operation
                    // timeouts. Still this is safer...
                    PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
                    for (MigrationInfo migration : migrations) {
                        int delta = migration.getPartitionVersionIncrement() + 1;
                        partitionStateManager.incrementPartitionVersion(migration.getPartitionId(), delta);
                    }
                }
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /**
         * Constructs a promotion migration if the partition owner is {@code null} and there exists a non-{@code null} replica.
         * If there are no other replicas, it will send a {@link IPartitionLostEvent}.
         *
         * @param partitionId the partition ID to check
         * @return the migration info or {@code null} if the partition owner is assigned
         */
        private MigrationInfo createPromotionMigrationIfOwnerIsNull(int partitionId) {
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
            if (partition.getOwnerReplicaOrNull() == null) {
                PartitionReplica destination = null;
                int index = 1;
                for (int i = index; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                    destination = partition.getReplica(i);
                    if (destination != null) {
                        index = i;
                        break;
                    }
                }
                if (logger.isFinestEnabled()) {
                    if (destination != null) {
                        logger.finest("partitionId=" + partition.getPartitionId() + " owner is removed. replicaIndex=" + index
                                + " will be shifted up to 0. " + partition);
                    } else {
                        logger.finest("partitionId=%s owner is removed. there is no other replica to shift up. %s",
                                partition.getPartitionId(), partition);
                    }
                }
                if (destination != null) {
                    MigrationInfo migration = new MigrationInfo(partitionId, null, destination, -1, -1, index, 0);
                    migration.setMaster(node.getThisAddress());
                    migration.setStatus(MigrationInfo.MigrationStatus.SUCCESS);
                    migration.setInitialPartitionVersion(partition.version());
                    return migration;
                }
            }
            if (partition.getOwnerReplicaOrNull() == null) {
                logger.warning("partitionId=" + partitionId + " is completely lost!");
                PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
                partitionEventManager.sendPartitionLostEvent(partitionId, IPartition.MAX_BACKUP_COUNT);
            }
            return null;
        }

        /**
         * Creates a new partition table by applying the {@code migrations} and send them via {@link PromotionCommitOperation}
         * to the destination.
         *
         * @return true if the promotions were applied on the destination
         */
        private boolean commitPromotionsToDestination(PartitionReplica destination, Collection<MigrationInfo> migrations) {
            assert !migrations.isEmpty() : "No promotions to commit! destination=" + destination;

            Member member = node.getClusterService().getMember(destination.address(), destination.uuid());
            if (member == null) {
                logger.warning("Cannot commit promotions. Destination " + destination + " is not a member anymore");
                return false;
            }
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest("Sending promotion commit operation to %s for %s", destination, migrations);
                }
                PartitionRuntimeState partitionState = partitionService.createPromotionCommitPartitionState(migrations);
                UUID destinationUuid = member.getUuid();
                PromotionCommitOperation op = new PromotionCommitOperation(partitionState, migrations, destinationUuid);
                Future<Boolean> future = nodeEngine.getOperationService()
                        .createInvocationBuilder(SERVICE_NAME, op, destination.address())
                        .setTryCount(Integer.MAX_VALUE)
                        .setCallTimeout(memberHeartbeatTimeoutMillis).invoke();

                boolean result = future.get();
                if (logger.isFinestEnabled()) {
                    logger.finest("Promotion commit result " + result + " from " + destination
                            + " for migrations " + migrations);
                }
                return result;
            } catch (Throwable t) {
                logPromotionCommitFailure(destination, migrations, t);

                if (t.getCause() instanceof OperationTimeoutException) {
                    return commitPromotionsToDestination(destination, migrations);
                }
            }
            return false;
        }

        private void logPromotionCommitFailure(PartitionReplica destination, Collection<MigrationInfo> migrations, Throwable t) {
            boolean memberLeft = t instanceof MemberLeftException
                    || t.getCause() instanceof TargetNotMemberException
                    || t.getCause() instanceof HazelcastInstanceNotActiveException;

            int migrationsSize = migrations.size();
            if (memberLeft) {
                if (destination.isIdentical(node.getLocalMember())) {
                    logger.fine("Promotion commit failed for %s migrations%s", migrationsSize,
                            " since this node is shutting down.");
                    return;
                }
                if (logger.isFinestEnabled()) {
                    logger.warning("Promotion commit failed for " + migrations
                            + " since destination " + destination + " left the cluster");
                } else {
                    logger.warning("Promotion commit failed for "
                            + (migrationsSize == 1 ? migrations.iterator().next() : migrationsSize + " migrations")
                            + " since destination " + destination + " left the cluster");
                }
                return;
            }
            if (logger.isFinestEnabled()) {
                logger.severe("Promotion commit to " + destination + " failed for " + migrations, t);
            } else {
                logger.severe("Promotion commit to " + destination + " failed for "
                        + (migrationsSize == 1 ? migrations.iterator().next() : migrationsSize + " migrations"), t);
            }
        }
    }

    /**
     * Task scheduled on the master node to fetch and repair the latest partition table.
     * It will first check if we need to fetch the new partition table and schedule a task to do so, along with a new
     * {@link ControlTask} to be executed afterwards. If we don't need to fetch the partition table it will send a
     * {@link RepairPartitionTableTask} to repair the existing partition table.
     * Invoked on the master node. It will acquire the partition service lock.
     *
     * @see InternalPartitionServiceImpl#isFetchMostRecentPartitionTableTaskRequired()
     */
    private class ControlTask implements MigrationRunnable {
        @Override
        public void run() {
            partitionServiceLock.lock();
            try {
                migrationQueue.clear();
                if (partitionService.scheduleFetchMostRecentPartitionTableTaskIfRequired()) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("FetchMostRecentPartitionTableTask scheduled");
                    }
                    migrationQueue.add(new ControlTask());
                    return;
                }
                if (logger.isFinestEnabled()) {
                    logger.finest("RepairPartitionTableTask scheduled");
                }
                migrationQueue.add(new RepairPartitionTableTask());
            } finally {
                partitionServiceLock.unlock();
            }
        }
    }

    /**
     * Processes demote requests, either for this node or for other members of the cluster. Checks if any member is
     * still in the partition table and triggers the control task.
     * Invoked on the master node. Acquires partition service lock.
     */
    private class ProcessDemoteRequestsTask implements MigrationRunnable {
        @Override
        public void run() {
            if (!partitionService.isLocalMemberMaster()) {
                return;
            }
            partitionServiceLock.lock();
            try {
                final int demoteRequestCount = demoteRequestedMembers.size();
                if (demoteRequestCount > 0) {
                    boolean present = false;
                    List<Member> demotedMembers = new ArrayList<>(demoteRequestCount);
                    for (Member member : demoteRequestedMembers) {
                        if (partitionStateManager.isAbsentInPartitionTable(member)) {
                            demotedMembers.add(member);
                        } else {
                            logger.warning(member + " requested to demote but still in partition table");
                            present = true;
                        }
                    }
                    if (!demotedMembers.isEmpty()) {
                        migrationQueue.add(new SendDemoteResponses(demotedMembers));
                    }
                    if (present) {
                        triggerControlTask();
                    }
                }
            } finally {
                partitionServiceLock.unlock();
            }
        }
    }

    private class SendDemoteResponses implements MigrationRunnable {

        private final List<Member> members;

        SendDemoteResponses(List<Member> members) {
            this.members = members;
        }

        @Override
        public void run() {
            // make sure that the partition table is in sync on all members
            List<CompletableFuture<Boolean>> futures = partitionService.checkClusterPartitionRuntimeStates();
            FutureUtil.waitWithDeadline(futures, CHECK_CLUSTER_PARTITION_RUNTIME_STATES_SYNC_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS, FutureUtil.RETHROW_ALL_EXCEPT_MEMBER_LEFT);

            for (Member member : members) {
                sendDemoteResponseOperation(member);
            }
        }
    }

    /**
     * Processes shutdown requests, either for this node or for other members of the cluster. If all members requested
     * shutdown it will simply send the shutdown response, otherwise checks if any member is still in the partition table
     * and triggers the control task.
     * Invoked on the master node. Acquires partition service lock.
     */
    private class ProcessShutdownRequestsTask implements MigrationRunnable {
        @Override
        public void run() {
            if (!partitionService.isLocalMemberMaster()) {
                return;
            }
            partitionServiceLock.lock();
            try {
                final int shutdownRequestCount = shutdownRequestedMembers.size();
                final int dataDisownRequestCount = getDataDisownRequestedMembers().size();
                if (shutdownRequestCount > 0) {
                    if (dataDisownRequestCount == nodeEngine.getClusterService().getSize(DATA_MEMBER_SELECTOR)) {
                        for (Member member : shutdownRequestedMembers) {
                            sendShutdownResponseOperation(member);
                        }
                    } else {
                        boolean present = false;
                        for (Member member : shutdownRequestedMembers) {
                            if (partitionStateManager.isAbsentInPartitionTable(member)) {
                                sendShutdownResponseOperation(member);
                            } else {
                                logger.warning(member + " requested to shutdown but still in partition table");
                                present = true;
                            }
                        }
                        if (present) {
                            triggerControlTask();
                        }
                    }
                }
            } finally {
                partitionServiceLock.unlock();
            }
        }
    }

    /**
     * Task to publish completed migrations to cluster members after all migration tasks are consumed.
     */
    private class PublishCompletedMigrationsTask implements MigrationRunnable {

        @Override
        public void run() {
            partitionService.getPartitionEventManager().sendMigrationProcessCompletedEvent(stats.toMigrationState());
            publishCompletedMigrations();
        }
    }

    /**
     * Comparator that compares distance of two {@link PartitionTableView}s against a base
     * {@link PartitionTableView} that is provided at construction time.
     * Distance of two {@link PartitionTableView}s is the sum of distances of their
     * respective {link InternalPartition} distances. The distance between two
     * {@link InternalPartition}s is calculated as follows:
     * <ul>
     *     <li>If a {@link PartitionReplica} occurs in both {@link InternalPartition}s, then
     *     their distance is the absolute difference of their respective replica indices.</li>
     *     <li>If {@code null} {@link PartitionReplica}s occur at the same replica index, then
     *     their distance is 0.</li>
     *     <li>If a non-{@code null} {@link PartitionReplica} is present in one {@link InternalPartition}
     *     and not the other, then its distance is {@link InternalPartition#MAX_REPLICA_COUNT}.</li>
     * </ul>
     */
    static class PartitionTableViewDistanceComparator implements Comparator<PartitionTableView> {
        final PartitionTableView basePartitionTableView;

        PartitionTableViewDistanceComparator(PartitionTableView basePartitionTableView) {
            this.basePartitionTableView = basePartitionTableView;
        }

        @Override
        public int compare(PartitionTableView o1, PartitionTableView o2) {
            return distanceFromBase(o1) - distanceFromBase(o2);
        }

        int distanceFromBase(PartitionTableView partitionTableView) {
            return partitionTableView.distanceOf(basePartitionTableView);
        }
    }
}
