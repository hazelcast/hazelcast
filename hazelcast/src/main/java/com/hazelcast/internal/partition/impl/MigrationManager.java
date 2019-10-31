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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.PartitionStateVersionMismatchException;
import com.hazelcast.internal.partition.impl.MigrationInterceptor.MigrationParticipant;
import com.hazelcast.internal.partition.impl.MigrationPlanner.MigrationDecisionCallback;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.partition.operation.MigrationCommitOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequest;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;
import com.hazelcast.internal.partition.operation.PromotionCommitOperation;
import com.hazelcast.internal.partition.operation.PublishCompletedMigrationsOperation;
import com.hazelcast.internal.partition.operation.ShutdownResponseOperation;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.internal.util.scheduler.CoalescingDelayedTrigger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.partition.IPartitionService.SERVICE_NAME;

/**
 * Maintains migration system state and manages migration operations performed within the cluster.
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class MigrationManager {

    private static final int MIGRATION_PAUSE_DURATION_SECONDS_ON_MIGRATION_FAILURE = 3;
    private static final int PUBLISH_COMPLETED_MIGRATIONS_BATCH_SIZE = 10;

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
    // updates will be done under lock, but reads will be multithreaded.
    private volatile MigrationInfo activeMigrationInfo;
    // both reads and updates will be done under lock!
    private final LinkedHashSet<MigrationInfo> completedMigrations = new LinkedHashSet<>();
    private final AtomicBoolean promotionPermit = new AtomicBoolean(false);
    private final MigrationStats stats = new MigrationStats();
    private volatile MigrationInterceptor migrationInterceptor = new MigrationInterceptor.NopMigrationInterceptor();
    private final Lock partitionServiceLock;
    private final MigrationPlanner migrationPlanner;
    private final boolean fragmentedMigrationEnabled;
    private final long memberHeartbeatTimeoutMillis;
    private boolean triggerRepartitioningWhenClusterStateAllowsMigration;

    MigrationManager(Node node, InternalPartitionServiceImpl service, Lock partitionServiceLock) {
        this.node = node;
        this.nodeEngine = node.getNodeEngine();
        this.partitionService = service;
        this.logger = node.getLogger(getClass());
        this.partitionServiceLock = partitionServiceLock;
        migrationPlanner = new MigrationPlanner(node.getLogger(MigrationPlanner.class));
        HazelcastProperties properties = node.getProperties();
        partitionMigrationInterval = properties.getPositiveMillisOrDefault(GroupProperty.PARTITION_MIGRATION_INTERVAL, 0);
        partitionMigrationTimeout = properties.getMillis(GroupProperty.PARTITION_MIGRATION_TIMEOUT);
        fragmentedMigrationEnabled = properties.getBoolean(GroupProperty.PARTITION_FRAGMENTED_MIGRATION_ENABLED);
        partitionStateManager = partitionService.getPartitionStateManager();
        ILogger migrationThreadLogger = node.getLogger(MigrationThread.class);
        String hzName = nodeEngine.getHazelcastInstance().getName();
        migrationThread = new MigrationThread(this, hzName, migrationThreadLogger, migrationQueue);
        long migrationPauseDelayMs = TimeUnit.SECONDS.toMillis(MIGRATION_PAUSE_DURATION_SECONDS_ON_MIGRATION_FAILURE);
        ExecutionService executionService = nodeEngine.getExecutionService();
        delayedResumeMigrationTrigger = new CoalescingDelayedTrigger(
                executionService, migrationPauseDelayMs, 2 * migrationPauseDelayMs, this::resumeMigration);
        this.memberHeartbeatTimeoutMillis = properties.getMillis(GroupProperty.MAX_NO_HEARTBEAT_SECONDS);
        nodeEngine.getMetricsRegistry().registerStaticMetrics(stats, "partitions");
    }

    @Probe(name = "migrationActive")
    private int migrationActiveProbe() {
        return migrationTasksAllowed.get() ? 1 : 0;
    }

    void pauseMigration() {
        migrationTasksAllowed.set(false);
    }

    void resumeMigration() {
        migrationTasksAllowed.set(true);
    }

    private void resumeMigrationEventually() {
        delayedResumeMigrationTrigger.executeWithDelay();
    }

    /**
     * Checks if migration tasks are allowed. This can include partition state and partition data sync tasks.
     * The migration is not allowed during membership changes (member removed or joining) or for a shorter period when
     * a migration fails before restarting the migration process.
     *
     * @see MigrationRunnable
     * @see PublishPartitionRuntimeStateTask
     * @see PartitionStateOperation
     * @see PartitionReplicaSyncRequest
     */
    boolean areMigrationTasksAllowed() {
        return migrationTasksAllowed.get();
    }

    /**
     * Finalizes a migration that has finished with {@link MigrationStatus#SUCCESS}
     * or {@link MigrationStatus#FAILED} by invoking {@link FinalizeMigrationOperation}
     * locally if this is the source or destination. The finalization is asynchronous
     * and there might be other ongoing migration finalizations.
     * <p>
     * It will also cleanup the migration state by removing the active migration and
     * clearing the migration flag on the partition owner.
     * <p>
     * This method should not be called on a node which is not the source, destination
     * or partition owner for this migration.
     *
     * @param migrationInfo the migration to be finalized
     */
    void finalizeMigration(MigrationInfo migrationInfo) {
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
                op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setValidateTarget(false)
                        .setService(partitionService);
                OperationServiceImpl operationService = nodeEngine.getOperationService();
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
                removeActiveMigration(partitionId);
            } else {
                PartitionReplica partitionOwner = partitionStateManager.getPartitionImpl(partitionId).getOwnerReplicaOrNull();
                if (localReplica.equals(partitionOwner)) {
                    removeActiveMigration(partitionId);
                    partitionStateManager.clearMigratingFlag(partitionId);
                } else {
                    logger.severe("Failed to finalize migration because " + localReplica
                            + " is not a participant of the migration: " + migrationInfo);
                }
            }
        } catch (Exception e) {
            logger.warning(e);
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    /**
     * Sets the active migration if none is set and returns {@code null}, otherwise returns the currently set active migration.
     * Acquires the partition service lock.
     */
    public MigrationInfo setActiveMigration(MigrationInfo migrationInfo) {
        partitionServiceLock.lock();
        try {
            if (activeMigrationInfo == null) {
                activeMigrationInfo = migrationInfo;
                return null;
            }
            if (!activeMigrationInfo.equals(migrationInfo)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Active migration is not set: " + migrationInfo
                            + ". Existing active migration: " + activeMigrationInfo);
                }
            }
            return activeMigrationInfo;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    MigrationInfo getActiveMigration() {
        return activeMigrationInfo;
    }

    /**
     * Acquires promotion commit permit which is needed while running promotion commit
     * to prevent concurrent commits.
     * <p>
     * Normally, promotions are submitted &amp; executed serially
     * but when the commit operation timeouts, it's retried which can cause concurrent execution
     * (promotion commit operation runs on generic operation threads).
     * <p>
     * Promotion commit operation is idempotent when executed serially.
     *
     * @return true if promotion commit is allowed to run, false otherwise
     */
    public boolean acquirePromotionPermit() {
        return promotionPermit.compareAndSet(false, true);
    }

    /**
     * Releases promotion commit permit.
     *
     * @see #acquirePromotionPermit()
     */
    public void releasePromotionPermit() {
        promotionPermit.set(false);
    }

    /**
     * Removes the current {@link #activeMigrationInfo} if the {@code partitionId} is the same and returns {@code true} if
     * removed.
     * Acquires the partition service lock.
     */
    private boolean removeActiveMigration(int partitionId) {
        partitionServiceLock.lock();
        try {
            if (activeMigrationInfo != null) {
                if (activeMigrationInfo.getPartitionId() == partitionId) {
                    activeMigrationInfo = null;
                    return true;
                }
                if (logger.isFineEnabled()) {
                    logger.fine("Active migration is not removed, because it has different partitionId! "
                            + "partitionId=" + partitionId + ", active migration=" + activeMigrationInfo);
                }
            }
        } finally {
            partitionServiceLock.unlock();
        }
        return false;
    }

    /**
     * Finalizes the active migration if it is equal to the {@code migrationInfo} or if this node was a backup replica before
     * the migration (see {@link FinalizeMigrationOperation}).
     * Acquires the partition service lock.
     */
    void scheduleActiveMigrationFinalization(final MigrationInfo migrationInfo) {
        partitionServiceLock.lock();
        try {
            if (migrationInfo.equals(activeMigrationInfo)) {
                if (activeMigrationInfo.startProcessing()) {
                    activeMigrationInfo.setStatus(migrationInfo.getStatus());
                    finalizeMigration(activeMigrationInfo);
                } else {
                    // This case happens when master crashes while migration operation is running
                    // and new master publishes the latest completed migrations
                    // after collecting active and completed migrations from existing members.
                    // See FetchMostRecentPartitionTableTask.
                    logger.info("Scheduling finalization of " + migrationInfo
                            + ", because migration process is currently running.");
                    nodeEngine.getExecutionService().schedule(() ->
                            scheduleActiveMigrationFinalization(migrationInfo), 1, TimeUnit.SECONDS);
                }
                return;
            }

            PartitionReplica source = migrationInfo.getSource();
            if (source != null && migrationInfo.getSourceCurrentReplicaIndex() > 0
                    && source.isIdentical(node.getLocalMember())) {
                // Finalize migration on old backup replica owner
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
    @SuppressWarnings("checkstyle:npathcomplexity")
    private boolean commitMigrationToDestination(MigrationInfo migration) {
        PartitionReplica destination = migration.getDestination();

        if (destination.isIdentical(node.getLocalMember())) {
            if (logger.isFinestEnabled()) {
                logger.finest("Shortcutting migration commit, since destination is master. -> " + migration);
            }
            return true;
        }

        Member member = node.getClusterService().getMember(destination.address(), destination.uuid());
        if (member == null) {
            logger.warning("Cannot commit " + migration + ". Destination " + destination + " is not a member anymore");
            return false;
        }

        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Sending migration commit operation to " + destination + " for " + migration);
            }
            migration.setStatus(MigrationStatus.SUCCESS);
            UUID destinationUuid = member.getUuid();

            MigrationCommitOperation operation = new MigrationCommitOperation(migration, destinationUuid);
            Future<Boolean> future = nodeEngine.getOperationService()
                    .createInvocationBuilder(SERVICE_NAME, operation, destination.address())
                    .setTryCount(Integer.MAX_VALUE)
                    .setCallTimeout(memberHeartbeatTimeoutMillis).invoke();

            boolean result = future.get();
            if (logger.isFinestEnabled()) {
                logger.finest("Migration commit result " + result + " from " + destination + " for " + migration);
            }
            return result;
        } catch (Throwable t) {
            logMigrationCommitFailure(migration, t);

            if (t.getCause() instanceof OperationTimeoutException) {
                return commitMigrationToDestination(migration);
            }
        }
        return false;
    }

    private void logMigrationCommitFailure(MigrationInfo migration, Throwable t) {
        boolean memberLeft = t instanceof MemberLeftException
                    || t.getCause() instanceof TargetNotMemberException
                    || t.getCause() instanceof HazelcastInstanceNotActiveException;

        PartitionReplica destination = migration.getDestination();
        if (memberLeft) {
            if (destination.isIdentical(node.getLocalMember())) {
                logger.fine("Migration commit failed for " + migration
                        + " since this node is shutting down.");
                return;
            }
            logger.warning("Migration commit failed for " + migration
                        + " since destination " + destination + " left the cluster");
        } else {
            logger.severe("Migration commit to " + destination + " failed for " + migration, t);
        }
    }

    /**
     * Adds the migration to the set of completed migrations and increases the completed migration counter.
     * Acquires the partition service lock to update the migrations.
     *
     * @param migrationInfo the completed migration
     * @return {@code true} if the migration has been added or {@code false} if this migration is already in the completed set
     * @throws IllegalArgumentException if the migration is not completed
     */
    boolean addCompletedMigration(MigrationInfo migrationInfo) {
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

    /** Retains only the {@code migrations} in the completed migration list. Acquires the partition service lock. */
    void retainCompletedMigrations(Collection<MigrationInfo> migrations) {
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

    /** Clears the migration queue and triggers the control task. Called on the master node. */
    void triggerControlTask() {
        migrationQueue.clear();
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

    MigrationInterceptor getMigrationInterceptor() {
        return migrationInterceptor;
    }

    void setMigrationInterceptor(MigrationInterceptor interceptor) {
        Preconditions.checkNotNull(interceptor);
        migrationInterceptor = interceptor;
    }

    void resetMigrationInterceptor() {
        migrationInterceptor = new MigrationInterceptor.NopMigrationInterceptor();
    }

    void onShutdownRequest(Member member) {
        if (!partitionStateManager.isInitialized()) {
            sendShutdownOperation(member.getAddress());
            return;
        }
        ClusterState clusterState = node.getClusterService().getClusterState();
        if (!clusterState.isMigrationAllowed() && clusterState != ClusterState.IN_TRANSITION) {
            sendShutdownOperation(member.getAddress());
            return;
        }
        if (shutdownRequestedMembers.add(member)) {
            logger.info("Shutdown request of " + member + " is handled");
            triggerControlTask();
        }
    }

    void onMemberRemove(Member member) {
        shutdownRequestedMembers.remove(member);
        MigrationInfo activeMigration = activeMigrationInfo;
        if (activeMigration != null) {
            PartitionReplica replica = PartitionReplica.from(member);
            if (replica.equals(activeMigration.getSource())
                    || replica.equals(activeMigration.getDestination())) {
                activeMigration.setStatus(MigrationStatus.INVALID);
            }
        }
    }

    void schedule(MigrationRunnable runnable) {
        migrationQueue.add(runnable);
    }

    /** Returns a copy of the list of completed migrations. Runs under the partition service lock. */
    List<MigrationInfo> getCompletedMigrationsCopy() {
        partitionServiceLock.lock();
        try {
            return new ArrayList<>(completedMigrations);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    boolean hasOnGoingMigration() {
        return activeMigrationInfo != null || migrationQueue.hasMigrationTasks();
    }

    int getMigrationQueueSize() {
        return migrationQueue.migrationTaskCount();
    }

    void reset() {
        migrationQueue.clear();
        activeMigrationInfo = null;
        completedMigrations.clear();
        shutdownRequestedMembers.clear();
        migrationTasksAllowed.set(true);
    }

    void start() {
        migrationThread.start();
    }

    void stop() {
        migrationThread.stopNow();
    }

    /** Schedules a migration by adding it to the migration queue. */
    void scheduleMigration(MigrationInfo migrationInfo) {
        migrationQueue.add(new MigrateTask(migrationInfo));
    }

    /** Mutates the partition state and applies the migration. */
    void applyMigration(InternalPartitionImpl partition, MigrationInfo migrationInfo) {
        final PartitionReplica[] members = Arrays.copyOf(partition.getReplicas(), InternalPartition.MAX_REPLICA_COUNT);
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

    Set<Member> getShutdownRequestedMembers() {
        return shutdownRequestedMembers;
    }

    /** Sends a {@link ShutdownResponseOperation} to the {@code address} or takes a shortcut if shutdown is local. */
    private void sendShutdownOperation(Address address) {
        if (node.getThisAddress().equals(address)) {
            assert !node.isRunning() : "Node state: " + node.getState();
            partitionService.onShutdownResponse();
        } else {
            nodeEngine.getOperationService().send(new ShutdownResponseOperation(), address);
        }
    }

    /**
     * Returns {@code true} if a repartitioning action occurred (member removal or addition)
     * while migrations are not allowed by current cluster state
     * (such as {@link ClusterState#NO_MIGRATION}, {@link ClusterState#PASSIVE}),
     * {@code false} otherwise.
     */
    boolean shouldTriggerRepartitioningWhenClusterStateAllowsMigration() {
        return triggerRepartitioningWhenClusterStateAllowsMigration;
    }

    private void publishCompletedMigrations() {
        assert partitionService.isLocalMemberMaster();
        assert partitionStateManager.isInitialized();

        final List<MigrationInfo> migrations = getCompletedMigrationsCopy();
        if (logger.isFineEnabled()) {
            logger.fine("Publishing completed migrations [" + migrations.size() + "]: " + migrations);
        }

        OperationService operationService = nodeEngine.getOperationService();
        ClusterServiceImpl clusterService = node.clusterService;
        final Collection<Member> members = clusterService.getMembers();
        final AtomicInteger latch = new AtomicInteger(members.size() - 1);

        for (final Member member : members) {
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
                        logger.fine("Evicting " + migrations.size() + " completed migrations.");
                        evictCompletedMigrations(migrations);
                    }
                } else {
                    logger.fine("Failure while publishing completed migrations to " + member, t);
                    partitionService.sendPartitionRuntimeState(member.getAddress());
                }
            });
        }
    }

    public MigrationStats getStats() {
        return stats;
    }

    /**
     * Invoked on the master node. Rearranges the partition table if there is no recent activity in the cluster after
     * this task has been scheduled, schedules migrations and syncs the partition state.
     * Also schedules a {@link ProcessShutdownRequestsTask}. Acquires partition service lock.
     */
    private class RepartitioningTask implements MigrationRunnable {
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
                        logger.fine("Migrations are not allowed yet, "
                                + "repartitioning will be triggered when cluster state allows migrations.");
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

            PartitionReplica[][] newState = partitionStateManager.repartition(shutdownRequestedMembers, null);
            if (newState == null) {
                migrationQueue.add(new ProcessShutdownRequestsTask());
                return null;
            }

            if (!migrationsTasksAllowed()) {
                return null;
            }
            return newState;
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

            logger.fine("Cluster state doesn't allow repartitioning. RepartitioningTask will only assign lost partitions.");
            InternalPartition[] partitions = partitionStateManager.getPartitions();
            PartitionIdSet partitionIds = Arrays.stream(partitions)
                    .filter(p -> InternalPartition.replicaIndices().allMatch(i -> p.getReplica(i) == null))
                    .map(InternalPartition::getPartitionId)
                    .collect(Collectors.toCollection(() -> new PartitionIdSet(partitions.length)));

            if (!partitionIds.isEmpty()) {
                PartitionReplica[][] state = partitionStateManager.repartition(shutdownRequestedMembers, partitionIds);
                if (state != null) {
                    logger.warning("Assigning new owners for " + partitionIds.size() + " LOST partitions!");

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
                } else {
                    logger.warning("Unable to assign LOST partitions");
                }
            }
        }

        /** Processes the new partition state by planning and scheduling migrations. */
        private void processNewPartitionState(PartitionReplica[][] newState) {
            int migrationCount = 0;
            List<Queue<MigrationInfo>> migrations = new ArrayList<>(newState.length);
            Int2ObjectHashMap<PartitionReplica> lostPartitions = new Int2ObjectHashMap<>();

            for (int partitionId = 0; partitionId < newState.length; partitionId++) {
                InternalPartitionImpl currentPartition = partitionStateManager.getPartitionImpl(partitionId);
                PartitionReplica[] currentReplicas = currentPartition.getReplicas();
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
                migrations.add(migrationCollector.migrations);
                migrationCount += migrationCollector.migrations.size();
            }
            migrationCount += lostPartitions.size();

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
            }

            partitionService.publishPartitionRuntimeState();

            if (migrationCount > 0) {
                scheduleMigrations(migrations);
                // Schedule a task to publish completed migrations after all migrations tasks are completed.
                migrationQueue.add(new PublishCompletedMigrationsTask());
            }
            logMigrationStatistics(migrationCount);
        }

        /** Schedules all migrations. */
        private void scheduleMigrations(List<Queue<MigrationInfo>> migrations) {
            boolean migrationScheduled;
            do {
                migrationScheduled = false;
                for (Queue<MigrationInfo> queue : migrations) {
                    MigrationInfo migration = queue.poll();
                    if (migration != null) {
                        migrationScheduled = true;
                        scheduleMigration(migration);
                    }
                }
            } while (migrationScheduled);
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
            public void migrate(PartitionReplica source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
                    PartitionReplica destination, int destinationCurrentReplicaIndex, int destinationNewReplicaIndex) {

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

    /**
     * Invoked on the master node to migrate a partition (not including promotions). It will execute the
     * {@link MigrationRequestOperation} on the partition owner.
     */
    class MigrateTask implements MigrationRunnable {
        private final MigrationInfo migrationInfo;

        MigrateTask(MigrationInfo migrationInfo) {
            this.migrationInfo = migrationInfo;
            migrationInfo.setMaster(node.getThisAddress());
        }

        @Override
        public void run() {
            if (!partitionService.isLocalMemberMaster()) {
                return;
            }
            if (migrationInfo.getSource() == null
                    && migrationInfo.getDestinationCurrentReplicaIndex() > 0
                    && migrationInfo.getDestinationNewReplicaIndex() == 0) {

                throw new AssertionError("Promotion migrations should be handled by "
                        + RepairPartitionTableTask.class.getSimpleName() + "! -> " + migrationInfo);
            }

            Member partitionOwner = checkMigrationParticipantsAndGetPartitionOwner();
            if (partitionOwner == null) {
                return;
            }
            long start = System.nanoTime();
            try {
                beforeMigration();
                Boolean result = executeMigrateOperation(partitionOwner);
                processMigrationResult(partitionOwner, result);
            } catch (Throwable t) {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINE;
                logger.log(level, "Error during " + migrationInfo, t);
                migrationOperationFailed(partitionOwner);
            } finally {
                long elapsed = System.nanoTime() - start;
                stats.recordMigrationTaskTime(elapsed);
                PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
                partitionEventManager.sendMigrationEvent(stats.toMigrationState(), migrationInfo,
                        TimeUnit.NANOSECONDS.toMillis(elapsed));
            }
        }

        /** Sends a migration event to the event listeners. */
        private void beforeMigration() {
            migrationInfo.setInitialPartitionVersion(partitionStateManager.getVersion());
            migrationInterceptor.onMigrationStart(MigrationParticipant.MASTER, migrationInfo);
            if (logger.isFineEnabled()) {
                logger.fine("Starting Migration: " + migrationInfo);
            }
        }

        /**
         * Checks if the partition owner is not {@code null}, the source and destinations are still members and returns the owner.
         * Returns {@code null} and reschedules the {@link ControlTask} if the checks failed.
         */
        private Member checkMigrationParticipantsAndGetPartitionOwner() {
            Member partitionOwner = getPartitionOwner();
            if (partitionOwner == null) {
                logger.fine("Partition owner is null. Ignoring " + migrationInfo);
                triggerRepartitioningAfterMigrationFailure();
                return null;
            }
            if (migrationInfo.getSource() != null) {
                PartitionReplica source = migrationInfo.getSource();
                if (node.getClusterService().getMember(source.address(), source.uuid()) == null) {
                    logger.fine("Source is not a member anymore. Ignoring " + migrationInfo);
                    triggerRepartitioningAfterMigrationFailure();
                    return null;
                }
            }
            PartitionReplica destination = migrationInfo.getDestination();
            if (node.getClusterService().getMember(destination.address(), destination.uuid()) == null) {
                logger.fine("Destination is not a member anymore. Ignoring " + migrationInfo);
                triggerRepartitioningAfterMigrationFailure();
                return null;
            }
            return partitionOwner;
        }

        /** Returns the partition owner or {@code null} if it is not set. */
        private Member getPartitionOwner() {
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
            PartitionReplica owner = partition.getOwnerReplicaOrNull();
            if (owner == null) {
                if (migrationInfo.isValid()) {
                    logger.severe("Skipping migration! Partition owner is not set! -> partitionId="
                            + migrationInfo.getPartitionId()
                            + ", " + partition + " -VS- " + migrationInfo);
                }
                return null;
            }
            return node.getClusterService().getMember(owner.address(), owner.uuid());
        }

        /** Completes the partition migration. The migration was successful if the {@code result} is {@link Boolean#TRUE}. */
        private void processMigrationResult(Member partitionOwner, Boolean result) {
            if (Boolean.TRUE.equals(result)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Finished Migration: " + migrationInfo);
                }
                migrationOperationSucceeded();
            } else {
                Level level = nodeEngine.isRunning() && migrationInfo.isValid() ? Level.WARNING : Level.FINE;
                if (logger.isLoggable(level)) {
                    logger.log(level, "Migration failed: " + migrationInfo);
                }
                migrationOperationFailed(partitionOwner);
            }
        }

        /**
         * Sends a {@link MigrationRequestOperation} to the {@code fromMember} and returns the migration result if the
         * migration was successful.
         */
        private Boolean executeMigrateOperation(Member fromMember) {
            long start = System.nanoTime();
            List<MigrationInfo> completedMigrations = getCompletedMigrationsCopy();
            int partitionStateVersion = partitionStateManager.getVersion();
            Operation op = new MigrationRequestOperation(migrationInfo, completedMigrations, partitionStateVersion,
                    fragmentedMigrationEnabled);
            Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, op,
                    fromMember.getAddress())
                    .setCallTimeout(partitionMigrationTimeout)
                    .setTryCount(InternalPartitionService.MIGRATION_RETRY_COUNT)
                    .setTryPauseMillis(InternalPartitionService.MIGRATION_RETRY_PAUSE).invoke();

            try {
                Object response = future.get();
                return (Boolean) nodeEngine.toObject(response);
            } catch (Throwable e) {
                Level level = nodeEngine.isRunning() && migrationInfo.isValid() ? Level.WARNING : Level.FINE;
                if (e instanceof ExecutionException && e.getCause() instanceof PartitionStateVersionMismatchException) {
                    level = Level.FINE;
                }
                if (logger.isLoggable(level)) {
                    logger.log(level, "Failed migration from " + fromMember + " for " + migrationInfo, e);
                }
            } finally {
                stats.recordMigrationOperationTime(System.nanoTime() - start);
            }
            return Boolean.FALSE;
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
            migrationInfo.setStatus(MigrationStatus.FAILED);
            migrationInterceptor.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, false);
            partitionServiceLock.lock();
            try {
                migrationInterceptor.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                scheduleActiveMigrationFinalization(migrationInfo);
                int delta = migrationInfo.getPartitionVersionIncrement() + 1;
                partitionStateManager.incrementVersion(delta);
                migrationInfo.setPartitionVersionIncrement(delta);
                node.getNodeExtension().onPartitionStateChange();
                addCompletedMigration(migrationInfo);

                if (!partitionOwner.localMember()) {
                    partitionService.sendPartitionRuntimeState(partitionOwner.getAddress());
                }
                if (!migrationInfo.getDestination().isIdentical(node.getLocalMember())) {
                    partitionService.sendPartitionRuntimeState(migrationInfo.getDestination().address());
                }

                triggerRepartitioningAfterMigrationFailure();
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /** Waits for some time and rerun the {@link ControlTask}. */
        private void triggerRepartitioningAfterMigrationFailure() {
            // Migration failed.
            // Pause migration process for a small amount of time, if a migration attempt is failed.
            // Otherwise, migration failures can do a busy spin until migration problem is resolved.
            // Migration can fail either a node's just joined and not completed start yet or it's just left the cluster.
            // Re-execute RepartitioningTask when all other migration tasks are done,
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
        private void migrationOperationSucceeded() {
            migrationInterceptor.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, true);
            long start = System.nanoTime();
            boolean commitSuccessful = commitMigrationToDestination(migrationInfo);
            stats.recordDestinationCommitTime(System.nanoTime() - start);
            partitionServiceLock.lock();
            try {
                if (commitSuccessful) {
                    migrationInfo.setStatus(MigrationStatus.SUCCESS);
                    migrationInterceptor.onMigrationCommit(MigrationParticipant.MASTER, migrationInfo);
                    assert migrationInfo.getInitialPartitionVersion() == partitionStateManager.getVersion()
                            : "Migration initial version: " + migrationInfo.getInitialPartitionVersion()
                            + ", Partition state version: " + partitionStateManager.getVersion();
                    // updates partition table after successful commit
                    InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
                    applyMigration(partition, migrationInfo);
                    assert migrationInfo.getFinalPartitionVersion() == partitionStateManager.getVersion()
                            : "Migration final version: " + migrationInfo.getFinalPartitionVersion()
                                + ", Partition state version: " + partitionStateManager.getVersion();
                } else {
                    migrationInfo.setStatus(MigrationStatus.FAILED);
                    migrationInterceptor.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                    int delta = migrationInfo.getPartitionVersionIncrement() + 1;
                    partitionStateManager.incrementVersion(delta);
                    migrationInfo.setPartitionVersionIncrement(delta);

                    if (!migrationInfo.getDestination().isIdentical(node.getLocalMember())) {
                        partitionService.sendPartitionRuntimeState(migrationInfo.getDestination().address());
                    }
                    triggerRepartitioningAfterMigrationFailure();
                }
                addCompletedMigration(migrationInfo);
                scheduleActiveMigrationFinalization(migrationInfo);
                node.getNodeExtension().onPartitionStateChange();

                if (completedMigrations.size() >= PUBLISH_COMPLETED_MIGRATIONS_BATCH_SIZE) {
                    publishCompletedMigrations();
                }
            } finally {
                partitionServiceLock.unlock();
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + "migrationInfo=" + migrationInfo + '}';
        }
    }

    /**
     * Checks if the partition table needs repairing once the partitions have been initialized (assigned).
     * This means that it will:
     * <li>Remove unknown addresses from the partition table</li>
     * <li>Promote the partition replicas if necessary (the partition owner is missing)</li>
     * </ul>
     * If the promotions are successful, schedules the {@link RepartitioningTask}. If the process was not successful
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
                logger.fine("Will not repair partition table at the moment. "
                        + "Cluster state does not allow to modify partition table.");
                return;
            }

            Map<PartitionReplica, Collection<MigrationInfo>> promotions = removeUnknownMembersAndCollectPromotions();
            boolean success = promoteBackupsForMissingOwners(promotions);
            partitionServiceLock.lock();
            try {
                if (success) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("RepartitioningTask scheduled");
                    }
                    migrationQueue.add(new RepartitioningTask());
                } else {
                    triggerControlTask();
                }
            } finally {
                partitionServiceLock.unlock();
            }
        }

        /**
         * Removes members from the partition table which are not registered as cluster members and checks
         * if any partitions need promotion (partition owners are missing).
         * Invoked on the master node. Acquires partition service lock.
         *
         * @return promotions that need to be sent, grouped by target replica
         */
        private Map<PartitionReplica, Collection<MigrationInfo>> removeUnknownMembersAndCollectPromotions() {
            partitionServiceLock.lock();
            try {
                partitionStateManager.removeUnknownMembers();

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
         *  @param destination the promotion destination
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
                    // each promotion increments version by 2
                    int delta = 2 * migrations.size() + 1;
                    partitionService.getPartitionStateManager().incrementVersion(delta);
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
                        logger.finest("partitionId=" + partition.getPartitionId()
                                + " owner is removed. there is no other replica to shift up. " + partition);
                    }
                }
                if (destination != null) {
                    MigrationInfo migration = new MigrationInfo(partitionId, null, destination, -1, -1, index, 0);
                    migration.setMaster(node.getThisAddress());
                    migration.setStatus(MigrationInfo.MigrationStatus.SUCCESS);
                    return migration;
                }
            }
            if (partition.getOwnerReplicaOrNull() == null) {
                logger.warning("partitionId=" + partitionId + " is completely lost!");
                PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
                partitionEventManager.sendPartitionLostEvent(partitionId, InternalPartition.MAX_BACKUP_COUNT);
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
            assert migrations.size() > 0 : "No promotions to commit! destination=" + destination;

            Member member = node.getClusterService().getMember(destination.address(), destination.uuid());
            if (member == null) {
                logger.warning("Cannot commit promotions. Destination " + destination + " is not a member anymore");
                return false;
            }
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest("Sending promotion commit operation to " + destination + " for " + migrations);
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
                    logger.fine("Promotion commit failed for " + migrationsSize + " migrations"
                            + " since this node is shutting down.");
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
                if (shutdownRequestCount > 0) {
                    if (shutdownRequestCount == nodeEngine.getClusterService().getSize(DATA_MEMBER_SELECTOR)) {
                        for (Member member : shutdownRequestedMembers) {
                            sendShutdownOperation(member.getAddress());
                        }
                    } else {
                        boolean present = false;
                        for (Member member : shutdownRequestedMembers) {
                            if (partitionStateManager.isAbsentInPartitionTable(member)) {
                                sendShutdownOperation(member.getAddress());
                            } else {
                                logger.warning(member + " requested to shutdown but still in partition table");
                                present  = true;
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
}
