/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.PartitionStateVersionMismatchException;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.impl.MigrationPlanner.MigrationDecisionCallback;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.partition.operation.LegacyMigrationRequestOperation;
import com.hazelcast.internal.partition.operation.MigrationCommitOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequest;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;
import com.hazelcast.internal.partition.operation.PromotionCommitOperation;
import com.hazelcast.internal.partition.operation.ShutdownResponseOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;
import com.hazelcast.util.MutableInteger;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.scheduler.CoalescingDelayedTrigger;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;

/**
 * Maintains migration system state and manages migration operations performed within the cluster.
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:methodcount"})
public class MigrationManager {

    private static final boolean ASSERTION_ENABLED = MigrationManager.class.desiredAssertionStatus();
    private static final int PARTITION_STATE_VERSION_INCREMENT_DELTA_ON_MIGRATION_FAILURE = 2;
    private static final int MIGRATION_PAUSE_DURATION_SECONDS_ON_MIGRATION_FAILURE = 3;
    private static final String INVALID_UUID = "<invalid-uuid>";

    final long partitionMigrationInterval;
    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;
    private final PartitionStateManager partitionStateManager;
    private final MigrationQueue migrationQueue = new MigrationQueue();
    private final MigrationThread migrationThread;
    private final AtomicBoolean migrationAllowed = new AtomicBoolean(true);
    @Probe(name = "lastRepartitionTime")
    private final AtomicLong lastRepartitionTime = new AtomicLong();
    private final long partitionMigrationTimeout;
    private final CoalescingDelayedTrigger delayedResumeMigrationTrigger;
    private final Set<Address> shutdownRequestedAddresses = new HashSet<Address>();
    // updates will be done under lock, but reads will be multithreaded.
    private volatile MigrationInfo activeMigrationInfo;
    // both reads and updates will be done under lock!
    private final LinkedHashSet<MigrationInfo> completedMigrations = new LinkedHashSet<MigrationInfo>();
    @Probe
    private final AtomicLong completedMigrationCounter = new AtomicLong();
    private volatile InternalMigrationListener internalMigrationListener
            = new InternalMigrationListener.NopInternalMigrationListener();
    private final Lock partitionServiceLock;
    private final MigrationPlanner migrationPlanner;
    private final boolean fragmentedMigrationEnabled;

    MigrationManager(Node node, InternalPartitionServiceImpl service, Lock partitionServiceLock) {
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.partitionService = service;
        this.logger = node.getLogger(getClass());
        this.partitionServiceLock = partitionServiceLock;
        migrationPlanner = new MigrationPlanner(node.getLogger(MigrationPlanner.class));
        HazelcastProperties properties = node.getProperties();
        long intervalMillis = properties.getMillis(GroupProperty.PARTITION_MIGRATION_INTERVAL);
        partitionMigrationInterval = (intervalMillis > 0 ? intervalMillis : 0);
        partitionMigrationTimeout = properties.getMillis(GroupProperty.PARTITION_MIGRATION_TIMEOUT);
        fragmentedMigrationEnabled = properties.getBoolean(GroupProperty.PARTITION_FRAGMENTED_MIGRATION_ENABLED);
        partitionStateManager = partitionService.getPartitionStateManager();
        ILogger migrationThreadLogger = node.getLogger(MigrationThread.class);
        String hzName = nodeEngine.getHazelcastInstance().getName();
        migrationThread = new MigrationThread(this, hzName, migrationThreadLogger, migrationQueue);
        long migrationPauseDelayMs = TimeUnit.SECONDS.toMillis(MIGRATION_PAUSE_DURATION_SECONDS_ON_MIGRATION_FAILURE);
        ExecutionService executionService = nodeEngine.getExecutionService();
        delayedResumeMigrationTrigger = new CoalescingDelayedTrigger(
                executionService, migrationPauseDelayMs, 2 * migrationPauseDelayMs, new Runnable() {
            @Override
            public void run() {
                resumeMigration();
            }
        });
    }

    @Probe(name = "migrationActive")
    private int migrationActiveProbe() {
        return migrationAllowed.get() ? 1 : 0;
    }

    void pauseMigration() {
        migrationAllowed.set(false);
    }

    void resumeMigration() {
        migrationAllowed.set(true);
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
    boolean isMigrationAllowed() {
        return migrationAllowed.get();
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
    private void finalizeMigration(MigrationInfo migrationInfo) {
        try {
            Address thisAddress = node.getThisAddress();
            int partitionId = migrationInfo.getPartitionId();

            boolean source = thisAddress.equals(migrationInfo.getSource());
            boolean destination = thisAddress.equals(migrationInfo.getDestination());

            assert migrationInfo.getStatus() == MigrationStatus.SUCCESS
                    || migrationInfo.getStatus() == MigrationStatus.FAILED : "Invalid migration: " + migrationInfo;

            if (source || destination) {
                boolean success = migrationInfo.getStatus() == MigrationStatus.SUCCESS;

                MigrationParticipant participant = source ? MigrationParticipant.SOURCE : MigrationParticipant.DESTINATION;
                if (success) {
                    internalMigrationListener.onMigrationCommit(participant, migrationInfo);
                } else {
                    internalMigrationListener.onMigrationRollback(participant, migrationInfo);
                }

                MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                FinalizeMigrationOperation op = new FinalizeMigrationOperation(migrationInfo, endpoint, success);

                op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setValidateTarget(false)
                        .setService(partitionService);
                nodeEngine.getOperationService().execute(op);
                removeActiveMigration(partitionId);
            } else {
                final Address partitionOwner = partitionStateManager.getPartitionImpl(partitionId).getOwnerOrNull();
                if (node.getThisAddress().equals(partitionOwner)) {
                    removeActiveMigration(partitionId);
                    partitionStateManager.clearMigratingFlag(partitionId);
                } else {
                    logger.severe("Failed to finalize migration because this member " + thisAddress
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
            // we use activeMigrationInfo because it contains migrated replica fragment namespaces
            final MigrationInfo activeMigrationInfo = this.activeMigrationInfo;
            if (activeMigrationInfo != null && migrationInfo.equals(activeMigrationInfo)) {
                if (activeMigrationInfo.startProcessing()) {
                    activeMigrationInfo.setStatus(migrationInfo.getStatus());
                    finalizeMigration(activeMigrationInfo);
                } else {
                    logger.info("Scheduling finalization of " + migrationInfo
                            + ", because migration process is currently running.");
                    nodeEngine.getExecutionService().schedule(new Runnable() {
                        @Override
                        public void run() {
                            scheduleActiveMigrationFinalization(activeMigrationInfo);
                        }
                    }, 3, TimeUnit.SECONDS);
                }
                return;
            }

            if (migrationInfo.getSourceCurrentReplicaIndex() > 0
                    && node.getThisAddress().equals(migrationInfo.getSource())) {
                // OLD BACKUP
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
    private boolean commitMigrationToDestination(Address destination, MigrationInfo migration) {
        assert migration != null : "No migrations to commit! destination=" + destination;

        if (node.getThisAddress().equals(destination)) {
            if (logger.isFinestEnabled()) {
                logger.finest("Shortcutting migration commit, since destination is master. -> " + migration);
            }
            return true;
        }

        MemberImpl member = node.getClusterService().getMember(destination);
        if (member == null) {
            logger.warning("Destination " + destination + " is not member anymore");
            return false;
        }

        try {
            if (logger.isFinestEnabled()) {
                logger.finest("Sending commit operation to " + destination + " for " + migration);
            }
            PartitionRuntimeState partitionState = partitionService.createMigrationCommitPartitionState(migration);
            String destinationUuid = member.getUuid();
            MigrationCommitOperation operation = new MigrationCommitOperation(partitionState, destinationUuid);
            Future<Boolean> future = nodeEngine.getOperationService()
                    .createInvocationBuilder(SERVICE_NAME, operation, destination)
                    .setTryCount(Integer.MAX_VALUE)
                    .setCallTimeout(Long.MAX_VALUE).invoke();

            boolean result = future.get();
            if (logger.isFinestEnabled()) {
                logger.finest("Migration commit result " + result + " from " + destination + " for " + migration);
            }
            return result;
        } catch (Throwable t) {
            logMigrationCommitFailure(destination, migration, t);
        }
        return false;
    }

    private void logMigrationCommitFailure(Address destination, MigrationInfo migration, Throwable t) {
        boolean memberLeft = t instanceof MemberLeftException
                    || t.getCause() instanceof TargetNotMemberException
                    || t.getCause() instanceof HazelcastInstanceNotActiveException;

        if (memberLeft) {
            if (node.getThisAddress().equals(destination)) {
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

        partitionServiceLock.lock();
        try {
            boolean added = completedMigrations.add(migrationInfo);
            if (added) {
                completedMigrationCounter.incrementAndGet();
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
     * Removes all completed migrations up to the given {@code currentMigration}.
     *
     * @param currentMigration the migration up to which migrations will be evicted
     */
    private void evictCompletedMigrations(MigrationInfo currentMigration) {
        partitionServiceLock.lock();
        try {
            assert completedMigrations.contains(currentMigration) : currentMigration + " to evict is not in completed migrations";

            Iterator<MigrationInfo> iter = completedMigrations.iterator();
            while (iter.hasNext()) {
                MigrationInfo migration = iter.next();
                iter.remove();
                // evict completed migrations including current migration
                if (migration.equals(currentMigration)) {
                    return;
                }
            }

        } finally {
            partitionServiceLock.unlock();
        }
    }

    /** Clears the migration queue and triggers the control task. Called on the master node. */
    void triggerControlTask() {
        migrationQueue.clear();
        if (!node.getClusterService().isJoined()) {
            logger.fine("Node is not joined, will not trigger ControlTask");
            return;
        }
        if (!node.isMaster()) {
            logger.fine("Node is not master, will not trigger ControlTask");
            return;
        }
        migrationQueue.add(new ControlTask());
        if (logger.isFinestEnabled()) {
            logger.finest("Migration queue is cleared and control task is scheduled");
        }
    }

    InternalMigrationListener getInternalMigrationListener() {
        return internalMigrationListener;
    }

    void setInternalMigrationListener(InternalMigrationListener listener) {
        Preconditions.checkNotNull(listener);
        internalMigrationListener = listener;
    }

    void resetInternalMigrationListener() {
        internalMigrationListener = new InternalMigrationListener.NopInternalMigrationListener();
    }

    void onShutdownRequest(Address address) {
        if (!partitionStateManager.isInitialized()) {
            sendShutdownOperation(address);
            return;
        }
        ClusterState clusterState = node.getClusterService().getClusterState();
        if (!clusterState.isMigrationAllowed() && clusterState != ClusterState.IN_TRANSITION) {
            sendShutdownOperation(address);
            return;
        }
        if (shutdownRequestedAddresses.add(address)) {
            logger.info("Shutdown request of " + address + " is handled");
            triggerControlTask();
        }
    }

    void onMemberRemove(MemberImpl member) {
        Address deadAddress = member.getAddress();
        shutdownRequestedAddresses.remove(deadAddress);
        MigrationInfo activeMigration = activeMigrationInfo;
        if (activeMigration != null) {
            if (deadAddress.equals(activeMigration.getSource())
                    || deadAddress.equals(activeMigration.getDestination())) {
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
            return new ArrayList<MigrationInfo>(completedMigrations);
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
        final Address[] addresses = Arrays.copyOf(partition.getReplicaAddresses(), InternalPartition.MAX_REPLICA_COUNT);
        if (migrationInfo.getSourceCurrentReplicaIndex() > -1) {
            addresses[migrationInfo.getSourceCurrentReplicaIndex()] = null;
        }
        if (migrationInfo.getDestinationCurrentReplicaIndex() > -1) {
            addresses[migrationInfo.getDestinationCurrentReplicaIndex()] = null;
        }
        addresses[migrationInfo.getDestinationNewReplicaIndex()] = migrationInfo.getDestination();
        if (migrationInfo.getSourceNewReplicaIndex() > -1) {
            addresses[migrationInfo.getSourceNewReplicaIndex()] = migrationInfo.getSource();
        }
        partition.setReplicaAddresses(addresses);
    }

    Set<Address> getShutdownRequestedAddresses() {
        return shutdownRequestedAddresses;
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

    MigrationRunnable getActiveTask() {
        return migrationThread.getActiveTask();
    }

    private String getMemberUuid(Address address) {
        MemberImpl member = node.getClusterService().getMember(address);
        return member != null ? member.getUuid() : INVALID_UUID;
    }

    /**
     * Invoked on the master node. Rearranges the partition table if there is no recent activity in the cluster after
     * this task has been scheduled, schedules migrations and syncs the partition state.
     * Also schedules a {@link ProcessShutdownRequestsTask}. Acquires partition service lock.
     */
    private class RepartitioningTask implements MigrationRunnable {
        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }
            partitionServiceLock.lock();
            try {
                Address[][] newState = repartition();
                if (newState == null) {
                    return;
                }
                lastRepartitionTime.set(Clock.currentTimeMillis());
                processNewPartitionState(newState);

                if (ASSERTION_ENABLED && isMigrationAllowedByClusterState()) {
                    migrationQueue.add(new AssertPartitionTableTask(partitionService.getMaxAllowedBackupCount()));
                }

                migrationQueue.add(new ProcessShutdownRequestsTask());
                partitionService.syncPartitionRuntimeState();
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
        private Address[][] repartition() {
            if (!isRepartitioningAllowed()) {
                return null;
            }
            if (!isMigrationAllowedByClusterState()) {
                logger.fine("Cluster state doesn't allow repartitioning. RepartitioningTask will only assign lost partitions.");
                assignCompletelyLostPartitions();
                return null;
            }

            Address[][] newState = partitionStateManager.repartition(shutdownRequestedAddresses, null);
            if (newState == null) {
                migrationQueue.add(new ProcessShutdownRequestsTask());
                return null;
            }
            if (!isRepartitioningAllowed()) {
                return null;
            }
            return newState;
        }

        private void assignCompletelyLostPartitions() {
            Collection<Integer> partitions = new ArrayList<Integer>();
            for (InternalPartition partition : partitionStateManager.getPartitions()) {
                boolean empty = true;
                for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                    empty &= partition.getReplicaAddress(index) == null;
                }
                if (empty) {
                    partitions.add(partition.getPartitionId());
                }
            }
            if (!partitions.isEmpty()) {
                Address[][] state = partitionStateManager.repartition(shutdownRequestedAddresses, partitions);
                for (int partitionId : partitions) {
                    InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
                    Address[] replicas = state[partitionId];

                    assignLostPartitionOwner(partition, replicas[0]);
                    partition.setReplicaAddresses(replicas);
                }
                logger.warning("Assigning new owners for " + partitions.size() + " LOST partitions!");
                partitionService.syncPartitionRuntimeState();
            }
        }

        /** Processes the new partition state by planning and scheduling migrations. */
        private void processNewPartitionState(Address[][] newState) {
            final MutableInteger lostCount = new MutableInteger();
            final MutableInteger migrationCount = new MutableInteger();
            final List<Queue<MigrationInfo>> migrations = new ArrayList<Queue<MigrationInfo>>(newState.length);

            for (int partitionId = 0; partitionId < newState.length; partitionId++) {
                InternalPartitionImpl currentPartition = partitionStateManager.getPartitionImpl(partitionId);
                Address[] currentReplicas = currentPartition.getReplicaAddresses();
                Address[] newReplicas = newState[partitionId];

                MigrationCollector migrationCollector = new MigrationCollector(currentPartition, migrationCount, lostCount);
                if (logger.isFinestEnabled()) {
                    logger.finest("Planning migrations for partitionId=" + partitionId
                            + ". Current replicas: " + Arrays.toString(currentReplicas)
                            + ", New replicas: " + Arrays.toString(newReplicas));
                }
                migrationPlanner.planMigrations(currentReplicas, newReplicas, migrationCollector);
                migrationPlanner.prioritizeCopiesAndShiftUps(migrationCollector.migrations);
                migrations.add(migrationCollector.migrations);
            }
            scheduleMigrations(migrations);
            logMigrationStatistics(migrationCount.value, lostCount.value);
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

        private void logMigrationStatistics(int migrationCount, int lostCount) {
            if (lostCount > 0) {
                logger.warning("Assigning new owners for " + lostCount + " LOST partitions!");
            }
            if (migrationCount > 0) {
                logger.info("Re-partitioning cluster data... Migration queue size: " + migrationCount);
            } else {
                logger.info("Partition balance is ok, no need to re-partition cluster data... ");
            }
        }

        private void assignLostPartitionOwner(InternalPartitionImpl partition, Address newOwner) {
            int partitionId = partition.getPartitionId();
            String destinationUuid = getMemberUuid(newOwner);
            MigrationInfo migrationInfo = new MigrationInfo(partitionId, null, null, newOwner, destinationUuid, -1, -1, -1, 0);
            PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
            partitionEventManager.sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.STARTED);
            partition.setReplicaAddress(0, newOwner);
            partitionEventManager.sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.COMPLETED);
        }

        /**
         * Returns {@code true} if there are no migrations in the migration queue, no new node is joining, there is no
         * ongoing repartitioning,
         * otherwise triggers the control task.
         */
        private boolean isRepartitioningAllowed() {
            boolean migrationAllowed = isMigrationAllowed();
            boolean hasMigrationTasks = migrationQueue.migrationTaskCount() > 1;
            if (migrationAllowed && !hasMigrationTasks) {
                return true;
            }
            triggerControlTask();
            return false;
        }

        private boolean isMigrationAllowedByClusterState() {
            ClusterState clusterState = node.getClusterService().getClusterState();
            return clusterState.isMigrationAllowed();
        }

        private class MigrationCollector implements MigrationDecisionCallback {
            private final int partitionId;
            private final InternalPartitionImpl partition;
            private final MutableInteger migrationCount;
            private final MutableInteger lostCount;
            private final LinkedList<MigrationInfo> migrations = new LinkedList<MigrationInfo>();

            MigrationCollector(InternalPartitionImpl partition, MutableInteger migrationCount, MutableInteger lostCount) {
                partitionId = partition.getPartitionId();
                this.partition = partition;
                this.migrationCount = migrationCount;
                this.lostCount = lostCount;
            }

            @Override
            public void migrate(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
                    Address destination, int destinationCurrentReplicaIndex, int destinationNewReplicaIndex) {

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

                    lostCount.value++;
                    assignLostPartitionOwner(partition, destination);

                } else if (destination == null && sourceNewReplicaIndex == -1) {
                    assert source != null : "partitionId=" + partitionId + " source is null";
                    assert sourceCurrentReplicaIndex != -1
                            : "partitionId=" + partitionId + " invalid index: " + sourceCurrentReplicaIndex;
                    assert sourceCurrentReplicaIndex != 0
                            : "partitionId=" + partitionId + " invalid index: " + sourceCurrentReplicaIndex;
                    final Address currentSource = partition.getReplicaAddress(sourceCurrentReplicaIndex);
                    assert source.equals(currentSource)
                            : "partitionId=" + partitionId + " current source="
                            + source + " is different than expected source=" + source;

                    partition.setReplicaAddress(sourceCurrentReplicaIndex, null);
                } else {
                    String sourceUuid = getMemberUuid(source);
                    String destinationUuid = getMemberUuid(destination);
                    MigrationInfo migration = new MigrationInfo(partitionId, source, sourceUuid, destination, destinationUuid,
                            sourceCurrentReplicaIndex, sourceNewReplicaIndex,
                            destinationCurrentReplicaIndex, destinationNewReplicaIndex);
                    migrationCount.value++;
                    migrations.add(migration);
                }
            }
        }
    }

    /**
     * Assertion task for checking the consistency of the partition table. Invoked on the master node to assert if the
     * partition table is :
     * <ul>
     *     <li>missing some replicas (the address is {@code null} but there are no nodes currently shutting down)</li>
     *     <li>has more than the maximum configured replica count</li>
     *     <li>has duplicate addresses in the same partition</li>
     * </ul>
     * Acquires partition service lock.
     */
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    private final class AssertPartitionTableTask implements MigrationRunnable {
        final int maxBackupCount;

        private AssertPartitionTableTask(int maxBackupCount) {
            this.maxBackupCount = maxBackupCount;
        }

        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }
            partitionServiceLock.lock();
            try {
                if (!partitionStateManager.isInitialized()) {
                    logger.info("Skipping partition table assertions since partition table state is reset");
                    return;
                }
                final InternalPartition[] partitions = partitionStateManager.getPartitions();
                final Set<Address> replicas = new HashSet<Address>();
                for (InternalPartition partition : partitions) {
                    replicas.clear();
                    for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                        final Address address = partition.getReplicaAddress(index);
                        if (index <= maxBackupCount) {
                            assert !shutdownRequestedAddresses.isEmpty() || address != null
                                    : "Repartitioning problem, missing replica! "
                                    + "Current replica: " + index + ", Max backups: " + maxBackupCount
                                    + " -> " + partition;
                        } else {
                            assert address == null : "Repartitioning problem, leaking replica! "
                                    + "Current replica: " + index + ", Max backups: " + maxBackupCount
                                    + " -> " + partition;
                        }
                        assert address == null || replicas.add(address) : "Duplicate address in " + partition;
                    }
                }
            } finally {
                partitionServiceLock.unlock();
            }
        }
    }

    /**
     * Invoked on the master node to migrate a partition (not including promotions). It will execute the
     * {@link MigrationRequestOperation} on the partition owner.
     */
    class MigrateTask implements MigrationRunnable {
        final MigrationInfo migrationInfo;

        MigrateTask(MigrationInfo migrationInfo) {
            this.migrationInfo = migrationInfo;
            migrationInfo.setMaster(node.getThisAddress());
        }

        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }
            if (migrationInfo.getSource() == null
                    && migrationInfo.getDestinationCurrentReplicaIndex() > 0
                    && migrationInfo.getDestinationNewReplicaIndex() == 0) {

                throw new AssertionError("Promotion migrations should be handled by "
                        + RepairPartitionTableTask.class.getSimpleName() + "! -> " + migrationInfo);
            }
            try {
                MemberImpl partitionOwner = checkMigrationParticipantsAndGetPartitionOwner();
                if (partitionOwner == null) {
                    return;
                }
                beforeMigration();
                Boolean result = executeMigrateOperation(partitionOwner);
                processMigrationResult(result);
            } catch (Throwable t) {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINE;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] during " + migrationInfo);
                logger.finest(t);
                migrationOperationFailed();
            }
        }

        /** Sends a migration event to the event listeners. */
        private void beforeMigration() {
            internalMigrationListener.onMigrationStart(MigrationParticipant.MASTER, migrationInfo);
            partitionService.getPartitionEventManager()
                    .sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.STARTED);
            if (logger.isFineEnabled()) {
                logger.fine("Starting Migration: " + migrationInfo);
            }
        }

        /**
         * Checks if the partition owner is not {@code null}, the source and destinations are still members and returns the owner.
         * Returns {@code null} and reschedules the {@link ControlTask} if the checks failed.
         */
        private MemberImpl checkMigrationParticipantsAndGetPartitionOwner() {
            MemberImpl partitionOwner = getPartitionOwner();
            if (partitionOwner == null) {
                logger.fine("Partition owner is null. Ignoring " + migrationInfo);
                triggerRepartitioningAfterMigrationFailure();
                return null;
            }
            if (migrationInfo.getSource() != null) {
                if (node.getClusterService().getMember(migrationInfo.getSource()) == null) {
                    logger.fine("Source is not member anymore. Ignoring " + migrationInfo);
                    triggerRepartitioningAfterMigrationFailure();
                    return null;
                }
            }
            if (node.getClusterService().getMember(migrationInfo.getDestination()) == null) {
                logger.fine("Destination is not member anymore. Ignoring " + migrationInfo);
                triggerRepartitioningAfterMigrationFailure();
                return null;
            }
            return partitionOwner;
        }

        /** Returns the partition owner or {@code null} if it is not set. */
        private MemberImpl getPartitionOwner() {
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                if (migrationInfo.isValid()) {
                    logger.severe("Skipping migration! Partition owner is not set! -> partitionId="
                            + migrationInfo.getPartitionId()
                            + ", " + partition + " -VS- " + migrationInfo);
                }
                return null;
            }
            return node.getClusterService().getMember(owner);
        }

        /** Completes the partition migration. The migration was successful if the {@code result} is {@link Boolean#TRUE}. */
        private void processMigrationResult(Boolean result) {
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
                migrationOperationFailed();
            }
        }

        /**
         * Sends a {@link MigrationRequestOperation} to the {@code fromMember} and returns the migration result if the
         * migration was successful.
         */
        private Boolean executeMigrateOperation(MemberImpl fromMember) {
            int partitionStateVersion = partitionService.getPartitionStateVersion();
            Version clusterVersion = node.getClusterService().getClusterVersion();
            Operation migrationRequestOp = clusterVersion.isGreaterOrEqual(Versions.V3_9)
                    ? new MigrationRequestOperation(migrationInfo, partitionStateVersion, fragmentedMigrationEnabled)
                    : new LegacyMigrationRequestOperation(migrationInfo, partitionStateVersion);
            Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, migrationRequestOp,
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
         * <li>publishes a {@link MigrationEvent}</li>
         * </ul>
         * <p>
         * Acquires the partition state lock.
         */
        private void migrationOperationFailed() {
            migrationInfo.setStatus(MigrationStatus.FAILED);
            internalMigrationListener.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, false);
            partitionServiceLock.lock();
            try {
                addCompletedMigration(migrationInfo);
                internalMigrationListener.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                scheduleActiveMigrationFinalization(migrationInfo);
                int delta = PARTITION_STATE_VERSION_INCREMENT_DELTA_ON_MIGRATION_FAILURE;
                partitionService.getPartitionStateManager().incrementVersion(delta);
                node.getNodeExtension().onPartitionStateChange();
                if (partitionService.syncPartitionRuntimeState()) {
                    evictCompletedMigrations(migrationInfo);
                }
                triggerRepartitioningAfterMigrationFailure();
            } finally {
                partitionServiceLock.unlock();
            }

            partitionService.getPartitionEventManager().sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.FAILED);

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
         * <li>publishes a {@link MigrationEvent}</li>
         * </ul>
         * <p>
         * Triggers the {@link ControlTask} if the migration failed. Acquires the partition state lock to process the result
         * of the migration commit.
         */
        private void migrationOperationSucceeded() {
            internalMigrationListener.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, true);
            boolean commitSuccessful = commitMigrationToDestination(migrationInfo.getDestination(), migrationInfo);
            partitionServiceLock.lock();
            try {
                if (commitSuccessful) {
                    migrationInfo.setStatus(MigrationStatus.SUCCESS);
                    internalMigrationListener.onMigrationCommit(MigrationParticipant.MASTER, migrationInfo);
                    // updates partition table after successful commit
                    InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
                    applyMigration(partition, migrationInfo);
                } else {
                    migrationInfo.setStatus(MigrationStatus.FAILED);
                    internalMigrationListener.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                    int delta = PARTITION_STATE_VERSION_INCREMENT_DELTA_ON_MIGRATION_FAILURE;
                    partitionService.getPartitionStateManager().incrementVersion(delta);
                    triggerRepartitioningAfterMigrationFailure();
                }
                addCompletedMigration(migrationInfo);
                scheduleActiveMigrationFinalization(migrationInfo);
                node.getNodeExtension().onPartitionStateChange();
                if (partitionService.syncPartitionRuntimeState()) {
                    evictCompletedMigrations(migrationInfo);
                }
            } finally {
                partitionServiceLock.unlock();
            }
            PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
            partitionEventManager.sendMigrationEvent(migrationInfo,  MigrationEvent.MigrationStatus.COMPLETED);
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
            Map<Address, Collection<MigrationInfo>> promotions = removeUnknownAddressesAndCollectPromotions();
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
         * Removes addresses from the partition table which are not registered as cluster members and checks
         * if any partitions need promotion (partition owners are missing).
         * Invoked on the master node. Acquires partition service lock.
         *
         * @return promotions that need to be sent, grouped by target address
         */
        private Map<Address, Collection<MigrationInfo>> removeUnknownAddressesAndCollectPromotions() {
            partitionServiceLock.lock();
            try {
                partitionStateManager.removeUnknownAddresses();

                Map<Address, Collection<MigrationInfo>> promotions = new HashMap<Address, Collection<MigrationInfo>>();
                for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                    MigrationInfo migration = createPromotionMigrationIfOwnerIsNull(partitionId);
                    if (migration == null) {
                        continue;
                    }
                    Collection<MigrationInfo> migrations = promotions.get(migration.getDestination());
                    if (migrations == null) {
                        migrations = new ArrayList<MigrationInfo>();
                        promotions.put(migration.getDestination(), migrations);
                    }
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
         * @param promotions the promotions that need to be sent, grouped by target address
         * @return if all promotions were successful
         */
        private boolean promoteBackupsForMissingOwners(Map<Address, Collection<MigrationInfo>> promotions) {
            boolean allSucceeded = true;
            for (Map.Entry<Address, Collection<MigrationInfo>> entry : promotions.entrySet()) {
                Address destination = entry.getKey();
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
        private boolean commitPromotionMigrations(Address destination, Collection<MigrationInfo> migrations) {
            boolean success = commitPromotionsToDestination(destination, migrations);
            boolean local = node.getThisAddress().equals(destination);
            if (!local) {
                processPromotionCommitResult(destination, migrations, success);
            }
            partitionService.syncPartitionRuntimeState();
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
        private void processPromotionCommitResult(Address destination, Collection<MigrationInfo> migrations,
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

                        assert partition.getOwnerOrNull() == null : "Owner should be null: " + partition;
                        assert destination.equals(partition.getReplicaAddress(migration.getDestinationCurrentReplicaIndex()))
                                : "Invalid replica! Destination: " + destination + ", index: "
                                        + migration.getDestinationCurrentReplicaIndex() + ", " + partition;
                        // single partition update increments partition state version by 1
                        partition.swapAddresses(0, migration.getDestinationCurrentReplicaIndex());
                    }
                } else {
                    int delta = migrations.size() + 1;
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
            if (partition.getOwnerOrNull() == null) {
                Address destination = null;
                int index = 1;
                for (int i = index; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                    destination = partition.getReplicaAddress(i);
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
                    String destinationUuid = getMemberUuid(destination);
                    MigrationInfo migration =
                            new MigrationInfo(partitionId, null, null, destination, destinationUuid, -1, -1, index, 0);
                    migration.setMaster(node.getThisAddress());
                    migration.setStatus(MigrationInfo.MigrationStatus.SUCCESS);
                    return migration;
                }
            }
            if (partition.getOwnerOrNull() == null) {
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
        private boolean commitPromotionsToDestination(Address destination, Collection<MigrationInfo> migrations) {
            assert migrations.size() > 0 : "No promotions to commit! destination=" + destination;

            MemberImpl member = node.getClusterService().getMember(destination);
            if (member == null) {
                logger.warning("Destination " + destination + " is not member anymore");
                return false;
            }
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest("Sending commit operation to " + destination + " for " + migrations);
                }
                PartitionRuntimeState partitionState = partitionService.createPromotionCommitPartitionState(migrations);
                String destinationUuid = member.getUuid();
                PromotionCommitOperation op = new PromotionCommitOperation(partitionState, migrations, destinationUuid);
                Future<Boolean> future = nodeEngine.getOperationService()
                        .createInvocationBuilder(SERVICE_NAME, op, destination)
                        .setTryCount(Integer.MAX_VALUE)
                        .setCallTimeout(Long.MAX_VALUE).invoke();

                boolean result = future.get();
                if (logger.isFinestEnabled()) {
                    logger.finest("Promotion commit result " + result + " from " + destination
                            + " for migrations " + migrations);
                }
                return result;
            } catch (Throwable t) {
                logPromotionCommitFailure(destination, migrations, t);
            }
            return false;
        }

        private void logPromotionCommitFailure(Address destination, Collection<MigrationInfo> migrations, Throwable t) {
            boolean memberLeft = t instanceof MemberLeftException
                    || t.getCause() instanceof TargetNotMemberException
                    || t.getCause() instanceof HazelcastInstanceNotActiveException;

            int migrationsSize = migrations.size();
            if (memberLeft) {
                if (node.getThisAddress().equals(destination)) {
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
            if (!node.isMaster()) {
                return;
            }
            partitionServiceLock.lock();
            try {
                final int shutdownRequestCount = shutdownRequestedAddresses.size();
                if (shutdownRequestCount > 0) {
                    if (shutdownRequestCount == nodeEngine.getClusterService().getSize(DATA_MEMBER_SELECTOR)) {
                        for (Address address : shutdownRequestedAddresses) {
                            sendShutdownOperation(address);
                        }
                    } else {
                        boolean present = false;
                        for (Address address : shutdownRequestedAddresses) {
                            if (partitionStateManager.isAbsentInPartitionTable(address)) {
                                sendShutdownOperation(address);
                            } else {
                                logger.warning(address + " requested to shutdown but still in partition table");
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
}
