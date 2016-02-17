/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.partition.operation.MigrationCommitOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.scheduler.CoalescingDelayedTrigger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;
import static java.lang.Math.min;

/**
 * TODO: Javadoc Pending...
 *
 */
public class MigrationManager {

    private static final int DEFAULT_PAUSE_MILLIS = 1000;

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;

    private final PartitionStateManager partitionStateManager;

    final MigrationQueue migrationQueue = new MigrationQueue();

    private final MigrationThread migrationThread;

    private final AtomicBoolean migrationAllowed = new AtomicBoolean(true);

    @Probe(name = "lastRepartitionTime")
    private final AtomicLong lastRepartitionTime = new AtomicLong();

    private final CoalescingDelayedTrigger delayedResumeMigrationTrigger;

    final long partitionMigrationInterval;
    private final long partitionMigrationTimeout;

    // updates will be done under lock, but reads will be multithreaded.
    private volatile MigrationInfo activeMigrationInfo;

    // both reads and updates will be done under lock!
    private final LinkedList<MigrationInfo> completedMigrations = new LinkedList<MigrationInfo>();

    @Probe
    private final AtomicLong completedMigrationCounter = new AtomicLong();

    private volatile InternalMigrationListener internalMigrationListener
            = new InternalMigrationListener.NopInternalMigrationListener();

    // TODO: clarify lock usages.
    // One option is to remove lock from this class and caller to guarantee thread safety.
    private final Lock partitionServiceLock;

    public MigrationManager(Node node, InternalPartitionServiceImpl service, Lock partitionServiceLock) {
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.partitionService = service;
        this.logger = node.getLogger(getClass());
        this.partitionServiceLock = partitionServiceLock;

        partitionStateManager = partitionService.getPartitionStateManager();

        ExecutionService executionService = nodeEngine.getExecutionService();

        migrationThread = new MigrationThread(this, node.getHazelcastThreadGroup(), node.getLogger(MigrationThread.class));

        long intervalMillis = node.groupProperties.getMillis(GroupProperty.PARTITION_MIGRATION_INTERVAL);
        partitionMigrationInterval = (intervalMillis > 0 ? intervalMillis : 0);
        partitionMigrationTimeout = node.groupProperties.getMillis(GroupProperty.PARTITION_MIGRATION_TIMEOUT);

        long maxMigrationDelayMs = calculateMaxMigrationDelayOnMemberRemoved();
        long minMigrationDelayMs = calculateMigrationDelayOnMemberRemoved(maxMigrationDelayMs);
        this.delayedResumeMigrationTrigger = new CoalescingDelayedTrigger(
                executionService, minMigrationDelayMs, maxMigrationDelayMs, new Runnable() {
            @Override
            public void run() {
                resumeMigration();
            }
        });
    }

    private long calculateMaxMigrationDelayOnMemberRemoved() {
        // hard limit for migration pause is half of the call timeout. otherwise we might experience timeouts
        return node.groupProperties.getMillis(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS) / 2;
    }

    private long calculateMigrationDelayOnMemberRemoved(long maxDelayMs) {
        long migrationDelayMs = node.groupProperties.getMillis(GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS);

        long connectionErrorDetectionIntervalMs = node.groupProperties.getMillis(GroupProperty.CONNECTION_MONITOR_INTERVAL)
                * node.groupProperties.getInteger(GroupProperty.CONNECTION_MONITOR_MAX_FAULTS) * 5;
        migrationDelayMs = Math.max(migrationDelayMs, connectionErrorDetectionIntervalMs);

        long heartbeatIntervalMs = node.groupProperties.getMillis(GroupProperty.HEARTBEAT_INTERVAL_SECONDS);
        migrationDelayMs = Math.max(migrationDelayMs, heartbeatIntervalMs * 3);

        migrationDelayMs = min(migrationDelayMs, maxDelayMs);
        return migrationDelayMs;
    }

    @Probe(name = "migrationActive")
    private int migrationActiveProbe() {
        return migrationAllowed.get() ? 1 : 0;
    }

    public void pauseMigration() {
        migrationAllowed.set(false);
    }

    public void resumeMigration() {
        migrationAllowed.set(true);
    }

    void resumeMigrationEventually() {
        delayedResumeMigrationTrigger.executeWithDelay();
    }

    public boolean isMigrationAllowed() {
        return migrationAllowed.get();
    }

    private void finalizeMigrationInfo(MigrationInfo migrationInfo) {
        try {
            Address thisAddress = node.getThisAddress();
            boolean source = thisAddress.equals(migrationInfo.getSource());
            boolean destination = thisAddress.equals(migrationInfo.getDestination());
            if (source || destination) {
                int partitionId = migrationInfo.getPartitionId();
                InternalPartitionImpl migratingPartition = partitionStateManager.getPartitionImpl(partitionId);
                Address ownerAddress = migratingPartition.getOwnerOrNull();
                boolean success = migrationInfo.getDestination().equals(ownerAddress);
                InternalMigrationListener.MigrationParticipant
                        participant = source ? InternalMigrationListener.MigrationParticipant.SOURCE : InternalMigrationListener.MigrationParticipant.DESTINATION;
                if (success) {
                    internalMigrationListener.onMigrationCommit(participant, migrationInfo);
                } else {
                    internalMigrationListener.onMigrationRollback(participant, migrationInfo);
                }

                MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                FinalizeMigrationOperation op = new FinalizeMigrationOperation(endpoint, success);
                op.setPartitionId(partitionId)
                        .setNodeEngine(nodeEngine)
                        .setValidateTarget(false)
                        .setService(partitionService);
                nodeEngine.getOperationService().executeOperation(op);
            } else {
                logger.severe("Failed to finalize migration because this member " + thisAddress
                        + " is not a participant of the migration: " + migrationInfo);
            }
        } catch (Exception e) {
            logger.warning(e);
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    public boolean addActiveMigration(MigrationInfo migrationInfo) {
        partitionServiceLock.lock();
        try {
            if (activeMigrationInfo == null) {
                partitionStateManager.setMigrating(migrationInfo.getPartitionId(), true);
                activeMigrationInfo = migrationInfo;
                return true;
            }

            logger.warning(migrationInfo + " not added! Already existing active migration: " + activeMigrationInfo);
            return false;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    public MigrationInfo getActiveMigration(int partitionId) {
        MigrationInfo activeMigrationInfo = this.activeMigrationInfo;
        if (activeMigrationInfo != null && activeMigrationInfo.getPartitionId() == partitionId) {
            return activeMigrationInfo;
        }

        return null;
    }

    MigrationInfo getActiveMigration() {
        return activeMigrationInfo;
    }

    public boolean removeActiveMigration(int partitionId) {
        boolean success = false;
        partitionServiceLock.lock();
        try {
            if (activeMigrationInfo != null) {
                if (activeMigrationInfo.getPartitionId() == partitionId) {
                    partitionStateManager.setMigrating(partitionId, false);
                    this.activeMigrationInfo = null;
                    success = true;
                } else if (logger.isFinestEnabled()) {
                    logger.finest("active migration not remove because it has different partitionId! partitionId=" + partitionId
                            + " active migration=" + activeMigrationInfo);
                }
            }
        } finally {
            partitionServiceLock.unlock();
        }

        return success;
    }

    void scheduleActiveMigrationFinalization(final MigrationInfo migrationInfo) {
        partitionServiceLock.lock();
        try {
            if (migrationInfo.equals(activeMigrationInfo)) {
                if (activeMigrationInfo.startProcessing()) {
                    finalizeMigrationInfo(activeMigrationInfo);
                } else {
                    logger.info("Scheduling finalization of " + migrationInfo
                            + ", because migration process is currently running.");
                    nodeEngine.getExecutionService().schedule(new Runnable() {
                        @Override
                        public void run() {
                            scheduleActiveMigrationFinalization(migrationInfo);
                        }
                    }, 3, TimeUnit.SECONDS);
                }
            }
        } finally {
            partitionServiceLock.unlock();
        }
    }

    private boolean commitMigrationToDestination(MigrationInfo migrationInfo, Address[] newAddresses) {
        if (!node.isMaster()) {
            return false;
        }

        if (node.getThisAddress().equals(migrationInfo.getDestination())) {
            return true;
        }

        try {
            PartitionRuntimeState partitionState = partitionService.createMigrationCommitPartitionState(migrationInfo, newAddresses);
            MigrationCommitOperation operation = new MigrationCommitOperation(partitionState);
            Future<Boolean> future = nodeEngine.getOperationService()
                    .createInvocationBuilder(SERVICE_NAME, operation,
                            migrationInfo.getDestination())
                    .setTryCount(Integer.MAX_VALUE)
                    .setCallTimeout(Long.MAX_VALUE).invoke();
            future.get();
            return true;
        } catch (Throwable t) {
            if (t instanceof MemberLeftException || t instanceof TargetNotMemberException) {
                logger.warning("Migration commit failed for " + migrationInfo + " since destination left the cluster");
            } else {
                logger.severe("Migration commit failed for " + migrationInfo + " with " + t);
            }

            return false;
        }
    }

    void addCompletedMigration(MigrationInfo migrationInfo) {
        completedMigrationCounter.incrementAndGet();

        partitionServiceLock.lock();
        try {
            if (completedMigrations.size() > 25) {
                completedMigrations.removeFirst();
            }
            completedMigrations.add(migrationInfo);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    void evictCompletedMigrations() {
        partitionServiceLock.lock();
        try {
            if (!completedMigrations.isEmpty()) {
                completedMigrations.removeFirst();
            }
        } finally {
            partitionServiceLock.unlock();
        }
    }

    void triggerRepartitioning() {
        migrationQueue.clear();
        migrationQueue.add(new RepartitioningTask());
    }

    public InternalMigrationListener getInternalMigrationListener() {
        return internalMigrationListener;
    }

    void setInternalMigrationListener(InternalMigrationListener listener) {
        Preconditions.checkNotNull(listener);
        internalMigrationListener = listener;
    }

    void resetInternalMigrationListener() {
        internalMigrationListener = new InternalMigrationListener.NopInternalMigrationListener();
    }

    void onMemberRemove(MemberImpl member) {
        // TODO: should not clear all! but only migration tasks? only to/from dead member?
        migrationQueue.clear();

        // TODO: if it's source...?
        Address deadAddress = member.getAddress();
        if (activeMigrationInfo != null) {
            if (deadAddress.equals(activeMigrationInfo.getSource())
                    || deadAddress.equals(activeMigrationInfo.getDestination())) {
                activeMigrationInfo.invalidate();
            }
        }
    }

    public void execute(Runnable runnable) {
        migrationQueue.add(runnable);
    }

    public List<MigrationInfo> getCompletedMigrations() {
        return new ArrayList<MigrationInfo>(completedMigrations);
    }

    public boolean hasOnGoingMigration() {
        return activeMigrationInfo != null || migrationQueue.isNonEmpty()
                || migrationQueue.hasMigrationTasks();
    }

    public int getMigrationQueueSize() {
        return migrationQueue.size();
    }

    public void reset() {
        migrationQueue.clear();
        // TODO BASRI IS THIS SAFE?
        activeMigrationInfo = null;
        completedMigrations.clear();
    }

    void start() {
        migrationThread.start();
    }

    void stop() {
        migrationThread.stopNow();
    }

    public void setCompletedMigrations(Collection<MigrationInfo> migrationInfos) {
        completedMigrations.clear();
        completedMigrations.addAll(migrationInfos);
    }

    private class RepartitioningTask implements Runnable {
        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }

            partitionServiceLock.lock();
            try {
                if (!isMigrationAllowed()) {
                    return;
                }

                Address[][] newState = partitionStateManager.repartition();
                if (newState == null) {
                    return;
                }

                if (!isMigrationAllowed()) {
                    return;
                }

                // TODO: ?
                migrationQueue.clear();

                processNewPartitionState(newState);
                partitionService.syncPartitionRuntimeState();
            } finally {
                partitionServiceLock.unlock();
            }
        }

        private void processNewPartitionState(Address[][] newState) {
            int migrationCount = 0;
            int lostCount = 0;
            lastRepartitionTime.set(Clock.currentTimeMillis());
            for (int partitionId = 0; partitionId < newState.length; partitionId++) {
                Address[] replicas = newState[partitionId];
                InternalPartitionImpl currentPartition = partitionStateManager.getPartitionImpl(partitionId);
                Address currentOwner = currentPartition.getOwnerOrNull();
                Address newOwner = replicas[0];

                if (currentOwner == null) {
                    // assign new owner for lost partition
                    lostCount++;
                    assignNewPartitionOwner(partitionId, replicas, currentPartition, newOwner);
                } else if (newOwner != null && !currentOwner.equals(newOwner)) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("PartitionToMigrate partitionId=" + partitionId
                                + " replicas=" + Arrays.toString(replicas) + " currentOwner="
                                + currentOwner + " newOwner=" + newOwner);
                    }

                    migrationCount++;
                    migratePartitionToNewOwner(partitionId, replicas, currentOwner, newOwner);
                } else {
                    currentPartition.setReplicaAddresses(replicas);
                }
            }
            logMigrationStatistics(migrationCount, lostCount);
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

        private void migratePartitionToNewOwner(int partitionId, Address[] replicas, Address currentOwner, Address newOwner) {
            MigrationInfo info = new MigrationInfo(partitionId, currentOwner, newOwner);
            MigrateTask migrateTask = new MigrateTask(info, replicas);
            migrationQueue.add(migrateTask);
        }

        private void assignNewPartitionOwner(int partitionId, Address[] replicas, InternalPartitionImpl currentPartition,
                Address newOwner) {
            currentPartition.setReplicaAddresses(replicas);
            MigrationInfo migrationInfo = new MigrationInfo(partitionId, null, newOwner);
            PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
            partitionEventManager.sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.STARTED);
            partitionEventManager.sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.COMPLETED);
        }

        private boolean isMigrationAllowed() {
            if (MigrationManager.this.isMigrationAllowed()) {
                return true;
            }
            migrationQueue.add(this);
            return false;
        }
    }

    private Member getMasterMember() {
        return node.clusterService.getMember(node.getMasterAddress());
    }

    class MigrateTask implements Runnable {
        final MigrationInfo migrationInfo;
        final Address[] addresses;

        public MigrateTask(MigrationInfo migrationInfo, Address[] addresses) {
            this.migrationInfo = migrationInfo;
            this.addresses = addresses;
            final Member masterMember = getMasterMember();
            if (masterMember != null) {
                migrationInfo.setMasterUuid(masterMember.getUuid());
                migrationInfo.setMaster(masterMember.getAddress());
            }
        }

        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }
            final MigrationRequestOperation migrationRequestOp = new MigrationRequestOperation(migrationInfo);
            try {
                if (!checkPartitionOwner()) {
                    return;
                }
                internalMigrationListener.onMigrationStart(InternalMigrationListener.MigrationParticipant.MASTER,
                        migrationInfo);
                partitionService.getPartitionEventManager().sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.STARTED);

                Boolean result;
                MemberImpl fromMember = partitionService.getMember(migrationInfo.getSource());
                if (logger.isFinestEnabled()) {
                    logger.finest("Starting Migration: " + migrationInfo);
                }
                if (fromMember == null) {
                    // Partition is lost! Assign new owner and exit.
                    logger.warning("Partition is lost! Assign new owner and exit... partitionId=" + migrationInfo.getPartitionId());
                    result = Boolean.TRUE;
                    // TODO BASRI UNDERSTAND HOW THIS PART WILL WORK. THIS CASE SHOULD CAN BE TESTED WITH OUR MIGRATION LISTENER
                } else {
                    result = executeMigrateOperation(migrationRequestOp, fromMember);
                }
                processMigrationResult(result);
            } catch (Throwable t) {
                final Level level = migrationEndpointsActive() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] while executing " + migrationRequestOp);
                logger.finest(t);
                migrationOperationFailed();
            }
        }

        private boolean checkPartitionOwner() {
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                logger.severe("ERROR: partition owner is not set! -> partitionId=" + migrationInfo.getPartitionId()
                        + " , " + partition + " -VS- " + migrationInfo);
                return false;
            }
            if (!owner.equals(migrationInfo.getSource())) {
                logger.severe("ERROR: partition owner is not the source of migration! -> partitionId="
                        + migrationInfo.getPartitionId() + " , " + partition + " -VS- " + migrationInfo + " found owner=" + owner);
                return false;
            }
            return true;
        }

        private void processMigrationResult(Boolean result) {
            if (Boolean.TRUE.equals(result)) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Finished Migration: " + migrationInfo);
                }
                migrationOperationSucceeded();
            } else {
                final Level level = migrationEndpointsActive() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Migration failed: " + migrationInfo);
                migrationOperationFailed();
            }
        }

        private Boolean executeMigrateOperation(MigrationRequestOperation migrationRequestOp, MemberImpl fromMember) {
            Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, migrationRequestOp,
                    migrationInfo.getSource())
                    .setCallTimeout(partitionMigrationTimeout)
                    .setTryPauseMillis(DEFAULT_PAUSE_MILLIS).invoke();

            try {
                Object response = future.get();
                return (Boolean) nodeEngine.toObject(response);
            } catch (Throwable e) {
                final Level level = nodeEngine.isRunning() && migrationEndpointsActive() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Failed migration from " + fromMember + " for " + migrationRequestOp.getMigrationInfo(), e);
            }
            return Boolean.FALSE;
        }

        private void migrationOperationFailed() {
            internalMigrationListener.onMigrationComplete(InternalMigrationListener.MigrationParticipant.MASTER, migrationInfo, false);
            partitionServiceLock.lock();
            try {
                addCompletedMigration(migrationInfo);
                internalMigrationListener.onMigrationRollback(InternalMigrationListener.MigrationParticipant.MASTER, migrationInfo);
                scheduleActiveMigrationFinalization(migrationInfo);
                partitionService.syncPartitionRuntimeState();
            } finally {
                partitionServiceLock.unlock();
            }
            partitionService.getPartitionEventManager().sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.FAILED);

            // Migration failed.
            // Pause migration process for a small amount of time, if a migration attempt is failed.
            // Otherwise, migration failures can do a busy spin until migration problem is resolved.
            // Migration can fail either a node's just joined and not completed start yet or it's just left the cluster.
            pauseMigration();
            // Re-execute RepartitioningTask when all other migration tasks are done,
            // an imbalance may occur because of this failure.
            migrationQueue.add(new RepartitioningTask());
            resumeMigrationEventually();
        }

        private void migrationOperationSucceeded() {
            internalMigrationListener.onMigrationComplete(InternalMigrationListener.MigrationParticipant.MASTER, migrationInfo, true);
            addCompletedMigration(migrationInfo);

            boolean commitSuccessful = commitMigrationToDestination(migrationInfo, addresses);

            partitionServiceLock.lock();
            try {
                if (commitSuccessful) {
                    partitionStateManager.updateReplicaAddresses(migrationInfo.getPartitionId(), addresses);
                    internalMigrationListener.onMigrationCommit(InternalMigrationListener.MigrationParticipant.MASTER, migrationInfo);
                } else {
                    internalMigrationListener.onMigrationRollback(InternalMigrationListener.MigrationParticipant.MASTER, migrationInfo);
                }

                scheduleActiveMigrationFinalization(migrationInfo);
                partitionService.syncPartitionRuntimeState();
            } finally {
                partitionServiceLock.unlock();
            }
            partitionService.getPartitionEventManager().sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.COMPLETED);
        }

        private boolean migrationEndpointsActive() {
            return partitionService.getMember(migrationInfo.getSource()) != null
                    && partitionService.getMember(migrationInfo.getDestination()) != null;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + "migrationInfo=" + migrationInfo + '}';
        }
    }


}
