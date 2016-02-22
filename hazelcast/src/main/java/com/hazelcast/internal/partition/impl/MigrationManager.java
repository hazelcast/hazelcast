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
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.operation.ClearReplicaOperation;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.partition.operation.MigrationCommitOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.scheduler.CoalescingDelayedTrigger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    public static final int COMPLETED_MIGRATION_MAX_SIZE = 100;

    enum MigrateTaskReason {
        REPAIR_PARTITION_TABLE,
        REPARTITIONING
    }

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;

    private final PartitionStateManager partitionStateManager;

    final MigrationQueue migrationQueue = new MigrationQueue();

    private final MigrationThread migrationThread;

    private final AtomicInteger migrationPauseCount = new AtomicInteger(0);

    @Probe(name = "lastRepartitionTime")
    private final AtomicLong lastRepartitionTime = new AtomicLong();

    private final CoalescingDelayedTrigger delayedResumeMigrationTrigger;

    final long partitionMigrationInterval;
    private final long partitionMigrationTimeout;

    // updates will be done under lock, but reads will be multithreaded.
    private volatile MigrationInfo activeMigrationInfo;

    // both reads and updates will be done under lock!
    private final LinkedHashSet<MigrationInfo> completedMigrations = new LinkedHashSet<MigrationInfo>();

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
        return migrationPauseCount.get() == 0 ? 1 : 0;
    }

    @Probe(name = "migrationPauseCount")
    public int getMigrationPauseCount() {
        return migrationPauseCount.get();
    }

    public void pauseMigration() {
        migrationPauseCount.incrementAndGet();
    }

    public void resumeMigration() {
        int val = migrationPauseCount.decrementAndGet();

        while (val < 0 && !migrationPauseCount.compareAndSet(val, 0)) {
            logger.severe("migrationPauseCount=" + val + " is negative! ");
            val = migrationPauseCount.get();
        }
    }

    void resumeMigrationEventually() {
        delayedResumeMigrationTrigger.executeWithDelay();
    }

    public boolean isMigrationAllowed() {
        return migrationPauseCount.get() == 0;
    }

    private void finalizeMigration(MigrationInfo migrationInfo) {
        try {
            Address thisAddress = node.getThisAddress();
            int partitionId = migrationInfo.getPartitionId();
            InternalPartitionImpl migratingPartition = partitionStateManager.getPartitionImpl(partitionId);

            // either source of the migration or owner of the partition (only when replicaIndex > 0)
            boolean source = thisAddress.equals(migrationInfo.getSource());
            if (migrationInfo.getReplicaIndex() > 0 && !source) {
                boolean owner = thisAddress.equals(migratingPartition.getOwnerOrNull());
                source = owner;

                if (owner) {
                    MigrationInfo newMigrationInfo = new MigrationInfo(partitionId, migrationInfo.getReplicaIndex(),
                            thisAddress, migrationInfo.getDestination(), MigrationType.COPY);
                    newMigrationInfo.setStatus(migrationInfo.getStatus());
                    migrationInfo = newMigrationInfo;
                }
            }

            logger.warning("FINALIZE: " + migrationInfo);
            boolean destination = thisAddress.equals(migrationInfo.getDestination());

            if (source || destination) {
//                Address ownerAddress = migratingPartition.getReplicaAddress(migrationInfo.getReplicaIndex());
//                boolean success = migrationInfo.getDestination().equals(ownerAddress);
                assert migrationInfo.getStatus() == MigrationStatus.SUCCESS
                        || migrationInfo.getStatus() == MigrationStatus.FAILED
                        : "Invalid migration: " + migrationInfo;
                boolean success = migrationInfo.getStatus() == MigrationStatus.SUCCESS;

                MigrationParticipant participant = source ? MigrationParticipant.SOURCE : MigrationParticipant.DESTINATION;
                if (success) {
                    internalMigrationListener.onMigrationCommit(participant, migrationInfo);
                } else {
                    internalMigrationListener.onMigrationRollback(participant, migrationInfo);
                }

                MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                FinalizeMigrationOperation op = new FinalizeMigrationOperation(migrationInfo, endpoint, success);
                op.setPartitionId(partitionId)
                        .setNodeEngine(nodeEngine)
                        .setValidateTarget(false)
                        .setService(partitionService);
                nodeEngine.getOperationService().executeOperation(op);
//                activeMigrationInfo = null;
                removeActiveMigration(partitionId);
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

    public MigrationInfo getActiveMigration() {
        return activeMigrationInfo;
    }

    public boolean removeActiveMigration(int partitionId) {
        partitionServiceLock.lock();
        try {
            if (activeMigrationInfo != null) {
                if (activeMigrationInfo.getPartitionId() == partitionId) {
                    partitionStateManager.setMigrating(partitionId, false);
                    activeMigrationInfo = null;
                    return true;
                }

                if (logger.isFinestEnabled()) {
                    logger.finest("Active migration is not removed, because it has different partitionId! "
                            + "PartitionId=" + partitionId + ", Active migration=" + activeMigrationInfo);
                }
            }
        } finally {
            partitionServiceLock.unlock();
        }
        return false;
    }

    void scheduleActiveMigrationFinalization(final MigrationInfo migrationInfo) {
        partitionServiceLock.lock();
        try {
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
                            scheduleActiveMigrationFinalization(migrationInfo);
                        }
                    }, 3, TimeUnit.SECONDS);
                }
            } else if (migrationInfo.getReplicaIndex() > 0
                    && node.getThisAddress().equals(migrationInfo.getSource())) {
                // OLD BACKUP
//                finalizeMigration(migrationInfo);
                // TODO: run ClearReplicaOperation instead of finalize
                ClearReplicaOperation op = new ClearReplicaOperation(migrationInfo.getReplicaIndex());
                op.setPartitionId(migrationInfo.getPartitionId())
                        .setNodeEngine(nodeEngine).setService(partitionService);
                nodeEngine.getOperationService().executeOperation(op);
            }
        } finally {
            partitionServiceLock.unlock();
        }
    }

    private boolean commitMigrationToDestination(MigrationInfo migrationInfo) {
        if (!node.isMaster()) {
            return false;
        }

        if (node.getThisAddress().equals(migrationInfo.getDestination())) {
            return true;
        }

        try {
            PartitionRuntimeState partitionState = partitionService.createMigrationCommitPartitionState(migrationInfo);
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
                logger.severe("Migration commit failed for " + migrationInfo, t);
            }

            return false;
        }
    }

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

    void retainCompletedMigrations(Collection<MigrationInfo> migrations) {
        partitionServiceLock.lock();
        try {
            completedMigrations.retainAll(migrations);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    void triggerRepartitioning() {
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
        migrationQueue.invalidatePendingMigrations(member.getAddress());

        // TODO: if it's source...?
        Address deadAddress = member.getAddress();
        if (activeMigrationInfo != null) {
            if (deadAddress.equals(activeMigrationInfo.getSource())
                    || deadAddress.equals(activeMigrationInfo.getDestination())) {
                activeMigrationInfo.setStatus(MigrationStatus.INVALID);
            }
        }
    }

    public void schedule(MigrationRunnable runnable) {
        migrationQueue.add(runnable);
    }

    // TODO BASRI addActiveMigration method runs with partition service lock but this one and some others do not.
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

    public void scheduleMigration(MigrationInfo migrationInfo, MigrateTaskReason reason) {
        migrationQueue.add(new MigrateTask(migrationInfo, reason));
    }

    private class RepartitioningTask implements MigrationRunnable {

        private volatile boolean valid = true;

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

                lastRepartitionTime.set(Clock.currentTimeMillis());

                processNewPartitionState(newState);
                partitionService.syncPartitionRuntimeState();
            } finally {
                partitionServiceLock.unlock();
            }
        }

        private void processNewPartitionState(Address[][] newState) {
            System.out.println("");
            for (int i = 0; i < newState.length; i++) {
                Address[] addresses = newState[i];
                System.out.print("partitionId: " + i + " -> ");
                for (Address address : addresses) {
                    System.out.print(address + ", ");
                }
                System.out.println();
            }
            System.out.println("");

            int lostCount = 0;

            if (node.getClusterService().getSize() == 5) {
                System.out.println();
            }

            List<MigrationInfo> migrations = new ArrayList<MigrationInfo>();
            for (int partitionId = 0; partitionId < newState.length; partitionId++) {

                InternalPartitionImpl currentPartition = partitionStateManager.getPartitionImpl(partitionId);
                Address[] newReplicas = newState[partitionId];

                for (int replicaIndex = 0; replicaIndex < InternalPartition.MAX_REPLICA_COUNT; replicaIndex++) {
                    Address currentOwner = currentPartition.getReplicaAddress(replicaIndex);
                    Address newOwner = newReplicas[replicaIndex];

                    // #CASE: either both current and new owners are null or they are same
                    if (currentOwner == null && newOwner == null
                            || currentOwner != null && currentOwner.equals(newOwner)) {
                        // nop
                        continue;
                    }

                    if (currentOwner == null) {
                        // #CASE: current owner is null and replica-index = 0
                        if (replicaIndex == 0) {
                            // TODO
                            // data lost or this is new assignment
                            // no need to run any migration operation
                            // just assign and go on...
                            lostCount++;
                            assignNewPartitionOwner(partitionId, currentPartition, newOwner);
                            continue;
                        }

                        // #CASE: current owner is null and replica-index > 0
                        // TODO: search for previous migrations for copy-back
//                        if (isCopyBack(migrations, currentOwner, newOwner, replicaIndex)) {
//                            continue;
//                        }

                        // replicate data from partition owner to new replica owner
                        // schedule replication operation
                        migrations.add(new MigrationInfo(partitionId, replicaIndex, null, newOwner, MigrationType.COPY));
                        continue;
                    }

                    // #CASE: current owner is removed and new owner is null
                    if (newOwner == null) {
                        // TODO
                        // A member is dead, this replica should not have an owner!
                        // clear assignment & data for this replica
                        continue;
                    }

                    // #CASE: both current and new owners are not null and they are different
                    // TODO: search for previous migrations for copy-back
//                    if (isCopyBack(migrations, currentOwner, newOwner, replicaIndex)) {
//                        continue;
//                    }
                    // migrate replica
                    migrations.add(new MigrationInfo(partitionId, replicaIndex, currentOwner, newOwner, MigrationType.MOVE));
                }

                MigrationInfo current = null;
                ListIterator<MigrationInfo> iter = migrations.listIterator(migrations.size());
                while (iter.hasPrevious()) {
                    MigrationInfo prev = iter.previous();
                    if (current == null) {
                        current = prev;
                        continue;
                    }

//                    if (prev.getType() == MigrationType.MOVE
//                            && newOwner.equals(current.getSource())
//                            && (currentOwner == null || current.getDestination().equals(currentOwner))) {
//                        logger.severe("Prev: " + current + ", Current: " + currentOwner + ", New: " + newOwner + ", index: " + replicaIndex);
//                        current.setKeepReplicaIndex(replicaIndex);
//                        //                    previous.setType(MigrationType.MOVE_COPY_BACK);
//                        return true;
//                    }

                }

                for (MigrationInfo migration : migrations) {
                    // TODO: need to order tasks depending on their priority
                    logger.severe("Scheduling " + migration);
                    scheduleMigration(migration, MigrateTaskReason.REPARTITIONING);
                }
                migrations.clear();
            }

            logMigrationStatistics(migrations.size(), lostCount);
        }

        private boolean isCopyBack(List<MigrationInfo> migrations, Address currentOwner, Address newOwner, int replicaIndex) {
            ListIterator<MigrationInfo> iter = migrations.listIterator(migrations.size());
            while (iter.hasPrevious()) {
                MigrationInfo previous = iter.previous();
                if (previous.getType() == MigrationType.MOVE
                        && newOwner.equals(previous.getSource())
                        && (currentOwner == null || previous.getDestination().equals(currentOwner))) {
                    logger.severe("Prev: " + previous + ", Current: " + currentOwner + ", New: " + newOwner + ", index: " + replicaIndex);
                    previous.setKeepReplicaIndex(replicaIndex);
//                    previous.setType(MigrationType.MOVE_COPY_BACK);
                    return true;
                }
            }
            return false;
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

        private void assignNewPartitionOwner(int partitionId, InternalPartitionImpl currentPartition, Address newOwner) {
            MigrationInfo migrationInfo = new MigrationInfo(partitionId, 0, null, newOwner);
            PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
            partitionEventManager.sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.STARTED);
            currentPartition.setReplicaAddress(0, newOwner);
            partitionEventManager.sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.COMPLETED);
        }

        private boolean isMigrationAllowed() {
            if (MigrationManager.this.isMigrationAllowed()) {
                return true;
            }
            migrationQueue.add(this);
            return false;
        }

        @Override
        public void invalidate(Address address) {
            valid = false;
        }

        @Override
        public boolean isValid() {
            return valid;
        }

        @Override
        public boolean isPauseable() {
            return true;
        }

    }

    private Member getMasterMember() {
        return node.clusterService.getMember(node.getMasterAddress());
    }

    class MigrateTask implements MigrationRunnable {

        final MigrationInfo migrationInfo;

        final MigrateTaskReason reason;

        public MigrateTask(MigrationInfo migrationInfo, MigrateTaskReason reason) {
            this.migrationInfo = migrationInfo;
            final Member masterMember = getMasterMember();
            if (masterMember != null) {
                migrationInfo.setMasterUuid(masterMember.getUuid());
                migrationInfo.setMaster(masterMember.getAddress());
            }
            this.reason = reason;
        }

        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }

            try {
                if (!checkPartitionOwner()) {
                    return;
                }
                internalMigrationListener.onMigrationStart(MigrationParticipant.MASTER, migrationInfo);
                partitionService.getPartitionEventManager()
                        .sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.STARTED);

                Boolean result;
                if (logger.isFinestEnabled()) {
                    logger.info("Starting Migration: " + migrationInfo);
                }

                MemberImpl fromMember = getSourceMember();
                if (fromMember == null) {
                    // Partition is lost! Assign new owner and exit.
                    logger.warning("Partition is lost! Assign new owner and exit... partitionId=" + migrationInfo.getPartitionId());
                    result = Boolean.TRUE;
                    // TODO BASRI UNDERSTAND HOW THIS PART WILL WORK. THIS CASE SHOULD CAN BE TESTED WITH OUR MIGRATION LISTENER
                } else {
                    MigrationRequestOperation migrationRequestOp = new MigrationRequestOperation(migrationInfo,
                            partitionService.getPartitionStateVersion());
                    result = executeMigrateOperation(migrationRequestOp, fromMember);
                }
                processMigrationResult(result);
            } catch (Throwable t) {
                final Level level = migrationEndpointsActive() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] during " + migrationInfo);
                logger.finest(t);
                migrationOperationFailed();
            }
        }

        private MemberImpl getSourceMember() {
            // always replicate data from partition owner
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
            Address source = partition.getOwnerOrNull();
            return node.getClusterService().getMember(source);
        }

        // TODO: needs cleanup
        private boolean checkPartitionOwner() {
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                logger.severe("ERROR: partition owner is not set! -> partitionId=" + migrationInfo.getPartitionId()
                        + " , " + partition + " -VS- " + migrationInfo);
                return false;
            }

            if (migrationInfo.getType() == MigrationType.MOVE/* || migrationInfo.getType() == MigrationType.MOVE_COPY_BACK*/) {
                Address replica = partition.getReplicaAddress(migrationInfo.getReplicaIndex());
                if (replica == null) {
                    logger.severe("ERROR: partition replica owner is not set! -> partitionId="
                            + migrationInfo.getPartitionId() + " , " + partition + " -VS- " + migrationInfo);
                    return false;
                }

                if (!replica.equals(migrationInfo.getSource())) {
                    logger.severe("ERROR: partition replica owner is not the source of migration! -> partitionId="
                            + migrationInfo.getPartitionId() + " , " + partition + " -VS- " + migrationInfo
                            + " found owner= " + replica);
                    return false;
                }
            }

            if (migrationInfo.getType() == MigrationType.COPY) {
                // TODO: anything?
            }

            return true;
        }

        private void processMigrationResult(Boolean result) {
            if (Boolean.TRUE.equals(result)) {
                if (logger.isFinestEnabled()) {
                    logger.warning("Finished Migration: " + migrationInfo);
                }
                migrationOperationSucceeded();
            } else {
                final Level level = migrationEndpointsActive() ? Level.WARNING : Level.FINEST;
                logger.log(Level.SEVERE, "Migration failed: " + migrationInfo);
                migrationOperationFailed();
            }
        }

        private Boolean executeMigrateOperation(MigrationRequestOperation migrationRequestOp, MemberImpl fromMember) {
            Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, migrationRequestOp,
                    fromMember.getAddress())
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
            migrationInfo.setStatus(MigrationStatus.FAILED);
            internalMigrationListener.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, false);
            partitionServiceLock.lock();
            try {
                addCompletedMigration(migrationInfo);
                internalMigrationListener.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                scheduleActiveMigrationFinalization(migrationInfo);
                partitionService.getPartitionStateManager().incrementVersion(2); // TODO move this into constant
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
            migrationInfo.setStatus(MigrationStatus.SUCCESS);
            internalMigrationListener.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, true);
            addCompletedMigration(migrationInfo);

            boolean commitSuccessful = commitMigrationToDestination(migrationInfo);

            partitionServiceLock.lock();
            try {
                if (commitSuccessful) {
                    internalMigrationListener.onMigrationCommit(MigrationParticipant.MASTER, migrationInfo);

                    // TODO BASRI after we remove internalpartitionlistener, ptable version update must be done manually
                    // TODO: inspect this part later!
                    // updates partition table after successful commit
                    InternalPartitionImpl partition =
                            partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
                    partition.setReplicaAddress(migrationInfo.getReplicaIndex(), migrationInfo.getDestination());

//                    if (migrationInfo.getType() == MigrationType.MOVE_COPY_BACK) {
                    if (migrationInfo.getKeepReplicaIndex() > -1) {
                        partition.setReplicaAddress(migrationInfo.getKeepReplicaIndex(), migrationInfo.getSource());
                    }

                } else {
                    internalMigrationListener.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                }

                scheduleActiveMigrationFinalization(migrationInfo);
                partitionService.syncPartitionRuntimeState();
            } finally {
                partitionServiceLock.unlock();
            }
            partitionService.getPartitionEventManager().sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.COMPLETED);
        }

        private boolean migrationEndpointsActive() {
            final ClusterServiceImpl clusterService = node.getClusterService();
            return clusterService.getMember(migrationInfo.getSource()) != null
                    && clusterService.getMember(migrationInfo.getDestination()) != null;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + "migrationInfo=" + migrationInfo + '}';
        }

        @Override
        public void invalidate(Address address) {
            if (migrationInfo.isValid()) {
                boolean valid = !(reason == MigrateTaskReason.REPARTITIONING
                        || migrationInfo.getSource().equals(address)
                        || migrationInfo.getDestination().equals(address));

                if (!valid) {
                    migrationInfo.setStatus(MigrationStatus.INVALID);
                }
            }
        }

        @Override
        public boolean isValid() {
            return migrationInfo.isValid();
        }

        @Override
        public boolean isPauseable() {
            return isValid();
        }

    }
}
