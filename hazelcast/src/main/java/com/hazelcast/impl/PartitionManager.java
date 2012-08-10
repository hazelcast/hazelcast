/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.cluster.AbstractRemotelyCallable;
import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.cluster.ClusterManager.AsyncRemotelyBooleanCallable;
import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.impl.base.DataRecordEntry;
import com.hazelcast.impl.base.RecordSet;
import com.hazelcast.impl.base.SystemLogService;
import com.hazelcast.impl.concurrentmap.CostAwareRecordList;
import com.hazelcast.impl.concurrentmap.ValueHolder;
import com.hazelcast.impl.partition.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.util.Clock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class PartitionManager {
    public static final String MIGRATION_EXECUTOR_NAME = "hz.migration";

    private static final long MIGRATING_PARTITION_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(300); // 5 MINUTES
    private static final long REPARTITIONING_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(300); // 5 MINUTES
    private static final int REPARTITIONING_TASK_COUNT_THRESHOLD = 20;
    private static final int REPARTITIONING_TASK_REPLICA_THRESHOLD = 2;

    private final ConcurrentMapManager concurrentMapManager;
    private final ILogger logger;
    private final int partitionCount;
    private final PartitionInfo[] partitions;

    // updates will come from ServiceThread (one exception is PartitionManager.reset())
    // but reads will be multithreaded.
    private volatile MigratingPartition migratingPartition;
    private volatile boolean initialized = false;
    private final AtomicInteger version = new AtomicInteger();
    private final List<PartitionListener> lsPartitionListeners = new CopyOnWriteArrayList<PartitionListener>();
    private final int partitionMigrationInterval;
    private final long partitionMigrationTimeout;
    private final int immediateBackupInterval;
    private final MigrationService migrationService;
    private boolean running = true; // accessed only by MigrationService thread
    private final BlockingQueue<Runnable> immediateTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final Queue<Runnable> scheduledTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final AtomicBoolean sendingDiffs = new AtomicBoolean(false);
    private final AtomicBoolean migrationActive = new AtomicBoolean(true); // for testing purposes only
    private final AtomicLong lastRepartitionTime = new AtomicLong();
    private final SystemLogService systemLogService;

    public PartitionManager(final ConcurrentMapManager concurrentMapManager) {
        this.partitionCount = concurrentMapManager.getPartitionCount();
        this.concurrentMapManager = concurrentMapManager;
        this.logger = concurrentMapManager.node.getLogger(PartitionManager.class.getName());
        this.partitions = new PartitionInfo[partitionCount];
        final Node node = concurrentMapManager.node;
        systemLogService = node.getSystemLogService();
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new PartitionInfo(i, new PartitionListener() {
                public void replicaChanged(PartitionReplicaChangeEvent event) {
                    for (PartitionListener partitionListener : lsPartitionListeners) {
                        partitionListener.replicaChanged(event);
                    }
                    if (event.getReplicaIndex() == 0 && event.getNewAddress() == null
                            && node.isActive() && node.joined()) {
                        final String warning = "Owner of partition is being removed! " +
                                "Possible data loss for partition[" + event.getPartitionId() + "]. "
                                + event;
                        logger.log(Level.WARNING, warning);
                        systemLogService.logPartition(warning);
                    }
                    if (concurrentMapManager.isMaster()) {
                        version.incrementAndGet();
                    }
                }
            });
        }
        partitionMigrationInterval = node.groupProperties.PARTITION_MIGRATION_INTERVAL.getInteger() * 1000;
        // partitionMigrationTimeout is 1.5 times of real timeout
        partitionMigrationTimeout = (long) (node.groupProperties.PARTITION_MIGRATION_TIMEOUT.getLong() * 1.5f);
        immediateBackupInterval = node.groupProperties.IMMEDIATE_BACKUP_INTERVAL.getInteger() * 1000;
        migrationService = new MigrationService(node);
        migrationService.start();
        int partitionTableSendInterval = node.groupProperties.PARTITION_TABLE_SEND_INTERVAL.getInteger();
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new SendClusterStateTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);
        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new CheckMigratingPartitionTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);
        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (concurrentMapManager.isMaster() && node.isActive()
                        && initialized && shouldCheckRepartitioning()) {
                    logger.log(Level.FINEST, "Checking partition table for repartitioning...");
                    immediateTasksQueue.add(new CheckRepartitioningTask());
                }
            }
        }, 180, 180, TimeUnit.SECONDS);
    }

    public MigratingPartition getMigratingPartition() {
        return migratingPartition;
    }

    public void addPartitionListener(PartitionListener partitionListener) {
        lsPartitionListeners.add(partitionListener);
    }

    public PartitionInfo[] getPartitions() {
        return partitions;
    }

    public Address getOwner(int partitionId) {
        concurrentMapManager.checkServiceThread();
        if (!initialized) {
            firstArrangement();
        }
        Address owner = partitions[partitionId].getOwner();
        if (owner == null && !concurrentMapManager.isMaster()) {
            concurrentMapManager.sendProcessableTo(new AssignPartitions(), concurrentMapManager.getMasterAddress());
        }
        return owner;
    }

    public void firstArrangement() {
        concurrentMapManager.checkServiceThread();
        if (!concurrentMapManager.isMaster() || !concurrentMapManager.isActive()) return;
        if (!hasStorageMember()) return;
        if (!initialized) {
            PartitionStateGenerator psg = getPartitionStateGenerator();
            logger.log(Level.INFO, "Initializing cluster partition table first arrangement...");
            PartitionInfo[] newState = psg.initialize(concurrentMapManager.lsMembers, partitionCount);
            if (newState != null) {
                for (PartitionInfo partitionInfo : newState) {
                    partitions[partitionInfo.getPartitionId()].setPartitionInfo(partitionInfo);
                }
            }
            sendPartitionRuntimeState();
            initialized = true;
        }
    }

    private void sendPartitionRuntimeState() {
        if (!concurrentMapManager.isMaster() || !concurrentMapManager.isActive()
            || !concurrentMapManager.node.joined()) {
            return;
        }
        if (!migrationActive.get()) {
            // migration is disabled because of a member leave, wait till enabled!
            return;
        }
        if (!initialized) {
            // do not send partition state until initialized!
            // sending partition state makes nodes believe initialization completed.
            return;
        }
        final long clusterTime = concurrentMapManager.node.getClusterImpl().getClusterTime();
        final List<MemberImpl> lsMembers = concurrentMapManager.lsMembers;
        final List<MemberInfo> memberInfos = new ArrayList<MemberInfo>(lsMembers.size());
        for (MemberImpl member : lsMembers) {
            memberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType(), member.getUuid()));
        }
        PartitionStateProcessable processable = new PartitionStateProcessable(memberInfos, partitions, clusterTime, version.get());
        concurrentMapManager.sendProcessableToAll(processable, false);
    }

    private PartitionStateGenerator getPartitionStateGenerator() {
        return PartitionStateGeneratorFactory.newConfigPartitionStateGenerator(
                concurrentMapManager.node.getConfig().getPartitionGroupConfig());
    }

    public CostAwareRecordList getActivePartitionRecords(final int partitionId, final int replicaIndex,
                                                         final Address newAddress, boolean diffOnly) {
        final Address thisAddress = concurrentMapManager.node.getThisAddress();
        concurrentMapManager.enqueueAndWait(new Processable() {
            public void process() {
                addActiveMigration(partitionId, replicaIndex, thisAddress, newAddress);
            }
        });
        long now = Clock.currentTimeMillis();
        final Collection<CMap> cmaps = concurrentMapManager.maps.values();
        CostAwareRecordList lsResultSet = new CostAwareRecordList(1000);
        for (final CMap cmap : cmaps) {
            boolean includeCMap = diffOnly
                    ? cmap.getTotalBackupCount() == replicaIndex
                    : cmap.getTotalBackupCount() >= replicaIndex;
            if (includeCMap) {
                for (Record rec : cmap.mapRecords.values()) {
                    if (rec.isActive() && rec.isValid(now)) {
                        if (rec.getKeyData() == null || rec.getKeyData().size() == 0) {
                            throw new RuntimeException("Record.key is null or empty " + rec.getKeyData());
                        }
                        if (rec.getBlockId() == partitionId) {
                            cmap.onMigrate(rec);
                            if (cmap.isMultiMap()) {
                                final Collection<ValueHolder> colValues = rec.getMultiValues();
                                if (colValues != null) {
                                    for (ValueHolder valueHolder : colValues) {
                                        Record record = rec.copy();
                                        record.setValueData(valueHolder.getData());
                                        lsResultSet.add(record);
                                    }
                                }
                            } else {
                                lsResultSet.add(rec);
                            }
                            lsResultSet.addCost(rec.getCost());
                        }
                    }
                }
            }
        }
        return lsResultSet;
    }

    private void addActiveMigration(final MigratingPartition migrationRequestTask) {
        addActiveMigration(migrationRequestTask.getPartitionId(), migrationRequestTask.getReplicaIndex(),
                migrationRequestTask.getFromAddress(), migrationRequestTask.getToAddress());
    }

    private void addActiveMigration(final int partitionId, final int replicaIndex,
                                    final Address currentAddress, final Address newAddress) {
        concurrentMapManager.checkServiceThread();
        final MigratingPartition currentMigratingPartition = migratingPartition;
        final MigratingPartition newMigratingPartition = new MigratingPartition(partitionId,
                replicaIndex, currentAddress, newAddress);
        if (!newMigratingPartition.equals(currentMigratingPartition)) {
            if (currentMigratingPartition != null) {
                logger.log(Level.FINEST, "Replacing current " + currentMigratingPartition
                        + " with " + newMigratingPartition);
            }
            migratingPartition = newMigratingPartition;
        }
    }

    private void compareAndSetActiveMigratingPartition(final MigratingPartition expectedMigratingPartition,
                                                       final MigratingPartition newMigratingPartition) {
        concurrentMapManager.checkServiceThread();
        if (expectedMigratingPartition == null) {
            if (migratingPartition == null) {
                migratingPartition = newMigratingPartition;
            }
        } else if (expectedMigratingPartition.equals(migratingPartition)) {
            migratingPartition = newMigratingPartition;
        }
    }

    public void doMigrate(final int partitionId, final int replicaIndex, final RecordSet recordSet, final Address from) {
        concurrentMapManager.enqueueAndWait(new Processable() {
            public void process() {
                addActiveMigration(partitionId, replicaIndex, from, concurrentMapManager.thisAddress);
                for (DataRecordEntry dataRecordEntry : recordSet.getRecords()) {
                    CMap cmap = concurrentMapManager.getOrCreateMap(dataRecordEntry.getName());
                    if (replicaIndex == 0) {
                        // owner
                        cmap.own(dataRecordEntry);
                    } else {
                        // backup
                        cmap.storeAsBackup(dataRecordEntry);
                    }
                }
            }
        });
    }

    public MemberImpl getMember(Address address) {
        if (address != null) {
            for (Member member : concurrentMapManager.node.getClusterImpl().getMembers()) {
                MemberImpl memberImpl = (MemberImpl) member;
                if (memberImpl.getAddress().equals(address)) return memberImpl;
            }
        }
        return null;
    }

    public void reset() {
        initialized = false;
        clearTaskQueues();
        migratingPartition = null;
        for (PartitionInfo partition : partitions) {
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                partition.setReplicaAddress(i, null);
            }
        }
        version.set(0);
    }

    private void clearTaskQueues() {
        immediateTasksQueue.clear();
        scheduledTasksQueue.clear();
    }

    public void shutdown() {
        logger.log(Level.FINEST, "Shutting down the partition manager");
        try {
            clearTaskQueues();
            final CountDownLatch stopLatch = new CountDownLatch(1);
            immediateTasksQueue.offer(new Runnable() {
                public void run() {
                    running = false;
                    stopLatch.countDown();
                }
            });
            stopLatch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    private boolean hasStorageMember() {
        for (MemberImpl member : concurrentMapManager.lsMembers) {
            if (!member.isLiteMember()) {
                return true;
            }
        }
        return false;
    }

    public void syncForDead(MemberImpl deadMember) {
        Address deadAddress = deadMember.getAddress();
        Address thisAddress = concurrentMapManager.getThisAddress();
        if (deadAddress == null || deadAddress.equals(thisAddress)) {
            return;
        }
        if (!hasStorageMember()) {
            reset();
        }
        // inactivate migration and sending of PartitionRuntimeState (@see #sendPartitionRuntimeState)
        // let all members notice the dead and fix their own records and indexes.
        // otherwise new master may take action fast and send new partition state
        // before other members realize the dead one and fix their records.
        final boolean migrationStatus = migrationActive.getAndSet(false);
        concurrentMapManager.partitionServiceImpl.reset();
        checkMigratingPartitionForDead(deadAddress);
        // list of partitions those have dead member in their replicas
        // !! this should be calculated before dead member is removed from partition table !!
        int[] indexesOfDead = new int[partitions.length];
        for (PartitionInfo partition : partitions) {
            indexesOfDead[partition.getPartitionId()] = partition.getReplicaIndexOf(deadAddress);
        }
        if (!deadMember.isLiteMember()) {
            clearTaskQueues();
            // shift partition table up.
            for (PartitionInfo partition : partitions) {
                // safe removal of dead address from partition table.
                // there might be duplicate dead address in partition table
                // during migration tasks' execution (when there are multiple backups and
                // copy backup tasks; see MigrationRequestTask selfCopyReplica.)
                // or because of a bug.
                while (partition.onDeadAddress(deadAddress)) ;
            }
        }
        fixCMapsForDead(deadAddress, indexesOfDead);
        fixReplicasAndPartitionsForDead(deadMember, indexesOfDead);

        final Node node = concurrentMapManager.node;
        // activate migration back after connectionDropTime x 10 milliseconds,
        // thinking optimistically that all nodes notice the dead one in this period.
        final long waitBeforeMigrationActivate = node.groupProperties.CONNECTION_MONITOR_INTERVAL.getLong()
                * node.groupProperties.CONNECTION_MONITOR_MAX_FAULTS.getInteger() * 10;
        node.executorManager.getScheduledExecutorService().schedule(new Runnable() {
            public void run() {
                migrationActive.compareAndSet(false, migrationStatus);
            }
        }, waitBeforeMigrationActivate, TimeUnit.MILLISECONDS);
    }

    private void fixReplicasAndPartitionsForDead(final MemberImpl deadMember, final int[] indexesOfDead) {
        if (!deadMember.isLiteMember() && concurrentMapManager.isMaster() && concurrentMapManager.isActive()) {
            sendingDiffs.set(true);
            logger.log(Level.INFO, "Starting to send partition replica diffs..." + sendingDiffs.get());
            int diffCount = 0;
            final int maxBackupCount = getMaxBackupCount();
            for (int partitionId = 0; partitionId < indexesOfDead.length; partitionId++) {
                int indexOfDead = indexesOfDead[partitionId];
                if (indexOfDead != -1) {
                    PartitionInfo partition = partitions[partitionId];
                    Address owner = partition.getOwner();
                    if (owner == null) {
                        logger.log(Level.FINEST, "Owner of one of the replicas of Partition[" +
                                partitionId + "] is dead, but partition owner " +
                                "could not be found either!");
                        logger.log(Level.FINEST, partition.toString());
                        continue;
                    }
                    // send replica diffs to new replica owners after partition table shift.
                    for (int replicaIndex = indexOfDead; replicaIndex < maxBackupCount; replicaIndex++) {
                        Address target = partition.getReplicaAddress(replicaIndex);
                        if (target != null && !target.equals(owner)) {
                            if (getMember(target) != null) {
                                MigrationRequestTask mrt = new MigrationRequestTask(partitionId, owner, target,
                                        replicaIndex, false, true);
                                immediateTasksQueue.offer(new Migrator(mrt));
                                diffCount++;
                            } else {
                                logger.log(Level.WARNING, "Target member of replica diff task couldn't found! "
                                        + "Replica: " + replicaIndex + ", Dead: " + deadMember + "\n" + partition);
                            }
                        }
                    }
                    // if index of dead member is equal to or less than maxBackupCount
                    // clear indexes of equal to and greater than maxBackupCount of partition.
                    if (indexOfDead <= maxBackupCount) {
                        for (int index = maxBackupCount; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                            partition.setReplicaAddress(index, null);
                        }
                    }
                }
            }
            sendPartitionRuntimeState();
            final int totalDiffCount = diffCount;
            immediateTasksQueue.offer(new Runnable() {
                public void run() {
                    logger.log(Level.INFO, "Total " + totalDiffCount + " partition replica diffs have been processed.");
                    sendingDiffs.set(false);
                }
            });
            immediateTasksQueue.offer(new PrepareRepartitioningTask());
        }
    }

    private void checkMigratingPartitionForDead(final Address deadAddress) {
        if (migratingPartition != null) {
            if (deadAddress.equals(migratingPartition.getFromAddress())
                    || deadAddress.equals(migratingPartition.getToAddress())) {
                migratingPartition = null;
            }
        }
    }

    private void fixCMapsForDead(final Address deadAddress, final int[] indexesOfDead) {
        Address thisAddress = concurrentMapManager.getThisAddress();
        for (CMap cmap : concurrentMapManager.maps.values()) {
            cmap.onDisconnect(deadAddress);
            Object[] records = cmap.mapRecords.values().toArray();
            for (Object recordObject : records) {
                if (recordObject != null) {
                    Record record = (Record) recordObject;
                    cmap.onDisconnect(record, deadAddress);
                    final int partitionId = record.getBlockId();
                    // owner of the partition is dead
                    // and record is active
                    // and new owner of partition is this member.
                    if (indexesOfDead[partitionId] == 0
                            && record.isActive()
                            && thisAddress.equals(partitions[partitionId].getOwner())) {
                        cmap.markAsDirty(record, true);
                        // update the indexes
                        cmap.updateIndexes(record);
                    }
                }
            }
        }
    }

    private int getMaxBackupCount() {
        final Collection<CMap> cmaps = concurrentMapManager.maps.values();
        if (!cmaps.isEmpty()) {
            int maxBackupCount = 0;
            for (final CMap cmap : cmaps) {
                maxBackupCount = Math.max(maxBackupCount, cmap.getTotalBackupCount());
            }
            return maxBackupCount;
        }
        return 1; // if there is no map, avoid extra processing.
    }

    public void syncForAdd() {
        if (concurrentMapManager.isMaster() && concurrentMapManager.node.isActive()) {
            if (sendingDiffs.get()) {
                logger.log(Level.INFO, "MigrationService is already sending diffs for dead member, " +
                        "no need to initiate task!");
            } else {
                // to avoid repartitioning during a migration process.
                clearTaskQueues();
                immediateTasksQueue.offer(new PrepareRepartitioningTask());
            }
        }
    }

    public int getVersion() {
        return version.get();
    }

    // for testing purposes only
    void forcePartitionOwnerMigration(int partitionId, int replicaIndex, Address from, Address to) {
        MigrationRequestTask mrt = new MigrationRequestTask(partitionId, from, to, replicaIndex, true);
        immediateTasksQueue.offer(new Migrator(mrt));
    }

    public void setPartitionRuntimeState(PartitionRuntimeState runtimeState) {
        if (!concurrentMapManager.isActive() || !concurrentMapManager.node.joined()) {
            return;
        }
        concurrentMapManager.checkServiceThread();
        final Connection conn = runtimeState.getConnection();
        final Address sender = conn != null ? conn.getEndPoint() : null;
        if (concurrentMapManager.isMaster()) {
            logger.log(Level.WARNING, "This is the master node and received a PartitionRuntimeState from "
                    + (sender != null ? sender : conn) + ". Ignoring incoming state! ");
            return;
        } else {
            final Address master = concurrentMapManager.getMasterAddress();
            if (sender == null || !sender.equals(master)) {
                logger.log(Level.WARNING, "Received a ClusterRuntimeState, but its sender doesn't seem master!" +
                        " => Sender: " + sender + ", Master: " + master + "! " +
                        "(Ignore if master node has changed recently.)");
            }
        }
        PartitionInfo[] newPartitions = runtimeState.getPartitions();
        int size = newPartitions.length;
        for (int i = 0; i < size; i++) {
            PartitionInfo newPartition = newPartitions[i];
            PartitionInfo currentPartition = partitions[newPartition.getPartitionId()];
            for (int index = 0; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                Address address = newPartition.getReplicaAddress(index);
                if (address != null && concurrentMapManager.getMember(address) == null) {
                    logger.log(Level.WARNING, "Unknown " + address + " is found in received partition table from master "
                            + sender + ". Probably it is dead. Partition: " + newPartition);
                }
            }
            currentPartition.setPartitionInfo(newPartition);
            checkMigratingPartitionFor(currentPartition);
        }
        initialized = true;
        version.set(runtimeState.getVersion());
    }

    private void checkMigratingPartitionFor(PartitionInfo partition) {
        concurrentMapManager.checkServiceThread();
        final MigratingPartition mPartition = migratingPartition;
        if (mPartition != null && partition.getPartitionId() == mPartition.getPartitionId()) {
            final Address targetAddress = mPartition.getToAddress();
            if (targetAddress != null
                    && targetAddress.equals(partition.getReplicaAddress(mPartition.getReplicaIndex()))) {
                migratingPartition = null;
            }
        }
    }

    public boolean shouldPurge(int partitionId, int maxBackupCount) {
        if (isPartitionMigrating(partitionId)) return false;
        Address thisAddress = concurrentMapManager.getThisAddress();
        PartitionInfo partitionInfo = getPartition(partitionId);
        return !partitionInfo.isOwnerOrBackup(thisAddress, maxBackupCount);
    }

    /**
     * @param partitionId
     * @return true if any replica of partition is migrating, false otherwise
     */
    public boolean isPartitionMigrating(int partitionId) {
        // volatile read
        final MigratingPartition currentMigratingPartition = migratingPartition;
        return currentMigratingPartition != null
                && currentMigratingPartition.getPartitionId() == partitionId;
    }

    /**
     * @param partitionId
     * @return true if owned replica (0) of partition is migrating, false otherwise
     */
    public boolean isOwnedPartitionMigrating(int partitionId) {
        return isPartitionMigrating(partitionId, 0);
    }

    /**
     * @param partitionId
     * @param replicaIndex
     * @return true if replicaIndex of partition is migrating, false otherwise
     */
    public boolean isPartitionMigrating(int partitionId, int replicaIndex) {
        // volatile read
        final MigratingPartition currentMigratingPartition = migratingPartition;
        return currentMigratingPartition != null
               && currentMigratingPartition.getPartitionId() == partitionId
               && currentMigratingPartition.getReplicaIndex() == replicaIndex;
    }

    public PartitionInfo getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public boolean hasActiveBackupTask() {
        if (!initialized) return false;
        if (concurrentMapManager.isLiteMember()) return false;
        int maxBackupCount = getMaxBackupCount();
        if (maxBackupCount == 0) return false;
        Set<MemberImpl> members = new HashSet<MemberImpl>();
        for (Member member : concurrentMapManager.node.getClusterImpl().getMembers()) {
            members.add((MemberImpl) member);
        }
        MemberGroupFactory mgf = PartitionStateGeneratorFactory.newMemberGroupFactory(
                concurrentMapManager.node.config.getPartitionGroupConfig());
        if (mgf.createMemberGroups(members).size() < 2) return false;
        boolean needBackup = false;
        if (immediateTasksQueue.isEmpty()) {
            for (PartitionInfo partition : partitions) {
                if (partition.getReplicaAddress(1) == null) {
                    needBackup = true;
                    logger.log(Level.WARNING, concurrentMapManager.thisAddress
                            + " still has no replica for partitionId:" + partition.getPartitionId());
                    break;
                }
            }
        }
        return needBackup || !immediateTasksQueue.isEmpty();
    }

    public void fireMigrationEvent(final boolean started, int partitionId, Address from, Address to) {
        final MemberImpl current = concurrentMapManager.getMember(from);
        final MemberImpl newOwner = concurrentMapManager.getMember(to);
        final MigrationEvent migrationEvent = new MigrationEvent(concurrentMapManager.node, partitionId, current, newOwner);
        systemLogService.logPartition("MigrationEvent [" + started + "] " + migrationEvent);
        concurrentMapManager.partitionServiceImpl.doFireMigrationEvent(started, migrationEvent);
    }

    private boolean shouldCheckRepartitioning() {
        return immediateTasksQueue.isEmpty() && scheduledTasksQueue.isEmpty()
                && lastRepartitionTime.get() < (Clock.currentTimeMillis() - REPARTITIONING_CHECK_INTERVAL)
                && migratingPartition == null;
    }

    public static class AssignPartitions extends AbstractRemotelyProcessable {
        public void process() {
            node.concurrentMapManager.getPartitionManager().getOwner(0);
        }
    }

    public static class RemotelyCheckMigratingPartition extends AbstractRemotelyCallable<Boolean> {
        MigratingPartition migratingPartition;

        public RemotelyCheckMigratingPartition() {
        }

        public RemotelyCheckMigratingPartition(final MigratingPartition migratingPartition) {
            this.migratingPartition = migratingPartition;
        }

        public Boolean call() throws Exception {
            if (migratingPartition != null) {
                final MigratingPartition masterMigratingPartition = node
                        .concurrentMapManager.getPartitionManager().migratingPartition;
                return migratingPartition.equals(masterMigratingPartition);
            }
            return Boolean.FALSE;
        }

        public void readData(final DataInput in) throws IOException {
            if (in.readBoolean()) {
                migratingPartition = new MigratingPartition();
                migratingPartition.readData(in);
            }
        }

        public void writeData(final DataOutput out) throws IOException {
            boolean b = migratingPartition != null;
            out.writeBoolean(b);
            if (b) {
                migratingPartition.writeData(out);
            }
        }
    }

    private class SendClusterStateTask implements Runnable {
        public void run() {
            if (concurrentMapManager.isMaster() && concurrentMapManager.node.isActive()) {
                if ((!scheduledTasksQueue.isEmpty() || !immediateTasksQueue.isEmpty()) && migrationActive.get()) {
                    logger.log(Level.INFO, "Remaining migration tasks in queue => Immediate-Tasks: " + immediateTasksQueue.size()
                            + ", Scheduled-Tasks: " + scheduledTasksQueue.size());
                }
                final Node node = concurrentMapManager.node;
                concurrentMapManager.enqueueAndReturn(new Processable() {
                    public void process() {
                        if (!node.isActive() || !node.isMaster()) return;
                        sendPartitionRuntimeState();
                    }
                });
            }
        }
    }

    private class CheckMigratingPartitionTask implements Runnable {
        public void run() {
            if (!concurrentMapManager.isMaster()) {
                final MigratingPartition currentMigratingPartition = migratingPartition;
                if (currentMigratingPartition != null
                        && (Clock.currentTimeMillis() - currentMigratingPartition.getCreationTime())
                        > MIGRATING_PARTITION_CHECK_INTERVAL) {
                    try {
                        final Node node = concurrentMapManager.node;
                        AsyncRemotelyBooleanCallable rrp = node.clusterManager.new AsyncRemotelyBooleanCallable();
                        rrp.executeProcess(node.getMasterAddress(),
                                new RemotelyCheckMigratingPartition(currentMigratingPartition));
                        boolean valid = rrp.getResultAsBoolean(1);
                        if (valid) {
                            logger.log(Level.FINEST, "Master has confirmed current " + currentMigratingPartition);
                        } else {
                            logger.log(Level.INFO, currentMigratingPartition +
                                    " could not be validated with master! " +
                                    "Removing current MigratingPartition...");
                            concurrentMapManager.enqueueAndReturn(new Processable() {
                                public void process() {
                                    migratingPartition = null;
                                }
                            });
                        }
                    } catch (Throwable t) {
                        logger.log(Level.WARNING, t.getMessage(), t);
                    }
                }
            }
        }
    }

    private class PrepareRepartitioningTask implements Runnable {
        final List<MigrationRequestTask> lostQ = new ArrayList<MigrationRequestTask>();
        final List<MigrationRequestTask> scheduledQ = new ArrayList<MigrationRequestTask>(partitionCount);
        final List<MigrationRequestTask> immediateQ = new ArrayList<MigrationRequestTask>(partitionCount * 2);

        private PrepareRepartitioningTask() {
        }

        public final void run() {
            if (concurrentMapManager.isMaster()
                    && concurrentMapManager.node.isActive() && initialized) {
                doRun();
            }
        }

        void doRun() {
            prepareMigrationTasks();
            logger.log(Level.INFO, "Re-partitioning cluster data... Immediate-Tasks: "
                    + immediateQ.size() + ", Scheduled-Tasks: " + scheduledQ.size());
            fillMigrationQueues();
        }

        void prepareMigrationTasks() {
            final Collection<MemberImpl> members = new LinkedList<MemberImpl>();
            Collection<Member> memberSet = concurrentMapManager.node.getClusterImpl().getMembers();
            for (Member member : memberSet) {
                members.add((MemberImpl) member);
            }
            PartitionStateGenerator psg = getPartitionStateGenerator();
            psg.reArrange(partitions, members, partitionCount, lostQ, immediateQ, scheduledQ);
        }

        void fillMigrationQueues() {
            lastRepartitionTime.set(Clock.currentTimeMillis());
            if (!lostQ.isEmpty()) {
                concurrentMapManager.enqueueAndReturn(new LostPartitionsAssignmentProcess(lostQ));
                logger.log(Level.WARNING, "Assigning new owners for " + lostQ.size() +
                        " LOST partitions!");
            }
            for (MigrationRequestTask migrationRequestTask : immediateQ) {
                immediateTasksQueue.offer(new Migrator(migrationRequestTask));
            }
            immediateQ.clear();
            for (MigrationRequestTask migrationRequestTask : scheduledQ) {
                scheduledTasksQueue.offer(new Migrator(migrationRequestTask));
            }
            scheduledQ.clear();
        }
    }

    private class LostPartitionsAssignmentProcess implements Processable {
        final List<MigrationRequestTask> lostQ;

        private LostPartitionsAssignmentProcess(final List<MigrationRequestTask> lostQ) {
            this.lostQ = lostQ;
        }

        public void process() {
            if (!concurrentMapManager.isMaster()
                    || !concurrentMapManager.node.isActive()) return;
            for (MigrationRequestTask migrationRequestTask : lostQ) {
                int partitionId = migrationRequestTask.getPartitionId();
                int replicaIndex = migrationRequestTask.getReplicaIndex();
                if (replicaIndex != 0 || partitionId >= partitionCount) {
                    logger.log(Level.WARNING, "Wrong task for lost partitions assignment process" +
                            " => " + migrationRequestTask);
                    continue;
                }
                PartitionInfo partition = partitions[partitionId];
                Address newOwner = migrationRequestTask.getToAddress();
                MemberImpl ownerMember = concurrentMapManager.getMember(newOwner);
                if (ownerMember != null) {
                    partition.setReplicaAddress(replicaIndex, newOwner);
                    concurrentMapManager.sendMigrationEvent(false, migrationRequestTask);
                }
            }
            sendPartitionRuntimeState();
        }
    }

    private class CheckRepartitioningTask extends PrepareRepartitioningTask implements Runnable {
        void doRun() {
            if (shouldCheckRepartitioning()) {
                final int v = version.get();
                prepareMigrationTasks();
                int totalTasks = 0;
                for (MigrationRequestTask task : immediateQ) {
                    if (task.getReplicaIndex() <= REPARTITIONING_TASK_REPLICA_THRESHOLD) {
                        totalTasks++;
                    }
                }
                for (MigrationRequestTask task : scheduledQ) {
                    if (task.getReplicaIndex() <= REPARTITIONING_TASK_REPLICA_THRESHOLD) {
                        totalTasks++;
                    }
                }
                if (!lostQ.isEmpty() || totalTasks > REPARTITIONING_TASK_COUNT_THRESHOLD) {
                    logger.log(Level.WARNING, "Something weird! Migration task queues are empty," +
                            " last repartitioning executed on " + lastRepartitionTime.get() +
                            " but repartitioning check resulted " + totalTasks + " tasks" +
                            " and " + lostQ.size() + " lost partitions!");
                    if (version.get() == v && shouldCheckRepartitioning()) {
                        fillMigrationQueues();
                    }
                }
            }
        }
    }

    private class Migrator implements Runnable {
        final MigrationRequestTask migrationRequestTask;

        Migrator(MigrationRequestTask migrationRequestTask) {
            this.migrationRequestTask = migrationRequestTask;
        }

        public void run() {
            try {
                if (!concurrentMapManager.node.isActive()
                        || !concurrentMapManager.node.isMaster()) {
                    return;
                }
                if (migrationRequestTask.isMigration() && migrationRequestTask.getReplicaIndex() == 0) {
                    concurrentMapManager.enqueueAndWait(new Processable() {
                        public void process() {
                            concurrentMapManager.sendMigrationEvent(true, migrationRequestTask);
                        }
                    }, 100);
                }
                if (migrationRequestTask.getToAddress() == null) {
                    // A member is dead, this replica should not have an owner!
                    logger.log(Level.FINEST, "Fixing partition, " + migrationRequestTask.getReplicaIndex()
                            + ". replica of partition[" + migrationRequestTask.getPartitionId() + "] should be removed.");
                    concurrentMapManager.enqueueAndWait(new Processable() {
                        public void process() {
                            int partitionId = migrationRequestTask.getPartitionId();
                            int replicaIndex = migrationRequestTask.getReplicaIndex();
                            PartitionInfo partition = partitions[partitionId];
                            partition.setReplicaAddress(replicaIndex, null);
                            migratingPartition = null;
                        }
                    });
                } else {
                    MemberImpl fromMember = null;
                    Object result = Boolean.FALSE;
                    if (migrationRequestTask.isMigration()) {
                        fromMember = getMember(migrationRequestTask.getFromAddress());
                    } else {
                        // ignore fromAddress of task and get actual owner from partition table
                        final int partitionId = migrationRequestTask.getPartitionId();
                        fromMember = getMember(partitions[partitionId].getOwner());
                    }
                    logger.log(Level.FINEST, "Started Migration : " + migrationRequestTask);
                    systemLogService.logPartition("Started Migration : " + migrationRequestTask);
                    if (fromMember != null) {
                        migrationRequestTask.setFromAddress(fromMember.getAddress());
                        DistributedTask task = new DistributedTask(migrationRequestTask, fromMember);
                        concurrentMapManager.enqueueAndWait(new Processable() {
                            public void process() {
                                addActiveMigration(migrationRequestTask);
                            }
                        });
                        Future future = concurrentMapManager.node.factory
                                .getExecutorService(MIGRATION_EXECUTOR_NAME).submit(task);
                        try {
                            result = future.get(partitionMigrationTimeout, TimeUnit.SECONDS);
                        } catch (Throwable e) {
                            logger.log(Level.WARNING, "Failed migrating from " + fromMember);
                        }
                    } else {
                        // Partition is lost! Assign new owner and exit.
                        result = Boolean.TRUE;
                    }
                    logger.log(Level.FINEST, "Finished Migration : " + migrationRequestTask);
                    systemLogService.logPartition("Finished Migration : " + migrationRequestTask);
                    if (Boolean.TRUE.equals(result)) {
                        concurrentMapManager.enqueueAndWait(new ProcessMigrationResult(migrationRequestTask), 10000);
                    } else {
                        // remove active partition migration
                        logger.log(Level.WARNING, "Migration task has failed => " + migrationRequestTask);
                        systemLogService.logPartition("Migration task has failed => " + migrationRequestTask);
                        concurrentMapManager.enqueueAndWait(new Processable() {
                            public void process() {
                                compareAndSetActiveMigratingPartition(migrationRequestTask, null);
                            }
                        });
                    }
                }
            } catch (Throwable t) {
                logger.log(Level.WARNING, "Error [" + t.getClass() + ": " + t.getMessage() + "] " +
                        "while executing " + migrationRequestTask);
                logger.log(Level.FINEST, t.getMessage(), t);
                systemLogService.logPartition("Failed! " + migrationRequestTask);
            }
        }
    }

    private class ProcessMigrationResult implements Processable {
        final MigrationRequestTask migrationRequestTask;

        private ProcessMigrationResult(final MigrationRequestTask migrationRequestTask) {
            this.migrationRequestTask = migrationRequestTask;
        }

        public void process() {
            int partitionId = migrationRequestTask.getPartitionId();
            int replicaIndex = migrationRequestTask.getReplicaIndex();
            PartitionInfo partition = partitions[partitionId];
            if (PartitionInfo.MAX_REPLICA_COUNT < replicaIndex) {
                String msg = "Migrated [" + partitionId + ":" + replicaIndex
                        + "] but cannot assign. Length:" + PartitionInfo.MAX_REPLICA_COUNT;
                logger.log(Level.WARNING, msg);
            } else {
                Address newOwner = migrationRequestTask.getToAddress();
                MemberImpl ownerMember = concurrentMapManager.getMember(newOwner);
                if (ownerMember == null) return;
                partition.setReplicaAddress(replicaIndex, newOwner);
                // if this partition should be copied back,
                // just set partition's replica address
                // before data is cleaned up.
                if (migrationRequestTask.getSelfCopyReplicaIndex() > -1) {
                    partition.setReplicaAddress(migrationRequestTask.getSelfCopyReplicaIndex(),
                            migrationRequestTask.getFromAddress());
                }
                sendPartitionRuntimeState();
                compareAndSetActiveMigratingPartition(migrationRequestTask, null);
                if (replicaIndex == 0) {
                    concurrentMapManager.sendMigrationEvent(false, migrationRequestTask);
                }
            }
        }
    }

    private class MigrationService extends Thread implements Runnable {
        MigrationService(Node node) {
            super(node.threadGroup, node.getThreadNamePrefix("MigrationThread"));
        }

        public void run() {
            ThreadContext.get().setCurrentFactory(concurrentMapManager.node.factory);
            try {
                while (running) {
                    Runnable r = null;
                    while (isActive() && (r = immediateTasksQueue.poll()) != null) {
                        safeRunImmediate(r);
                    }
                    if (!running) {
                        break;
                    }
                    // wait for partitionMigrationInterval before executing scheduled tasks
                    // and poll immediate tasks occasionally during wait time.
                    long totalWait = 0L;
                    while (isActive() && (r != null || totalWait < partitionMigrationInterval)) {
                        long start = Clock.currentTimeMillis();
                        r = immediateTasksQueue.poll(1, TimeUnit.SECONDS);
                        safeRunImmediate(r);
                        totalWait += (Clock.currentTimeMillis() - start);
                    }
                    if (isActive()) {
                        r = scheduledTasksQueue.poll();
                        safeRun(r);
                    }
                    if (!migrationActive.get() || hasNoTasks()) {
                        Thread.sleep(250);
                        continue;
                    }
                }
            } catch (InterruptedException e) {
                logger.log(Level.FINEST, "MigrationService is interrupted: " + e.getMessage(), e);
                running = false;
            } finally {
                clearTaskQueues();
            }
        }

        private boolean hasNoTasks() {
            return (immediateTasksQueue.isEmpty() && scheduledTasksQueue.isEmpty());
        }

        private boolean isActive() {
            return migrationActive.get() && running;
        }

        boolean safeRun(final Runnable r) {
            if (r == null || !running) return false;
            try {
                r.run();
            } catch (Throwable t) {
                logger.log(Level.WARNING, t.getMessage(), t);
            }
            return true;
        }

        void safeRunImmediate(final Runnable r) throws InterruptedException {
            if (safeRun(r) && immediateBackupInterval > 0) {
                Thread.sleep(immediateBackupInterval);
            }
        }
    }

    // for testing purposes only
    public boolean activateMigration() {
        return migrationActive.compareAndSet(false, true);
    }

    // for testing purposes only
    public boolean inactivateMigration() {
        migrationActive.compareAndSet(true, false);
        while (migratingPartition != null) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                return true;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionManager[" + version + "] {\n");
        sb.append("migratingPartition: " + migratingPartition);
        sb.append("\n");
        sb.append("immediateQ:" + immediateTasksQueue.size());
        sb.append(", scheduledQ:" + scheduledTasksQueue.size());
        sb.append("\n}");
        return sb.toString();
    }
}
