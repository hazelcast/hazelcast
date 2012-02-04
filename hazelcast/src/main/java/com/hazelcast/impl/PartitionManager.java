/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
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
import com.hazelcast.impl.concurrentmap.CostAwareRecordList;
import com.hazelcast.impl.concurrentmap.ValueHolder;
import com.hazelcast.impl.partition.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEvent;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class PartitionManager {
    private final ConcurrentMapManager concurrentMapManager;
    private final ILogger logger;
    private final int PARTITION_COUNT;
    private final PartitionInfo[] partitions;

    // updates will come from ServiceThread (one exception is PartitionManager.reset())
    // but reads will be multithreaded.
    private volatile MigratingPartition migratingPartition;
    private boolean initialized = false;
    private final AtomicInteger version = new AtomicInteger();
    private final List<PartitionListener> lsPartitionListeners = new CopyOnWriteArrayList<PartitionListener>();
    private final int partitionMigrationInterval;
    private final int immediateBackupInterval;
    private final MigrationService migrationService;
    private boolean running = true; // accessed only by MigrationService thread
    private final BlockingQueue<Runnable> immediateTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final Queue<Runnable> scheduledTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final AtomicBoolean sendingDiffs = new AtomicBoolean(false);

    public PartitionManager(final ConcurrentMapManager concurrentMapManager) {
        this.PARTITION_COUNT = concurrentMapManager.getPartitionCount();
        this.concurrentMapManager = concurrentMapManager;
        this.logger = concurrentMapManager.node.getLogger(PartitionManager.class.getName());
        this.partitions = new PartitionInfo[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++) {
            this.partitions[i] = new PartitionInfo(i, new PartitionListener() {
                public void replicaChanged(PartitionReplicaChangeEvent event) {
                    for (PartitionListener partitionListener : lsPartitionListeners) {
                        partitionListener.replicaChanged(event);
                    }
                    if (!concurrentMapManager.isMaster()) return;
                    version.incrementAndGet();
                }
            });
        }
        final Node node = concurrentMapManager.node;
        partitionMigrationInterval = node.groupProperties.PARTITION_MIGRATION_INTERVAL.getInteger() * 1000;
        immediateBackupInterval = node.groupProperties.IMMEDIATE_BACKUP_INTERVAL.getInteger() * 1000;
        migrationService = new MigrationService(concurrentMapManager.node);
        migrationService.start();
        int partitionTableSendInterval = node.groupProperties.PARTITION_TABLE_SEND_INTERVAL.getInteger();
        if (partitionTableSendInterval <= 0) {
            partitionTableSendInterval = 1;
        }
        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new SendClusterStateTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);
        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new CheckMigratingPartitionTask(),
                partitionTableSendInterval, partitionTableSendInterval, TimeUnit.SECONDS);
//        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new CheckRepartitioningTask(),
//                60, 60, TimeUnit.SECONDS);
    }

    private void sendClusterRuntimeState() {
        if (!concurrentMapManager.isMaster()) return;
        // do not send partition state until initialized!
        // sending partition state makes nodes believe initialization completed.
        if (!initialized) return;
        long clusterTime = concurrentMapManager.node.getClusterImpl().getClusterTime();
        List<MemberImpl> lsMembers = concurrentMapManager.lsMembers;
        ArrayList<MemberInfo> memberInfos = new ArrayList<MemberInfo>(lsMembers.size());
        for (MemberImpl member : lsMembers) {
            memberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType(), member.getUuid()));
        }
        ClusterRuntimeState crs = new ClusterRuntimeState(memberInfos, partitions, clusterTime, version.get());
        concurrentMapManager.sendProcessableToAll(crs, false);
    }

    public void addPartitionListener(PartitionListener partitionListener) {
        lsPartitionListeners.add(partitionListener);
    }

    public PartitionInfo[] getPartitions() {
        return partitions;
    }

    public Address getOwner(int partitionId) {
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
        if (!concurrentMapManager.isMaster()) return;
        if (!hasStorageMember()) return;
        PartitionStateGenerator psg = getPartitionStateGenerator();
        PartitionInfo[] newState = psg.initialize(concurrentMapManager.lsMembers, PARTITION_COUNT);
        if (newState != null) {
            for (PartitionInfo partitionInfo : newState) {
                partitions[partitionInfo.getPartitionId()].setPartitionInfo(partitionInfo);
            }
        }
        sendClusterRuntimeState();
        initialized = true;
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
        long now = System.currentTimeMillis();
        final Collection<CMap> cmaps = concurrentMapManager.maps.values();
        CostAwareRecordList lsResultSet = new CostAwareRecordList(1000);
        for (final CMap cmap : cmaps) {
            boolean includeCMap = diffOnly
                    ? cmap.getBackupCount() == replicaIndex
                    : cmap.getBackupCount() >= replicaIndex;
            if (includeCMap) {
                for (Record rec : cmap.mapRecords.values()) {
                    if (rec.isActive() && rec.isValid(now)) {
                        if (rec.getKeyData() == null || rec.getKeyData().size() == 0) {
                            throw new RuntimeException("Record.key is null or empty " + rec.getKeyData());
                        }
                        if (rec.getBlockId() == partitionId) {
                            if (cmap.isMultiMap()) {
                                Collection<ValueHolder> colValues = rec.getMultiValues();
                                for (ValueHolder valueHolder : colValues) {
                                    Record record = rec.copy();
                                    record.setValue(valueHolder.getData());
                                    lsResultSet.add(record);
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
        concurrentMapManager.partitionServiceImpl.reset();
        if (!hasStorageMember()) {
            reset();
        }
        Address deadAddress = deadMember.getAddress();
        Address thisAddress = concurrentMapManager.getThisAddress();
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
                partition.onDeadAddress(deadAddress);
            }
            if (deadAddress == null || deadAddress.equals(thisAddress)) {
                return;
            }
        }
        fixCMapsForDead(deadAddress);
        fixReplicasAndPartitionsForDead(deadMember, indexesOfDead);
    }

    private void fixReplicasAndPartitionsForDead(final MemberImpl deadMember, final int[] indexesOfDead) {
        boolean isMember = !deadMember.isLiteMember();
        if (isMember && concurrentMapManager.isMaster()) {
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
//                  for (int replicaIndex = indexOfDead; replicaIndex <= maxBackupCount; replicaIndex++) {
                        Address target = partition.getReplicaAddress(replicaIndex);
                        if (target != null && !target.equals(owner)) {
                            MigrationRequestTask mrt = new MigrationRequestTask(partitionId, owner, target,
                                    replicaIndex, false, true);
                            immediateTasksQueue.offer(new Migrator(mrt));
                            diffCount++;
                        }
                    }
                    // if index of dead member is equal to or less than maxBackupCount
                    // clear that index of maxBackupCount of partition, because we need full replica copy
                    // after repartitioning on that index.
                    if (indexOfDead <= maxBackupCount) {
                        partition.setReplicaAddress(maxBackupCount, null);
                    }
                }
            }
            sendClusterRuntimeState();
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

    private void fixCMapsForDead(final Address deadAddress) {
        Address thisAddress = concurrentMapManager.getThisAddress();
        for (CMap cmap : concurrentMapManager.maps.values()) {
            cmap.onDisconnect(deadAddress);
            Object[] records = cmap.mapRecords.values().toArray();
            for (Object recordObject : records) {
                if (recordObject != null) {
                    Record record = (Record) recordObject;
                    if (record.isLocked() && cmap.isMapForQueue()) {
                        if (deadAddress.equals(record.getLock().getLockAddress())) {
                            cmap.sendKeyToMaster(record.getKeyData());
                        }
                    }
                    cmap.onDisconnect(record, deadAddress);
                    if (record.isActive() && thisAddress.equals(partitions[record.getBlockId()].getOwner())) {
                        cmap.markAsDirty(record);
                        // you have to update the indexes
                        cmap.updateIndexes(record);
                    }
                }
            }
        }
    }

    private int getMaxBackupCount() {
        int maxBackupCount = 0;
        for (final CMap cmap : concurrentMapManager.maps.values()) {
            maxBackupCount = Math.max(maxBackupCount, cmap.getBackupCount());
        }
        return maxBackupCount;
    }

    public void syncForAdd() {
        if (concurrentMapManager.isMaster()) {
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

    public void forcePartitionOwnerMigration(int partitionId, int replicaIndex, Address from, Address to) {
        MigrationRequestTask mrt = new MigrationRequestTask(partitionId, from, to, replicaIndex, true);
        immediateTasksQueue.offer(new Migrator(mrt));
    }

    public void setClusterRuntimeState(ClusterRuntimeState clusterRuntimeState) {
        setPartitions(clusterRuntimeState.getPartitions());
        version.set(clusterRuntimeState.getVersion());
    }

    private void setPartitions(PartitionInfo[] newPartitions) {
        concurrentMapManager.checkServiceThread();
        int size = newPartitions.length;
        for (int i = 0; i < size; i++) {
            PartitionInfo newPartition = newPartitions[i];
            PartitionInfo currentPartition = partitions[newPartition.getPartitionId()];
            currentPartition.setPartitionInfo(newPartition);
            checkMigratingPartitionFor(currentPartition);
        }
        initialized = true;
    }

    private void checkMigratingPartitionFor(PartitionInfo partition) {
        concurrentMapManager.checkServiceThread();
        final MigratingPartition mPartition = migratingPartition;
        if (mPartition != null) {
            if (partition.getPartitionId() == mPartition.getPartitionId()) {
                final Address targetAddress = mPartition.getToAddress();
                if (targetAddress != null
                        && targetAddress.equals(partition.getReplicaAddress(mPartition.getReplicaIndex()))) {
                    migratingPartition = null;
                }
            }
        }
    }

    public boolean shouldPurge(int partitionId, int maxBackupCount) {
        if (isMigrating(partitionId)) return false;
        Address thisAddress = concurrentMapManager.getThisAddress();
        PartitionInfo partitionInfo = getPartition(partitionId);
        return !partitionInfo.isOwnerOrBackup(thisAddress, maxBackupCount);
    }

    public boolean isMigrating(int partitionId) {
        // volatile read
        final MigratingPartition currentMigratingPartition = migratingPartition;
        return currentMigratingPartition != null
                && currentMigratingPartition.getPartitionId() == partitionId
                && currentMigratingPartition.getReplicaIndex() == 0;
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
                    logger.log(Level.WARNING, concurrentMapManager.thisAddress + " still has no replica for partitionId:" + partition.getPartitionId());
                    break;
                }
            }
        }
        return needBackup || !immediateTasksQueue.isEmpty();
    }

    void fireMigrationEvent(final boolean started, int partitionId, Address from, Address to) {
//        System.out.println(concurrentMapManager.getThisAddress() + "  fireMigrationEvent " + partitionId);
        final MemberImpl current = concurrentMapManager.getMember(from);
        final MemberImpl newOwner = concurrentMapManager.getMember(to);
        final MigrationEvent migrationEvent = new MigrationEvent(concurrentMapManager.node, partitionId, current, newOwner);
        concurrentMapManager.partitionServiceImpl.doFireMigrationEvent(started, migrationEvent);
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
            if (concurrentMapManager.isMaster()) {
                // TODO: Change log level to FINEST
                logger.log(Level.INFO, "Remaining migration tasks in queue => Immediate-Tasks: " + immediateTasksQueue.size()
                        + ", Scheduled-Tasks: " + scheduledTasksQueue.size());
                final Node node = concurrentMapManager.node;
                concurrentMapManager.enqueueAndReturn(new Processable() {
                    public void process() {
                        if (!node.isActive()) return;
                        sendClusterRuntimeState();
                    }
                });
            }
        }
    }

    private class CheckMigratingPartitionTask implements Runnable {
        public void run() {
            if (!concurrentMapManager.isMaster()) {
                final MigratingPartition currentMigratingPartition = migratingPartition;
                if (currentMigratingPartition != null) {
                    try {
                        final Node node = concurrentMapManager.node;
                        AsyncRemotelyBooleanCallable rrp = node.clusterManager.new AsyncRemotelyBooleanCallable();
                        rrp.executeProcess(node.getMasterAddress(), new RemotelyCheckMigratingPartition(currentMigratingPartition));
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
        final Collection<MemberImpl> members = new LinkedList<MemberImpl>();
        final List<MigrationRequestTask> scheduledQ = new ArrayList<MigrationRequestTask>(PARTITION_COUNT);
        final List<MigrationRequestTask> immediateQ = new ArrayList<MigrationRequestTask>(PARTITION_COUNT);

        private PrepareRepartitioningTask() {
        }

        public final void run() {
            if (concurrentMapManager.isMaster() && initialized) {
                doRun();
            }
        }

        void doRun() {
            loadCurrentMembers();
            prepareMigrationTasks();
            logger.log(Level.INFO, "Re-partitioning cluster data... Immediate-Tasks: "
                    + immediateQ.size() + ", Scheduled-Tasks: " + scheduledQ.size());
            fillMigrationQueues();
        }

        void loadCurrentMembers() {
            //            concurrentMapManager.enqueueAndWait(new Processable() {
//                public void process() {
//                    members.addAll(concurrentMapManager.lsMembers);
//                }
//            });
            Collection<Member> memberSet = concurrentMapManager.node.getClusterImpl().getMembers();
            for (Member member : memberSet) {
                members.add((MemberImpl) member);
            }
        }

        void prepareMigrationTasks() {
            PartitionStateGenerator psg = getPartitionStateGenerator();
            psg.reArrange(partitions, members, PARTITION_COUNT, scheduledQ, immediateQ);
        }

        void fillMigrationQueues() {
            clearTaskQueues();
            for (MigrationRequestTask migrationRequestTask : immediateQ) {
                immediateTasksQueue.offer(new Migrator(migrationRequestTask));
            }
            immediateQ.clear();
            for (MigrationRequestTask migrationRequestTask : scheduledQ) {
                scheduledTasksQueue.offer(new Migrator(migrationRequestTask));
            }
            scheduledQ.clear();
            // sendClusterRuntimeState();
        }
    }

    private class CheckRepartitioningTask implements Runnable {
        public void run() {
            if (shouldCheck()) {
                final int v = version.get();
//                prepareMigrationTasks();
//                final int totalTasks = immediateQ.size() + scheduledQ.size();
//                if (totalTasks > 20) {
//                    logger.log(Level.WARNING, "Something weird! Migration task queues are empty, " +
//                            "but repartitioning check resulted " + totalTasks + " tasks!");
//                    if (version.get() == v && shouldCheck()) {
//                        fillMigrationQueues();
//                    }
//                }
            }
        }

        boolean shouldCheck() {
            return immediateTasksQueue.isEmpty() && scheduledTasksQueue.isEmpty()
                    && migratingPartition == null;
        }
    }

    private class Migrator implements Runnable {
        final MigrationRequestTask migrationRequestTask;

        Migrator(MigrationRequestTask migrationRequestTask) {
            this.migrationRequestTask = migrationRequestTask;
        }

        public void run() {
            try {
                if (!concurrentMapManager.node.isActive()) return;
                if (migrationRequestTask.isMigration() && migrationRequestTask.getReplicaIndex() == 0) {
                    concurrentMapManager.enqueueAndWait(new Processable() {
                        public void process() {
                            concurrentMapManager.sendMigrationEvent(true, migrationRequestTask);
                        }
                    }, 100);
                }
                if (migrationRequestTask.getToAddress() == null) {
                    // A member is dead, this replica should not have an owner!
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
                    if (fromMember != null) {
                        migrationRequestTask.setFromAddress(fromMember.getAddress());
                        DistributedTask task = new DistributedTask(migrationRequestTask, fromMember);
                        concurrentMapManager.enqueueAndWait(new Processable() {
                            public void process() {
                                addActiveMigration(migrationRequestTask);
                            }
                        });
                        Future future = concurrentMapManager.node.factory.getExecutorService().submit(task);
                        try {
                            result = future.get(600, TimeUnit.SECONDS);
                        } catch (Throwable e) {
                            logger.log(Level.WARNING, "Failed migrating to " + fromMember);
                        }
                    } else {
                        // Partition is lost! Assign new owner and exit.
                        result = Boolean.TRUE;
                    }
                    if (Boolean.TRUE.equals(result)) {
                        concurrentMapManager.enqueueAndWait(new ProcessMigrationResult(migrationRequestTask), 10000);
                    } else {
                        // remove active partition migration
                        concurrentMapManager.enqueueAndWait(new Processable() {
                            public void process() {
                                compareAndSetActiveMigratingPartition(migrationRequestTask, null);
                            }
                        });
                    }
                }
            } catch (Throwable ignored) {
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
                if (replicaIndex == 0) {
                    concurrentMapManager.sendMigrationEvent(false, migrationRequestTask);
                }
                // if this partition should be copied back,
                // just set partition's replica address
                // before data is cleaned up.
                if (migrationRequestTask.getSelfCopyReplicaIndex() > -1) {
                    partition.setReplicaAddress(migrationRequestTask.getSelfCopyReplicaIndex(),
                            migrationRequestTask.getFromAddress());
                }
                compareAndSetActiveMigratingPartition(migrationRequestTask, null);
                sendClusterRuntimeState();
            }
        }
    }

    private class MigrationService extends Thread implements Runnable {
        MigrationService(Node node) {
            super(node.threadGroup, node.getThreadNamePrefix("MigrationService"));
        }

        public void run() {
            try {
                while (running) {
                    Runnable r;
                    while ((r = immediateTasksQueue.poll()) != null) {
                        safeRunImmediate(r);
                    }
                    if (!running) {
                        break;
                    }
                    if (scheduledTasksQueue.isEmpty()) {
                        Thread.sleep(250);
                        continue;
                    }
                    // wait for partitionMigrationInterval before executing scheduled tasks
                    // and poll immediate tasks occasionally during wait time.
                    long totalWait = 0L;
                    while (running && r == null && totalWait < partitionMigrationInterval) {
                        long start = System.currentTimeMillis();
                        r = immediateTasksQueue.poll(1, TimeUnit.SECONDS);
                        safeRunImmediate(r);
                        totalWait += (System.currentTimeMillis() - start);
                    }
                    if (running) {
                        r = scheduledTasksQueue.poll();
                        safeRun(r);
                    }
                }
            } catch (InterruptedException e) {
                logger.log(Level.FINEST, "MigrationService is interrupted: " + e.getMessage(), e);
                running = false;
            } finally {
                clearTaskQueues();
            }
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionManager{\n");
        int count = 0;
        for (PartitionInfo partitionInfo : partitions) {
            sb.append(partitionInfo.toString());
            sb.append("\n");
            if (count++ > 10) break;
        }
        sb.append("\n}");
        return sb.toString();
    }
}
