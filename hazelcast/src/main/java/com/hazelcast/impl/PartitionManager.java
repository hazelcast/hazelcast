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

import com.hazelcast.cluster.AbstractRemotelyProcessable;
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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class PartitionManager {
    private final ConcurrentMapManager concurrentMapManager;
    private final ILogger logger;
    private final int PARTITION_COUNT;
    private final PartitionInfo[] partitions;

    // updates will come from ServiceThread but reads will be multithreaded. So concurrencyLevel is 1.
    private final ConcurrentMap<Integer, MigratingPartition> mapActiveMigrations = new ConcurrentHashMap<Integer, MigratingPartition>(271, 0.75f, 1);
    private boolean initialized = false;
    private final AtomicInteger version = new AtomicInteger();
    private final List<PartitionListener> lsPartitionListeners = new CopyOnWriteArrayList<PartitionListener>();
    private boolean running = true; // accessed only by MigrationService thread
    private final int partitionMigrationInterval;
    private final int immediateBackupInterval;
    private final MigrationService migrationService;
    private final BlockingQueue<Runnable> immediateTasksQueue = new LinkedBlockingQueue<Runnable>();
    private final Queue<Runnable> scheduledTasksQueue = new LinkedBlockingQueue<Runnable>();

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
        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                concurrentMapManager.enqueueAndReturn(new Processable() {
                    public void process() {
                        if (!node.isActive()) return;
                        sendClusterRuntimeState();
                    }
                });
            }
        }, 5, 5, TimeUnit.SECONDS);
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

    private void addActiveMigration(final int partitionId, final int replicaIndex,
                                    final Address currentAddress, final Address newAddress) {
        concurrentMapManager.checkServiceThread();
        MigratingPartition migratingPartition = mapActiveMigrations.get(partitionId);
        if (migratingPartition == null) {
            migratingPartition = new MigratingPartition(partitionId);
            MigratingPartition existing = mapActiveMigrations.putIfAbsent(partitionId, migratingPartition);
            if (existing != null) {
                migratingPartition = existing;
            }
        }
        migratingPartition.setAddresses(replicaIndex, currentAddress, newAddress);
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
        for (PartitionInfo partition : partitions) {
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                partition.setReplicaAddress(i, null);
            }
        }
        mapActiveMigrations.clear();
        version.set(0);
    }

    private void clearTaskQueues() {
        immediateTasksQueue.clear();
        scheduledTasksQueue.clear();
    }

    public void shutdown() {
        try {
            final CountDownLatch stopLatch = new CountDownLatch(1);
            immediateTasksQueue.offer(new Runnable() {
                public void run() {
                    clearTaskQueues();
                    running = false;
                    stopLatch.countDown();
                }
            });
            migrationService.interrupt();
            stopLatch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    public boolean hasStorageMember() {
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
        boolean isMember = !deadMember.isLiteMember();
        Address deadAddress = deadMember.getAddress();
        Address thisAddress = concurrentMapManager.getThisAddress();
        Iterator<MigratingPartition> migratingPartitions = mapActiveMigrations.values().iterator();
        while (migratingPartitions.hasNext()) {
            MigratingPartition mp = migratingPartitions.next();
            mp.onDeadAddress(deadAddress);
            if (mp.isEmpty()) {
                migratingPartitions.remove();
            }
        }
        int[] indexesOfDead = new int[partitions.length];
        for (PartitionInfo partition : partitions) {
            indexesOfDead[partition.getPartitionId()] = partition.getReplicaIndexOf(deadMember.getAddress());
        }
        if (isMember) {
            clearTaskQueues();
            for (PartitionInfo partition : partitions) {
                partition.onDeadAddress(deadAddress);
            }
            if (deadAddress == null || deadAddress.equals(thisAddress)) {
                return;
            }
        }
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
        if (isMember && concurrentMapManager.isMaster()) {
            int maxBackupCount = 0;
            for (final CMap cmap : concurrentMapManager.maps.values()) {
                maxBackupCount = Math.max(maxBackupCount, cmap.getBackupCount());
            }
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
                    for (int replicaIndex = indexOfDead; replicaIndex <= maxBackupCount; replicaIndex++) {
                        Address target = partition.getReplicaAddress(replicaIndex);
                        if (target != null && !target.equals(owner)) {
                            MigrationRequestTask mrt = new MigrationRequestTask(partitionId, owner, target,
                                    replicaIndex, false, true);
                            immediateTasksQueue.offer(new Migrator(mrt));
                        }
                    }
                }
            }
            sendClusterRuntimeState();
            immediateTasksQueue.offer(new InitRepartitioningTask(new ArrayList<MemberImpl>(concurrentMapManager.lsMembers)));
        }
    }

    public void syncForAdd() {
        if (concurrentMapManager.isMaster()) {
            // to avoid repartitioning during a migration process.
            clearTaskQueues();
            immediateTasksQueue.offer(new InitRepartitioningTask(new ArrayList<MemberImpl>(concurrentMapManager.lsMembers)));
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
            checkCurrentMigrations(currentPartition);
            Address ownerAddress = currentPartition.getOwner();
            if (ownerAddress != null) {
                MemberImpl ownerMember = concurrentMapManager.getMember(ownerAddress);
                if (ownerMember != null) {
                    concurrentMapManager.partitionServiceImpl.setOwner(currentPartition.getPartitionId(), ownerMember);
                } else {
                    concurrentMapManager.partitionServiceImpl.setOwner(currentPartition.getPartitionId(), null);
                }
            } else {
                concurrentMapManager.partitionServiceImpl.setOwner(currentPartition.getPartitionId(), null);
            }
        }
        initialized = true;
    }

    private void checkCurrentMigrations(PartitionInfo partition) {
        concurrentMapManager.checkServiceThread();
        MigratingPartition migratingPartition = mapActiveMigrations.get(partition.getPartitionId());
        if (migratingPartition != null) {
            boolean migrationCompleted = false;
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                Address targetAddress = migratingPartition.getTargetAddress(i);
                if (targetAddress != null) {
                    if (targetAddress.equals(partition.getReplicaAddress(i))) {
                        migratingPartition.removeAddresses(i);
                        migrationCompleted = true;
                    }
                }
            }
            if (migratingPartition.isEmpty()) {
                mapActiveMigrations.remove(partition.getPartitionId());
            }
            // do we need cleanup here? it will eventually run soon.
//            if (migrationCompleted) {
//                concurrentMapManager.startCleanup(false, false);
//            }
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
        return mapActiveMigrations.containsKey(partitionId);
    }

    public PartitionInfo getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public boolean hasActiveBackupTask() {
        if (!initialized) return false;
        if (concurrentMapManager.isLiteMember()) return false;
        int maxBackupCount = 0;
        for (final CMap cmap : concurrentMapManager.maps.values()) {
            maxBackupCount = Math.max(maxBackupCount, cmap.getBackupCount());
        }
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
        concurrentMapManager.partitionServiceImpl.getPartition(partitionId).resetOwner();
        concurrentMapManager.partitionServiceImpl.doFireMigrationEvent(started, migrationEvent);
    }

    public static class AssignPartitions extends AbstractRemotelyProcessable {
        public void process() {
            node.concurrentMapManager.getPartitionManager().getOwner(0);
        }
    }

    // not thread safe
    // accessed only by ServiceThread
    private class MigratingPartition {
        final int partitionId;
        final Address[] owners = new Address[PartitionInfo.MAX_REPLICA_COUNT];
        final Address[] targets = new Address[PartitionInfo.MAX_REPLICA_COUNT];

        private MigratingPartition(final int partitionId) {
            this.partitionId = partitionId;
        }

        private void setAddresses(int index, Address owner, Address target) {
            owners[index] = owner;
            targets[index] = target;
        }

        private void removeAddresses(int index) {
            owners[index] = null;
            targets[index] = null;
        }

        private Address getTargetAddress(int index) {
            return targets[index];
        }

        private boolean isEmpty() {
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                if (targets[i] != null) {
                    return false;
                }
            }
            return true;
        }

        private void onDeadAddress(Address address) {
            for (int i = 0; i < owners.length; i++) {
                Address owner = owners[i];
                if (owner != null && owner.equals(address)) {
                    removeAddresses(i);
                }
            }
            for (int i = 0; i < targets.length; i++) {
                Address target = targets[i];
                if (target != null && target.equals(address)) {
                    removeAddresses(i);
                }
            }
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("MigratingPartition");
            sb.append("{partitionId=").append(partitionId);
            sb.append(", owners=").append(owners == null ? "null" : Arrays.asList(owners).toString());
            sb.append(", targets=").append(targets == null ? "null" : Arrays.asList(targets).toString());
            sb.append('}');
            return sb.toString();
        }
    }

    private class InitRepartitioningTask implements Runnable {
        final List<MemberImpl> members;

        private InitRepartitioningTask(final List<MemberImpl> members) {
            this.members = members;
        }

        public void run() {
            if (concurrentMapManager.isMaster()) {
                if (initialized) {
                    PartitionStateGenerator psg = getPartitionStateGenerator();
                    LinkedList<MigrationRequestTask> scheduledQ = new LinkedList<MigrationRequestTask>();
                    LinkedList<MigrationRequestTask> immediateQ = new LinkedList<MigrationRequestTask>();
                    psg.reArrange(partitions, members, PARTITION_COUNT, scheduledQ, immediateQ);
                    logger.log(Level.INFO, "Re-partitioning cluster data... Immediate-Tasks: "
                            + immediateQ.size() + ", Scheduled-Tasks: " + scheduledQ.size());
                    while (!immediateQ.isEmpty()) {
                        MigrationRequestTask migrationRequestTask = immediateQ.poll();
                        immediateTasksQueue.offer(new Migrator(migrationRequestTask));
                    }
                    while (!scheduledQ.isEmpty()) {
                        MigrationRequestTask migrationRequestTask = scheduledQ.poll();
                        scheduledTasksQueue.offer(new Migrator(migrationRequestTask));
                    }
                }
                sendClusterRuntimeState();
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
                if (!concurrentMapManager.node.isActive()) return;
                if (migrationRequestTask.isMigration() && migrationRequestTask.getReplicaIndex() == 0) {
                    concurrentMapManager.enqueueAndWait(new Processable() {
                        public void process() {
                            concurrentMapManager.sendMigrationEvent(true, migrationRequestTask);
                        }
                    }, 100);
                }
                MemberImpl fromMember = null;
                Object result = Boolean.FALSE;
                if (migrationRequestTask.isMigration()) {
                    fromMember = getMember(migrationRequestTask.getFromAddress());
                } else {
                    // ignore fromAddress of task and get actual owner from partition table
                    final int partitionId = migrationRequestTask.getPartitionId();
                    fromMember = getMember(partitions[partitionId].getOwner());
                }
                if (migrationRequestTask.getToAddress() == null) {
                    // A member is dead, this replica should not have an owner!
                    result = Boolean.TRUE;
                } else if (fromMember != null) {
                    migrationRequestTask.setFromAddress(fromMember.getAddress());
                    DistributedTask task = new DistributedTask(migrationRequestTask, fromMember);
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
                    concurrentMapManager.enqueueAndWait(new Processable() {
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
                                partition.setReplicaAddress(replicaIndex, newOwner);
                                if (replicaIndex == 0) {
                                    MemberImpl ownerMember = concurrentMapManager.getMember(newOwner);
                                    if (ownerMember != null) {
                                        concurrentMapManager.partitionServiceImpl.setOwner(partition.getPartitionId(), ownerMember);
                                    }
                                    concurrentMapManager.sendMigrationEvent(false, migrationRequestTask);
                                }
                                // if this partition should be copied back,
                                // just set partition's replica address
                                // before data is cleaned up.
                                if (migrationRequestTask.getSelfCopyReplicaIndex() > -1) {
                                    partition.setReplicaAddress(migrationRequestTask.getSelfCopyReplicaIndex(),
                                            migrationRequestTask.getFromAddress());
                                }
                                checkCurrentMigrations(partition);
                                sendClusterRuntimeState();
                            }
                        }
                    }, 10000);
                }
            } catch (Throwable ignored) {
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
                    if (!concurrentMapManager.isMaster()) {
                        Thread.sleep(1000);
                        continue;
                    }
                    Runnable r;
                    while ((r = immediateTasksQueue.poll()) != null) {
                        safeRunImmediate(r);
                    }
                    if (scheduledTasksQueue.isEmpty()) {
                        Thread.sleep(1000);
                        continue;
                    }
                    // wait for partitionMigrationInterval before executing scheduled tasks
                    // and poll immediate tasks occasionally during wait time.
                    long totalWait = 0L;
                    while (r == null && totalWait < partitionMigrationInterval) {
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
