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
import com.hazelcast.impl.partition.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEvent;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
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
    private final ConcurrentMap<Integer, PartitionInfo> mapActiveMigrations = new ConcurrentHashMap<Integer, PartitionInfo>(271, 0.75f, 1);
    private boolean initialized = false;
    private final ScheduledThreadPoolExecutor esMigrationService = new ScheduledThreadPoolExecutor(1); // !! pool size = 1
    private final AtomicInteger version = new AtomicInteger();

    public PartitionManager(final ConcurrentMapManager concurrentMapManager) {
        this.PARTITION_COUNT = concurrentMapManager.getPartitionCount();
        this.concurrentMapManager = concurrentMapManager;
        this.logger = concurrentMapManager.node.getLogger(PartitionManager.class.getName());
        this.partitions = new PartitionInfo[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++) {
            this.partitions[i] = new PartitionInfo(i, new ChangeListener() {
                public void stateChanged(ChangeEvent e) {
                    if (!concurrentMapManager.isMaster()) return;
                    version.incrementAndGet();
                }
            });
        }
        concurrentMapManager.node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                concurrentMapManager.enqueueAndReturn(new Processable() {
                    public void process() {
                        if (!concurrentMapManager.node.isActive()) return;
                        sendClusterRuntimeState();
                    }
                });
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public PartitionInfo[] getPartitions() {
        return partitions;
    }

    public PartitionInfo getPartition(int partitionId) {
        return partitions[partitionId];
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

    public static class AssignPartitions extends AbstractRemotelyProcessable {
        public void process() {
            node.concurrentMapManager.getPartitionManager().getOwner(0);
        }
    }

    public boolean isMigrating(int partitionId) {
        // volatile read
        return mapActiveMigrations.containsKey(partitionId);
    }

    public void addActiveMigration(final int partitionId, final int replicaIndex, final Address newAddress) {
        concurrentMapManager.checkServiceThread();
        PartitionInfo migratingPartition = mapActiveMigrations.get(partitionId);
        if (migratingPartition == null) {
            migratingPartition = new PartitionInfo(partitionId, null);
            PartitionInfo existing = mapActiveMigrations.putIfAbsent(partitionId, migratingPartition);
            if (existing != null) {
                migratingPartition = existing;
            }
        }
        migratingPartition.setReplicaAddress(replicaIndex, newAddress);
    }

    public void checkCurrentMigrations(PartitionInfo partition) {
        concurrentMapManager.checkServiceThread();
        PartitionInfo migratingPartition = mapActiveMigrations.get(partition.getPartitionId());
        if (migratingPartition != null) {
            boolean migrationCompleted = false;
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                Address targetAddress = migratingPartition.getReplicaAddress(i);
                if (targetAddress != null) {
                    if (targetAddress.equals(partition.getReplicaAddress(i))) {
                        migratingPartition.setReplicaAddress(i, null);
                        migrationCompleted = true;
                    }
                }
            }
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                Address targetAddress = migratingPartition.getReplicaAddress(i);
                if (targetAddress != null) {
                    return;
                }
            }
            mapActiveMigrations.remove(partition.getPartitionId());
            // do we need cleanup here? it will eventually run soon.
            if (migrationCompleted) {
                concurrentMapManager.startCleanup(false, false);
            }
        }
    }

    public CostAwareRecordList getActivePartitionRecords(final int partitionId, final int replicaIndex,
                                                         final Address newAddress, boolean diffOnly) {
        concurrentMapManager.enqueueAndWait(new Processable() {
            public void process() {
                addActiveMigration(partitionId, replicaIndex, newAddress);
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
                            lsResultSet.add(rec);
                            lsResultSet.addCost(rec.getCost());
                        }
                    }
                }
            }
        }
        return lsResultSet;
    }

    public void doMigrate(final int partitionId, final int replicaIndex, final RecordSet recordSet) {
        concurrentMapManager.enqueueAndWait(new Processable() {
            public void process() {
                addActiveMigration(partitionId, replicaIndex, concurrentMapManager.thisAddress);
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

    public void firstArrangement() {
        if (!concurrentMapManager.isMaster()) return;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ClusterPartitionManager{\n");
        int count = 0;
        for (PartitionInfo partitionInfo : partitions) {
            sb.append(partitionInfo.toString());
            sb.append("\n");
            if (count++ > 10) break;
        }
        sb.append("\n}");
        return sb.toString();
    }

    public void reset() {
        initialized = false;
        esMigrationService.getQueue().clear();
        for (PartitionInfo partition : partitions) {
            partition.setPartitionInfo(new PartitionInfo(partition.getPartitionId()));
        }
        mapActiveMigrations.clear();
        version.set(0);
    }

    public void shutdown() {
        try {
            esMigrationService.shutdownNow();
            esMigrationService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    public void syncForDead(MemberImpl deadMember) {
        concurrentMapManager.partitionServiceImpl.reset();
        boolean isMember = !deadMember.isLiteMember();
        Address deadAddress = deadMember.getAddress();
        Address thisAddress = concurrentMapManager.getThisAddress();
        int[] indexesOfDead = new int[partitions.length];
        for (PartitionInfo partition : partitions) {
            indexesOfDead[partition.getPartitionId()] = partition.getReplicaIndexOf(deadMember.getAddress());
        }
        if (isMember) {
            esMigrationService.getQueue().clear();
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
                        logger.log(Level.WARNING, "Owner of one of the replicas of Partition[" +
                                partitionId + "] is dead, but partition owner " +
                                "could not be found either!");
                        logger.log(Level.FINEST, partition.toString());
                        continue;
                    }
                    for (int replicaIndex = indexOfDead; replicaIndex <= maxBackupCount; replicaIndex++) {
                        Address target = partition.getReplicaAddress(replicaIndex);
                        if (target != null) {
                            MigrationRequestTask mrt = new MigrationRequestTask(partitionId, owner, target,
                                    replicaIndex, false, true);
                            esMigrationService.execute(new Migrator(mrt));
                        }
                    }
                }
            }
            sendClusterRuntimeState();
            esMigrationService.execute(new InitRepartitioningTask());
        }
    }

    public void syncForAdd() {
        if (concurrentMapManager.isMaster()) {
            // esMigrationService is a single thread scheduled service.
            // we should clear executor task queue
            // and submit repartitioning task to executor
            // to avoid repartitioning during a migration process.
            esMigrationService.getQueue().clear();
            esMigrationService.execute(new InitRepartitioningTask());
        }
    }

    class InitRepartitioningTask implements Runnable {
        public void run() {
            initRepartitioning();
        }
    }

    private void initRepartitioning() {
        if (concurrentMapManager.isMaster()) {
            if (initialized) {
                esMigrationService.getQueue().clear();
                PartitionStateGenerator psg = getPartitionStateGenerator();
                Queue<MigrationRequestTask> scheduledQ = new LinkedList<MigrationRequestTask>();
                Queue<MigrationRequestTask> immediateQ = new LinkedList<MigrationRequestTask>();
                psg.reArrange(partitions, concurrentMapManager.lsMembers, PARTITION_COUNT, scheduledQ, immediateQ);
                int count = 0;
                for (MigrationRequestTask migrationRequestTask : immediateQ) {
                    esMigrationService.schedule(new Migrator(migrationRequestTask), 0 * count++, TimeUnit.SECONDS);
                }
                for (MigrationRequestTask migrationRequestTask : scheduledQ) {
                    esMigrationService.schedule(new Migrator(migrationRequestTask), 0 * count++, TimeUnit.SECONDS);
                }
            }
            sendClusterRuntimeState();
        }
    }

    void fireMigrationEvent(final boolean started, int partitionId, Address from, Address to) {
//        System.out.println(concurrentMapManager.getThisAddress() + "  fireMigrationEvent " + mapActiveMigrations.size());
        final MemberImpl current = concurrentMapManager.getMember(from);
        final MemberImpl newOwner = concurrentMapManager.getMember(to);
        final MigrationEvent migrationEvent = new MigrationEvent(concurrentMapManager.node, partitionId, current, newOwner);
        concurrentMapManager.partitionServiceImpl.doFireMigrationEvent(started, migrationEvent);
    }

    public int getVersion() {
        return version.get();
    }

    public void setClusterRuntimeState(ClusterRuntimeState clusterRuntimeState) {
        setPartitions(clusterRuntimeState.getPartitions());
        version.set(clusterRuntimeState.getVersion());
    }

    class Migrator implements Runnable {
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
                if (fromMember != null) {
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

    public void sendClusterRuntimeState() {
        if (!concurrentMapManager.isMaster()) return;
        long clusterTime = concurrentMapManager.node.getClusterImpl().getClusterTime();
        List<MemberImpl> lsMembers = concurrentMapManager.lsMembers;
        ArrayList<MemberInfo> memberInfos = new ArrayList<MemberInfo>(lsMembers.size());
        for (MemberImpl member : lsMembers) {
            memberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType(), member.getUuid()));
        }
        ClusterRuntimeState crs = new ClusterRuntimeState(memberInfos, partitions, clusterTime, version.get());
        concurrentMapManager.sendProcessableToAll(crs, false);
    }

    public boolean shouldPurge(int partitionId, int maxBackupCount) {
        if (isMigrating(partitionId)) return false;
        Address thisAddress = concurrentMapManager.getThisAddress();
        PartitionInfo partitionInfo = getPartition(partitionId);
        return !partitionInfo.isOwnerOrBackup(thisAddress, maxBackupCount);
    }

    public boolean hasActiveBackupTask() {
        return false;
    }

    public void setPartitions(PartitionInfo[] newPartitions) {
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
}
