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

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.impl.base.DataRecordEntry;
import com.hazelcast.impl.base.RecordSet;
import com.hazelcast.impl.concurrentmap.ClusterRuntimeState;
import com.hazelcast.impl.concurrentmap.MigrationRequestTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class PartitionManager {
    private final ConcurrentMapManager concurrentMapManager;
    private final ILogger logger;
    private final int PARTITION_COUNT;
    private final PartitionInfo[] partitions;
    private final Map<Integer, PartitionInfo> mapActiveMigrations = new HashMap<Integer, PartitionInfo>(271);
    private boolean initialized = false;
    private final ScheduledThreadPoolExecutor esMigrationService = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);

    public PartitionManager(final ConcurrentMapManager concurrentMapManager) {
        this.PARTITION_COUNT = concurrentMapManager.getPartitionCount();
        this.concurrentMapManager = concurrentMapManager;
        this.logger = concurrentMapManager.node.getLogger(PartitionManager.class.getName());
        this.partitions = new PartitionInfo[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++) {
            this.partitions[i] = new PartitionInfo(i);
        }
        esMigrationService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                concurrentMapManager.enqueueAndWait(new Processable() {
                    public void process() {
                        if (!concurrentMapManager.node.isActive()) return;
                        sendClusterRuntimeState();
                    }
                }, 1000);
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
        return partitions[partitionId].getOwner();
    }

    public boolean isMigrating(int partitionId) {
        return mapActiveMigrations.containsKey(partitionId);
    }

    public void invalidateMigratedRecords(List<Record> migratedRecords) {
        for (Record migratedRecord : migratedRecords) {
            CMap cmap = concurrentMapManager.getMap(migratedRecord.getName());
            cmap.onMigrate(migratedRecord);
            cmap.markAsEvicted(migratedRecord);
        }
    }

    public List<Record> getActivePartitionRecords(final int partitionId, final int replicaIndex, final Address newAddress) {
        concurrentMapManager.enqueueAndWait(new Processable() {
            public void process() {
                PartitionInfo migratingPartition = mapActiveMigrations.get(partitionId);
                if (migratingPartition == null) {
                    migratingPartition = new PartitionInfo(partitionId);
                    mapActiveMigrations.put(partitionId, migratingPartition);
                }
                migratingPartition.setReplicaAddress(replicaIndex, newAddress);
            }
        }, 10000);
        long now = System.currentTimeMillis();
        final Collection<CMap> cmaps = concurrentMapManager.maps.values();
        List<Record> lsResultSet = new ArrayList<Record>(1000);
        for (final CMap cmap : cmaps) {
            if (cmap.getBackupCount() >= replicaIndex) {
                for (Record rec : cmap.mapRecords.values()) {
                    if (rec.isActive() && rec.isValid(now)) {
                        if (rec.getKeyData() == null || rec.getKeyData().size() == 0) {
                            throw new RuntimeException("Record.key is null or empty " + rec.getKeyData());
                        }
                        if (rec.getBlockId() == partitionId) {
                            lsResultSet.add(rec);
                        }
                    }
                }
            }
        }
        return lsResultSet;
    }

    public void doMigrate(final int replicaIndex, final RecordSet recordSet) {
        concurrentMapManager.enqueueAndWait(new Processable() {
            public void process() {
                for (DataRecordEntry dataRecordEntry : recordSet.getRecords()) {
                    CMap cmap = concurrentMapManager.getOrCreateMap(dataRecordEntry.getName());
                    if (replicaIndex == 0) {
                        // owner
                        cmap.own(dataRecordEntry);
                    } else {
                        // backup
                        cmap.storeAsBackup(dataRecordEntry);
                    }
                    System.out.println(concurrentMapManager.getThisAddress() + " now size  " + cmap.mapRecords.size());
                }
            }
        }, 200);
    }

    public Member getMember(Address address) {
        for (Member member : concurrentMapManager.node.getClusterImpl().getMembers()) {
            MemberImpl memberImpl = (MemberImpl) member;
            if (memberImpl.getAddress().equals(address)) return member;
        }
        return null;
    }

    public void firstArrangement() {
        if (!concurrentMapManager.isMaster()) return;
        PartitionStateGenerator psg = new PartitionStateGenerator();
        PartitionInfo[] newState = psg.initialize(concurrentMapManager.lsMembers, PARTITION_COUNT);
        for (PartitionInfo partitionInfo : newState) {
            partitions[partitionInfo.getPartitionId()].setPartitionInfo(partitionInfo);
        }
        sendClusterRuntimeState();
        initialized = true;
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
    }

    public void syncForDead(MemberImpl deadMember) {
        Address deadAddress = deadMember.getAddress();
        for (PartitionInfo partition : partitions) {
            partition.onDeadAddress(deadAddress);
        }
        Address thisAddress = concurrentMapManager.getThisAddress();
        if (deadAddress == null || deadAddress.equals(thisAddress)) {
            return;
        }
        Collection<CMap> cmaps = concurrentMapManager.maps.values();
        for (CMap cmap : cmaps) {
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
        if (concurrentMapManager.isMaster()) {
            System.out.println("-====== NOW ====");
            System.out.println(this);
            sendClusterRuntimeState();
            initRepartitioning();
        }
    }

    public void syncForAdd() {
        initRepartitioning();
    }

    private void initRepartitioning() {
        if (concurrentMapManager.isMaster()) {
            if (initialized) {
                esMigrationService.getQueue().clear();
                PartitionStateGenerator psg = new PartitionStateGenerator();
                Queue<MigrationRequestTask> migrationQ = new LinkedList<MigrationRequestTask>();
                Queue<MigrationRequestTask> replicaQ = new LinkedList<MigrationRequestTask>();
                psg.arrange(partitions, concurrentMapManager.lsMembers, PARTITION_COUNT, migrationQ, replicaQ);
                for (MigrationRequestTask migrationRequestTask : replicaQ) {
                    esMigrationService.execute(new Migrator(migrationRequestTask));
                }
                for (MigrationRequestTask migrationRequestTask : migrationQ) {
                    esMigrationService.execute(new Migrator(migrationRequestTask));
                }
            }
            sendClusterRuntimeState();
        }
    }

    class Migrator implements Runnable {
        final MigrationRequestTask migrationRequestTask;

        Migrator(MigrationRequestTask migrationRequestTask) {
            this.migrationRequestTask = migrationRequestTask;
        }

        public void run() {
            try {
                if (!concurrentMapManager.node.isActive()) return;
                Member fromMember = getMember(migrationRequestTask.getFromAddress());
                DistributedTask task = new DistributedTask(migrationRequestTask, fromMember);
                Future future = concurrentMapManager.node.factory.getExecutorService().submit(task);
                Object result = future.get(600, TimeUnit.SECONDS);
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
                                }
                                checkCurrentMigrations(partition);
                                sendClusterRuntimeState();
                            }
                        }
                    }, 10000);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public void sendClusterRuntimeState() {
        long clusterTime = concurrentMapManager.node.getClusterImpl().getClusterTime();
        List<MemberImpl> lsMembers = concurrentMapManager.lsMembers;
        ArrayList<MemberInfo> memberInfos = new ArrayList<MemberInfo>(lsMembers.size());
        for (MemberImpl member : lsMembers) {
            memberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType(), member.getUuid()));
        }
        for (MemberImpl member : concurrentMapManager.getMembers()) {
            if (!member.localMember()) {
                concurrentMapManager.sendProcessableToAll(new ClusterRuntimeState(memberInfos, partitions, clusterTime), false);
            }
        }
    }

    public boolean shouldPurge(int partitionId) {
        Address thisAddress = concurrentMapManager.getThisAddress();
        PartitionInfo partitionInfo = getPartition(partitionId);
        for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
            Address address = partitionInfo.getReplicaAddress(i);
            if (thisAddress.equals(address)) {
                return false;
            }
        }
        return true;
    }

    public boolean hasActiveBackupTask() {
        return false;
    }

    public void setPartitions(PartitionInfo[] newPartitions) {
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

    public void checkCurrentMigrations(PartitionInfo partition) {
        PartitionInfo migratingPartition = mapActiveMigrations.get(partition.getPartitionId());
        if (migratingPartition != null) {
            for (int i = 0; i < PartitionInfo.MAX_REPLICA_COUNT; i++) {
                Address targetAddress = migratingPartition.getReplicaAddress(i);
                if (targetAddress != null) {
                    if (targetAddress.equals(partition.getReplicaAddress(i))) {
                        migratingPartition.setReplicaAddress(i, null);
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
        }
    }
}
