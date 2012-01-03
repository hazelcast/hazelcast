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

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.impl.base.DataRecordEntry;
import com.hazelcast.impl.base.RecordSet;
import com.hazelcast.impl.concurrentmap.MigrationRequestTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.toObject;

public class ClusterPartitionManager {
    private final ConcurrentMapManager concurrentMapManager;
    private final ILogger logger;
    private final int PARTITION_COUNT;
    private final PartitionInfo[] partitions;
    private final ConcurrentMap<Integer, MigrationRequestTask> activeMigrations = new ConcurrentHashMap<Integer, MigrationRequestTask>();

    public ClusterPartitionManager(ConcurrentMapManager concurrentMapManager) {
        this.PARTITION_COUNT = concurrentMapManager.getPartitionCount();
        this.concurrentMapManager = concurrentMapManager;
        this.logger = concurrentMapManager.node.getLogger(ClusterPartitionManager.class.getName());
        this.partitions = new PartitionInfo[PARTITION_COUNT];
        for (int i = 0; i < PARTITION_COUNT; i++) {
            this.partitions[i] = new PartitionInfo(i);
        }
    }

    public PartitionInfo[] getPartitions() {
        return partitions;
    }

    public PartitionInfo getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public Address getOwner(int partitionId) {
        return partitions[partitionId].addresses[0];
    }

    public Address getBackup(int partitionId, int backupIndex) {
        return partitions[partitionId].addresses[backupIndex];
    }

    public boolean isMigrating(int partitionId) {
        return activeMigrations.containsKey(partitionId);
    }

    public MigrationRequestTask addActiveMigration(MigrationRequestTask migrationRequestTask) {
        return activeMigrations.putIfAbsent(migrationRequestTask.getPartitionId(), migrationRequestTask);
    }

    public void initMigration(final int partitionId, final int partitionStorageNodeIndex, final Address from, final Address to) {
        concurrentMapManager.node.executorManager.executeNow(new Runnable() {
            public void run() {
                try {
                    Member fromMember = getMember(from);
                    DistributedTask task = new DistributedTask(new MigrationRequestTask(partitionId, from, to), fromMember);
                    Future future = concurrentMapManager.node.factory.getExecutorService().submit(task);
                    if (future.get(10, TimeUnit.MINUTES) == Boolean.TRUE) {
                        concurrentMapManager.enqueueAndReturn(new Processable() {
                            public void process() {
                                PartitionInfo partitionInfo = partitions[partitionId];
                                if (partitionInfo.addresses.length < partitionStorageNodeIndex) {
                                    String msg = "Migrated [" + partitionId + ":" + partitionStorageNodeIndex
                                            + "] but cannot assign. Length:" + partitionInfo.addresses.length;
                                    logger.log(Level.WARNING, msg);
                                } else {
                                    partitionInfo.addresses[partitionStorageNodeIndex] = to;
                                }
                            }
                        });
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void invalidateMigratedRecords(List<Record> migratedRecords) {
        for (Record migratedRecord : migratedRecords) {
            CMap cmap = concurrentMapManager.getMap(migratedRecord.getName());
            cmap.onMigrate(migratedRecord);
            cmap.markAsEvicted(migratedRecord);
        }
    }

    public List<Record> getActivePartitionRecords(int partitionId) {
        long now = System.currentTimeMillis();
        final Collection<CMap> cmaps = concurrentMapManager.maps.values();
        List<Record> lsResultSet = new ArrayList<Record>(1000);
        for (final CMap cmap : cmaps) {
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
        return lsResultSet;
    }

    public void doMigrate(final int partitionId, final RecordSet recordSet) {
        concurrentMapManager.enqueueAndWait(new Processable() {
            public void process() {
                for (DataRecordEntry dataRecordEntry : recordSet.getRecords()) {
                    System.out.println("owning " + toObject(dataRecordEntry.getKeyData()));
                    CMap cmap = concurrentMapManager.getMap(dataRecordEntry.getName());
                    cmap.own(dataRecordEntry);
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
}
