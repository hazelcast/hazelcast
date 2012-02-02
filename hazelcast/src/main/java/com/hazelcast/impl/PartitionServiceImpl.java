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

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.util.ResponseQueueFactory;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.toData;

public class PartitionServiceImpl implements PartitionService {
    private final ILogger logger;
    private final ConcurrentMap<Integer, PartitionProxy> mapPartitions = new ConcurrentHashMap<Integer, PartitionProxy>();
    private final List<MigrationListener> lsMigrationListeners = new CopyOnWriteArrayList<MigrationListener>();
    private final ConcurrentMapManager concurrentMapManager;
    private final Set<Partition> partitions;
    private volatile int ownedPartitionCount = -1;

    public PartitionServiceImpl(ConcurrentMapManager concurrentMapManager) {
        this.logger = concurrentMapManager.node.getLogger(PartitionService.class.getName());
        this.concurrentMapManager = concurrentMapManager;
        this.partitions = new TreeSet<Partition>();
        for (int i = 0; i < concurrentMapManager.PARTITION_COUNT; i++) {
            PartitionProxy partitionProxy = new PartitionProxy(i);
            partitions.add(partitionProxy);
            mapPartitions.put(i, partitionProxy);
        }
    }

    public int getOwnedPartitionCount() {
        int currentCount = ownedPartitionCount;
        if (currentCount > 0) {
            return currentCount;
        } else {
            currentCount = 0;
            for (Partition partition : partitions) {
                if (partition.getOwner() == null || partition.getOwner().localMember()) {
                    currentCount++;
                }
            }
            ownedPartitionCount = currentCount;
            return currentCount;
        }
    }

    public Set<Partition> getPartitions() {
        return partitions;
    }

    public PartitionProxy getPartition(Object key) {
        final Data keyData = toData(key);
        final int partitionId = concurrentMapManager.getPartitionId(keyData);
        return getPartition(partitionId);
    }

    public PartitionProxy getPartition(int partitionId) {
        return mapPartitions.get(partitionId);
    }

    public MemberImpl getPartitionOwner(final int partitionId) throws InterruptedException {
        final BlockingQueue<MemberImpl> responseQ = ResponseQueueFactory.newResponseQueue();
        concurrentMapManager.enqueueAndReturn(new Processable() {
            public void process() {
                MemberImpl memberOwner = null;
                try {
                    Address ownerAddress = concurrentMapManager.getPartitionManager().getOwner(partitionId);
                    if (ownerAddress != null) {
                        if (concurrentMapManager.thisAddress.equals(ownerAddress)) {
                            memberOwner = concurrentMapManager.thisMember;
                        } else {
                            memberOwner = concurrentMapManager.getMember(ownerAddress);
                        }
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                } finally {
                    responseQ.offer(memberOwner);
                }
            }
        });
        return responseQ.poll(10, TimeUnit.SECONDS);
    }

    void doFireMigrationEvent(final boolean started, final MigrationEvent migrationEvent) {
//        System.out.println(concurrentMapManager.getThisAddress() + "  has listeners " + lsMigrationListeners.size());
        if (migrationEvent == null) throw new IllegalArgumentException("MigrationEvent is null.");
        for (final MigrationListener migrationListener : lsMigrationListeners) {
            concurrentMapManager.executeLocally(new Runnable() {
                public void run() {
                    if (started) {
                        migrationListener.migrationStarted(migrationEvent);
                    } else {
                        migrationListener.migrationCompleted(migrationEvent);
                    }
                }
            });
        }
    }

    public void addMigrationListener(MigrationListener migrationListener) {
        lsMigrationListeners.add(migrationListener);
    }

    public void removeMigrationListener(MigrationListener migrationListener) {
        lsMigrationListeners.remove(migrationListener);
    }

    public void reset() {
        for (PartitionProxy partitionProxy : mapPartitions.values()) {
            partitionProxy.owner = null;
        }
    }

    public void setOwner(int partitionId, MemberImpl ownerMember) {
        mapPartitions.get(partitionId).owner = ownerMember;
    }

    class PartitionProxy implements Partition, Comparable {
        final int partitionId;
        volatile Member owner;

        PartitionProxy(int partitionId) {
            this.partitionId = partitionId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public Member getOwner() {
            if (owner == null) {
                try {
                    owner = getPartitionOwner(partitionId);
                } catch (InterruptedException e) {
                    owner = null;
                }
            }
            return owner;
        }

        public void resetOwner() {
            owner = null;
        }

        public int compareTo(Object o) {
            PartitionProxy partition = (PartitionProxy) o;
            Integer id = partitionId;
            return (id.compareTo(partition.getPartitionId()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionProxy partition = (PartitionProxy) o;
            return partitionId == partition.partitionId;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "Partition [" +
                    +partitionId +
                    "], owner=" + getOwner();
        }
    }
}
