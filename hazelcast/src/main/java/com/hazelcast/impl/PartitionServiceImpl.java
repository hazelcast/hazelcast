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
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.nio.IOUtil.toData;

public class PartitionServiceImpl implements PartitionService {
    private final ConcurrentMap<Integer, PartitionReal> mapRealPartitions = new ConcurrentHashMap<Integer, PartitionReal>();
    private final ConcurrentMap<Integer, PartitionProxy> mapPartitions = new ConcurrentHashMap<Integer, PartitionProxy>();
    private final List<MigrationListener> lsMigrationListeners = new CopyOnWriteArrayList<MigrationListener>();
    private final ConcurrentMapManager concurrentMapManager;
    private final AtomicLong partitionVersion = new AtomicLong();
    private final Set<Partition> partitions;

    public PartitionServiceImpl(ConcurrentMapManager concurrentMapManager) {
        this.concurrentMapManager = concurrentMapManager;
        this.partitions = new TreeSet<Partition>();
        for (int i = 0; i < concurrentMapManager.PARTITION_COUNT; i++) {
            PartitionProxy partitionProxy = new PartitionProxy(i);
            partitions.add(partitionProxy);
            mapPartitions.put(i, partitionProxy);
        }
    }

    public Set<Partition> getPartitions() {
        return partitions;
    }

    public PartitionProxy getPartition(Object key) {
        final Data keyData = toData(key);
        final int partitionId = concurrentMapManager.getBlockId(keyData);
        return getPartition(partitionId);
    }

    public PartitionProxy getPartition(int partitionId) {
        return mapPartitions.get(partitionId);
    }

    public PartitionReal getPartitionReal(final int partitionId) {
        PartitionReal partitionReal = mapRealPartitions.get(partitionId);
        if (partitionReal == null) {
            final BlockingQueue<PartitionReal> responseQ = ResponseQueueFactory.newResponseQueue();
            concurrentMapManager.enqueueAndReturn(new Processable() {
                public void process() {
                    Block block = concurrentMapManager.partitionManager.getOrCreateBlock(partitionId);
                    MemberImpl memberOwner = null;
                    MemberImpl memberMigration = null;
                    if (block.getOwner() != null) {
                        if (concurrentMapManager.thisAddress.equals(block.getOwner())) {
                            memberOwner = concurrentMapManager.thisMember;
                        } else {
                            memberOwner = concurrentMapManager.getMember(block.getOwner());
                        }
                    }
                    if (block.getMigrationAddress() != null) {
                        if (concurrentMapManager.thisAddress.equals(block.getMigrationAddress())) {
                            memberMigration = concurrentMapManager.thisMember;
                        } else {
                            memberMigration = concurrentMapManager.getMember(block.getMigrationAddress());
                        }
                    }
                    responseQ.offer(new PartitionReal(partitionId, memberOwner, memberMigration));
                }
            });
            try {
                while (partitionReal == null) {
                    partitionReal = responseQ.poll(5, TimeUnit.SECONDS);
                    if (partitionVersion == null) {
                        concurrentMapManager.node.checkNodeState();
                    }
                }
                mapRealPartitions.put(partitionId, partitionReal);
                return partitionReal;
            } catch (InterruptedException ignored) {
            }
        }
        return partitionReal;
    }

    void doFireMigrationEvent(final boolean started, final MigrationEvent migrationEvent) {
        partitionVersion.incrementAndGet();
        if (migrationEvent == null) throw new IllegalArgumentException("MigrationEvent is null.");
        Member owner = (started) ? migrationEvent.getOldOwner() : migrationEvent.getNewOwner();
        Member migrationMember = (started) ? migrationEvent.getNewOwner() : null;
        final PartitionReal partitionReal = new PartitionReal(migrationEvent.getPartitionId(), owner, migrationMember);
        mapRealPartitions.put(partitionReal.getPartitionId(), partitionReal);
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
        mapRealPartitions.clear();
    }

    class PartitionProxy implements Partition, Comparable {
        final int partitionId;

        PartitionProxy(int partitionId) {
            this.partitionId = partitionId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public Member getOwner() {
            PartitionReal partitionReal = getPartitionReal(partitionId);
            if (partitionReal == null) {
                return null;
            } else {
                return partitionReal.getOwner();
            }
        }

        public boolean isMigrating() {
            PartitionReal partitionReal = getPartitionReal(partitionId);
            return partitionReal != null && partitionReal.isMigrating();
        }

        public Member getEventualOwner() {
            PartitionReal partitionReal = getPartitionReal(partitionId);
            if (partitionReal == null) {
                return null;
            } else {
                return partitionReal.getEventualOwner();
            }
        }

        public Member getMigrationMember() {
            PartitionReal partitionReal = getPartitionReal(partitionId);
            if (partitionReal == null) {
                return null;
            } else {
                return partitionReal.getMigrationMember();
            }
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

    static class PartitionReal implements Partition, Comparable {
        final int partitionId;
        final Member owner;
        final Member migrationMember;

        PartitionReal(int partitionId, Member owner, Member migrationMember) {
            this.partitionId = partitionId;
            this.owner = owner;
            this.migrationMember = migrationMember;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public Member getOwner() {
            return owner;
        }

        public Member getMigrationMember() {
            return migrationMember;
        }

        public Member getEventualOwner() {
            return (migrationMember != null) ? migrationMember : owner;
        }

        public boolean isMigrating() {
            return migrationMember != null;
        }

        public int compareTo(Object o) {
            PartitionReal partition = (PartitionReal) o;
            Integer id = partitionId;
            return (id.compareTo(partition.getPartitionId()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionReal partition = (PartitionReal) o;
            return partitionId == partition.partitionId;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "PartitionReal [" +
                    +partitionId +
                    "], owner=" + getOwner() +
                    ", migrationMember=" + migrationMember;
        }
    }
}
