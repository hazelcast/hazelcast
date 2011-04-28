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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.Prefix;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import org.junit.Ignore;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.toData;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@Ignore
public class TestUtil {

    public static boolean migratePartition(int partitionId, HazelcastInstance oldest, HazelcastInstance to) throws Exception {
        final MemberImpl currentOwnerMember = (MemberImpl) getPartitionById(oldest.getPartitionService(), partitionId).getOwner();
        final MemberImpl toMember = (MemberImpl) to.getCluster().getLocalMember();
        if (currentOwnerMember.equals(toMember)) {
            return false;
        }
        final ConcurrentMapManager concurrentMapManagerOldest = getConcurrentMapManager(oldest);
        final Address addressCurrentOwner = currentOwnerMember.getAddress();
        final Address addressNewOwner = toMember.getAddress();
        final int blockId = getPartitionById(oldest.getPartitionService(), partitionId).getPartitionId();
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == blockId && toMember.equals(migrationEvent.getNewOwner())) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        oldest.getPartitionService().addMigrationListener(migrationListener);
        to.getPartitionService().addMigrationListener(migrationListener);
        concurrentMapManagerOldest.enqueueAndReturn(new Processable() {
            public void process() {
                Block blockToMigrate = new Block(blockId, addressCurrentOwner, addressNewOwner);
                concurrentMapManagerOldest.partitionManager.lsBlocksToMigrate.add(blockToMigrate);
                concurrentMapManagerOldest.partitionManager.initiateMigration();
            }
        });
        if (!migrationLatch.await(30, TimeUnit.SECONDS)) {
            fail("Migration should get completed in 30 seconds!!");
        }
        assertEquals(toMember, getPartitionById(oldest.getPartitionService(), partitionId).getOwner());
        assertEquals(toMember, getPartitionById(to.getPartitionService(), partitionId).getOwner());
        return true;
    }

    public static boolean initiateMigration(final int partitionId, final int completeWaitSeconds, HazelcastInstance oldest, HazelcastInstance from, HazelcastInstance to) throws Exception {
        final MemberImpl currentOwnerMember = (MemberImpl) getPartitionById(oldest.getPartitionService(), partitionId).getOwner();
        final MemberImpl toMember = (MemberImpl) to.getCluster().getLocalMember();
        if (currentOwnerMember.equals(toMember)) {
            return false;
        }
        final ConcurrentMapManager concurrentMapManagerOldest = getConcurrentMapManager(oldest);
        final ConcurrentMapManager concurrentMapManagerFrom = getConcurrentMapManager(from);
        final Address addressCurrentOwner = currentOwnerMember.getAddress();
        final Address addressNewOwner = toMember.getAddress();
        final int blockId = getPartitionById(oldest.getPartitionService(), partitionId).getPartitionId();
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
                migrationLatch.countDown();
            }
        };
        from.getPartitionService().addMigrationListener(migrationListener);
        to.getPartitionService().addMigrationListener(migrationListener);
        concurrentMapManagerFrom.enqueueAndReturn(new Processable() {
            public void process() {
                concurrentMapManagerFrom.partitionManager.MIGRATION_COMPLETE_WAIT_SECONDS = completeWaitSeconds;
            }
        });
        concurrentMapManagerOldest.enqueueAndReturn(new Processable() {
            public void process() {
                Block blockToMigrate = new Block(blockId, addressCurrentOwner, addressNewOwner);
                concurrentMapManagerOldest.partitionManager.lsBlocksToMigrate.clear();
                concurrentMapManagerOldest.partitionManager.lsBlocksToMigrate.add(blockToMigrate);
                concurrentMapManagerOldest.partitionManager.initiateMigration();
            }
        });
        if (!migrationLatch.await(20, TimeUnit.SECONDS)) {
            fail("Migration should get started in 20 seconds!!");
        }
        return true;
    }

    public abstract static class MigrationProcess {

        public abstract boolean prepare() throws Exception;

        public abstract boolean finish() throws Exception;
    }

    public static MigrationProcess createMigrationProcess(final int partitionId,
                                                          final int completeWaitSeconds,
                                                          final HazelcastInstance oldest,
                                                          final HazelcastInstance from,
                                                          final HazelcastInstance to) throws Exception {
        final MemberImpl currentOwnerMember = (MemberImpl) getPartitionById(oldest.getPartitionService(), partitionId).getOwner();
        final MemberImpl toMember = (MemberImpl) to.getCluster().getLocalMember();
        if (currentOwnerMember.equals(toMember)) {
            throw new RuntimeException();
        }
        final ConcurrentMapManager concurrentMapManagerOldest = getConcurrentMapManager(oldest);
        final ConcurrentMapManager concurrentMapManagerFrom = getConcurrentMapManager(from);
        final ConcurrentMapManager concurrentMapManagerTo = getConcurrentMapManager(to);
        final Address addressCurrentOwner = currentOwnerMember.getAddress();
        final Address addressNewOwner = toMember.getAddress();
        final int blockId = getPartitionById(oldest.getPartitionService(), partitionId).getPartitionId();
        final Block blockToMigrate = new Block(blockId, addressCurrentOwner, addressNewOwner);
        return new MigrationProcess() {
            List<Record> lsMigratingRecords = null;
            final CountDownLatch migrationLatch = new CountDownLatch(1);

            @Override
            public boolean prepare() throws Exception {
                MigrationListener migrationListener = new MigrationListener() {
                    public void migrationCompleted(MigrationEvent migrationEvent) {
                        System.out.println(migrationEvent.getSource() + "  " + migrationEvent);
                        migrationLatch.countDown();
                    }

                    public void migrationStarted(MigrationEvent migrationEvent) {
                    }
                };
                from.getPartitionService().addMigrationListener(migrationListener);
                to.getPartitionService().addMigrationListener(migrationListener);
                return concurrentMapManagerOldest.enqueueAndWait(new Processable() {
                    public void process() {
                        concurrentMapManagerOldest.partitionManager.lsBlocksToMigrate.clear();
                        concurrentMapManagerOldest.partitionManager.fireMigrationEvent(true, blockToMigrate);
                        concurrentMapManagerOldest.partitionManager.invalidateBlocksHash();
                        lsMigratingRecords = concurrentMapManagerOldest.partitionManager.prepareMigratingRecords(blockToMigrate);
                    }
                }, 10);
            }

            @Override
            public boolean finish() throws Exception {
                concurrentMapManagerFrom.enqueueAndWait(new Processable() {
                    public void process() {
                        concurrentMapManagerFrom.partitionManager.MIGRATION_COMPLETE_WAIT_SECONDS = completeWaitSeconds;
                        concurrentMapManagerFrom.partitionManager.invalidateBlocksHash();
                    }
                }, 5);
                concurrentMapManagerOldest.enqueueAndWait(new Processable() {
                    public void process() {
                        concurrentMapManagerOldest.partitionManager.invalidateBlocksHash();
                        concurrentMapManagerOldest.partitionManager.migrateRecords(blockToMigrate, lsMigratingRecords);
                    }
                }, 5);
                return migrationLatch.await(20, TimeUnit.SECONDS);
            }
        };
    }

    public static boolean migrateKey(Object key, HazelcastInstance oldest, HazelcastInstance to) throws Exception {
        final MemberImpl currentOwnerMember = (MemberImpl) oldest.getPartitionService().getPartition(key).getOwner();
        final MemberImpl toMember = (MemberImpl) to.getCluster().getLocalMember();
        if (currentOwnerMember.equals(toMember)) {
            return false;
        }
        final ConcurrentMapManager concurrentMapManagerOldest = getConcurrentMapManager(oldest);
        final Address addressCurrentOwner = currentOwnerMember.getAddress();
        final Address addressNewOwner = toMember.getAddress();
        final int blockId = oldest.getPartitionService().getPartition(key).getPartitionId();
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == blockId && migrationEvent.getNewOwner().equals(toMember)) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        oldest.getPartitionService().addMigrationListener(migrationListener);
        to.getPartitionService().addMigrationListener(migrationListener);
        concurrentMapManagerOldest.enqueueAndReturn(new Processable() {
            public void process() {
                Block blockToMigrate = new Block(blockId, addressCurrentOwner, addressNewOwner);
                concurrentMapManagerOldest.partitionManager.lsBlocksToMigrate.add(blockToMigrate);
                concurrentMapManagerOldest.partitionManager.initiateMigration();
            }
        });
        if (!migrationLatch.await(20, TimeUnit.SECONDS)) {
            fail("Migration should get completed in 20 seconds!!");
        }
        assertEquals(toMember, oldest.getPartitionService().getPartition(key).getOwner());
        assertEquals(toMember, to.getPartitionService().getPartition(key).getOwner());
        return true;
    }

    public static Node getNode(HazelcastInstance h) {
        FactoryImpl.HazelcastInstanceProxy hiProxy = (FactoryImpl.HazelcastInstanceProxy) h;
        return hiProxy.getFactory().node;
    }

    public static ConcurrentMapManager getConcurrentMapManager(HazelcastInstance h) {
        return getNode(h).concurrentMapManager;
    }

    public static CMap mockCMap(String name) {
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        Node node = new Node(mockFactory, new Config());
        node.serviceThread = Thread.currentThread();
        return new CMap(node.concurrentMapManager, "c:" + name);
    }

    public static CMap getCMap(HazelcastInstance h, String name) {
        ConcurrentMapManager concurrentMapManager = getConcurrentMapManager(h);
        String fullName = Prefix.MAP + name;
        return concurrentMapManager.getMap(fullName);
    }

    public static Partition getPartitionById(PartitionService partitionService, int partitionId) {
        for (Partition partition : partitionService.getPartitions()) {
            if (partition.getPartitionId() == partitionId) {
                return partition;
            }
        }
        return null;
    }

    public static Record newRecord(CMap cmap, long recordId, Data key, Data value) {
        return new Record(cmap, 1, key, value, 0, 0, recordId);
    }

    public static Record newRecord(long recordId, Data key, Data value) {
        CMap cmap = mock(CMap.class);
        return newRecord(cmap, recordId, key, value);
    }

    public static Record newRecord(CMap cmap, long recordId, Object key, Object value) {
        return newRecord(cmap, recordId, toData(key), toData(value));
    }

    public static Record newRecord(long recordId, Object key, Object value) {
        return newRecord(recordId, toData(key), toData(value));
    }

    public static Record newRecord(long recordId) {
        return newRecord(recordId, null, null);
    }

    public static Request newPutRequest(Data key, Data value) {
        return newPutRequest(key, value, -1);
    }

    public static Request newPutRequest(Data key, Data value, long ttl) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_PUT, key, value, ttl);
    }

    public static Request newRequest(ClusterOperation operation, Data key, Data value, long ttl) {
        Request request = new Request();
        request.setLocal(operation, null, key, value, -1, -1, ttl, null);
        return request;
    }

    public static Request newRemoveRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_REMOVE, key, null, -1);
    }

    public static Request newEvictRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_EVICT, key, null, -1);
    }

    public static Request newGetRequest(Data key) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_GET, key, null, -1);
    }

    public static Request newContainsRequest(Data key, Data value) {
        return newRequest(ClusterOperation.CONCURRENT_MAP_CONTAINS, key, value, -1);
    }

    @Ignore
    public static class ValueType implements Serializable {
        String typeName;

        public ValueType(String typeName) {
            this.typeName = typeName;
        }

        public ValueType() {
        }

        public String getTypeName() {
            return typeName;
        }
    }

    @Ignore
    public static abstract class AbstractValue implements Serializable {
        public String name;

        public AbstractValue(String name) {
            this.name = name;
        }

        protected AbstractValue() {
        }
    }

    @Ignore
    public static class Value extends AbstractValue implements Serializable {
        ValueType type;
        int index;

        public Value(String name, ValueType type, int index) {
            super(name);
            this.type = type;
            this.index = index;
        }

        public Value(String name) {
            this(name, null, 0);
        }

        public Value() {
            super("unknown");
        }

        public ValueType getType() {
            return type;
        }

        public int getIndex() {
            return index;
        }
    }

    @Ignore
    public static class EmptyMapEntry implements MapEntry {
        private long cost;
        private long creationTime;
        private long expirationTime;
        private int hits;
        private long lastAccessTime;
        private long lastUpdateTime;
        private long lastStoredTime;
        private int version;
        private boolean valid;
        private Object key;
        private Object value;
        private long id;

        public EmptyMapEntry(long id) {
            this.id = id;
        }

        public long getCost() {
            return cost;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public long getExpirationTime() {
            return expirationTime;
        }

        public int getHits() {
            return hits;
        }

        public long getLastAccessTime() {
            return lastAccessTime;
        }

        public long getLastStoredTime() {
            return lastStoredTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public long getVersion() {
            return version;
        }

        public boolean isValid() {
            return valid;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public Object setValue(Object value) {
            Object oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        public void setCost(long cost) {
            this.cost = cost;
        }

        public void setCreationTime(long creationTime) {
            this.creationTime = creationTime;
        }

        public void setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
        }

        public void setHits(int hits) {
            this.hits = hits;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public void setLastAccessTime(long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
        }

        public void setLastUpdateTime(long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public long getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EmptyMapEntry that = (EmptyMapEntry) o;
            if (id != that.id) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }

        @Override
        public String toString() {
            return "EmptyMapEntry{" +
                    "id=" + id +
                    ", expirationTime=" + expirationTime +
                    ", hits=" + hits +
                    ", lastAccessTime=" + lastAccessTime +
                    ", lastUpdateTime=" + lastUpdateTime +
                    ", key=" + key +
                    ", value=" + value +
                    ", valid=" + valid +
                    ", creationTime=" + creationTime +
                    ", cost=" + cost +
                    ", version=" + version +
                    '}';
        }
    }

    @Ignore
    public static class Employee implements Serializable {
        long id;
        String name;
        int age;
        boolean active;
        double salary;

        public Employee(long id, String name, int age, boolean live, double price) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = price;
        }

        public Employee(String name, int age, boolean live, double price) {
            this(-1, name, age, live, price);
        }

        public Employee() {
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        public double getSalary() {
            return salary;
        }

        public boolean isActive() {
            return active;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Employee employee = (Employee) o;
            if (active != employee.active) return false;
            if (age != employee.age) return false;
            if (Double.compare(employee.salary, salary) != 0) return false;
            if (name != null ? !name.equals(employee.name) : employee.name != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = name != null ? name.hashCode() : 0;
            result = 31 * result + age;
            result = 31 * result + (active ? 1 : 0);
            temp = salary != +0.0d ? Double.doubleToLongBits(salary) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("Employee");
            sb.append("{name='").append(name).append('\'');
            sb.append(", age=").append(age);
            sb.append(", active=").append(active);
            sb.append(", salary=").append(salary);
            sb.append('}');
            return sb.toString();
        }
    }
}
