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
import com.hazelcast.core.*;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.impl.partition.PartitionListener;
import com.hazelcast.impl.partition.PartitionReplicaChangeEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import org.junit.Ignore;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.toData;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Ignore
public class TestUtil {

    public static boolean migrateKey(Object key, HazelcastInstance oldest, HazelcastInstance to, final int replicaIndex) throws Exception {
        final int partitionId = oldest.getPartitionService().getPartition(key).getPartitionId();
        final ConcurrentMapManager concurrentMapManagerOldest = getConcurrentMapManager(oldest);
        final ConcurrentMapManager concurrentMapManagerTo = getConcurrentMapManager(to);
        final PartitionInfo partitionInfoOldest = concurrentMapManagerOldest.getPartitionInfo(partitionId);
        final PartitionInfo partitionInfoTo = concurrentMapManagerTo.getPartitionInfo(partitionId);
        final MemberImpl currentOwnerMember = concurrentMapManagerOldest.getMember(partitionInfoOldest.getReplicaAddress(replicaIndex));
        final MemberImpl toMember = (MemberImpl) to.getCluster().getLocalMember();
        if (currentOwnerMember.equals(toMember)) {
            return true;
        }
        final Address addressCurrentOwner = currentOwnerMember.getAddress();
        final Address addressNewOwner = toMember.getAddress();
        PartitionListenerLatch latchOldest = new PartitionListenerLatch(toMember.getAddress(), partitionId, replicaIndex);
        PartitionListenerLatch latchTo = new PartitionListenerLatch(toMember.getAddress(), partitionId, replicaIndex);
        concurrentMapManagerOldest.getPartitionManager().addPartitionListener(latchOldest);
        concurrentMapManagerTo.getPartitionManager().addPartitionListener(latchTo);
        concurrentMapManagerOldest.enqueueAndReturn(new Processable() {
            public void process() {
                concurrentMapManagerOldest.partitionManager.forcePartitionOwnerMigration(partitionId, replicaIndex, addressCurrentOwner, addressNewOwner);
            }
        });
        assertTrue("Migration should get completed in 20 seconds!!", latchOldest.await(20, TimeUnit.SECONDS));
        assertTrue("Migration should get completed in 20 seconds!!", latchTo.await(20, TimeUnit.SECONDS));
        assertEquals(toMember.getAddress(), partitionInfoOldest.getReplicaAddress(replicaIndex));
        assertEquals(toMember.getAddress(), partitionInfoTo.getReplicaAddress(replicaIndex));
        return true;
    }

    static class PartitionListenerLatch implements PartitionListener {
        final CountDownLatch migrationLatch = new CountDownLatch(1);
        final Address toAddress;
        final int partitionId;
        final int replicaIndex;

        PartitionListenerLatch(Address toAddress, int partitionId, int replicaIndex) {
            this.toAddress = toAddress;
            this.partitionId = partitionId;
            this.replicaIndex = replicaIndex;
        }

        public void replicaChanged(PartitionReplicaChangeEvent event) {
            if (event.getReplicaIndex() == replicaIndex
                    && event.getPartitionId() == partitionId
                    && toAddress != null
                    && toAddress.equals(event.getNewAddress())) {
                migrationLatch.countDown();
            }
        }

        public boolean await(int time, TimeUnit timeUnit) throws InterruptedException {
            return migrationLatch.await(time, timeUnit);
        }
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
        return new DefaultRecord(cmap, 1, key, value, 0, 0, recordId);
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
        if (value == null) {
            return newRequest(ClusterOperation.CONCURRENT_MAP_CONTAINS_KEY, key, value, -1);
        } else {
            return newRequest(ClusterOperation.CONCURRENT_MAP_CONTAINS_ENTRY, key, value, -1);
        }
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
    public static class OrderUpdateRunnable implements Serializable, Runnable, PartitionAware<Integer>, HazelcastInstanceAware {
        int customerId;
        int orderId;
        transient HazelcastInstance hazelcastInstance;

        public OrderUpdateRunnable(int orderId, int customerId) {
            this.customerId = customerId;
            this.orderId = orderId;
        }

        public void run() {
            if (!hazelcastInstance.getPartitionService().getPartition(customerId).getOwner().localMember()) {
                throw new RuntimeException("Not local member");
            }
        }

        public int getCustomerId() {
            return customerId;
        }

        public int getOrderId() {
            return orderId;
        }

        public Integer getPartitionKey() {
            return customerId;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public String toString() {
            return "OrderUpdateRunnable{" +
                    "customerId=" + customerId +
                    ", orderId=" + orderId +
                    '}';
        }
    }

    @Ignore
    public static class OrderUpdateCallable implements Serializable, Callable<Boolean>, PartitionAware, HazelcastInstanceAware {
        int customerId;
        int orderId;
        transient HazelcastInstance hazelcastInstance;

        public OrderUpdateCallable(int orderId, int customerId) {
            this.customerId = customerId;
            this.orderId = orderId;
        }

        public Boolean call() throws Exception {
            return hazelcastInstance.getPartitionService().getPartition(customerId).getOwner().localMember();
        }

        public int getCustomerId() {
            return customerId;
        }

        public int getOrderId() {
            return orderId;
        }

        public Object getPartitionKey() {
            return customerId;
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public String toString() {
            return "OrderUpdateCallable{" +
                    "customerId=" + customerId +
                    ", orderId=" + orderId +
                    '}';
        }
    }

    @Ignore
    public static class OrderKey implements Serializable, PartitionAware {
        int customerId;
        int orderId;

        public OrderKey(int orderId, int customerId) {
            this.customerId = customerId;
            this.orderId = orderId;
        }

        public int getCustomerId() {
            return customerId;
        }

        public int getOrderId() {
            return orderId;
        }

        public Object getPartitionKey() {
            return customerId;
        }

        @Override
        public String toString() {
            return "OrderKey{" +
                    "customerId=" + customerId +
                    ", orderId=" + orderId +
                    '}';
        }
    }

    @Ignore
    public static class Employee implements Serializable {
        long id;
        String name;
        String city;
        int age;
        boolean active;
        double salary;
        Timestamp date;
        Date createDate;
        java.sql.Date sqlDate;

        public Employee(long id, String name, int age, boolean live, double price) {
            this(id, name, null, age, live, price);
        }

        public Employee(String name, int age, boolean live, double price) {
            this(-1, name, age, live, price);
        }

        public Employee(long id, String name, String city, int age, boolean live, double price) {
            this.id = id;
            this.name = name;
            this.city = city;
            this.age = age;
            this.active = live;
            this.salary = price;
            this.createDate = new Date();
            this.date = new Timestamp(createDate.getTime());
            this.sqlDate = new java.sql.Date(createDate.getTime());
        }

        public Employee() {
        }

        public Timestamp getDate() {
            return date;
        }

        public String getName() {
            return name;
        }

        public String getCity() {
            return city;
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
            sb.append(", city=").append(city);
            sb.append(", age=").append(age);
            sb.append(", active=").append(active);
            sb.append(", salary=").append(salary);
            sb.append('}');
            return sb.toString();
        }
    }
}
