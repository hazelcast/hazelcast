/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.mapstore.MapStoreTest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.CollectionUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindWithEntryProcessorTest extends HazelcastTestSupport {

    @Test
    public void testAllPartialUpdatesStored_whenInMemoryFormatIsObject() {
        CountDownLatch pauseStoreOp = new CountDownLatch(1);
        JournalingMapStore<Integer, Employee> mapStore = new JournalingMapStore<>(pauseStoreOp);
        IMap<Integer, Employee> map = TestMapUsingMapStoreBuilder.<Integer, Employee>create()
                .withMapStore(mapStore)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withWriteCoalescing(false)
                .withInMemoryFormat(InMemoryFormat.OBJECT)
                .build();

        Double[] salaries = {73D, 111D, -23D, 99D, 12D, 77D, 33D};
        for (Double salary : salaries) {
            updateSalary(map, 1, salary);
        }

        pauseStoreOp.countDown();

        assertStoreOperationsCompleted(salaries.length, mapStore);
        assertArrayEquals("Map store should contain all partial updates on the object", salaries, getStoredSalaries(mapStore));
    }

    @Test
    public void updates_on_same_key_when_in_memory_format_is_object() {
        long customerId = 0L;
        int numberOfSubscriptions = 1000;
        MapStore<Long, Customer> mapStore = new CustomerDataStore(customerId);
        IMap<Long, Customer> map = createMap(mapStore);

        // 1 store op
        addCustomer(customerId, map);
        // + 1000 store op
        addSubscriptions(map, customerId, numberOfSubscriptions);
        // + 500 store op
        removeSubscriptions(map, customerId, numberOfSubscriptions / 2);

        assertStoreOperationCount(mapStore, 1 + numberOfSubscriptions + numberOfSubscriptions / 2);
        assertFinalSubscriptionCountInStore(mapStore, numberOfSubscriptions / 2);
    }

    @Test
    public void testCoalescingMode_doesNotCauseSerialization_whenInMemoryFormatIsObject() {
        MapStore<Integer, TestObject> mapStore = new MapStoreTest.SimpleMapStore<>();
        IMap<Integer, TestObject> map = TestMapUsingMapStoreBuilder.<Integer, TestObject>create()
                .withMapStore(mapStore)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withWriteCoalescing(true)
                .withInMemoryFormat(InMemoryFormat.OBJECT)
                .build();

        final TestObject testObject = new TestObject();
        map.executeOnKey(1, new EntryProcessor<Integer, TestObject, Object>() {
            @Override
            public Object process(Map.Entry<Integer, TestObject> entry) {
                entry.setValue(testObject);
                return null;
            }

            @Override
            public EntryProcessor<Integer, TestObject, Object> getBackupProcessor() {
                return null;
            }
        });

        assertEquals(0, testObject.serializedCount);
        assertEquals(0, testObject.deserializedCount);
    }

    private Double[] getStoredSalaries(JournalingMapStore<Integer, Employee> mapStore) {
        List<Double> salaries = new ArrayList<>();
        Iterator<Employee> iterator = mapStore.iterator();
        while (iterator.hasNext()) {
            Employee storedEmployee = iterator.next();
            double salary = storedEmployee.getSalary();
            salaries.add(salary);
        }

        return salaries.toArray(new Double[0]);
    }

    private void assertStoreOperationsCompleted(final int size, final JournalingMapStore mapStore) {
        assertTrueEventually(() -> assertEquals(size, mapStore.queue.size()));
    }

    private void updateSalary(IMap<Integer, Employee> map, int key, final double value) {
        map.executeOnKey(key,
                entry -> {
                    Employee employee = entry.getValue();
                    if (employee == null) {
                        employee = new Employee();
                    }
                    employee.setSalary(value);
                    entry.setValue(employee);
                    return null;
                });
    }

    private static class JournalingMapStore<K, V> extends MapStoreAdapter<K, V> {

        private final Queue<V> queue = new ConcurrentLinkedQueue<>();
        private final CountDownLatch pauseStoreOp;

        JournalingMapStore(CountDownLatch pauseStoreOp) {
            this.pauseStoreOp = pauseStoreOp;
        }

        @Override
        public void store(K key, V value) {
            pause();
            queue.add(value);
        }

        private void pause() {
            try {
                pauseStoreOp.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public Iterator<V> iterator() {
            return queue.iterator();
        }
    }

    private static class TestObject implements DataSerializable {

        int serializedCount = 0;
        int deserializedCount = 0;

        TestObject() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(++serializedCount);
            out.writeInt(deserializedCount);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            serializedCount = in.readInt();
            deserializedCount = in.readInt() + 1;
        }
    }

    private IMap<Long, Customer> createMap(MapStore<Long, Customer> mapStore) {
        final TestMapUsingMapStoreBuilder<Long, Customer> builder = TestMapUsingMapStoreBuilder.<Long, Customer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(3)
                .withWriteCoalescing(false)
                .withInMemoryFormat(InMemoryFormat.OBJECT);
        return builder.build();
    }

    private void assertFinalSubscriptionCountInStore(MapStore mapStore, final int numberOfSubscriptions) {
        final CustomerDataStore store = (CustomerDataStore) mapStore;
        assertTrueEventually(() -> assertEquals(numberOfSubscriptions, store.subscriptionCount()));
    }

    private void assertStoreOperationCount(MapStore mapStore, final int expectedStoreCallCount) {
        final CustomerDataStore store = (CustomerDataStore) mapStore;
        assertTrueEventually(() -> {
            final int storeCallCount = store.getStoreCallCount();
            assertEquals(expectedStoreCallCount, storeCallCount);
        });
    }

    private void addSubscriptions(IMap<Long, Customer> map, long customerId, int numberOfSubscriptions) {
        for (long i = 0; i < numberOfSubscriptions; i++) {
            addSubscription(map, customerId, i);
        }
    }

    private void removeSubscriptions(IMap<Long, Customer> map, long customerId, int numberOfSubscriptions) {
        for (long i = 0; i < numberOfSubscriptions; i++) {
            removeSubscription(map, customerId, i);
        }
    }

    private void addSubscription(IMap<Long, Customer> map, long customerId, final long productId) {
        map.executeOnKey(customerId,
                entry -> {
                    final Customer customer = entry.getValue();
                    customer.addSubscription(new Subscription(productId));
                    entry.setValue(customer);
                    return customer.getSubscriptions().size();
                });
    }

    private void removeSubscription(IMap<Long, Customer> map, long customerId, final long productId) {
        map.executeOnKey(customerId,
                entry -> {
                    final Customer customer = entry.getValue();
                    customer.removeSubscription(productId);
                    entry.setValue(customer);
                    return customer.getSubscriptions().size();
                });
    }

    private void addCustomer(long customerId, IMap<Long, Customer> map) {
        final Customer customer = new Customer();
        map.put(customerId, customer);
    }

    private static class CustomerDataStore extends MapStoreAdapter<Long, Customer> {

        private AtomicInteger storeCallCount;
        private final Map<Long, List<Subscription>> store;
        private final long customerId;

        private CustomerDataStore(long customerId) {
            this.store = new ConcurrentHashMap<>();
            this.storeCallCount = new AtomicInteger(0);
            this.customerId = customerId;
        }

        @Override
        public void store(Long key, Customer customer) {
            storeCallCount.incrementAndGet();

            List<Subscription> subscriptions = customer.getSubscriptions();
            if (CollectionUtil.isEmpty(subscriptions)) {
                return;
            }
            store.put(key, subscriptions);
        }

        int subscriptionCount() {
            final List<Subscription> list = store.get(customerId);
            return list == null ? 0 : list.size();
        }

        int getStoreCallCount() {
            return storeCallCount.get();
        }
    }

    private static class Customer implements Serializable {

        private List<Subscription> subscriptions;

        private Customer() {
        }

        void addSubscription(Subscription subscription) {
            if (subscriptions == null) {
                subscriptions = new ArrayList<>();
            }
            subscriptions.add(subscription);
        }

        void removeSubscription(long productId) {
            if (subscriptions == null || subscriptions.isEmpty()) {
                return;
            }
            final Iterator<Subscription> iterator = subscriptions.iterator();
            while (iterator.hasNext()) {
                final Subscription next = iterator.next();
                if (next.getProductId() == productId) {
                    iterator.remove();
                    break;
                }
            }
        }

        List<Subscription> getSubscriptions() {
            return subscriptions;
        }
    }

    private static class Subscription implements Serializable {
        private long productId;

        private Subscription(long productId) {
            this.productId = productId;
        }

        long getProductId() {
            return productId;
        }

        @Override
        public String toString() {
            return "Subscription{"
                    + "productId=" + productId
                    + '}';
        }
    }
}
