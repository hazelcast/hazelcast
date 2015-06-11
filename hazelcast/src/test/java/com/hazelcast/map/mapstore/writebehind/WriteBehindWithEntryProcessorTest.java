package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
    public void testAllPartialUpdatesStored_whenInMemoryFormatIsObject() throws Exception {
        CountDownLatch pauseStoreOp = new CountDownLatch(1);
        JournalingMapStore mapStore = new JournalingMapStore(pauseStoreOp);
        IMap map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withWriteCoalescing(false)
                .withInMemoryFormat(InMemoryFormat.OBJECT)
                .build();

        Double[] salaries = {73D, 111D, -23D, 99D, 12D, 77D, 33D};
        for (int i = 0; i < salaries.length; i++) {
            updateSalary(map, 1, salaries[i]);
        }

        pauseStoreOp.countDown();

        assertStoreOperationsCompleted(salaries.length, mapStore);
        assertArrayEquals("Map store should contain all partial updates on the object",
                salaries, getStoredSalaries(mapStore));
    }

    private Double[] getStoredSalaries(JournalingMapStore mapStore) {
        List<Double> salaries = new ArrayList<Double>();
        Iterator iterator = mapStore.iterator();
        while (iterator.hasNext()) {
            SampleObjects.Employee storedEmployee = (SampleObjects.Employee) iterator.next();
            double salary = storedEmployee.getSalary();
            salaries.add(salary);
        }

        return salaries.toArray(new Double[mapStore.queue.size()]);
    }

    private void assertStoreOperationsCompleted(final int size, final JournalingMapStore mapStore) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(size, mapStore.queue.size());
            }
        });
    }

    private void updateSalary(IMap map, int key, final double value) {
        map.executeOnKey(key, new AbstractEntryProcessor() {

            @Override
            public Object process(Map.Entry entry) {
                SampleObjects.Employee employee = (SampleObjects.Employee) entry.getValue();
                if (employee == null) {
                    employee = new SampleObjects.Employee();
                }
                employee.setSalary(value);

                entry.setValue(employee);

                return null;
            }
        });
    }


    private static class JournalingMapStore<K, V> extends MapStoreAdapter<K, V> {

        private final Queue<V> queue = new ConcurrentLinkedQueue<V>();
        private final CountDownLatch pauseStoreOp;

        public JournalingMapStore(CountDownLatch pauseStoreOp) {
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

    @Test
    public void updates_on_same_key_when_in_memory_format_is_object() throws Exception {
        final long customerId = 0L;
        final int numberOfSubscriptions = 1000;
        final MapStore mapStore = new DataStore();
        final IMap<Long, Customer> map = createMap(mapStore);

        addCustomer(map, customerId);

        // add subscriptions.
        addSubscriptions(map, customerId, numberOfSubscriptions);

        // remove half of subscriptions.
        removeSubscriptions(map, customerId, numberOfSubscriptions / 2);

        assertAllSubscriptionsInStore(mapStore, numberOfSubscriptions / 2);
        assertStoreCallCount(mapStore, 1);
    }

    private IMap<Long, Customer> createMap(MapStore mapStore) {
        final TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(3)
                .withInMemoryFormat(InMemoryFormat.OBJECT);
        return builder.build();
    }

    private void assertAllSubscriptionsInStore(MapStore mapStore, final int numberOfSubscriptions) {
        final DataStore store = (DataStore) mapStore;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(numberOfSubscriptions, store.subscriptionCount());
            }
        }, 20);
    }

    private void assertStoreCallCount(MapStore mapStore, final int expectedStoreCallcount) {
        final DataStore store = (DataStore) mapStore;
        final int storeCallCount = store.getStoreCallCount();
        assertEquals(expectedStoreCallcount, storeCallCount);
    }

    private void addSubscriptions(IMap map, long customerId, int numberOfSubscriptions) {
        for (long i = 0; i < numberOfSubscriptions; i++) {
            addSubscription(map, customerId, i);
        }
    }

    private void removeSubscriptions(IMap map, long customerId, int numberOfSubscriptions) {
        for (long i = 0; i < numberOfSubscriptions; i++) {
            removeSubscription(map, customerId, i);
        }
    }

    private void addSubscription(IMap map, long customerId, final long productId) {
        map.executeOnKey(customerId, new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                final Customer customer = (Customer) entry.getValue();
                customer.addSubscription(new Subscription(productId, false));
                entry.setValue(customer);
                return customer.getSubscriptions().size();
            }
        });
    }

    private void removeSubscription(IMap map, long customerId, final long productId) {
        map.executeOnKey(customerId, new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                final Customer customer = (Customer) entry.getValue();
                customer.removeSubscription(productId);
                entry.setValue(customer);
                return customer.getSubscriptions().size();
            }
        });
    }


    private void addCustomer(IMap map, long customerId) {
        final Customer customer = new Customer(customerId);
        map.put(Long.valueOf(customerId), customer);
    }


    private static class DataStore extends MapStoreAdapter<Long, Customer> {

        private AtomicInteger storeCallCount;
        private final Map<Long, List<Subscription>> store;

        private DataStore() {
            store = new ConcurrentHashMap<Long, List<Subscription>>();
            storeCallCount = new AtomicInteger(0);
        }

        @Override
        public void store(Long key, Customer customer) {
            storeCallCount.incrementAndGet();
            final List<Subscription> subscriptions = customer.getSubscriptions();
            for (Subscription subscription : subscriptions) {
                if (!subscription.isVerified()) {
                    List<Subscription> list = store.get(key);
                    if (list == null) {
                        list = new ArrayList<Subscription>();
                        store.put(key, list);
                    }
                    subscription.setVerified(true);
                    list.add(subscription);
                }
            }
        }

        public int subscriptionCount() {
            final List<Subscription> list = store.get(0L);
            return list == null ? 0 : list.size();
        }

        public int getStoreCallCount() {
            return storeCallCount.get();
        }
    }


    private static class Customer implements Serializable {

        private long customerId;
        private List<Subscription> subscriptions;

        private Customer() {
        }

        private Customer(long customerId) {
            this.customerId = customerId;
        }

        public void addSubscription(Subscription subscription) {
            if (subscriptions == null) {
                subscriptions = new ArrayList<Subscription>();
            }
            subscriptions.add(subscription);
        }

        public void removeSubscription(long productId) {
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

        public List<Subscription> getSubscriptions() {
            return subscriptions;
        }

        public void setSubscriptions(List<Subscription> subscriptions) {
            this.subscriptions = subscriptions;
        }

        public long getCustomerId() {
            return customerId;
        }

        public void setCustomerId(long customerId) {
            this.customerId = customerId;
        }
    }

    private static class Subscription implements Serializable {
        private long productId;
        private boolean verified;

        private Subscription(long productId, boolean verified) {
            this.productId = productId;
            this.verified = verified;
        }

        public long getProductId() {
            return productId;
        }

        public void setProductId(long productId) {
            this.productId = productId;
        }

        public boolean isVerified() {
            return verified;
        }

        public void setVerified(boolean verified) {
            this.verified = verified;
        }

        @Override
        public String toString() {
            return "Subscription{"
                    + "productId=" + productId
                    + ", verified=" + verified
                    + '}';
        }
    }
}
