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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.QueueStoreFactory;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalQueue;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueStoreTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Test
    public void testQueueStoreLoadMoreThanMaxSize() {
        Config config = getConfig();
        int maxSize = 2000;

        TestQueueStore queueStore = new TestQueueStore();
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setStoreImplementation(queueStore);
        config.getQueueConfig("testQueueStore")
              .setMaxSize(maxSize)
              .setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        for (int i = 0; i < maxSize * 2; i++) {
            queueStore.store.put((long) i, new VersionedObject<>(i, i));
        }

        IQueue<Object> queue = instance.getQueue("testQueueStore");
        assertEquals("Queue Size should be equal to max size", maxSize, queue.size());
    }

    @Test
    public void testQueueStoreDrainTo() {
        int maxSize = 10000;
        // in the case of priority queue, all items are preloaded
        TestQueueStore queueStore = comparatorClassName != null
                ? new TestQueueStore(0, 0, 0, 0, 0, 1)
                : new TestQueueStore(0, 0, 0, 0, 2 * maxSize, 0);
        Config config = getConfigForDrainToTest(maxSize, 1, queueStore);

        HazelcastInstance instance = createHazelcastInstance(config);
        // initialize queue store with 2 * maxSize
        for (int i = 0; i < maxSize * 2; i++) {
            queueStore.store.put((long) i, new VersionedObject<>(i));
        }

        IQueue<VersionedObject<Object>> queue = instance.getQueue("testQueueStore");
        List<Object> items = new ArrayList<>();
        int count = queue.drainTo(items);
        assertEquals(2 * maxSize, count);
        assertOpenEventually(queueStore.latchLoad);
        assertOpenEventually(queueStore.latchLoadAll);
    }

    @Test
    public void testQueueStoreDrainTo_whenBulkLoadEnabled() {
        int maxSize = 10000;
        int queueStoreSize = 2 * maxSize;
        int bulkLoadSize = 10;
        // in the case of priority queue, all items are preloaded
        // so we can sort them
        TestQueueStore queueStore = comparatorClassName != null
                ? new TestQueueStore(0, 0, 0, 0, 0, 1)
                : new TestQueueStore(0, 0, 0, 0, 0, queueStoreSize / bulkLoadSize);
        Config config = getConfigForDrainToTest(maxSize, bulkLoadSize, queueStore);

        HazelcastInstance instance = createHazelcastInstance(config);
        // setup queue store with 2 * maxSize
        for (int i = 0; i < queueStoreSize; i++) {
            queueStore.store.put((long) i, new VersionedObject<>(i, i));
        }

        IQueue<VersionedObject<Integer>> queue = instance.getQueue("testQueueStore");
        List<VersionedObject<Integer>> items = new ArrayList<>();
        int count = queue.drainTo(items);
        assertEquals(queueStoreSize, count);
        assertOpenEventually(queueStore.latchLoadAll);
    }

    @Test
    public void testRemoveAll() {
        int maxSize = 2000;
        Config config = getConfig();
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setStoreImplementation(new TestQueueStore())
                .setProperty("bulk-load", String.valueOf(200));
        config.getQueueConfig("testQueueStore")
              .setMaxSize(maxSize)
              .setQueueStoreConfig(queueStoreConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        IQueue<VersionedObject<Integer>> queue = instance.getQueue("testQueueStore");

        for (int i = 0; i < maxSize; i++) {
            queue.add(new VersionedObject<>(i));
        }
        assertEquals(maxSize, queue.size());

        for (VersionedObject<Integer> o : queue) {
            queue.remove(o);
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void testIssue1401QueueStoreWithTxnPoll() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setStoreImplementation(new MyQueueStore())
                .setEnabled(true)
                .setProperty("binary", "false")
                .setProperty("memory-limit", "0")
                .setProperty("bulk-load", "100");

        Config config = getConfig();
        config.getQueueConfig("test")
              .setMaxSize(10)
              .setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        for (int i = 0; i < 10; i++) {
            TransactionContext context = instance.newTransactionContext();
            context.beginTransaction();

            TransactionalQueue<VersionedObject<String>> queue = context.getQueue("test");
            VersionedObject<String> queueData = queue.poll();
            assertNotNull(queueData);
            context.commitTransaction();
        }
    }

    @Test
    public void testQueueStore() throws Exception {
        Config config = getConfig();
        int maxSize = 2000;

        TestQueueStore queueStore = new TestQueueStore(1000, 0, 2000, 0, 0, 0, 1);

        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setStoreImplementation(queueStore);
        config.getQueueConfig("testQueueStore")
              .setMaxSize(maxSize)
              .setQueueStoreConfig(queueStoreConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        for (int i = 0; i < maxSize / 2; i++) {
            queueStore.store.put((long) i, new VersionedObject<>(i, i));
        }

        IQueue<VersionedObject<Integer>> queue = instance.getQueue("testQueueStore");

        for (int i = 0; i < maxSize / 2; i++) {
            int id = i + maxSize / 2;
            queue.offer(new VersionedObject<>(id, id));
        }

        instance.shutdown();
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IQueue<VersionedObject<Integer>> queue2 = instance2.getQueue("testQueueStore");
        assertEquals(maxSize, queue2.size());

        assertEquals(maxSize, queueStore.store.size());
        for (int i = 0; i < maxSize; i++) {
            assertEquals(new VersionedObject<>(i, i), queue2.poll());
        }

        queueStore.assertAwait(3);
    }

    @Test
    public void testStoreId_whenNodeDown() {
        Config config = getConfig();
        IdCheckerQueueStore idCheckerQueueStore = new IdCheckerQueueStore();
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(idCheckerQueueStore);
        config.getQueueConfig("default")
              .setQueueStoreConfig(queueStoreConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        String name = generateKeyOwnedBy(instance1);
        IQueue<VersionedObject<String>> queue = instance2.getQueue(name);
        queue.offer(new VersionedObject<>(randomString()));
        queue.offer(new VersionedObject<>(randomString()));
        queue.offer(new VersionedObject<>(randomString()));

        instance1.shutdown();

        queue.offer(new VersionedObject<>(randomString()));
    }

    @Test
    public void testQueueStoreFactory() {
        String queueName = randomString();
        Config config = getConfig();
        QueueStoreFactory<VersionedObject<Integer>> queueStoreFactory = new SimpleQueueStoreFactory();
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setEnabled(true)
                .setFactoryImplementation(queueStoreFactory);
        config.getQueueConfig(queueName)
              .setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        IQueue<VersionedObject<Integer>> queue = instance.getQueue(queueName);
        queue.add(new VersionedObject<>(1));

        TestQueueStore testQueueStore = (TestQueueStore) queueStoreFactory.newQueueStore(queueName, null);
        int size = testQueueStore.store.size();

        assertEquals("Queue store size should be 1 but found " + size, 1, size);
    }

    @Test
    public void testQueueStoreFactoryIsNotInitialized_whenDisabledInQueueStoreConfig() {
        String queueName = randomString();
        Config config = getConfig();
        QueueStoreFactory<VersionedObject<Integer>> queueStoreFactory = new SimpleQueueStoreFactory();
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setEnabled(false)
                .setFactoryImplementation(queueStoreFactory);
        config.getQueueConfig(queueName)
              .setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        instance.getQueue(queueName).add(new VersionedObject<>(1));

        TestQueueStore testQueueStore = (TestQueueStore) queueStoreFactory.newQueueStore(queueName, null);
        int size = testQueueStore.store.size();

        assertEquals("Expected no queue store operation since we disabled it in QueueStoreConfig, but found initialized ",
                0, size);
    }

    @Test
    public void testQueueStore_withBinaryModeOn() {
        String queueName = randomString();
        Config config = getConfig();
        QueueStoreConfig queueStoreConfig = getBinaryQueueStoreConfig();
        config.getQueueConfig(queueName)
              .setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IQueue<VersionedObject<Integer>> queue = node.getQueue(queueName);
        queue.add(new VersionedObject<>(1));
        queue.add(new VersionedObject<>(2));
        queue.add(new VersionedObject<>(3));

        // this triggers bulk loading
        VersionedObject<Integer> value = queue.peek();

        assertEquals(new VersionedObject<>(1), value);
    }

    private QueueStoreConfig getBinaryQueueStoreConfig() {
        QueueStore<Data> binaryQueueStore = new BasicQueueStore<>();
        return new QueueStoreConfig()
                .setStoreImplementation(binaryQueueStore)
                .setEnabled(true)
                .setProperty("binary", "true")
                .setProperty("memory-limit", "0")
                .setProperty("bulk-load", "100");
    }


    private Config getConfigForDrainToTest(int maxSize, int bulkLoadSize, QueueStore<VersionedObject<Integer>> queueStore) {
        Config config = getConfig();
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig().setStoreImplementation(queueStore);
        if (bulkLoadSize > 0) {
            queueStoreConfig.setProperty("bulk-load", Integer.toString(bulkLoadSize));
        }
        config.getQueueConfig("testQueueStore")
              .setMaxSize(maxSize)
              .setQueueStoreConfig(queueStoreConfig);

        return config;
    }

    private static class MyQueueStore implements QueueStore<VersionedObject<String>>, Serializable {
        private final Map<Long, VersionedObject<String>> map = new HashMap<>();

        MyQueueStore() {
            map.put(1L, new VersionedObject<>("hola"));
            map.put(3L, new VersionedObject<>("dias"));
            map.put(4L, new VersionedObject<>("pescado"));
            map.put(6L, new VersionedObject<>("oso"));
            map.put(2L, new VersionedObject<>("manzana"));
            map.put(10L, new VersionedObject<>("manana"));
            map.put(12L, new VersionedObject<>("perro"));
            map.put(17L, new VersionedObject<>("gato"));
            map.put(19L, new VersionedObject<>("toro"));
            map.put(15L, new VersionedObject<>("tortuga"));
        }

        @Override
        public void store(Long key, VersionedObject<String> value) {
            map.put(key, value);
        }

        @Override
        public void storeAll(Map<Long, VersionedObject<String>> valueMap) {
            map.putAll(valueMap);
        }

        @Override
        public void delete(Long key) {
            map.remove(key);
        }

        @Override
        public void deleteAll(Collection<Long> keys) {
            for (Long key : keys) {
                map.remove(key);
            }
        }

        @Override
        public VersionedObject<String> load(Long key) {
            return map.get(key);
        }

        @Override
        public Map<Long, VersionedObject<String>> loadAll(Collection<Long> keys) {
            Map<Long, VersionedObject<String>> resultMap = new HashMap<>();
            for (Long key : keys) {
                resultMap.put(key, map.get(key));
            }
            return resultMap;
        }

        @Override
        public Set<Long> loadAllKeys() {
            return map.keySet();
        }
    }

    static class SimpleQueueStoreFactory implements QueueStoreFactory<VersionedObject<Integer>> {

        private final ConcurrentMap<String, QueueStore<VersionedObject<Integer>>> stores = new ConcurrentHashMap<>();

        @Override
        public QueueStore<VersionedObject<Integer>> newQueueStore(String name, Properties properties) {
            return ConcurrencyUtil.getOrPutIfAbsent(stores, name, arg -> new TestQueueStore());
        }
    }

    static class IdCheckerQueueStore implements QueueStore<VersionedObject<String>> {
        Long lastKey;

        @Override
        public void store(Long key, VersionedObject<String> value) {
            if (lastKey != null && lastKey >= key) {
                throw new RuntimeException("key[" + key + "] is already stored");
            }
            lastKey = key;
        }

        @Override
        public void storeAll(Map<Long, VersionedObject<String>> map) {
        }

        @Override
        public void delete(Long key) {
        }

        @Override
        public void deleteAll(Collection<Long> keys) {
        }

        @Override
        public VersionedObject<String> load(Long key) {
            return null;
        }

        @Override
        public Map<Long, VersionedObject<String>> loadAll(Collection<Long> keys) {
            return null;
        }

        @Override
        public Set<Long> loadAllKeys() {
            return null;
        }
    }

    public static class TestQueueStore implements QueueStore<VersionedObject<Integer>> {

        final Map<Long, VersionedObject<Integer>> store = new LinkedHashMap<>();
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicInteger destroyCount = new AtomicInteger();

        final CountDownLatch latchStore;
        final CountDownLatch latchStoreAll;
        final CountDownLatch latchDelete;
        final CountDownLatch latchDeleteAll;
        final CountDownLatch latchLoad;
        final CountDownLatch latchLoadAllKeys;
        final CountDownLatch latchLoadAll;

        private boolean loadAllKeys = true;

        public TestQueueStore() {
            this(0, 0, 0, 0, 0, 0);
        }

        TestQueueStore(int expectedStore, int expectedStoreAll, int expectedDelete,
                       int expectedDeleteAll, int expectedLoad, int expectedLoadAll) {
            this(expectedStore, expectedStoreAll, expectedDelete, expectedDeleteAll,
                    expectedLoad, expectedLoadAll, 0);
        }

        TestQueueStore(int expectedStore, int expectedStoreAll, int expectedDelete,
                       int expectedDeleteAll, int expectedLoad, int expectedLoadAll,
                       int expectedLoadAllKeys) {
            latchStore = new CountDownLatch(expectedStore);
            latchStoreAll = new CountDownLatch(expectedStoreAll);
            latchDelete = new CountDownLatch(expectedDelete);
            latchDeleteAll = new CountDownLatch(expectedDeleteAll);
            latchLoad = new CountDownLatch(expectedLoad);
            latchLoadAll = new CountDownLatch(expectedLoadAll);
            latchLoadAllKeys = new CountDownLatch(expectedLoadAllKeys);
        }

        public boolean isLoadAllKeys() {
            return loadAllKeys;
        }

        public void setLoadAllKeys(boolean loadAllKeys) {
            this.loadAllKeys = loadAllKeys;
        }

        public void destroy() {
            destroyCount.incrementAndGet();
        }

        void assertAwait(int seconds) throws Exception {
            assertTrue("Store remaining: " + latchStore.getCount(), latchStore.await(seconds, SECONDS));
            assertTrue("Store-all remaining: " + latchStoreAll.getCount(), latchStoreAll.await(seconds, SECONDS));
            assertTrue("Delete remaining: " + latchDelete.getCount(), latchDelete.await(seconds, SECONDS));
            assertTrue("Delete-all remaining: " + latchDeleteAll.getCount(), latchDeleteAll.await(seconds, SECONDS));
            assertTrue("Load remaining: " + latchLoad.getCount(), latchLoad.await(seconds, SECONDS));
            assertTrue("Load-al remaining: " + latchLoadAll.getCount(), latchLoadAll.await(seconds, SECONDS));
            assertTrue("Load-all keys remaining: " + latchLoadAllKeys.getCount(), latchLoadAllKeys.await(seconds, SECONDS));
        }

        @Override
        public Set<Long> loadAllKeys() {
            callCount.incrementAndGet();
            latchLoadAllKeys.countDown();
            if (!loadAllKeys) {
                return null;
            }
            return store.keySet();
        }

        @Override
        public void store(Long key, VersionedObject<Integer> value) {
            store.put(key, value);
            callCount.incrementAndGet();
            latchStore.countDown();
        }

        @Override
        public void storeAll(Map<Long, VersionedObject<Integer>> map) {
            store.putAll(map);
            callCount.incrementAndGet();
            latchStoreAll.countDown();
        }

        @Override
        public void delete(Long key) {
            store.remove(key);
            callCount.incrementAndGet();
            latchDelete.countDown();
        }

        @Override
        public VersionedObject<Integer> load(Long key) {
            callCount.incrementAndGet();
            latchLoad.countDown();
            return store.get(key);
        }

        @Override
        public Map<Long, VersionedObject<Integer>> loadAll(Collection<Long> keys) {
            Map<Long, VersionedObject<Integer>> map = new HashMap<>(keys.size());
            for (Long key : keys) {
                VersionedObject<Integer> value = store.get(key);
                if (value != null) {
                    map.put(key, value);
                }
            }
            callCount.incrementAndGet();
            latchLoadAll.countDown();
            return map;
        }

        @Override
        public void deleteAll(Collection<Long> keys) {
            for (Long key : keys) {
                store.remove(key);
            }
            callCount.incrementAndGet();
            latchDeleteAll.countDown();
        }
    }

    public static class BasicQueueStore<T> implements QueueStore<T> {

        final Map<Long, T> store = new LinkedHashMap<>();

        /**
         * Stores the key-value pair.
         *
         * @param key   key of the entry to store
         * @param value value of the entry to store
         */
        @Override
        public void store(Long key, T value) {
            store.put(key, value);
        }

        /**
         * Stores multiple entries. Implementation of this method can optimize the
         * store operation by storing all entries in one database connection for instance.
         *
         * @param map map of entries to store
         */
        @Override
        public void storeAll(Map<Long, T> map) {
            for (Map.Entry<Long, T> entry : map.entrySet()) {
                store(entry.getKey(), entry.getValue());
            }
        }

        /**
         * Deletes the entry with a given key from the store.
         *
         * @param key key to delete from the store.
         */
        @Override
        public void delete(Long key) {
            store.remove(key);
        }

        /**
         * Deletes multiple entries from the store.
         *
         * @param keys keys of the entries to delete.
         */
        @Override
        public void deleteAll(Collection<Long> keys) {
            for (Long key : keys) {
                store.remove(key);
            }
        }

        /**
         * Loads the value of a given key. If distributed map doesn't contain the value
         * for the given key then Hazelcast will call implementation's load (key) method
         * to obtain the value. Implementation can use any means of loading the given key;
         * such as an O/R mapping tool, simple SQL or reading a file etc.
         *
         * @param key key to load
         * @return value of the key
         */
        @Override
        public T load(Long key) {
            return store.get(key);
        }

        /**
         * Loads given keys. This is batch load operation so that implementation can
         * optimize the multiple loads.
         *
         * @param keys keys of the values entries to load
         * @return map of loaded key-value pairs.
         */
        @Override
        public Map<Long, T> loadAll(Collection<Long> keys) {
            Map<Long, T> loadedEntries = new HashMap<>();
            for (Long key : keys) {
                T value = load(key);
                loadedEntries.put(key, value);
            }
            return loadedEntries;
        }

        /**
         * Loads all of the keys from the store.
         *
         * @return all the keys
         */
        @Override
        public Set<Long> loadAllKeys() {
            return store.keySet();
        }
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        return config;
    }
}
