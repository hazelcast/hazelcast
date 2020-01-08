/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.QueueStoreFactory;
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
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

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QueueStoreTest extends HazelcastTestSupport {

    @Test
    public void testQueueStoreLoadMoreThanMaxSize() {
        Config config = new Config();
        int maxSize = 2000;
        QueueConfig queueConfig = config.getQueueConfig("testQueueStore");
        queueConfig.setMaxSize(maxSize);
        TestQueueStore queueStore = new TestQueueStore();
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setStoreImplementation(queueStore);
        queueConfig.setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        for (int i = 0; i < maxSize * 2; i++) {
            queueStore.store.put((long) i, i);
        }

        IQueue<Object> queue = instance.getQueue("testQueueStore");
        assertEquals("Queue Size should be equal to max size", maxSize, queue.size());
    }

    @Test
    public void testQueueStoreDrainTo() {
        int maxSize = 10000;
        TestQueueStore queueStore = new TestQueueStore(0, 0, 0, 0, 2 * maxSize, 0);
        Config config = getConfigForDrainToTest(maxSize, 1, queueStore);

        HazelcastInstance instance = createHazelcastInstance(config);
        // initialize queue store with 2 * maxSize
        for (int i = 0; i < maxSize * 2; i++) {
            queueStore.store.put((long) i, i);
        }

        IQueue<Object> queue = instance.getQueue("testQueueStore");
        List<Object> items = new ArrayList<Object>();
        int count = queue.drainTo(items);
        assertEquals(2 * maxSize, count);
        assertOpenEventually(queueStore.latchLoad);
    }

    @Test
    public void testQueueStoreDrainTo_whenBulkLoadEnabled() {
        int maxSize = 10000;
        int queueStoreSize = 2 * maxSize;
        int bulkLoadSize = 10;
        TestQueueStore queueStore = new TestQueueStore(0, 0, 0,
                0, 0, queueStoreSize / bulkLoadSize);
        Config config = getConfigForDrainToTest(maxSize, bulkLoadSize, queueStore);

        HazelcastInstance instance = createHazelcastInstance(config);
        // setup queue store with 2 * maxSize
        for (int i = 0; i < queueStoreSize; i++) {
            queueStore.store.put((long) i, i);
        }

        IQueue<Object> queue = instance.getQueue("testQueueStore");
        List<Object> items = new ArrayList<Object>();
        int count = queue.drainTo(items);
        assertEquals(queueStoreSize, count);
        assertOpenEventually(queueStore.latchLoadAll);
    }

    @Test
    public void testRemoveAll() {
        int maxSize = 2000;
        final Config config = new Config();
        final QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setStoreImplementation(new TestQueueStore())
                .setProperty("bulk-load", String.valueOf(200));
        config.getQueueConfig("testQueueStore")
                .setMaxSize(maxSize)
                .setQueueStoreConfig(queueStoreConfig);
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IQueue<Object> queue = instance.getQueue("testQueueStore");

        for (int i = 0; i < maxSize; i++) {
            queue.add(i);
        }
        assertEquals(maxSize, queue.size());

        for (Object o : queue) {
            queue.remove(o);
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void testIssue1401QueueStoreWithTxnPoll() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setStoreImplementation(new MyQueueStore());
        queueStoreConfig.setEnabled(true);
        queueStoreConfig.setProperty("binary", "false");
        queueStoreConfig.setProperty("memory-limit", "0");
        queueStoreConfig.setProperty("bulk-load", "100");

        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig("test");
        queueConfig.setMaxSize(10);
        queueConfig.setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        for (int i = 0; i < 10; i++) {
            TransactionContext context = instance.newTransactionContext();
            context.beginTransaction();

            TransactionalQueue<String> queue = context.getQueue("test");
            String queueData = queue.poll();
            assertNotNull(queueData);
            context.commitTransaction();
        }
    }

    @Test
    public void testQueueStore() throws Exception {
        Config config = new Config();
        int maxSize = 2000;
        QueueConfig queueConfig = config.getQueueConfig("testQueueStore");
        queueConfig.setMaxSize(maxSize);
        TestQueueStore queueStore = new TestQueueStore(1000, 0, 2000, 0, 0, 0, 1);

        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setStoreImplementation(queueStore);
        queueConfig.setQueueStoreConfig(queueStoreConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        for (int i = 0; i < maxSize / 2; i++) {
            queueStore.store.put((long) i, i);
        }

        IQueue<Object> queue = instance.getQueue("testQueueStore");

        for (int i = 0; i < maxSize / 2; i++) {
            queue.offer(i + maxSize / 2);
        }

        instance.shutdown();
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IQueue<Object> queue2 = instance2.getQueue("testQueueStore");
        assertEquals(maxSize, queue2.size());

        assertEquals(maxSize, queueStore.store.size());
        for (int i = 0; i < maxSize; i++) {
            assertEquals(i, queue2.poll());
        }

        queueStore.assertAwait(3);
    }

    @Test
    public void testStoreId_whenNodeDown() {
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig("default");
        IdCheckerQueueStore idCheckerQueueStore = new IdCheckerQueueStore();
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setEnabled(true).setStoreImplementation(idCheckerQueueStore);
        queueConfig.setQueueStoreConfig(queueStoreConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        String name = generateKeyOwnedBy(instance1);
        IQueue<Object> queue = instance2.getQueue(name);
        queue.offer(randomString());
        queue.offer(randomString());
        queue.offer(randomString());

        instance1.shutdown();

        queue.offer(randomString());
    }

    @Test
    public void testQueueStoreFactory() {
        String queueName = randomString();
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig(queueName);
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setEnabled(true);
        QueueStoreFactory queueStoreFactory = new SimpleQueueStoreFactory();
        queueStoreConfig.setFactoryImplementation(queueStoreFactory);
        queueConfig.setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        IQueue<Integer> queue = instance.getQueue(queueName);
        queue.add(1);

        QueueStore queueStore = queueStoreFactory.newQueueStore(queueName, null);
        TestQueueStore testQueueStore = (TestQueueStore) queueStore;
        int size = testQueueStore.store.size();

        assertEquals("Queue store size should be 1 but found " + size, 1, size);
    }

    @Test
    public void testQueueStoreFactoryIsNotInitialized_whenDisabledInQueueStoreConfig() {
        String queueName = randomString();
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig(queueName);
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setEnabled(false);
        QueueStoreFactory queueStoreFactory = new SimpleQueueStoreFactory();
        queueStoreConfig.setFactoryImplementation(queueStoreFactory);
        queueConfig.setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        IQueue<Integer> queue = instance.getQueue(queueName);
        queue.add(1);

        QueueStore queueStore = queueStoreFactory.newQueueStore(queueName, null);
        TestQueueStore testQueueStore = (TestQueueStore) queueStore;
        int size = testQueueStore.store.size();

        assertEquals("Expected not queue store operation since we disabled it in QueueStoreConfig, but found initialized ",
                0, size);
    }

    @Test
    public void testQueueStore_withBinaryModeOn() {
        String queueName = randomString();
        QueueStoreConfig queueStoreConfig = getBinaryQueueStoreConfig();
        QueueConfig queueConfig = new QueueConfig();
        queueConfig.setName(queueName);
        queueConfig.setQueueStoreConfig(queueStoreConfig);
        Config config = new Config();
        config.addQueueConfig(queueConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IQueue<Integer> queue = node.getQueue(queueName);
        queue.add(1);
        queue.add(2);
        queue.add(3);

        // this triggers bulk loading
        int value = queue.peek();

        assertEquals(1, value);
    }

    private QueueStoreConfig getBinaryQueueStoreConfig() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        QueueStore<Data> binaryQueueStore = new BasicQueueStore<Data>();
        queueStoreConfig.setStoreImplementation(binaryQueueStore);
        queueStoreConfig.setEnabled(true);
        queueStoreConfig.setProperty("binary", "true");
        queueStoreConfig.setProperty("memory-limit", "0");
        queueStoreConfig.setProperty("bulk-load", "100");
        return queueStoreConfig;
    }


    private Config getConfigForDrainToTest(int maxSize, int bulkLoadSize, QueueStore queueStore) {
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig("testQueueStore");
        queueConfig.setMaxSize(maxSize);

        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setStoreImplementation(queueStore);
        queueConfig.setQueueStoreConfig(queueStoreConfig);
        if (bulkLoadSize > 0) {
            queueConfig.getQueueStoreConfig().setProperty("bulk-load", Integer.toString(bulkLoadSize));
        }
        return config;
    }

    private static class MyQueueStore implements QueueStore<Object>, Serializable {
        static final Map<Long, Object> map = new HashMap<Long, Object>();

        static {
            map.put(1L, "hola");
            map.put(3L, "dias");
            map.put(4L, "pescado");
            map.put(6L, "oso");
            map.put(2L, "manzana");
            map.put(10L, "manana");
            map.put(12L, "perro");
            map.put(17L, "gato");
            map.put(19L, "toro");
            map.put(15L, "tortuga");
        }

        @Override
        public void store(Long key, Object value) {
            map.put(key, value);
        }

        @Override
        public void storeAll(Map<Long, Object> valueMap) {
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
        public Object load(Long key) {
            return map.get(key);
        }

        @Override
        public Map<Long, Object> loadAll(Collection<Long> keys) {
            Map<Long, Object> resultMap = new HashMap<Long, Object>();
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

    static class SimpleQueueStoreFactory implements QueueStoreFactory<Integer> {

        private final ConcurrentMap<String, QueueStore> stores = new ConcurrentHashMap<String, QueueStore>();

        @Override
        @SuppressWarnings("unchecked")
        public QueueStore<Integer> newQueueStore(String name, Properties properties) {
            return ConcurrencyUtil.getOrPutIfAbsent(stores, name, new ConstructorFunction<String, QueueStore>() {
                @Override
                public QueueStore createNew(String arg) {
                    return new TestQueueStore();
                }
            });
        }
    }

    static class IdCheckerQueueStore implements QueueStore {

        Long lastKey;

        @Override
        public void store(final Long key, final Object value) {
            if (lastKey != null && lastKey >= key) {
                throw new RuntimeException("key[" + key + "] is already stored");
            }
            lastKey = key;
        }

        @Override
        public void storeAll(final Map map) {
        }

        @Override
        public void delete(final Long key) {
        }

        @Override
        public void deleteAll(final Collection keys) {
        }

        @Override
        public Object load(final Long key) {
            return null;
        }

        @Override
        public Map loadAll(final Collection keys) {
            return null;
        }

        @Override
        public Set<Long> loadAllKeys() {
            return null;
        }
    }

    public static class TestQueueStore implements QueueStore<Integer> {

        final Map<Long, Integer> store = new LinkedHashMap<Long, Integer>();
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

        Map getStore() {
            return store;
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
        public void store(Long key, Integer value) {
            store.put(key, value);
            callCount.incrementAndGet();
            latchStore.countDown();
        }

        @Override
        public void storeAll(Map<Long, Integer> map) {
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
        public Integer load(Long key) {
            callCount.incrementAndGet();
            latchLoad.countDown();
            return store.get(key);
        }

        @Override
        public Map<Long, Integer> loadAll(Collection<Long> keys) {
            Map<Long, Integer> map = new HashMap<Long, Integer>(keys.size());
            for (Long key : keys) {
                Integer value = store.get(key);
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

        final Map<Long, T> store = new LinkedHashMap<Long, T>();

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
            final Map<Long, T> loadedEntries = new HashMap<Long, T>();
            for (Long key : keys) {
                final T value = load(key);
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
}
