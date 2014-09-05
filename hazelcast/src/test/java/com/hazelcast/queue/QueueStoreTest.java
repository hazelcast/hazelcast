/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.QueueStore;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testIssue1401QueueStoreWithTxnPoll() {
        final MyQueueStore store = new MyQueueStore();
        final Config config = new Config();
        final QueueConfig qConfig = config.getQueueConfig("test");
        qConfig.setMaxSize(10);
        final QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setStoreImplementation(store);
        queueStoreConfig.setEnabled(true);
        queueStoreConfig.setProperty("binary", "false");
        queueStoreConfig.setProperty("memory-limit", "0");
        queueStoreConfig.setProperty("bulk-load", "100");
        qConfig.setQueueStoreConfig(queueStoreConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        for (int i = 0; i < 10; i++) {
            TransactionContext context = instance.newTransactionContext();
            context.beginTransaction();

            TransactionalQueue<String> queue = context.getQueue("test");
            String queue_data = queue.poll();
            assertNotNull(queue_data);
            context.commitTransaction();
        }
    }

    static class MyQueueStore implements QueueStore, Serializable {

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

        public void store(Long key, Object value) {
            map.put(key, value);
        }

        public void storeAll(Map map) {
            map.putAll(map);
        }

        public void delete(Long key) {
            map.remove(key);
        }

        public void deleteAll(Collection keys) {
            for (Object key : keys) {
                map.remove(key);
            }
        }

        public Object load(Long key) {
            return map.get(key);
        }

        public Map loadAll(Collection keys) {
            Map m = new HashMap();
            for (Object key : keys) {
                m.put(key, map.get(key));
            }
            return m;
        }

        public Set<Long> loadAllKeys() {
            return map.keySet();
        }
    }

    @Test
    public void testQueueStore() throws InterruptedException {
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
        final Config config = new Config();
        final QueueConfig queueConfig = config.getQueueConfig("default");
        final IdCheckerQueueStore idCheckerQueueStore = new IdCheckerQueueStore();
        final QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        queueStoreConfig.setEnabled(true).setStoreImplementation(idCheckerQueueStore);
        queueConfig.setQueueStoreConfig(queueStoreConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        final String name = generateKeyOwnedBy(instance1);
        final IQueue<Object> queue = instance2.getQueue(name);
        queue.offer(randomString());
        queue.offer(randomString());
        queue.offer(randomString());

        instance1.shutdown();

        queue.offer(randomString());

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


    public static class TestQueueStore implements QueueStore {

        final Map<Long, Integer> store = new LinkedHashMap<Long, Integer>();
        final CountDownLatch latchStore;
        final CountDownLatch latchStoreAll;
        final CountDownLatch latchDelete;
        final CountDownLatch latchDeleteAll;
        final CountDownLatch latchLoad;
        final CountDownLatch latchLoadAllKeys;
        final CountDownLatch latchLoadAll;
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicInteger initCount = new AtomicInteger();
        final AtomicInteger destroyCount = new AtomicInteger();

        private boolean loadAllKeys = true;

        public TestQueueStore() {
            this(0, 0, 0, 0, 0, 0);
        }

        public TestQueueStore(int expectedStore, int expectedDelete, int expectedLoad) {
            this(expectedStore, 0, expectedDelete, 0, expectedLoad, 0);
        }

        public TestQueueStore(int expectedStore, int expectedStoreAll, int expectedDelete,
                              int expectedDeleteAll, int expectedLoad, int expectedLoadAll) {
            this(expectedStore, expectedStoreAll, expectedDelete, expectedDeleteAll,
                    expectedLoad, expectedLoadAll, 0);
        }

        public TestQueueStore(int expectedStore, int expectedStoreAll, int expectedDelete,
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

        public int getInitCount() {
            return initCount.get();
        }

        public int getDestroyCount() {
            return destroyCount.get();
        }

        public void assertAwait(int seconds) throws InterruptedException {
            assertTrue("Store remaining: " + latchStore.getCount(), latchStore.await(seconds, TimeUnit.SECONDS));
            assertTrue("Store-all remaining: " + latchStoreAll.getCount(), latchStoreAll.await(seconds, TimeUnit.SECONDS));
            assertTrue("Delete remaining: " + latchDelete.getCount(), latchDelete.await(seconds, TimeUnit.SECONDS));
            assertTrue("Delete-all remaining: " + latchDeleteAll.getCount(), latchDeleteAll.await(seconds, TimeUnit.SECONDS));
            assertTrue("Load remaining: " + latchLoad.getCount(), latchLoad.await(seconds, TimeUnit.SECONDS));
            assertTrue("Load-al remaining: " + latchLoadAll.getCount(), latchLoadAll.await(seconds, TimeUnit.SECONDS));
            assertTrue("Load-all keys remaining: " + latchLoadAllKeys.getCount(), latchLoadAllKeys.await(seconds, TimeUnit.SECONDS));
        }

        Map getStore() {
            return store;
        }

        public Set loadAllKeys() {
            callCount.incrementAndGet();
            latchLoadAllKeys.countDown();
            if (!loadAllKeys) return null;
            return store.keySet();
        }

        public void store(Long key, Object value) {
            store.put(key, (Integer) value);
            callCount.incrementAndGet();
            latchStore.countDown();
        }

        public void storeAll(Map map) {
            store.putAll(map);
            callCount.incrementAndGet();
            latchStoreAll.countDown();
        }

        public void delete(Long key) {
            store.remove(key);
            callCount.incrementAndGet();
            latchDelete.countDown();
        }

        public Object load(Long key) {
            callCount.incrementAndGet();
            latchLoad.countDown();
            return store.get(key);
        }

        public Map loadAll(Collection keys) {
            Map map = new HashMap(keys.size());
            for (Object key : keys) {
                Object value = store.get(key);
                if (value != null) {
                    map.put(key, value);
                }
            }
            callCount.incrementAndGet();
            latchLoadAll.countDown();
            return map;
        }

        public void deleteAll(Collection keys) {
            for (Object key : keys) {
                store.remove(key);
            }
            callCount.incrementAndGet();
            latchDeleteAll.countDown();
        }
    }
}
