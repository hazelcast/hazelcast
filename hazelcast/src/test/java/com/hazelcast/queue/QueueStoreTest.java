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
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

/**
 * User: sancar
 * Date: 2/22/13
 * Time: 2:20 PM
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
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

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        for (int i = 0; i < maxSize * 2; i++) {
            queueStore.store.put((long) i, i);
        }

        IQueue<Object> queue = instance.getQueue("testQueueStore");
        Assert.assertEquals("Queue Size should be equal to max size", maxSize, queue.size());
    }

    @Test
    public void testQueueStore() {
        Config config = new Config();
        int maxSize = 2000;
        QueueConfig queueConfig = config.getQueueConfig("testQueueStore");
        queueConfig.setMaxSize(maxSize);
        TestQueueStore queueStore = new TestQueueStore(1000,0,2000,0,0,0,1);

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

        instance.getLifecycleService().shutdown();
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IQueue<Object> queue2 = instance2.getQueue("testQueueStore");
        Assert.assertEquals(maxSize,queue2.size());


        Assert.assertEquals(maxSize,queueStore.store.size());
        for (int i = 0; i < maxSize; i++) {
            Assert.assertEquals(i,queue2.poll());
        }

        try {
            queueStore.assertAwait(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
