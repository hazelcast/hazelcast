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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.query.SqlPredicate;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class MapStoreTest extends TestUtil {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPersistentQueue() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Config config = newConfig("themap", testMapStore, 0);
        config.getQueueConfig("default").setBackingMapName("themap");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IQueue q1 = h1.getQueue("default");
        for (int i = 0; i < 100; i++) {
            q1.put("value" + i);
        }
        assertEquals(100, q1.size());
        assertEquals(100, testMapStore.store.size());
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(config);
        q1 = h1.getQueue("default");
        assertEquals(100, q1.size());
        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, q1.take());
        }
        for (int i = 0; i < 100; i++) {
            q1.put("value" + i);
        }
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IQueue q2 = h2.getQueue("default");
        assertEquals(100, q2.size());
        h1.getLifecycleService().shutdown();
        assertEquals(100, q2.size());
        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, q2.take());
        }
    }

    @Test
    public void testThreeMemberGetAll() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Map store = testMapStore.getStore();
        Set keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.put(i, "value" + i);
            keys.add(i);
        }
        Config config = newConfig(testMapStore, 2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        assertEquals("value1", map1.get(1));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(5));
        assertEquals("value1", map1.get(1));
        assertEquals(null, testMapStore.waitForEvent(3));
        Map loaded = map1.getAll(keys);
        assertEquals(size, loaded.size());
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL, testMapStore.waitForEvent(5));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL, testMapStore.waitForEvent(5));
        loaded = map2.getAll(keys);
        assertEquals(size, loaded.size());
        assertEquals(null, testMapStore.waitForEvent(5));
        for (int i = 0; i < size; i++) {
            map1.evict(i);
        }
        assertEquals(0, map1.size());
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        loaded = map1.getAll(keys);
        assertEquals(size, loaded.size());
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL, testMapStore.waitForEvent(5));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL, testMapStore.waitForEvent(5));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL, testMapStore.waitForEvent(5));
        assertEquals(0, testMapStore.getEventCount());
        loaded = map2.getAll(keys);
        assertEquals(size, loaded.size());
        assertEquals(null, testMapStore.waitForEvent(5));
    }

    @Test
    public void testOneMemberWriteThroughTxnalFailingStore() {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        testMapStore.setFail(false);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        Transaction txn = h1.getTransaction();
        txn.begin();
        assertEquals(0, map.size());
        assertEquals(0, testMapStore.dbSize());
        map.put("1", "value1");
        map.put("2", "value2");
        txn.commit();
        assertEquals(2, map.size());
        assertEquals(2, testMapStore.dbSize());
        txn = h1.getTransaction();
        txn.begin();
        assertEquals(2, map.size());
        assertEquals(2, testMapStore.dbSize());
        map.put("3", "value3");
        assertEquals(3, map.size());
        assertEquals(2, testMapStore.dbSize());
        testMapStore.setFail(true);
        map.put("4", "value4");
        try {
            txn.commit();
            fail("Should not commit the txn");
        } catch (Exception e) {
        }
        assertEquals(2, map.size());
        assertEquals(2, testMapStore.dbSize());
    }

    @Test
    public void testOneMemberWriteThroughFailingStore() throws Exception {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        testMapStore.setFail(true);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        try {
            map.get("1");
            fail("should have thrown exception");
        } catch (Exception e) {
        }
        assertEquals(1, testMapStore.loads.get());
        try {
            map.get("1");
            fail("should have thrown exception");
        } catch (Exception e) {
        }
        assertEquals(2, testMapStore.loads.get());
        try {
            map.put("1", "value");
            fail("should have thrown exception");
        } catch (Exception e) {
        }
        assertEquals(1, testMapStore.stores.get());
    }

    @Test
    public void testTwoMemberWriteThrough2() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1000, 0, 0);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        for (int i = 0; i < 1000; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(1000, testMapStore.getStore().size());
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        testMapStore.assertAwait(10);
        assertEquals(1000, testMapStore.callCount.get());
    }

    @Test
    public void testTwoMemberWriteThrough() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        Employee employee = new Employee("joe", 25, true, 100.00);
        Employee employee2 = new Employee("jay", 35, false, 100.00);
        testMapStore.insert("1", employee);
        IMap map = h1.getMap("default");
        map.addIndex("name", false);
        assertEquals(0, map.size());
        assertEquals(employee, map.get("1"));
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        Collection values = map.values(new SqlPredicate("name = 'joe'"));
        assertEquals(1, values.size());
        assertEquals(employee, values.iterator().next());
        map.put("2", employee2);
        assertEquals(employee2, testMapStore.getStore().get("2"));
        assertEquals(2, testMapStore.getStore().size());
        assertEquals(2, map.size());
        map.remove("2");
        assertEquals(1, testMapStore.getStore().size());
        assertEquals(1, map.size());
        testMapStore.assertAwait(10);
        assertEquals(4, testMapStore.callCount.get());
    }

    @Test
    public void testOneMemberWriteThrough() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        Employee employee = new Employee("joe", 25, true, 100.00);
        testMapStore.insert("1", employee);
        IMap map = h1.getMap("default");
        map.addIndex("name", false);
        assertEquals(0, map.size());
        assertEquals(employee, map.get("1"));
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        Collection values = map.values(new SqlPredicate("name = 'joe'"));
        assertEquals(1, values.size());
        assertEquals(employee, values.iterator().next());
        map.remove("1");
        map.put("1", employee, 1, TimeUnit.SECONDS);
        Thread.sleep(2000);
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(employee, map.get("1"));
    }

    @Test
    public void testOneMemberWriteThroughWithLRU() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        Config config = newConfig(testMapStore, 0);
        config.getMapConfig("default").setMaxSize(10).setEvictionPolicy("LRU");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        for (int i = 0; i < 20; i++) {
            map.put(String.valueOf(i), new Employee("joe", i, true, 100.00));
        }
        assertTrue(map.size() > 5);
        assertTrue(map.size() <= 10);
    }

    @Test
    public void testOneMemberWriteThroughWithIndex() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        Config config = newConfig(testMapStore, 0);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        testMapStore.insert("1", "value1");
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        assertTrue(map.tryLock("1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.get("1"));
        map.unlock("1");
        assertEquals("value1", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        assertEquals("value2", testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        assertTrue(map.evict("1"));
        assertEquals(0, map.size());
        assertEquals(1, testMapStore.getStore().size());
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        map.remove("1");
        assertEquals(0, map.size());
        assertEquals(0, testMapStore.getStore().size());
        testMapStore.assertAwait(1);
        assertEquals(1, testMapStore.getInitCount());
        assertEquals("default", testMapStore.getMapName());
        assertEquals(h1, testMapStore.getHazelcastInstance());
    }

    @Test
    public void testOneMemberFailingWriteBehind() throws Exception {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        Config config = newConfig(testMapStore, 2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        CMap cmap = getCMap(h1, "default");
        assertEquals(0, testMapStore.db.size());
        testMapStore.setFail(true);
        map.put("1", "value1");
        Thread.sleep(3000);
        BlockingQueue listener = new LinkedBlockingQueue();
        testMapStore.addListener(listener);
        cmap.startCleanup(false);
        assertNotNull(listener.poll(20, TimeUnit.SECONDS));
        assertEquals(1, map.size());
        assertEquals(0, testMapStore.db.size());
        testMapStore.setFail(false);
        cmap.startCleanup(false);
        assertNotNull(listener.poll(20, TimeUnit.SECONDS));
        assertEquals(1, testMapStore.db.size());
        assertEquals("value1", testMapStore.db.get("1"));
    }

    @Test
    public void testOneMemberWriteBehind() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        Config config = newConfig(testMapStore, 2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        testMapStore.insert("1", "value1");
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        assertEquals("value1", map.get("1"));
        assertEquals("value1", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        // store should have the old data as we will write-behind
        assertEquals("value1", testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        assertTrue(map.evict("1"));
        assertEquals("value2", testMapStore.getStore().get("1"));
        assertEquals(0, map.size());
        assertEquals(1, testMapStore.getStore().size());
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        map.remove("1");
        // store should have the old data as we will delete-behind
        assertEquals(1, testMapStore.getStore().size());
        assertEquals(0, map.size());
        testMapStore.assertAwait(12);
        assertEquals(0, testMapStore.getStore().size());
    }

    @Test
    public void testOneMemberWriteBehindWithEvictions() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Config config = newConfig(testMapStore, 2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        for (int i = 0; i < 100; i++) {
            map.put(i, "value" + i);
        }
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE_ALL, testMapStore.waitForEvent(20));
        assertEquals(100, testMapStore.getStore().size());
        for (int i = 0; i < 100; i++) {
            map.evict(i);
        }
        // we should not receive any store event.
        assertEquals(null, testMapStore.waitForEvent(10));
        assertEquals(100, testMapStore.getStore().size());
        for (int i = 0; i < 100; i++) {
            map.put(i, "value" + i);
        }
        for (int i = 0; i < 100; i++) {
            map.evict(i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE, testMapStore.waitForEvent(10));
        }
        assertEquals(null, testMapStore.waitForEvent(10));
        assertEquals(100, testMapStore.getStore().size());
        assertEquals(0, map.size());
        for (int i = 0; i < 100; i++) {
            map.put(i, "value" + i);
        }
        for (int i = 0; i < 100; i++) {
            map.remove(i);
        }
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.DELETE_ALL, testMapStore.waitForEvent(20));
        assertEquals(0, testMapStore.getStore().size());
        assertEquals(0, map.size());
        assertEquals(null, testMapStore.waitForEvent(10));
    }

    @Test
    public void testOneMemberWriteBehindWithMaxIdle() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Config config = newConfig(testMapStore, 1);
        config.getMapConfig("default").setMaxIdleSeconds(4);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        for (int i = 0; i < 10; i++) {
            map.put(i, "value" + i);
        }
        CMap cmap = getCMap(h1, "default");
        cmap.startCleanup(true);
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE_ALL, testMapStore.waitForEvent(10));
        Thread.sleep(5000);
        cmap.startCleanup(true);
        assertEquals(null, testMapStore.waitForEvent(1));
        assertEquals(10, testMapStore.getStore().size());
        assertEquals(0, map.size());
        cmap.startCleanup(true);
        assertEquals(null, testMapStore.waitForEvent(10));
    }

    protected Config newConfig(Object storeImpl, int writeDelaySeconds) {
        return newConfig("default", storeImpl, writeDelaySeconds);
    }

    protected Config newConfig(String mapName, Object storeImpl, int writeDelaySeconds) {
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = config.getMapConfig(mapName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(storeImpl);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }

    public static class TestMapStore implements MapLoaderLifecycleSupport, MapStore {

        final Map store = new ConcurrentHashMap();

        final CountDownLatch latchStore;
        final CountDownLatch latchStoreAll;
        final CountDownLatch latchDelete;
        final CountDownLatch latchDeleteAll;
        final CountDownLatch latchLoad;
        final CountDownLatch latchLoadAllKeys;
        final CountDownLatch latchLoadAll;
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicInteger initCount = new AtomicInteger();
        private HazelcastInstance hazelcastInstance;
        private Properties properties;
        private String mapName;

        public TestMapStore(int expectedStore, int expectedDelete, int expectedLoad) {
            this(expectedStore, 0, expectedDelete, 0, expectedLoad, 0);
        }

        public TestMapStore(int expectedStore, int expectedStoreAll, int expectedDelete,
                            int expectedDeleteAll, int expectedLoad, int expectedLoadAll) {
            this(expectedStore, expectedStoreAll, expectedDelete, expectedDeleteAll,
                    expectedLoad, expectedLoadAll, 0);
        }

        public TestMapStore(int expectedStore, int expectedStoreAll, int expectedDelete,
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

        public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
            this.hazelcastInstance = hazelcastInstance;
            this.properties = properties;
            this.mapName = mapName;
            initCount.incrementAndGet();
        }

        public void destroy() {
        }

        public int getInitCount() {
            return initCount.get();
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }

        public String getMapName() {
            return mapName;
        }

        public Properties getProperties() {
            return properties;
        }

        public void assertAwait(int seconds) throws Exception {
            assertTrue(latchStore.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchStoreAll.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchDelete.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchDeleteAll.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchLoad.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchLoadAll.await(seconds, TimeUnit.SECONDS));
        }

        Map getStore() {
            return store;
        }

        public void insert(Object key, Object value) {
            store.put(key, value);
        }

        public void store(Object key, Object value) {
            store.put(key, value);
            callCount.incrementAndGet();
            latchStore.countDown();
        }

        public Set loadAllKeys() {
            callCount.incrementAndGet();
            latchLoadAllKeys.countDown();
            return store.keySet();
        }

        public Object load(Object key) {
            callCount.incrementAndGet();
            latchLoad.countDown();
            return store.get(key);
        }

        public void storeAll(Map map) {
            store.putAll(map);
            callCount.incrementAndGet();
            latchStoreAll.countDown();
        }

        public void delete(Object key) {
            store.remove(key);
            callCount.incrementAndGet();
            latchDelete.countDown();
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

    public static class TestEventBasedMapStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {

        protected enum STORE_EVENTS {
            STORE, STORE_ALL, DELETE, DELETE_ALL, LOAD, LOAD_ALL, LOAD_ALL_KEYS
        }

        protected final Map<K, V> store = new ConcurrentHashMap();
        protected final BlockingQueue events = new LinkedBlockingQueue();
        protected final AtomicInteger callCount = new AtomicInteger();
        protected final AtomicInteger initCount = new AtomicInteger();
        protected HazelcastInstance hazelcastInstance;
        protected Properties properties;
        protected String mapName;

        public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
            this.hazelcastInstance = hazelcastInstance;
            this.properties = properties;
            this.mapName = mapName;
            initCount.incrementAndGet();
        }

        public void destroy() {
        }

        public int getEventCount() {
            return events.size();
        }

        public int getInitCount() {
            return initCount.get();
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }

        public String getMapName() {
            return mapName;
        }

        public Properties getProperties() {
            return properties;
        }

        Object waitForEvent(int seconds) throws InterruptedException {
            return events.poll(seconds, TimeUnit.SECONDS);
        }

        Map getStore() {
            return store;
        }

        public void insert(K key, V value) {
            store.put(key, value);
        }

        public void store(K key, V value) {
            store.put(key, value);
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.STORE);
        }

        public V load(K key) {
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.LOAD);
            return store.get(key);
        }

        public void storeAll(Map map) {
            store.putAll(map);
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.STORE_ALL);
        }

        public void delete(K key) {
            store.remove(key);
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.DELETE);
        }

        public Set<K> loadAllKeys() {
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.LOAD_ALL_KEYS);
            return store.keySet();
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
            events.offer(STORE_EVENTS.LOAD_ALL);
            return map;
        }

        public void deleteAll(Collection keys) {
            for (Object key : keys) {
                store.remove(key);
            }
            callCount.incrementAndGet();
            events.offer(STORE_EVENTS.DELETE_ALL);
        }
    }

    public static class FailAwareMapStore implements MapStore {
        final Map db = new ConcurrentHashMap();

        final AtomicLong deletes = new AtomicLong();
        final AtomicLong deleteAlls = new AtomicLong();
        final AtomicLong stores = new AtomicLong();
        final AtomicLong storeAlls = new AtomicLong();
        final AtomicLong loads = new AtomicLong();
        final AtomicLong loadAlls = new AtomicLong();
        final AtomicLong loadAllKeys = new AtomicLong();
        final AtomicBoolean shouldFail = new AtomicBoolean(false);
        final List<BlockingQueue> listeners = new CopyOnWriteArrayList<BlockingQueue>();

        public void addListener(BlockingQueue obj) {
            listeners.add(obj);
        }

        public void notifyListeners() {
            for (BlockingQueue listener : listeners) {
                listener.offer(new Object());
            }
        }

        public void delete(Object key) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    db.remove(key);
                }
            } finally {
                deletes.incrementAndGet();
                notifyListeners();
            }
        }

        public void setFail(boolean shouldFail) {
            this.shouldFail.set(shouldFail);
        }

        public int dbSize() {
            return db.size();
        }

        public boolean dbContainsKey(Object key) {
            return db.containsKey(key);
        }

        public Object dbGet(Object key) {
            return db.get(key);
        }

        public void store(Object key, Object value) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    db.put(key, value);
                }
            } finally {
                stores.incrementAndGet();
                notifyListeners();
            }
        }

        public Set loadAllKeys() {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    return db.keySet();
                }
            } finally {
                loadAllKeys.incrementAndGet();
            }
        }

        public Object load(Object key) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    return db.get(key);
                }
            } finally {
                loads.incrementAndGet();
            }
        }

        public void storeAll(Map map) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    db.putAll(map);
                }
            } finally {
                storeAlls.incrementAndGet();
                notifyListeners();
            }
        }

        public Map loadAll(Collection keys) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    Map results = new HashMap();
                    for (Object key : keys) {
                        Object value = db.get(key);
                        if (value != null) {
                            results.put(key, value);
                        }
                    }
                    return results;
                }
            } finally {
                loadAlls.incrementAndGet();
                notifyListeners();
            }
        }

        public void deleteAll(Collection keys) {
            try {
                if (shouldFail.get()) {
                    throw new RuntimeException();
                } else {
                    for (Object key : keys) {
                        db.remove(key);
                    }
                }
            } finally {
                deleteAlls.incrementAndGet();
                notifyListeners();
            }
        }
    }
}
