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

package com.hazelcast.map.mapstore;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.query.SampleObjects.Employee;
import static org.junit.Assert.*;


/**
 * @author enesakar 1/21/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class MapStoreTest extends HazelcastTestSupport {

    @Test
    public void testMapInitialLoad() throws InterruptedException {
        int size = 100000;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        Config cfg = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapLoader(size));
        cfg.getMapConfig("default").setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap map = instance1.getMap("testMapInitialLoad");
        Thread.sleep(10000);

        assertEquals(size, map.size());

        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }

        assertNull(map.put(size, size));
        assertEquals(size, map.remove(size));
        assertNull(map.get(size));

        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }
    }

    private class SimpleMapLoader implements MapLoader {

        final int size;

        SimpleMapLoader(int size) {
            this.size = size;
        }

        @Override
        public Object load(Object key) {
            return null;
        }

        @Override
        public Map loadAll(Collection keys) {
            Map result = new HashMap();
            for (Object key : keys) {
                result.put(key, key);
            }
            return result;
        }

        @Override
        public Set loadAllKeys() {
            Set keys = new HashSet();
            for (int i = 0; i < size; i++) {
                keys.add(i);
            }
            return keys;
        }
    }

    @Test
    public void issue614() {
        final ConcurrentMap<Long, String> STORE = new ConcurrentHashMap<Long, String>();
        STORE.put(1l, "Event1");
        STORE.put(2l, "Event2");
        STORE.put(3l, "Event3");
        STORE.put(4l, "Event4");
        STORE.put(5l, "Event5");
        STORE.put(6l, "Event6");
        Config config = new Config();
        config.getMapConfig("map")
                .setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1)
                        .setImplementation(new SimpleMapStore<Long, String>(STORE)));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h = nodeFactory.newHazelcastInstance(config);
        IMap map = h.getMap("map");
        Collection collection = map.values();
        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(0, localMapStats.getDirtyEntryCount());
    }

    @Test
    public void testIssue583MapReplaceShouldTriggerMapStore() {
        final ConcurrentMap<String, Long> store = new ConcurrentHashMap<String, Long>();
        final MapStore<String, Long> myMapStore = new SimpleMapStore<String, Long>(store);
        Config config = new Config();
        config
                .getMapConfig("myMap")
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(myMapStore));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance hc = nodeFactory.newHazelcastInstance(config);
        IMap<String, Long> myMap = hc.getMap("myMap");
        myMap.put("one", 1L);
        assertEquals(1L, myMap.get("one").longValue());
        assertEquals(1L, store.get("one").longValue());
        myMap.putIfAbsent("two", 2L);
        assertEquals(2L, myMap.get("two").longValue());
        assertEquals(2L, store.get("two").longValue());
        myMap.putIfAbsent("one", 5L);
        assertEquals(1L, myMap.get("one").longValue());
        assertEquals(1L, store.get("one").longValue());
        myMap.replace("one", 1L, 111L);
        assertEquals(111L, myMap.get("one").longValue());
        assertEquals(111L, store.get("one").longValue());
        myMap.replace("one", 1L);
        assertEquals(1L, myMap.get("one").longValue());
        assertEquals(1L, store.get("one").longValue());
    }

    @Test
    public void issue587CallMapLoaderDuringRemoval() {
        final AtomicInteger loadCount = new AtomicInteger(0);
        final AtomicInteger storeCount = new AtomicInteger(0);
        final AtomicInteger deleteCount = new AtomicInteger(0);
        class SimpleMapStore2<K, V> extends SimpleMapStore<K, V> {

            SimpleMapStore2(ConcurrentMap<K, V> store) {
                super(store);
            }

            public V load(K key) {
                loadCount.incrementAndGet();
                return super.load(key);
            }

            public void store(K key, V value) {
                storeCount.incrementAndGet();
                super.store(key, value);
            }

            public void delete(K key) {
                deleteCount.incrementAndGet();
                super.delete(key);
            }
        }
        final ConcurrentMap<String, Long> store = new ConcurrentHashMap<String, Long>();
        final MapStore<String, Long> myMapStore = new SimpleMapStore2<String, Long>(store);
        Config config = new Config();
        config
                .getMapConfig("myMap")
                .setMapStoreConfig(new MapStoreConfig()
                        //.setWriteDelaySeconds(1)
                        .setImplementation(myMapStore));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance hc = nodeFactory.newHazelcastInstance(config);
        store.put("one", 1l);
        store.put("two", 2l);
        assertEquals(0, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        IMap<String, Long> myMap = hc.getMap("myMap");
        assertEquals(1l, myMap.get("one").longValue());
        assertEquals(2l, myMap.get("two").longValue());
        assertEquals(2, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        assertNull(myMap.remove("ten"));
        assertEquals(3, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        myMap.put("three", 3L);
        myMap.put("four", 4L);
        assertEquals(5, loadCount.get());
        assertEquals(2, storeCount.get());
        assertEquals(0, deleteCount.get());
        myMap.remove("one");
        assertEquals(2, storeCount.get());
        assertEquals(1, deleteCount.get());
        assertEquals(5, loadCount.get());
    }

    @Test
    public void testOneMemberWriteBehindWithMaxIdle() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Config config = newConfig(testMapStore, 5);
        config.getMapConfig("default").setMaxIdleSeconds(10);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map = h1.getMap("default");

        final int total = 10;
        final CountDownLatch latch = new CountDownLatch(total);
        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, true);

        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL_KEYS, testMapStore.waitForEvent(10));

        for (int i = 0; i < total; i++) {
            map.put(i, "value" + i);
        }
        latch.await(30, TimeUnit.SECONDS);
        assertEquals(0, map.size());
        assertEquals(total, testMapStore.getStore().size());
    }

    @Test
    @Category(SerialTest.class)
    public void testOneMemberWriteBehindWithEvictions() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Config config = newConfig(testMapStore, 2);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map = h1.getMap("testOneMemberWriteBehindWithEvictions");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL_KEYS, testMapStore.waitForEvent(20));
        for (int i = 0; i < 100; i++) {
            map.put(i, "value" + i);
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(10));
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
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(10));
        }
        for (int i = 0; i < 100; i++) {
            map.evict(i);
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE, testMapStore.waitForEvent(3));
        }
        assertEquals(null, testMapStore.waitForEvent(2));
        assertEquals(100, testMapStore.getStore().size());
        assertEquals(0, map.size());
        for (int i = 0; i < 100; i++) {
            map.put(i, "value" + i);
            assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(10));
        }
        for (int i = 0; i < 100; i++) {
            map.remove(i);
        }
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE_ALL, testMapStore.waitForEvent(10));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.DELETE_ALL, testMapStore.waitForEvent(10));
        assertEquals(0, testMapStore.getStore().size());
        assertEquals(0, map.size());
        assertEquals(null, testMapStore.waitForEvent(10));
    }

    @Test
    public void testOneMemberWriteBehind() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 2);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        testMapStore.insert("1", "value1");
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        assertEquals("value1", map.get("1"));
        assertEquals("value1", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        // store should have the old data as we will write-behind
        assertEquals("value1", testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        map.flush();
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
    public void testOneMemberWriteBehindFlush() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 2);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        assertEquals(null, map.put("1", "value1"));
        assertEquals("value1", map.get("1"));
        assertEquals(null, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        map.flush();
        assertEquals("value1", testMapStore.getStore().get("1"));
    }

    @Test
    public void testOneMemberWriteBehind2() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 1);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD_ALL_KEYS, testMapStore.waitForEvent(2));
        assertEquals(0, map.size());
        map.put("1", "value1");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.LOAD, testMapStore.waitForEvent(2));
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.STORE, testMapStore.waitForEvent(2));
        assertEquals(1, map.size());
        assertEquals(1, testMapStore.getStore().size());
        map.remove("1");
        assertEquals(TestEventBasedMapStore.STORE_EVENTS.DELETE, testMapStore.waitForEvent(5));
        assertEquals(0, map.size());
        assertEquals(0, testMapStore.getStore().size());
    }

    @Test
    public void testOneMemberFlush() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        int size = 100;
        Config config = newConfig(testMapStore, 200);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        assertEquals(size, map.size());
        assertEquals(0, testMapStore.getStore().size());
        assertEquals(size, map.getLocalMapStats().getDirtyEntryCount());
        map.flush();
        assertEquals(size, testMapStore.getStore().size());
        assertEquals(0, map.getLocalMapStats().getDirtyEntryCount());
        assertEquals(size, map.size());

        for (int i = 0; i < size / 2; i++) {
            map.remove(i);
        }
        assertEquals(size / 2, map.size());
        assertEquals(size, testMapStore.getStore().size());
        map.flush();
        assertEquals(size / 2, testMapStore.getStore().size());
        assertEquals(size / 2, map.size());
    }

    @Test
    public void testOneMemberFlushOnShutdown() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 200);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        assertEquals(0, map1.size());
        for (int i = 0; i < 100; i++) {
            map1.put(i, i);
        }
        assertEquals(100, map1.size());
        assertEquals(0, testMapStore.getStore().size());
        h1.getLifecycleService().shutdown();
        assertEquals(100, testMapStore.getStore().size());
        assertEquals(1, testMapStore.getDestroyCount());
    }

    @Test
    public void testOneMemberWriteThroughWithIndex() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
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
        assertEquals(TestUtil.getNode((HazelcastInstanceProxy) h1), TestUtil.getNode(testMapStore.getHazelcastInstance()));
    }

    @Test
    public void testOneMemberWriteThroughWithLRU() throws Exception {
        int size = 10000;
        TestMapStore testMapStore = new TestMapStore(size * 2, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setSize(size);
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        for (int i = 0; i < size * 2; i++) {
            map.put(i, new Employee("joe", i, true, 100.00));
        }
        assertEquals(testMapStore.getStore().size(), size * 2);
        Thread.sleep(1000);
        assertTrue(map.size() > size / 2);
        assertTrue(map.size() <= size);
        assertEquals(testMapStore.getStore().size(), size * 2);
    }

    @Test
    public void testOneMemberWriteThrough() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        Employee employee = new Employee("joe", 25, true, 100.00);
        Employee newEmployee = new Employee("ali", 26, true, 1000);
        testMapStore.insert("1", employee);
        testMapStore.insert("2", employee);
        testMapStore.insert("3", employee);
        testMapStore.insert("4", employee);
        testMapStore.insert("5", employee);
        testMapStore.insert("6", employee);
        testMapStore.insert("7", employee);

        IMap map = h1.getMap("default");
        map.addIndex("name", false);
        assertEquals(0, map.size());
        assertEquals(employee, map.get("1"));
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        assertEquals(employee, map.put("2", newEmployee));
        assertEquals(newEmployee, testMapStore.getStore().get("2"));
        assertEquals(2, map.size());

        map.remove("1");
        map.put("1", employee, 1, TimeUnit.SECONDS);
        map.put("1", employee);
        Thread.sleep(2000);
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(employee, map.get("1"));

        map.evict("2");
        assertEquals(newEmployee, map.get("2"));

        assertEquals(employee, map.get("3"));
        assertEquals(employee, map.put("3", newEmployee));
        assertEquals(newEmployee, map.get("3"));

        assertEquals(employee, map.remove("4"));

        assertEquals(employee, map.get("5"));
        assertEquals(employee, map.remove("5"));

        assertEquals(employee, map.putIfAbsent("6", newEmployee));
        assertEquals(employee, map.get("6"));
        assertEquals(employee, testMapStore.getStore().get("6"));

        assertTrue(map.containsKey("7"));
        assertEquals(employee, map.get("7"));

        assertNull(map.get("8"));
        assertFalse(map.containsKey("8"));
        assertNull(map.putIfAbsent("8", employee));
        assertEquals(employee, map.get("8"));
        assertEquals(employee, testMapStore.getStore().get("8"));
    }

    @Test
    public void testTwoMemberWriteThrough() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        Employee employee = new Employee("joe", 25, true, 100.00);
        Employee employee2 = new Employee("jay", 35, false, 100.00);
        testMapStore.insert("1", employee);
        IMap map = h1.getMap("default");
        map.addIndex("name", false);
        assertEquals(0, map.size());
        assertEquals(employee, map.get("1"));
        assertEquals(employee, testMapStore.getStore().get("1"));
        assertEquals(1, map.size());
        map.put("2", employee2);
        assertEquals(employee2, testMapStore.getStore().get("2"));
        assertEquals(2, testMapStore.getStore().size());
        assertEquals(2, map.size());
        map.remove("2");
        assertEquals(1, testMapStore.getStore().size());
        assertEquals(1, map.size());
        testMapStore.assertAwait(10);
        assertEquals(6, testMapStore.callCount.get());
    }

    @Test
    public void testTwoMemberWriteThrough2() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1000, 0, 0);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        for (int i = 0; i < 1000; i++) {
            map1.put(i, "value" + i);
        }
        assertEquals(1000, testMapStore.getStore().size());
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        testMapStore.assertAwait(10);
        // 1000 put-load 1000 put-store call and 2 loadAllKeys
        assertEquals(2002, testMapStore.callCount.get());
    }

    @Test
    public void testOneMemberWriteThroughFailingStore() throws Exception {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        testMapStore.setFail(true, true);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
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
        assertEquals(0, testMapStore.stores.get());
        assertEquals(0, map.size());
    }

    @Test
    public void testOneMemberWriteThroughFailingStore2() throws Exception {
        FailAwareMapStore testMapStore = new FailAwareMapStore();
        testMapStore.setFail(true, false);
        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());

        try {
            map.put("1", "value");
            fail("should have thrown exception");
        } catch (Exception e) {
        }
        assertEquals(0, map.size());
    }

    @Test
    public void testGetAllKeys() throws Exception {
        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        Map store = testMapStore.getStore();
        Set keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.put(i, "value" + i);
            keys.add(i);
        }
        Config config = newConfig(testMapStore, 2);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");
        Thread.sleep(10000);
        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap map3 = h3.getMap("default");
        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));
        assertEquals("value1", map3.get(1));
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        assertEquals(1000, map3.size());
        h3.getLifecycleService().shutdown();
        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
    }

    /*
     * Test for Issue 572
    */
    @Test
    public void testMapstoreDeleteOnClear() throws Exception {
        Config config = new Config();
        MapStoreTest.SimpleMapStore store = new MapStoreTest.SimpleMapStore();
        config.getMapConfig("testMapstoreDeleteOnClear").setMapStoreConfig(new MapStoreConfig().setEnabled(true).setImplementation(store));

        final HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> map = hz.getMap("testMapstoreDeleteOnClear");
        int size = 10;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        assertEquals(size, map.size());
        assertEquals(size, store.loadAllKeys().size());
        map.clear();
        assertEquals(0, map.size());
        assertEquals(0, store.loadAllKeys().size());
    }

    public static Config newConfig(Object storeImpl, int writeDelaySeconds) {
        return newConfig("default", storeImpl, writeDelaySeconds);
    }

    public static Config newConfig(String mapName, Object storeImpl, int writeDelaySeconds) {
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = config.getMapConfig(mapName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(storeImpl);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }

    public static class TestEventBasedMapStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {

        protected enum STORE_EVENTS {
            STORE, STORE_ALL, DELETE, DELETE_ALL, LOAD, LOAD_ALL, LOAD_ALL_KEYS
        }

        protected final Map<K, V> store = new ConcurrentHashMap();
        protected final BlockingQueue events = new LinkedBlockingQueue();
        protected final AtomicInteger storeCount = new AtomicInteger();
        protected final AtomicInteger loadCount = new AtomicInteger();
        protected final AtomicInteger callCount = new AtomicInteger();
        protected final AtomicInteger initCount = new AtomicInteger();
        protected HazelcastInstance hazelcastInstance;
        protected Properties properties;
        protected String mapName;
        protected boolean loadAllKeys = true;

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

        public boolean isLoadAllKeys() {
            return loadAllKeys;
        }

        public void setLoadAllKeys(boolean loadAllKeys) {
            this.loadAllKeys = loadAllKeys;
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
            storeCount.incrementAndGet();
            events.offer(STORE_EVENTS.STORE);
        }

        public V load(K key) {
            callCount.incrementAndGet();
            loadCount.incrementAndGet();
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
            if (!loadAllKeys) return null;
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
        final AtomicBoolean storeFail = new AtomicBoolean(false);
        final AtomicBoolean loadFail = new AtomicBoolean(false);
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
                if (storeFail.get()) {
                    throw new RuntimeException();
                } else {
                    db.remove(key);
                }
            } finally {
                deletes.incrementAndGet();
                notifyListeners();
            }
        }

        public void setFail(boolean shouldFail, boolean loadFail) {
            this.storeFail.set(shouldFail);
            this.loadFail.set(loadFail);
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
                if (storeFail.get()) {
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
                return db.keySet();
            } finally {
                loadAllKeys.incrementAndGet();
            }
        }

        public Object load(Object key) {
            try {
                if (loadFail.get()) {
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
                if (storeFail.get()) {
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
                if (loadFail.get()) {
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
                if (storeFail.get()) {
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

    public static class TestMapStore extends MapStoreAdapter implements MapLoaderLifecycleSupport, MapStore {

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
        final AtomicInteger destroyCount = new AtomicInteger();
        private HazelcastInstance hazelcastInstance;
        private Properties properties;
        private String mapName;
        private boolean loadAllKeys = true;

        public TestMapStore() {
            this(0, 0, 0, 0, 0, 0);
        }

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

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }

        public String getMapName() {
            return mapName;
        }

        public Properties getProperties() {
            return properties;
        }

        public void assertAwait(int seconds) throws InterruptedException {
            assertTrue("Store remaining: " + latchStore.getCount(), latchStore.await(seconds, TimeUnit.SECONDS));
            assertTrue("Store-all remaining: " + latchStoreAll.getCount(), latchStoreAll.await(seconds, TimeUnit.SECONDS));
            assertTrue("Delete remaining: " + latchDelete.getCount(), latchDelete.await(seconds, TimeUnit.SECONDS));
            assertTrue("Delete-all remaining: " + latchDeleteAll.getCount(), latchDeleteAll.await(seconds, TimeUnit.SECONDS));
            assertTrue("Load remaining: " + latchLoad.getCount(), latchLoad.await(seconds, TimeUnit.SECONDS));
            assertTrue("Load-al remaining: " + latchLoadAll.getCount(), latchLoadAll.await(seconds, TimeUnit.SECONDS));
        }

        public Map getStore() {
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
            if (!loadAllKeys) return null;
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

    public static class SimpleMapStore<K, V> extends MapStoreAdapter<K, V> {
        public final Map<K, V> store;
        private boolean loadAllKeys = true;

        public SimpleMapStore() {
            store = new ConcurrentHashMap<K, V>();
        }

        public SimpleMapStore(final Map<K, V> store) {
            this.store = store;
        }

        @Override
        public void delete(final K key) {
            store.remove(key);
        }

        @Override
        public V load(final K key) {
            return store.get(key);
        }

        @Override
        public void store(final K key, final V value) {
            store.put(key, value);
        }

        public Set<K> loadAllKeys() {
            if (loadAllKeys) {
                return store.keySet();
            }
            return null;
        }

        public void setLoadAllKeys(boolean loadAllKeys) {
            this.loadAllKeys = loadAllKeys;
        }

        @Override
        public void storeAll(final Map<K, V> kvMap) {
            store.putAll(kvMap);
        }
    }


}
