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
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapStoreWrapper;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.proxy.MapProxyImpl;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;
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
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapStoreTest extends HazelcastTestSupport {

    @Test
    public void testMapGetAll() throws InterruptedException {

        final Map<String, String> _map = new HashMap<String, String>();
        _map.put("key1", "value1");
        _map.put("key2", "value2");
        _map.put("key3", "value3");

        final AtomicBoolean loadAllCalled = new AtomicBoolean(false);
        final AtomicBoolean loadCalled = new AtomicBoolean(false);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapLoader<String, String>() {

            public String load(String key) {
                loadCalled.set(true);
                return _map.get(key);
            }

            public Map<String, String> loadAll(Collection<String> keys) {
                loadAllCalled.set(true);
                final HashMap<String, String> temp = new HashMap<String, String>();
                for (String key : keys) {
                    temp.put(key, _map.get(key));
                }
                return temp;
            }

            public Set<String> loadAllKeys() {
                return _map.keySet();
            }
        });
        cfg.getMapConfig("testMapGetAll").setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap map = instance1.getMap("testMapGetAll");

        final HashSet<String> keys = new HashSet<String>();
        keys.add("key1");
        keys.add("key3");
        keys.add("key4");

        final Map subMap = map.getAll(keys);
        assertEquals(2, subMap.size());
        assertEquals("value1", subMap.get("key1"));
        assertEquals("value3", subMap.get("key3"));

        assertTrue(loadAllCalled.get());
        assertFalse(loadCalled.get());
    }


    @Test
    @Category(ProblematicTest.class)
    public void loadAllKeysTest_withNodeAdded() throws InterruptedException {

        final Map<String, String> _map = new HashMap<String, String>();
        _map.put("key1", "value1");
        _map.put("key2", "value2");
        _map.put("key3", "value3");

        final AtomicInteger loadAllCalled = new AtomicInteger(0);
        final AtomicInteger loadCalled = new AtomicInteger(0);
        final AtomicInteger loadAllKeys = new AtomicInteger(0);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapLoader<String, String>() {

            public String load(String key) {
                loadCalled.getAndIncrement();
                return _map.get(key);
            }

            public Map<String, String> loadAll(Collection<String> keys) {
                loadAllCalled.getAndIncrement();
                final HashMap<String, String> temp = new HashMap<String, String>();
                for (String key : keys) {
                    temp.put(key, _map.get(key));
                }
                return temp;
            }

            public Set<String> loadAllKeys() {
                loadAllKeys.getAndIncrement();
                return _map.keySet();
            }
        });
        cfg.getMapConfig("testMapGetAll").setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        IMap map = instance1.getMap("testMapGetAll");

        map.size();

        assertEquals(1, loadAllKeys.get());

        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        Thread.sleep(1000*10);

        assertEquals("after node added to cluster load all keys called times ", 1, loadAllKeys.get());
    }



    @Test
    public void testSlowStore() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final TestMapStore store = new WaitingOnFirstTestMapStore();
        Config cfg = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(1);
        mapStoreConfig.setImplementation(store);
        cfg.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        final IMap<Integer, Integer> map = h1.getMap("testSlowStore");
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put(i,1);
        }
        Thread.sleep(2000); // sleep for scheduling following puts to a different second
        for (int i = 0; i < count; i++) {
            map.put(i,2);
        }
        Thread.sleep(15000); // sleep for waiting all stores to be completed for checking correctness
        for (int i = 0; i < count; i++) {
            assertEquals(map.get(i),store.getStore().get(i));
        }
    }

    @Test
    public void testMapInitialLoad() throws InterruptedException {
        int size = 10000;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        Config cfg = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapLoader(size));
        cfg.getMapConfig("default").setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap map = instance1.getMap("testMapInitialLoad");
        assertEquals(size, map.size());
        System.out.println(map.size());

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
//        assertEquals(2, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        assertNull(myMap.remove("ten"));
//        assertEquals(3, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        myMap.put("three", 3L);
        myMap.put("four", 4L);
//        assertEquals(5, loadCount.get());
        assertEquals(2, storeCount.get());
        assertEquals(0, deleteCount.get());
        myMap.remove("one");
        assertEquals(2, storeCount.get());
        assertEquals(1, deleteCount.get());
//        assertEquals(5, loadCount.get());
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
    public void testOneMemberWriteBehindWithEvictions() throws Exception {
        final String mapName = "testOneMemberWriteBehindWithEvictions";
        final TestEventBasedMapStore testMapStore = new TestEventBasedMapStore();
        testMapStore.loadAllLatch = new CountDownLatch(1);
        final Config config = newConfig(testMapStore, 2);
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        final HazelcastInstance node1 = nodeFactory.newHazelcastInstance(config);
        final IMap map = node1.getMap(mapName);
        // check if load all called.
        assertTrue("map store loadAllKeys must be called", testMapStore.loadAllLatch.await(10, TimeUnit.SECONDS));
        // map population count.
        final int populationCount = 100;
        // latch for store & storeAll events.
        testMapStore.storeLatch = new CountDownLatch(populationCount);
        //populate map.
        for (int i = 0; i < populationCount; i++) {
            map.put(i, "value" + i);
        }
        //wait for all store ops.
        assertTrue(testMapStore.storeLatch.await(10, TimeUnit.SECONDS));
        // init before eviction.
        testMapStore.storeLatch = new CountDownLatch(populationCount);
        //evict.
        for (int i = 0; i < populationCount; i++) {
            map.evict(i);
        }
        //expect no store op.
        assertEquals(populationCount, testMapStore.storeLatch.getCount());
        //check store size
        assertEquals(populationCount, testMapStore.getStore().size());
        //check map size
        assertEquals(0, map.size());
        //re-populate map.
        for (int i = 0; i < populationCount; i++) {
            map.put(i, "value" + i);
        }
        //evict again.
        for (int i = 0; i < populationCount; i++) {
            map.evict(i);
        }
        //wait for all store ops.
        testMapStore.storeLatch.await(10, TimeUnit.SECONDS);
        //check store size
        assertEquals(populationCount, testMapStore.getStore().size());
        //check map size
        assertEquals(0, map.size());

        //re-populate map.
        for (int i = 0; i < populationCount; i++) {
            map.put(i, "value" + i);
        }
        testMapStore.deleteLatch = new CountDownLatch(populationCount);
        //clear map.
        for (int i = 0; i < populationCount; i++) {
            map.remove(i);
        }
        testMapStore.deleteLatch.await(10, TimeUnit.SECONDS);
        //check map size
        assertEquals(0, map.size());
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
    public void testWriteBehindUpdateSameKey() throws Exception {
        TestMapStore testMapStore = new TestMapStore(2, 0, 0);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 5);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);
        IMap<Object, Object> map = h1.getMap("map");
        map.put("key", "value");
        Thread.sleep(2000);
        map.put("key", "value2");
        testMapStore.latchStore.await(10, TimeUnit.SECONDS);
        assertEquals("value2", testMapStore.getStore().get("key"));
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
        assertTrue("store operations could not be done wisely ",
                testMapStore.latchStore.await(30, TimeUnit.SECONDS));
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
        checkIfMapLoaded("default", h1);
        checkIfMapLoaded("default", h2);
        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));
        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap map3 = h3.getMap("default");
        checkIfMapLoaded("default", h3);
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

    private boolean checkIfMapLoaded(String mapName, HazelcastInstance instance) throws InterruptedException {
        NodeEngineImpl nodeEngine = TestUtil.getNode(instance).nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        boolean loaded = false;

        final long end = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);

        while (!loaded) {
            for (int i = 0; i < partitionCount; i++) {
                final RecordStore recordStore = service.getPartitionContainer(i).getRecordStore(mapName);
                if (recordStore != null) {
                    loaded = recordStore.isLoaded();
                    if (!loaded) {
                        break;
                    }
                }
            }
            if (System.currentTimeMillis() >= end) {
                break;
            }
            //give a rest to cpu.
            Thread.sleep(10);
        }
        return loaded;
    }

    /*
     * Test for Issue 572
    */
    @Test
    public void testMapstoreDeleteOnClear() throws Exception {
        Config config = new Config();
        SimpleMapStore store = new SimpleMapStore();
        config.getMapConfig("testMapstoreDeleteOnClear").setMapStoreConfig(new MapStoreConfig().setEnabled(true).setImplementation(store));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = nodeFactory.newHazelcastInstance(config);
        IMap<Object, Object> map = hz.getMap("testMapstoreDeleteOnClear");
        int size = 10;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        assertEquals(size, map.size());
        assertEquals(size, store.store.size());
        assertEquals(size, store.loadAllKeys().size());
        map.clear();
        assertEquals(0, map.size());
        assertEquals(0, store.loadAllKeys().size());
    }

    // bug: store is called twice on loadAll
    @Test
    public void testIssue1070() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        final NoDuplicateMapStore myMapStore = new NoDuplicateMapStore();
        myMapStore.store.put(1, 2);
        Config config = new Config();
        config
                .getMapConfig("testIssue1070")
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(myMapStore));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hc = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance hc2 = nodeFactory.newHazelcastInstance(config);
        IMap<Object, Object> map = hc.getMap("testIssue1070");
        for (int i = 0; i < 271; i++) {
            map.get(i);
        }
        assertFalse(myMapStore.failed);
    }

    static class NoDuplicateMapStore extends TestMapStore {
        boolean failed = false;

        @Override
        public void store(Object key, Object value) {
            if (store.containsKey(key)) {
                failed = true;
                throw new RuntimeException("duplicate is not allowed");
            }
            super.store(key, value);
        }

        @Override
        public void storeAll(Map map) {
            for (Object key : map.keySet()) {
                if (store.containsKey(key)) {
                    failed = true;
                    throw new RuntimeException("duplicate is not allowed");
                }
            }
            super.storeAll(map);
        }
    }


    @Test
    public void testIssue806CustomTTLForNull() {
        final ConcurrentMap<String, String> store = new ConcurrentHashMap<String, String>();
        final MapStore<String, String> myMapStore = new SimpleMapStore<String, String>(store);
        Config config = new Config();
        config
                .getMapConfig("testIssue806CustomTTLForNull")
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(myMapStore));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance hc = nodeFactory.newHazelcastInstance(config);
        IMap<Object, Object> map = hc.getMap("testIssue806CustomTTLForNull");
        map.get("key");
        assertNull(map.get("key"));
        store.put("key", "value");
        assertEquals("value", map.get("key"));
    }

    @Test
    public void testIssue991EvictedNullIssue() throws InterruptedException {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapLoader<String, String>() {
            @Override
            public String load(String key) {
                return null;
            }

            @Override
            public Map<String, String> loadAll(Collection<String> keys) {
                return null;
            }

            @Override
            public Set<String> loadAllKeys() {
                return null;
            }
        });
        Config config = new Config();
        config
                .getMapConfig("testIssue991EvictedNullIssue")
                .setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance hc = nodeFactory.newHazelcastInstance(config);
        IMap<Object, Object> map = hc.getMap("testIssue991EvictedNullIssue");
        map.get("key");
        assertNull(map.get("key"));
        map.put("key", "value");
        Thread.sleep(2000);
        assertEquals("value", map.get("key"));
    }

    @Test
    public void testIssue1019() throws InterruptedException {
        final String keyWithNullValue = "keyWithNullValue";

        TestEventBasedMapStore testMapStore = new TestEventBasedMapStore() {
            @Override
            public Set loadAllKeys() {
                Set keys = new HashSet(super.loadAllKeys());
                // Include an extra key that will *not* be returned by loadAll().
                keys.add(keyWithNullValue);
                return keys;
            }
        };

        Map mapForStore = new HashMap();
        mapForStore.put("key1", 17);
        mapForStore.put("key2", 37);
        mapForStore.put("key3", 47);
        testMapStore.getStore().putAll(mapForStore);

        Config config = newConfig(testMapStore, 0);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap map = instance.getMap("default");

        assertEquals(map.keySet(), mapForStore.keySet());
        assertEquals(new HashSet(map.values()), new HashSet(mapForStore.values()));
        assertEquals(map.entrySet(), mapForStore.entrySet());

        assertFalse(map.containsKey(keyWithNullValue));
        assertNull(map.get(keyWithNullValue));
    }

    static class ProcessingStore extends MapStoreAdapter<Integer, Employee> implements PostProcessingMapStore {
        @Override
        public void store(Integer key, Employee employee) {
            employee.setSalary(employee.getAge() * 1000);
        }
    }

    @Test
    public void testIssue1115EnablingMapstoreMutatingValue() throws InterruptedException {
        Config cfg = new Config();
        String mapName = "testIssue1115";
        MapStore mapStore = new ProcessingStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(mapStore);
        cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Employee> map = instance1.getMap(mapName);
        Random random = new Random();
        // testing put with new object
        for (int i = 0; i < 10; i++) {
            Employee emp = new Employee();
            emp.setAge(random.nextInt(20) + 20);
            map.put(i, emp);
        }

        for (int i = 0; i < 10; i++) {
            Employee employee = map.get(i);
            assertEquals(employee.getAge() * 1000, employee.getSalary(), 0);
        }

        // testing put with existing object
        for (int i = 0; i < 10; i++) {
            Employee emp = map.get(i);
            emp.setAge(random.nextInt(20) + 20);
            map.put(i, emp);
        }

        for (int i = 0; i < 10; i++) {
            Employee employee = map.get(i);
            assertEquals(employee.getAge() * 1000, employee.getSalary(), 0);
        }

        // testing put with replace
        for (int i = 0; i < 10; i++) {
            Employee emp = map.get(i);
            emp.setAge(random.nextInt(20) + 20);
            map.replace(i, emp);
        }

        for (int i = 0; i < 10; i++) {
            Employee employee = map.get(i);
            assertEquals(employee.getAge() * 1000, employee.getSalary(), 0);
        }

        // testing put with putIfAbsent
        for (int i = 10; i < 20; i++) {
            Employee emp = new Employee();
            emp.setAge(random.nextInt(20) + 20);
            map.putIfAbsent(i, emp);
        }

        for (int i = 10; i < 20; i++) {
            Employee employee = map.get(i);
            assertEquals(employee.getAge() * 1000, employee.getSalary(), 0);
        }

    }

    @Test
    public void testIssue1110() throws InterruptedException {
        final int mapSize = 10;
        final String mapName = "testIssue1110";
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.setProperty(GroupProperties.PROP_MAP_LOAD_CHUNK_SIZE, "5");
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapLoader(mapSize));
        cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap map = instance1.getMap(mapName);
        final CountDownLatch latch = new CountDownLatch(mapSize);
        map.addEntryListener(new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
            }

            @Override
            public void entryUpdated(EntryEvent event) {
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        }, true);
        // create all partition recordstores.
        map.size();
        //wait map load.
        latch.await();
        assertEquals(mapSize, map.size());
    }

    @Test
    public void testIssue1142ExceptionWhenLoadAllReturnsNull() {
        Config config = new Config();
        String mapname = "testIssue1142ExceptionWhenLoadAllReturnsNull";
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(new MapStoreAdapter<String, String>() {
            @Override
            public Set<String> loadAllKeys() {
                Set keys = new HashSet();
                keys.add("key");
                return keys;
            }

            public Map loadAll(Collection keys) {
                return null;
            }
        });
        config.getMapConfig(mapname).setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final IMap map = instance.getMap(mapname);
        for (int i = 0; i < 300; i++) {
            map.put(i, i);
        }
        assertEquals(300, map.size());
    }

    @Test
    public void testIssue1085WriteBehindBackup() throws InterruptedException {
        Config config = new Config();
        String name = "testIssue1085WriteBehindBackup";
        MapConfig writeBehindBackup = config.getMapConfig(name);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setWriteDelaySeconds(5);
        int size = 1000;
        MapStoreWithStoreCount mapStore = new MapStoreWithStoreCount(size, 20);
        mapStoreConfig.setImplementation(mapStore);
        writeBehindBackup.setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final IMap map = instance.getMap(name);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        instance2.getLifecycleService().terminate();
        mapStore.awaitStores();
    }

    @Test
    @Category(ProblematicTest.class)
    public void testIssue1085WriteBehindBackupWithLongRunnigMapStore() throws InterruptedException {
        Config config = new Config();
        String name = "testIssue1085WriteBehindBackup";
        config.setProperty(GroupProperties.PROP_MAP_REPLICA_WAIT_SECONDS_FOR_SCHEDULED_OPERATIONS, "50");
        MapConfig writeBehindBackup = config.getMapConfig(name);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setWriteDelaySeconds(5);
        int size = 1000;
        MapStoreWithStoreCount mapStore = new MapStoreWithStoreCount(size, 90, 30);
        mapStoreConfig.setImplementation(mapStore);
        writeBehindBackup.setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final IMap map = instance.getMap(name);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        instance2.getLifecycleService().terminate();
        mapStore.awaitStores();
    }

    @Test
    public void testIssue1085WriteBehindBackupTransactional() throws InterruptedException {
        Config config = new Config();
        String name = "testIssue1085WriteBehindBackupTransactional";
        MapConfig writeBehindBackup = config.getMapConfig(name);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setWriteDelaySeconds(5);
        int size = 1000;
        MapStoreWithStoreCount mapStore = new MapStoreWithStoreCount(size, 20);
        mapStoreConfig.setImplementation(mapStore);
        writeBehindBackup.setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final IMap map = instance.getMap(name);
        TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        TransactionalMap<Object, Object> tmap = context.getMap(name);
        for (int i = 0; i < size; i++) {
            tmap.put(i, i);
        }
        context.commitTransaction();
        instance2.getLifecycleService().terminate();
        mapStore.awaitStores();
    }

    @Test
    public void testWriteBehindSameSecondSameKey() throws Exception {
        TestMapStore testMapStore = new TestMapStore(100, 0, 0); // In some cases 2 store operation may happened
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 2);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap<Object, Object> map = h1.getMap("testWriteBehindSameSecondSameKey");
        final int size1 = 20;
        final int size2 = 10;
        //store op count.
        testMapStore.latchStoreOpCount = new CountDownLatch(size1);
        testMapStore.latchStoreAllOpCount = new CountDownLatch(size2);

        for (int i = 0; i < size1; i++) {
            map.put("key", "value" + i);
        }
        for (int i = 0; i < size2; i++) {
            map.put("key"+i, "value" + i);
        }

        assertTrue("store operations must be finished.",
                testMapStore.latchStoreOpCount.await(30, TimeUnit.SECONDS));

        assertTrue("store all operations must be finished.",
                testMapStore.latchStoreAllOpCount.await(30, TimeUnit.SECONDS));

        assertEquals("value"+ (size1-1), testMapStore.getStore().get("key"));
        assertEquals("value"+ (size2-1), testMapStore.getStore().get("key"+(size2-1)));
    }

    @Test
    public void testReadingConfiguration() throws Exception {
        String mapName = "mapstore-test";
        InputStream is = getClass().getResourceAsStream("/com/hazelcast/config/hazelcast-mapstore-config.xml");
        XmlConfigBuilder builder = new XmlConfigBuilder(is);
        Config config = builder.build();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        MapProxyImpl map = (MapProxyImpl) hz.getMap(mapName);
        MapService mapService = (MapService) map.getService();
        MapContainer mapContainer = mapService.getMapContainer(mapName);
        MapStoreWrapper mapStoreWrapper = mapContainer.getStore();
        Set keys = mapStoreWrapper.loadAllKeys();
        assertEquals(2, keys.size());
        assertEquals("true", mapStoreWrapper.load("my-prop-1"));
        assertEquals("foo", mapStoreWrapper.load("my-prop-2"));
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

    public static class BasicMapStoreFactory implements MapStoreFactory<String, String> {

        @Override
        public MapLoader<String, String> newMapStore(String mapName, final Properties properties) {
            return new MapStore<String, String>() {
                @Override
                public void store(String key, String value) {
                }

                @Override
                public void storeAll(Map map) {
                }

                @Override
                public void delete(String key) {
                }

                @Override
                public void deleteAll(Collection keys) {
                }

                @Override
                public String load(String key) {
                    return properties.getProperty(key.toString());
                }

                @Override
                public Map<String, String> loadAll(Collection<String> keys) {
                    Map<String, String> map = new HashMap<String, String>();
                    for (String key : keys) {
                        map.put(key, properties.getProperty(key));
                    }
                    return map;
                }

                @Override
                public Set<String> loadAllKeys() {
                    return new HashSet<String>(properties.stringPropertyNames());
                }
            };
        }
    }

    public static class MapStoreWithStoreCount extends SimpleMapStore {
        final CountDownLatch latch;
        final int waitSecond;
        final AtomicInteger count = new AtomicInteger(0);
        final int sleepStoreAllSeconds;

        public MapStoreWithStoreCount(int expectedStore, int seconds) {
            latch = new CountDownLatch(expectedStore);
            waitSecond = seconds;
            sleepStoreAllSeconds = 0;
        }

        public MapStoreWithStoreCount(int expectedStore, int seconds, int sleepStoreAllSeconds) {
            latch = new CountDownLatch(expectedStore);
            waitSecond = seconds;
            this.sleepStoreAllSeconds = sleepStoreAllSeconds;
        }

        public void awaitStores() {
            try {
                assertTrue(latch.await(waitSecond, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
            }
        }

        @Override
        public void store(Object key, Object value) {
            latch.countDown();
            super.store(key, value);
            count.incrementAndGet();
        }

        @Override
        public void storeAll(Map map) {
            if(sleepStoreAllSeconds > 0) {
                try {
                    Thread.sleep(sleepStoreAllSeconds*1000);
                } catch (InterruptedException e) {
                }
            }
            for (Object o : map.keySet()) {
                latch.countDown();
                count.incrementAndGet();
            }
            super.storeAll(map);
        }
    }

    public static class TestEventBasedMapStore<K, V> implements MapLoaderLifecycleSupport, MapStore<K, V> {

        protected enum STORE_EVENTS {
            STORE, STORE_ALL, DELETE, DELETE_ALL, LOAD, LOAD_ALL, LOAD_ALL_KEYS
        }

        protected final Map<K, V> store = new ConcurrentHashMap();
        protected final BlockingQueue events = new LinkedBlockingQueue();
        protected final AtomicInteger storeCount = new AtomicInteger();
        protected final AtomicInteger storeAllCount = new AtomicInteger();
        protected final AtomicInteger loadCount = new AtomicInteger();
        protected final AtomicInteger callCount = new AtomicInteger();
        protected final AtomicInteger initCount = new AtomicInteger();
        protected HazelcastInstance hazelcastInstance;
        protected Properties properties;
        protected String mapName;
        protected boolean loadAllKeys = true;
        protected CountDownLatch storeLatch;
        protected CountDownLatch deleteLatch;
        protected CountDownLatch loadAllLatch;

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
            if (storeLatch != null) {
                storeLatch.countDown();
            }
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
            final int size = map.size();
            if (storeLatch != null) {
                for (int i = 0; i < size; i++) {
                    storeLatch.countDown();
                }
            }
            events.offer(STORE_EVENTS.STORE_ALL);
        }

        public void delete(K key) {
            store.remove(key);
            callCount.incrementAndGet();
            if (deleteLatch != null) {
                deleteLatch.countDown();
            }
            events.offer(STORE_EVENTS.DELETE);
        }

        public Set<K> loadAllKeys() {
            if (loadAllLatch != null) {
                loadAllLatch.countDown();
            }
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

            if (deleteLatch != null) {
                for (int i = 0; i < keys.size(); i++) {
                    deleteLatch.countDown();
                }
            }
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
    public static class WaitingOnFirstTestMapStore extends TestMapStore{
        private AtomicInteger count;
        public WaitingOnFirstTestMapStore() {
            super();
            this.count = new AtomicInteger(0);
        }
        @Override
        public void storeAll(Map map) {
            if(count.get() == 0)
            {
                count.incrementAndGet();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            super.storeAll(map);
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
        CountDownLatch latchStoreOpCount;
        CountDownLatch latchStoreAllOpCount;
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
            if (latchStoreOpCount != null) {
                latchStoreOpCount.countDown();
            }
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

            if (latchStoreAllOpCount != null) {
                for (int i = 0; i < map.size(); i++) {
                    latchStoreAllOpCount.countDown();
                }
            }
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
