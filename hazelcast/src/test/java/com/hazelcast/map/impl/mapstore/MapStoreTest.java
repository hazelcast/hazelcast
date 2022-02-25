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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.MapStoreFactory;
import com.hazelcast.map.PostProcessingMapStore;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapStoreWrapper;
import com.hazelcast.map.impl.mapstore.writebehind.MapStoreWithCounter;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindFlushTest.assertWriteBehindQueuesEmpty;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStoreTest extends AbstractMapStoreTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test(timeout = 120000)
    public void testMapGetAll() {
        final Map<String, String> _map = new HashMap<>();
        _map.put("key1", "value1");
        _map.put("key2", "value2");
        _map.put("key3", "value3");

        final AtomicBoolean loadAllCalled = new AtomicBoolean(false);
        final AtomicBoolean loadCalled = new AtomicBoolean(false);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapLoader<String, String>() {

            public String load(String key) {
                loadCalled.set(true);
                return _map.get(key);
            }

            public Map<String, String> loadAll(Collection<String> keys) {
                loadAllCalled.set(true);
                final HashMap<String, String> temp = new HashMap<>();
                for (String key : keys) {
                    String value = _map.get(key);
                    if (value != null) {
                        temp.put(key, value);
                    }
                }
                return temp;
            }

            public Set<String> loadAllKeys() {
                return _map.keySet();
            }
        });
        String mapName = "default";
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        IMap<String, String> map = instance.getMap(mapName);

        final HashSet<String> keys = new HashSet<>(3);
        keys.add("key1");
        keys.add("key3");
        keys.add("key4");

        final Map<String, String> subMap = map.getAll(keys);
        assertEquals(2, subMap.size());
        assertEquals("value1", subMap.get("key1"));
        assertEquals("value3", subMap.get("key3"));

        assertTrue(loadAllCalled.get());
        assertFalse(loadCalled.get());
    }

    @Test
    public void testNullValuesFromMapLoaderAreNotInsertedIntoMap() {
        Config config = newConfig(new NullLoader());
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<String, String> map = node.getMap(randomName());

        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(startsWith("Neither key nor value can be loaded as null"));
        // load entries.
        map.getAll(new HashSet<>(asList("key1", "key2", "key3")));
    }

    /**
     * Always loads null values for requested keys.
     */
    public static class NullLoader implements MapLoader<Object, Object> {

        @Override
        public Object load(Object key) {
            return null;
        }

        @Override
        public Map<Object, Object> loadAll(Collection keys) {
            Map<Object, Object> map = new HashMap<>();
            for (Object key : keys) {
                map.put(key, null);
            }
            return map;
        }

        @Override
        public Iterable<Object> loadAllKeys() {
            return null;
        }
    }

    @Test(timeout = 120000)
    public void testSlowStore() {
        final TestMapStore store = new WaitingOnFirstTestMapStore();
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(1);
        mapStoreConfig.setImplementation(store);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance h1 = createHazelcastInstance(config);
        final IMap<Integer, Integer> map = h1.getMap("testSlowStore");
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put(i, 1);
        }
        // sleep for scheduling following puts to a different second
        sleepSeconds(2);
        for (int i = 0; i < count; i++) {
            map.put(i, 2);
        }
        for (int i = 0; i < count; i++) {
            final int index = i;
            assertTrueEventually(() -> {
                final Integer valueInMap = map.get(index);
                final Integer valueInStore = (Integer) store.getStore().get(index);

                assertEquals(valueInMap, valueInStore);
            });
        }
    }

    @Test(timeout = 120000)
    public void testInitialLoadModeEager() {
        int size = 10000;
        String mapName = "default";
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapLoader(size, true));
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        IMap map = instance.getMap(mapName);

        assertSizeEventually(size, map);
    }

    @Test(timeout = 120000)
    public void testInitialLoadModeEagerMultipleThread() {
        final String mapName = "default";
        final int instanceCount = 2;
        final int size = 10000;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(instanceCount);
        final CountDownLatch countDownLatch = new CountDownLatch(instanceCount - 1);
        final Config config = getConfig();
        config.setClusterName("testEager");
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapLoader(size, true));
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        Runnable runnable = () -> {
            HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
            final IMap<Object, Object> map = instance2.getMap(mapName);
            assertEquals(size, map.size());
            countDownLatch.countDown();
        };
        new Thread(runnable).start();

        assertOpenEventually(countDownLatch, 120);
        IMap map = instance1.getMap(mapName);
        assertEquals(size, map.size());
    }

    @Test(timeout = 120000)
    public void testInitialLoadModeEagerWhileStoppigOneNode() {
        final int instanceCount = 2;
        final int size = 10000;
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(instanceCount);
        final CountDownLatch countDownLatch = new CountDownLatch(instanceCount - 1);
        final Config config = getConfig();
        config.setClusterName("testEager");
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapLoader(size, true));
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        config.getMapConfig("testInitialLoadModeEagerWhileStoppigOneNode").setMapStoreConfig(mapStoreConfig);
        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        new Thread(() -> {
            sleepSeconds(3);
            instance1.getLifecycleService().shutdown();
            sleepSeconds(3);
            final IMap<Object, Object> map = instance2.getMap("testInitialLoadModeEagerWhileStoppigOneNode");
            assertEquals(size, map.size());
            countDownLatch.countDown();

        }).start();

        assertOpenEventually(countDownLatch);

        final IMap<Object, Object> map2 = instance2.getMap("testInitialLoadModeEagerWhileStoppigOneNode");
        final int map2Size = map2.size();
        assertEquals(size, map2Size);
    }

    @Test(timeout = 240000)
    public void testMapInitialLoad() {
        int size = 10000;
        String mapName = randomMapName();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapLoader(size, true));

        MapConfig mc = config.getMapConfig(mapName);
        mc.setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(mapName);

        // trigger initial loads by touching partitions.
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i).intValue());
        }

        assertSizeEventually(size, map);
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i).intValue());
        }

        assertNull(map.put(size, size));
        assertEquals(size, map.remove(size).intValue());
        assertNull(map.get(size));

        nodeFactory.newHazelcastInstance(config);
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i).intValue());
        }
    }

    @Test(timeout = 120000)
    public void issue614() {
        final ConcurrentMap<Long, String> STORE = new ConcurrentHashMap<>();
        STORE.put(1L, "Event1");
        STORE.put(2L, "Event2");
        STORE.put(3L, "Event3");
        STORE.put(4L, "Event4");
        STORE.put(5L, "Event5");
        STORE.put(6L, "Event6");
        Config config = getConfig();
        config.getMapConfig("map")
                .setMapStoreConfig(new MapStoreConfig()
                        .setWriteDelaySeconds(1)
                        .setImplementation(new SimpleMapStore<>(STORE)));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap map = instance.getMap("map");
        map.values();

        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(0, localMapStats.getDirtyEntryCount());
    }

    @Test(timeout = 120000)
    public void testIssue583MapReplaceShouldTriggerMapStore() {
        final ConcurrentMap<String, Long> store = new ConcurrentHashMap<>();
        final MapStore<String, Long> myMapStore = new SimpleMapStore<>(store);
        Config config = getConfig();
        config.getMapConfig("myMap")
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
    public void test_givenKeyNotExists_mapLoaderShouldServeOldValueForMutatingOperations() {
        ConcurrentMap<String, Long> store = new ConcurrentHashMap<>();
        MapStore<String, Long> myMapStore = new SimpleMapStore<>(store);
        Config config = getConfig();
        config.getMapConfig("myMap")
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(myMapStore));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance hc = nodeFactory.newHazelcastInstance(config);
        IMap<String, Long> myMap = hc.getMap("myMap");

        // partitions may be created lazily when we first do an operation, causing
        // map loader to be triggered for all entries during initialization. So
        // we make sure to initialize all required partitions before hand.
        myMap.get("replace");
        myMap.get("replaceIfSame");
        myMap.get("remove");
        myMap.get("put");
        myMap.get("putIfAbsent");

        store.put("replace", -1L);
        store.put("replaceIfSame", -2L);
        store.put("remove", -3L);
        store.put("put", -4L);
        store.put("putIfAbsent", -5L);

        assertEquals(-1, (long) myMap.replace("replace", 1L));
        assertTrue(myMap.replace("replaceIfSame", -2L, 2L));
        assertEquals(-3, (long) myMap.remove("remove"));
        assertEquals(-4, (long) myMap.put("put", 4L));
        assertEquals(-5, (long) myMap.putIfAbsent("putIfAbsent", 5L));

        assertEquals(1L, (long) store.get("replace"));
        assertEquals(2L, (long) store.get("replaceIfSame"));
        assertNull(store.get("remove"));
        assertEquals(4L, (long) store.get("put"));
        assertEquals(-5L, (long) store.get("putIfAbsent"));
    }

    @Test(timeout = 120000)
    public void issue587CallMapLoaderDuringRemoval() {
        final AtomicInteger loadCount = new AtomicInteger(0);
        final AtomicInteger storeCount = new AtomicInteger(0);
        final AtomicInteger deleteCount = new AtomicInteger(0);

        class SimpleMapStore2 extends SimpleMapStore<String, Long> {

            private SimpleMapStore2(ConcurrentMap<String, Long> store) {
                super(store);
            }

            public Long load(String key) {
                loadCount.incrementAndGet();
                return super.load(key);
            }

            public void store(String key, Long value) {
                storeCount.incrementAndGet();
                super.store(key, value);
            }

            public void delete(String key) {
                deleteCount.incrementAndGet();
                super.delete(key);
            }
        }
        final ConcurrentMap<String, Long> store = new ConcurrentHashMap<>();
        final MapStore<String, Long> myMapStore = new SimpleMapStore2(store);
        Config config = getConfig();
        config
                .getMapConfig("myMap")
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(myMapStore));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance hc = nodeFactory.newHazelcastInstance(config);
        store.put("one", 1L);
        store.put("two", 2L);
        assertEquals(0, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        IMap<String, Long> myMap = hc.getMap("myMap");
        assertEquals(1L, myMap.get("one").longValue());
        assertEquals(2L, myMap.get("two").longValue());
        //assertEquals(2, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        assertNull(myMap.remove("ten"));
        //assertEquals(3, loadCount.get());
        assertEquals(0, storeCount.get());
        assertEquals(0, deleteCount.get());
        myMap.put("three", 3L);
        myMap.put("four", 4L);
        //assertEquals(5, loadCount.get());
        assertEquals(2, storeCount.get());
        assertEquals(0, deleteCount.get());
        myMap.remove("one");
        assertEquals(2, storeCount.get());
        assertEquals(1, deleteCount.get());
        //assertEquals(5, loadCount.get());
    }

    @Test(timeout = 120000)
    public void testOneMemberFlush() {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        int size = 100;
        Config config = newConfig(testMapStore, 200);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> map = h1.getMap("default");
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

    @Test(timeout = 120000)
    public void testOneMemberFlushOnShutdown() {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.setLoadAllKeys(false);
        Config config = newConfig(testMapStore, 200);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap("default");
        assertEquals(0, map.size());
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        assertEquals(100, map.size());
        assertEquals(0, testMapStore.getStore().size());
        instance.getLifecycleService().shutdown();
        assertEquals(100, testMapStore.getStore().size());
        assertEquals(1, testMapStore.getDestroyCount());
    }

    @Test(timeout = 120000)
    public void testGetAllKeys() {
        EventBasedMapStore<Integer, String> testMapStore = new EventBasedMapStore<>();
        Map<Integer, String> store = testMapStore.getStore();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.put(i, "value" + i);
        }
        Config config = newConfig(testMapStore, 2);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(config);

        IMap map1 = h1.getMap("default");
        IMap map2 = h2.getMap("default");

        //checkIfMapLoaded("default", h1);
        //checkIfMapLoaded("default", h2);

        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));

        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());

        HazelcastInstance h3 = nodeFactory.newHazelcastInstance(config);
        IMap map3 = h3.getMap("default");
        //checkIfMapLoaded("default", h3);

        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));
        assertEquals("value1", map3.get(1));

        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
        assertEquals(1000, map3.size());

        h3.shutdown();

        assertEquals("value1", map1.get(1));
        assertEquals("value1", map2.get(1));

        assertEquals(1000, map1.size());
        assertEquals(1000, map2.size());
    }

    /*
     * Test for Issue 572
     */
    @Test(timeout = 120000)
    public void testMapstoreDeleteOnClear() {
        Config config = getConfig();
        SimpleMapStore store = new SimpleMapStore();
        config.getMapConfig("testMapstoreDeleteOnClear").setMapStoreConfig(new MapStoreConfig().setEnabled(true).setImplementation(store));
        HazelcastInstance hz = createHazelcastInstance(config);
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
    @Test(timeout = 120000)
    public void testIssue1070() {
        final String mapName = randomMapName();
        final Config config = getConfig();
        final MapConfig mapConfig = config.getMapConfig(mapName);
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        final NoDuplicateMapStore myMapStore = new NoDuplicateMapStore();
        final MapStoreConfig implementation = mapStoreConfig.setImplementation(myMapStore);
        mapConfig.setMapStoreConfig(implementation);

        myMapStore.store.put(1, 2);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance.getMap(mapName);
        for (int i = 0; i < 271; i++) {
            map.get(i);
        }
        assertFalse(myMapStore.failed);
    }

    @Test(timeout = 120000)
    public void testIssue806CustomTTLForNull() {
        final ConcurrentMap<String, String> store = new ConcurrentHashMap<>();
        final MapStore<String, String> myMapStore = new SimpleMapStore<>(store);
        Config config = getConfig();
        config.getMapConfig("testIssue806CustomTTLForNull")
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(myMapStore));
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap("testIssue806CustomTTLForNull");
        map.get("key");
        assertNull(map.get("key"));
        store.put("key", "value");
        assertEquals("value", map.get("key"));
    }

    @Test(timeout = 120000)
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
        Config config = getConfig();
        config.getMapConfig("testIssue991EvictedNullIssue")
                .setMapStoreConfig(mapStoreConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap("testIssue991EvictedNullIssue");
        map.get("key");
        assertNull(map.get("key"));
        map.put("key", "value");
        Thread.sleep(2000);
        assertEquals("value", map.get("key"));
    }

    @Test(timeout = 120000)
    public void testIssue1019() {
        final String keyWithNullValue = "keyWithNullValue";

        EventBasedMapStore<String, Integer> testMapStore = new EventBasedMapStore<String, Integer>() {
            @Override
            public Set<String> loadAllKeys() {
                Set<String> keys = new HashSet<>(super.loadAllKeys());
                // include an extra key that will *not* be returned by loadAll()
                keys.add(keyWithNullValue);
                return keys;
            }
        };

        Map<String, Integer> mapForStore = new HashMap<>();
        mapForStore.put("key1", 17);
        mapForStore.put("key2", 37);
        mapForStore.put("key3", 47);
        testMapStore.getStore().putAll(mapForStore);

        Config config = newConfig(testMapStore, 0);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, Integer> map = instance.getMap("default");

        Set expected = map.keySet();
        Set actual = mapForStore.keySet();
        assertEquals(expected, actual);

        List<Integer> actualList = new ArrayList<>(map.values());
        List<Integer> expectedList = new ArrayList<>(mapForStore.values());
        Collections.sort(actualList);
        Collections.sort(expectedList);

        assertEquals(expectedList, actualList);
        assertEquals(map.entrySet(), mapForStore.entrySet());

        assertFalse(map.containsKey(keyWithNullValue));
        assertNull(map.get(keyWithNullValue));
    }

    @Test(timeout = 120000)
    public void testIssue1115EnablingMapstoreMutatingValue() {
        Config config = getConfig();
        String mapName = "testIssue1115";
        MapStore mapStore = new ProcessingStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(mapStore);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);
        IMap<Integer, Employee> map = instance.getMap(mapName);
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

    /**
     * Test for issue https://github.com/hazelcast/hazelcast/issues/1110
     */
    @Test(timeout = 300000)
    public void testMapLoader_withMapLoadChunkSize() {
        final int chunkSize = 5;
        final int numberOfEntriesToLoad = 100;
        final String mapName = randomString();

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        ChunkedLoader chunkedLoader = new ChunkedLoader(numberOfEntriesToLoad, false);
        Config config = createChunkedMapLoaderConfig(mapName, chunkSize, chunkedLoader);

        HazelcastInstance node = nodeFactory.newHazelcastInstance(config);

        IMap map = node.getMap(mapName);

        final CountDownLatch latch = new CountDownLatch(numberOfEntriesToLoad);
        map.addEntryListener((EntryLoadedListener<Object, Object>) event -> latch.countDown(), true);
        // force creation of all partition record-stores.
        for (int i = 0; i < numberOfEntriesToLoad; i++) {
            map.get(i);
        }

        //await finish of map load.
        assertOpenEventually(latch, 240);

        final int expectedChunkCount = numberOfEntriesToLoad / chunkSize;
        final int actualChunkCount = chunkedLoader.numberOfChunks.get();
        assertEquals(expectedChunkCount, actualChunkCount);

        assertEquals(numberOfEntriesToLoad, map.size());
    }

    private Config createChunkedMapLoaderConfig(String mapName, int chunkSize, ChunkedLoader chunkedLoader) {
        Config cfg = getConfig();
        cfg.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        cfg.setProperty(ClusterProperty.MAP_LOAD_CHUNK_SIZE.getName(), String.valueOf(chunkSize));

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(chunkedLoader);

        MapConfig mapConfig = cfg.getMapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return cfg;
    }

    private static class ChunkedLoader extends SimpleMapLoader {

        private AtomicInteger numberOfChunks = new AtomicInteger(0);

        ChunkedLoader(int size, boolean slow) {
            super(size, slow);
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            numberOfChunks.incrementAndGet();
            return super.loadAll(keys);
        }
    }

    @Test(timeout = 120000)
    public void testIssue1142ExceptionWhenLoadAllReturnsNull() {
        Config config = getConfig();
        String mapName = "testIssue1142ExceptionWhenLoadAllReturnsNull";
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(new MapStoreAdapter<String, String>() {
            @Override
            public Set<String> loadAllKeys() {
                Set<String> keys = new HashSet<>();
                keys.add("key");
                return keys;
            }

            public Map<String, String> loadAll(Collection<String> keys) {
                return null;
            }
        });
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);
        final IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = 0; i < 300; i++) {
            map.put(i, i);
        }
        assertEquals(300, map.size());
    }

    @Test(timeout = 120000)
    public void testReadingConfiguration() {
        String mapName = "mapstore-test";
        InputStream is = getClass().getResourceAsStream("/com/hazelcast/config/hazelcast-mapstore-config.xml");
        XmlConfigBuilder builder = new XmlConfigBuilder(is);
        Config config = builder.build();
        HazelcastInstance hz = createHazelcastInstance(config);
        MapProxyImpl map = (MapProxyImpl) hz.getMap(mapName);
        MapService mapService = (MapService) map.getService();
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(mapName);
        MapStoreWrapper mapStoreWrapper = mapContainer.getMapStoreContext().getMapStoreWrapper();
        Iterator keys = mapStoreWrapper.loadAllKeys().iterator();

        final Set<String> loadedKeySet = loadedKeySet(keys);
        final Set<String> expectedKeySet = expectedKeySet();

        assertEquals(expectedKeySet, loadedKeySet);

        assertEquals("true", mapStoreWrapper.load("my-prop-1"));
        assertEquals("foo", mapStoreWrapper.load("my-prop-2"));
    }

    private Set<String> expectedKeySet() {
        final Set<String> expectedKeySet = new HashSet<>();
        expectedKeySet.add("my-prop-1");
        expectedKeySet.add("my-prop-2");
        return expectedKeySet;
    }

    private Set<String> loadedKeySet(Iterator keys) {
        final Set<String> keySet = new HashSet<>();
        while (keys != null && keys.hasNext()) {
            final String key = (String) keys.next();
            keySet.add(key);
        }
        return keySet;
    }

    @Test(timeout = 120000)
    public void testMapStoreNotCalledFromEntryProcessorBackup() {
        final String mapName = "testMapStoreNotCalledFromEntryProcessorBackup_" + randomString();
        final int instanceCount = 2;
        Config config = getConfig();
        // configure map with one backup and dummy map store
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setBackupCount(1);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        MapStoreWithStoreCount mapStore = new MapStoreWithStoreCount(1, 120);
        mapStoreConfig.setImplementation(mapStore);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(instanceCount);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        final IMap<String, String> map = instance.getMap(mapName);
        final String key = "key";
        final String value = "value";
        //executeOnKey
        map.executeOnKey(key, new ValueSetterEntryProcessor(value));
        mapStore.awaitStores();

        assertEquals(value, map.get(key));
        assertEquals(1, mapStore.getCount());
    }

    @Test(timeout = 120000)
    public void testMapStoreWriteRemoveOrder() {
        String mapName = randomMapName("testMapStoreWriteDeleteOrder");
        final SimpleMapStore store = new SimpleMapStore();
        Config config = newConfig(mapName, store, 3);
        HazelcastInstance hzInstance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = hzInstance.getMap(mapName);

        for (int key = 0; key < 10; key++) {
            map.put(key, key);
            sleepMillis(10);
            map.remove(key);
        }

        assertWriteBehindQueuesEmpty(mapName, singletonList(hzInstance));
        assertTrueEventually(() -> assertEquals(0, store.store.size()));
    }

    @Test
    public void testEntryProcessor_calls_load_only_one_time_per_key() {
        Config config = getConfig();
        // configure map with one backup and dummy map store
        MapConfig mapConfig = config.getMapConfig("default");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        MapStoreWithCounter mapStore = new MapStoreWithCounter();
        mapStoreConfig.setImplementation(mapStore);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        HazelcastInstance member = createHazelcastInstance(config);
        IMap<Integer, Integer> map = member.getMap("default");

        map.executeOnKey(1, new EntryProcessor<Integer, Integer, Object>() {

            @Override
            public Object process(Map.Entry<Integer, Integer> entry) {
                entry.setValue(2);
                return null;
            }

            @Override
            public EntryProcessor<Integer, Integer, Object> getBackupProcessor() {
                return null;
            }
        });

        assertEquals(1, mapStore.getLoadCount());
    }

    @Test
    public void testMapListener_containsOldValue_afterPutAll() {
        Config config = newConfig(new SimpleMapStore<Integer, Integer>());

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(randomName());

        // 1. first value is 1
        map.put(1, 1);

        final AtomicReference<Integer> oldValue = new AtomicReference<>();
        map.addEntryListener((EntryUpdatedListener<Integer, Integer>) event -> oldValue.set(event.getOldValue()), true);

        // 2. second value is 2
        HashMap<Integer, Integer> batch = new HashMap<>();
        batch.put(1, 2);

        map.putAll(batch);

        // expect oldValue equals 1
        assertTrueEventually(() -> {
            Integer value = oldValue.get();
            assertNotNull(value);
            assertEquals(1, value.intValue());
        });
    }

    @Test
    public void testMapListener_doesNotContainOldValue_afterSetAll() {
        Config config = newConfig(new SimpleMapStore<Integer, Integer>());

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(randomName());

        // 1. first value is 1
        map.put(1, 1);

        final AtomicReference<Integer> oldValue = new AtomicReference<>();
        map.addEntryListener((EntryUpdatedListener<Integer, Integer>) event -> oldValue.set(event.getOldValue()), true);

        // 2. second value is 2
        map.setAll(Collections.singletonMap(1, 2));

        // expect oldValue is null
        assertTrueEventually(() -> assertNull(oldValue.get()));
    }

    public Config newConfig(Object storeImpl) {
        return newConfig("default", storeImpl, 0, MapStoreConfig.InitialLoadMode.LAZY);
    }

    public static class WaitingOnFirstTestMapStore extends TestMapStore {
        private AtomicInteger count;

        public WaitingOnFirstTestMapStore() {
            this.count = new AtomicInteger(0);
        }

        @Override
        public void storeAll(Map map) {
            if (count.get() == 0) {
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

        final Map<Object, Object> store = new ConcurrentHashMap<>();

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
        private final AtomicLong lastStoreTimestamp = new AtomicLong();

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

        private void updateLastStoreTimestamp() {
            long timeNow = System.nanoTime();
            long currentLastStore = lastStoreTimestamp.get();
            if (timeNow > currentLastStore) {
                //try to update the timestamp. if we lose the CAS then not a big deal
                // -> some concurrent call managed to set it.
                lastStoreTimestamp.compareAndSet(currentLastStore, timeNow);
            }
        }

        public void store(Object key, Object value) {
            updateLastStoreTimestamp();
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
            if (!loadAllKeys) {
                return null;
            }
            return store.keySet();
        }

        public Object load(Object key) {
            callCount.incrementAndGet();
            latchLoad.countDown();
            return store.get(key);
        }

        public void storeAll(Map map) {
            updateLastStoreTimestamp();
            store.putAll(map);
            callCount.incrementAndGet();
            latchStoreAll.countDown();

            if (latchStoreAllOpCount != null) {
                for (int i = 0; i < map.size(); i++) {
                    latchStoreAllOpCount.countDown();
                }
            }
        }

        /**
         * @return monotonically increasing timestamp of last store() or storeAll() method calls.
         * Timestamp is obtained via {@link System#nanoTime()}
         */
        public long getLastStoreNanos() {
            return lastStoreTimestamp.get();
        }

        public void delete(Object key) {
            store.remove(key);
            callCount.incrementAndGet();
            latchDelete.countDown();
        }

        public Map loadAll(Collection keys) {
            Map<Object, Object> map = new HashMap<>(keys.size());
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

        public final ConcurrentMap<K, V> store;

        private boolean loadAllKeys = true;

        public SimpleMapStore() {
            store = new ConcurrentHashMap<>();
        }

        SimpleMapStore(final ConcurrentMap<K, V> store) {
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

    private static class ValueSetterEntryProcessor implements EntryProcessor<String, String, Object> {

        private final String value;

        ValueSetterEntryProcessor(String value) {
            this.value = value;
        }

        @Override
        public Object process(Map.Entry<String, String> entry) {
            entry.setValue(value);
            return null;
        }
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

    static class ProcessingStore extends MapStoreAdapter<Integer, Employee> implements PostProcessingMapStore {

        @Override
        public void store(Integer key, Employee employee) {
            employee.setSalary(employee.getAge() * 1000);
        }
    }

    public static class MapStoreWithStoreCount extends SimpleMapStore<Object, Object> {

        public final CountDownLatch latch;
        public final int waitSecond;
        public final AtomicInteger count = new AtomicInteger(0);
        public final int sleepStoreAllSeconds;

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
            assertOpenEventually(latch, waitSecond);
        }

        @Override
        public void store(Object key, Object value) {
            latch.countDown();
            super.store(key, value);
            count.incrementAndGet();
        }

        @Override
        public void storeAll(Map<Object, Object> map) {
            if (sleepStoreAllSeconds > 0) {
                try {
                    Thread.sleep(sleepStoreAllSeconds * 1000);
                } catch (InterruptedException e) {
                    ignore(e);
                }
            }
            for (Object ignored : map.keySet()) {
                latch.countDown();
                count.incrementAndGet();
            }
            super.storeAll(map);
        }

        public int getCount() {
            return count.get();
        }
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
                    return properties.getProperty(key);
                }

                @Override
                public Map<String, String> loadAll(Collection<String> keys) {
                    Map<String, String> map = new HashMap<>();
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
}
