/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStoreEvictionTest extends HazelcastTestSupport {

    private static final int MAP_STORE_ENTRY_COUNT = 1000;
    private static final int NODE_COUNT = 2;
    private static final int MAX_SIZE_PER_NODE = MAP_STORE_ENTRY_COUNT / 4;
    private static final int MAX_SIZE_PER_CLUSTER = MAX_SIZE_PER_NODE * NODE_COUNT;

    private CountingMapLoader loader;
    private TestHazelcastInstanceFactory nodeFactory;

    @Before
    public void setUp() throws Exception {
        loader = new CountingMapLoader(MAP_STORE_ENTRY_COUNT) {
            @Override
            public Integer load(Integer key) {
                return key;
            }
        };
        nodeFactory = createHazelcastInstanceFactory(NODE_COUNT);
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsAll_whenEvictionDisabled() {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, false, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(MAP_STORE_ENTRY_COUNT, loader.getLoadedValueCount());
        assertLoaderIsClosedEventually();
    }

    private IMap<Object, Object> getMap(final String mapName, Config cfg) {
        HazelcastInstance hz = nodeFactory.newInstances(cfg)[0];
        assertClusterSizeEventually(NODE_COUNT, hz);
        IMap<Object, Object> map = hz.getMap(mapName);
        waitClusterForSafeState(hz);

        return map;
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_whenEvictionEnabled() {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertFalse(map.isEmpty());
        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
        assertTrue(MAX_SIZE_PER_CLUSTER >= loader.getLoadedValueCount());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_AfterContainsKey_whenEvictionEnabled() {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        for (int i = 0; i < MAP_STORE_ENTRY_COUNT; i++) {
            map.containsKey(i);
        }

        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_AfterGet_whenEvictionEnabled() {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        for (int i = 0; i < MAP_STORE_ENTRY_COUNT; i++) {
            map.get(i);
        }

        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_whenEvictionEnabledAndReloaded() {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);
        map.evictAll();
        loader.reset();
        map.loadAll(true);

        assertFalse("Map is not empty", map.isEmpty());
        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
        assertTrue(MAX_SIZE_PER_CLUSTER >= loader.getLoadedValueCount());
        assertLoaderIsClosedEventually();
    }

    @Test(timeout = 2 * MINUTE)
    public void testEvictionOnMapStoreLoad() {
        String mapName = randomMapName();
        int entryCount = 10;

        RecordingMapStore store = new RecordingMapStore();

        MapConfig mapConfig = new MapConfig(mapName)
                .setTimeToLiveSeconds(3);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(1);
        mapStoreConfig.setImplementation(store);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        IMap<Object, Object> map = getMap(mapName, config);

        // put some sample data
        for (int i = 0; i < entryCount; i++) {
            map.put("key" + i, "value" + i);
        }

        // wait until the data is stored and evicted
        assertTrueEventually(() -> assertEquals(entryCount, store.getTotalStoreCounts()));
        assertTrueEventually(() -> assertEquals(0, map.size()));
        assertEquals(entryCount, store.getTotalStoreCounts());

        // retrieve and check data
        for (int i = 0; i < 10; i++) {
            Object value = map.get("key" + i);
            assertEquals("value" + i, value);
        }

        // modify some data
        for (int i = 0; i < 6; i++) {
            map.put("key" + i, "newValue" + i);
        }

        // wait until the data is stored and evicted
        assertTrueEventually(() -> assertEquals(16, store.getTotalStoreCounts()));
        assertTrueEventually(() -> assertEquals(0, map.size()));
        assertEquals(16, store.getTotalStoreCounts());

        // check the modified values
        for (int i = 0; i < 6; i++) {
            Object value = map.get("key" + i);
            assertEquals("newValue" + i, value);
        }
    }

    private void assertLoaderIsClosedEventually() {
        assertTrueEventually(() -> assertTrue(loader.isLoadAllKeysClosed()));
    }

    private Config newConfig(String mapName, boolean sizeLimited, MapStoreConfig.InitialLoadMode loadMode) {
        Config cfg = new Config();
        cfg.setClusterName(getClass().getSimpleName());
        cfg.setProperty("hazelcast.partition.count", "5");

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(loader)
                .setInitialLoadMode(loadMode);

        MapConfig mapConfig = cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        if (sizeLimited) {
            EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
            evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
            evictionConfig.setMaxSizePolicy(MaxSizePolicy.PER_NODE);
            evictionConfig.setSize(MAX_SIZE_PER_NODE);
        }

        return cfg;
    }

    private static class RecordingMapStore implements MapStore<String, String> {

        private ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
        private ConcurrentHashMap<String, AtomicInteger> loadCounts = new ConcurrentHashMap<>();
        private ConcurrentHashMap<String, AtomicInteger> storeCounts = new ConcurrentHashMap<>();

        public ConcurrentHashMap<String, AtomicInteger> getLoadCounts() {
            return loadCounts;
        }

        @Override
        public String load(String key) {
            String result = store.get(key);
            incrementCount(loadCounts, key);
            return result;
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            List<String> keysList = new ArrayList<>(keys);
            Collections.sort(keysList);
            Map<String, String> result = new HashMap<>();
            for (String key : keys) {
                String value = store.get(key);
                incrementCount(loadCounts, key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }

        @Override
        public Set<String> loadAllKeys() {
            return new HashSet<>(store.keySet());
        }

        @Override
        public void store(String key, String value) {
            store.put(key, value);
            incrementCount(storeCounts, key);
        }

        @Override
        public void storeAll(Map<String, String> map) {
            store.putAll(map);
            for (String key : map.keySet()) {
                incrementCount(storeCounts, key);
            }
        }

        @Override
        public void delete(String key) {
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            for (String key : keys) {
                store.remove(key);
            }
        }

        private void incrementCount(ConcurrentHashMap<String, AtomicInteger> counts, String key) {
            AtomicInteger count = counts.get(key);
            if (count == null) {
                count = new AtomicInteger();
                AtomicInteger prev = counts.putIfAbsent(key, count);
                if (prev != null) {
                    count = prev;
                }
            }
            count.incrementAndGet();
        }

        private int getTotalCount(ConcurrentHashMap<String, AtomicInteger> counts) {
            int result = 0;
            for (AtomicInteger value : counts.values()) {
                result += value.get();
            }
            return result;
        }


        public int getTotalStoreCounts() {
            return getTotalCount(storeCounts);
        }
    }
}
