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
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
    public void testLoadsAll_whenEvictionDisabled() throws Exception {
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
    public void testLoadsLessThanMaxSize_whenEvictionEnabled() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        assertFalse(map.isEmpty());
        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
        assertTrue(MAX_SIZE_PER_CLUSTER >= loader.getLoadedValueCount());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_AfterContainsKey_whenEvictionEnabled() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        for (int i = 0; i < MAP_STORE_ENTRY_COUNT; i++) {
            map.containsKey(i);
        }

        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_AfterGet_whenEvictionEnabled() throws Exception {
        final String mapName = randomMapName();
        Config cfg = newConfig(mapName, true, EAGER);

        IMap<Object, Object> map = getMap(mapName, cfg);

        for (int i = 0; i < MAP_STORE_ENTRY_COUNT; i++) {
            map.get(i);
        }

        assertTrue(MAX_SIZE_PER_CLUSTER >= map.size());
    }

    @Test(timeout = 2 * MINUTE)
    public void testLoadsLessThanMaxSize_whenEvictionEnabledAndReloaded() throws Exception {
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

    private void assertLoaderIsClosedEventually() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(loader.isLoadAllKeysClosed());
            }
        });
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
}
