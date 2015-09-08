/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import java.util.concurrent.Future;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static com.hazelcast.test.TimeConstants.MINUTE;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapLoaderFailoverTest extends HazelcastTestSupport {

    private static final int MAP_STORE_ENTRY_COUNT = 10000;
    private static final int BATCH_SIZE = 100;
    private static final int NODE_COUNT = 3;

    private TestHazelcastInstanceFactory nodeFactory;
    private CountingMapLoader mapLoader;

    @Before
    public void setUp() throws Exception {
        nodeFactory = createHazelcastInstanceFactory(NODE_COUNT + 2);
        mapLoader = new CountingMapLoader(MAP_STORE_ENTRY_COUNT);
    }

    @Test(timeout = MINUTE)
    public void testDoesntLoadAgain_whenLoaderNodeGoesDown() throws Exception {
        Config cfg = newConfig("default", LAZY);
        HazelcastInstance[] nodes = nodeFactory.newInstances(cfg, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        map.size();
        assertSizeAndLoadCount(map);

        hz3.getLifecycleService().terminate();
        waitAllForSafeState(nodeFactory.getAllHazelcastInstances());

        assertSizeAndLoadCount(map);
        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
    }

    @Test(timeout = MINUTE)
    public void testLoads_whenInitialLoaderNodeRemoved() throws Exception {
        Config cfg = newConfig("default", LAZY);
        HazelcastInstance[] nodes = nodeFactory.newInstances(cfg, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);
        hz3.getLifecycleService().terminate();

        // trigger loading
        map.size();

        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
        assertSizeAndLoadCount(map);
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenInitialLoaderNodeRemovedAfterLoading() throws Exception {
        Config cfg = newConfig("default", LAZY);
        HazelcastInstance[] nodes = nodeFactory.newInstances(cfg, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        map.size();
        assertSizeAndLoadCount(map);

        hz3.getLifecycleService().terminate();
        assertClusterSizeEventually(2, nodes[0]);
        map.loadAll(true);

        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(2, mapLoader.getLoadAllKeysInvocations());
        assertEquals(2 * MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenInitialLoaderNodeRemovedWhileLoading() throws Exception {
        PausingMapLoader<Integer, Integer> pausingLoader = new PausingMapLoader<Integer, Integer>(mapLoader, 5000);

        Config cfg = newConfig("default", LAZY, 1, pausingLoader);
        HazelcastInstance[] nodes = nodeFactory.newInstances(cfg, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        // trigger loading and pause half way through
        Future<Object> asyncVal = map.getAsync(1);
        pausingLoader.awaitPause();

        hz3.getLifecycleService().terminate();
        assertClusterSizeEventually(2, nodes[0]);

        pausingLoader.resume();

        assertEquals(1, asyncVal.get());
        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertTrue(mapLoader.getLoadedValueCount() >= MAP_STORE_ENTRY_COUNT);
        assertEquals(2, mapLoader.getLoadAllKeysInvocations());
    }

    @Test(timeout = MINUTE)
    public void testLoadsAll_whenInitialLoaderNodeRemovedWhileLoadingAndNoBackups() throws Exception {
        PausingMapLoader<Integer, Integer> pausingLoader = new PausingMapLoader<Integer, Integer>(mapLoader, 5000);

        Config cfg = newConfig("default", LAZY, 0, pausingLoader);
        HazelcastInstance[] nodes = nodeFactory.newInstances(cfg, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        // trigger loading and pause half way through
        map.putAsync(1, 2);
        pausingLoader.awaitPause();

        hz3.getLifecycleService().terminate();
        waitAllForSafeState(nodeFactory.getAllHazelcastInstances());

        pausingLoader.resume();

        int size = map.size();
        assertEquals(MAP_STORE_ENTRY_COUNT, size);
        assertTrue(mapLoader.getLoadedValueCount() >= MAP_STORE_ENTRY_COUNT);
        assertEquals(2, mapLoader.getLoadAllKeysInvocations());
    }

    private void assertSizeAndLoadCount(IMap<Object, Object> map) {
        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    private Config newConfig(String mapName, MapStoreConfig.InitialLoadMode loadMode) {
        return newConfig(mapName, loadMode, 1, mapLoader);
    }

    private Config newConfig(String mapName, MapStoreConfig.InitialLoadMode loadMode, int backups, MapLoader loader) {
        Config cfg = new Config();
        cfg.setGroupConfig(new GroupConfig(getClass().getSimpleName()));
        cfg.setProperty(GroupProperties.PROP_MAP_LOAD_CHUNK_SIZE, Integer.toString(BATCH_SIZE));
        cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "31");

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setImplementation(loader).setInitialLoadMode(loadMode);

        cfg.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig).setBackupCount(backups);

        return cfg;
    }

}
