/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static org.junit.Assert.assertEquals;

/**
 * Tests that map loading works correctly when the node responsible for loading the map goes down before or after,
 * but not during, the load process.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapLoaderFailoverTest extends HazelcastTestSupport {

    @Rule
    public Timeout timeout = new Timeout(MINUTE, TimeUnit.MILLISECONDS);

    @Parameterized.Parameters(name = "coordinator failover:{0}, gracefully shutdown:{1}")
    public static List<Object[]> parameters() {
        return cartesianProduct(
            List.of(true, false),
            List.of(true, false)
        );
    }

    @Parameterized.Parameter
    public boolean coordinatorFailover;

    @Parameterized.Parameter(1)
    public boolean isGracefulShutdown;

    private static final int MAP_STORE_ENTRY_COUNT = 10000;
    private static final int BATCH_SIZE = 100;
    private static final int NODE_COUNT = 3;
    protected static final String MAP_NAME = "default";

    private TestHazelcastInstanceFactory nodeFactory;
    private CountingMapLoader mapLoader;

    @Before
    public void setUp() {
        assumeNotZing();
        nodeFactory = createHazelcastInstanceFactory(NODE_COUNT + 2);
        mapLoader = new CountingMapLoader(MAP_STORE_ENTRY_COUNT);
    }

    @Test
    public void testDoesNotLoadAgain_whenLoaderNodeGoesDown() {
        HazelcastInstance[] nodes
                = nodeFactory.newInstances(this::newConfig, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        assertSizeAndLoadCount(map);

        terminate(hz3, nodes[1]);
        waitAllForSafeState(nodeFactory.getAllHazelcastInstances());

        assertSizeAndLoadCount(map);
        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
    }

    @Test
    public void testLoads_whenInitialLoaderNodeRemoved() {
        HazelcastInstance[] nodes
                = nodeFactory.newInstances(this::newConfig, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);
        terminate(hz3, nodes[1]);

        // trigger loading
        map.size();

        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
        assertSizeAndLoadCount(map);
    }

    /** @see <a href="https://github.com/hazelcast/hazelcast/issues/6056">Fixes</a> */
    @Test
    public void testLoadsAll_whenInitialLoaderNodeRemovedAfterLoading() {
        HazelcastInstance[] nodes
                = nodeFactory.newInstances(this::newConfig, 3);
        HazelcastInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        assertSizeAndLoadCount(map);

        terminate(hz3, nodes[1]);
        assertClusterSizeEventually(2, nodes[0]);

        map.loadAll(true);

        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(2, mapLoader.getLoadAllKeysInvocations());
        assertEquals(2 * MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    private void terminate(HazelcastInstance coordinator, HazelcastInstance notCoordinator) {
        var shutdownNode = coordinatorFailover ? coordinator : notCoordinator;
        if (isGracefulShutdown) {
            shutdownNode.shutdown();
        } else {
            shutdownNode.getLifecycleService().terminate();
        }
    }

    private void assertSizeAndLoadCount(IMap<?, ?> map) {
        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    private Config newConfig() {
        return newConfig(1, mapLoader);
    }

    protected Config newConfig(int backups, MapLoader<?, ?> loader) {
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName(getClass().getSimpleName())
                .setProperty(ClusterProperty.MAP_LOAD_CHUNK_SIZE.getName(), Integer.toString(BATCH_SIZE))
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "13");

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setInitialLoadMode(LAZY)
                .setImplementation(loader);

        config.getMapConfig(MAP_NAME)
                .setBackupCount(backups)
                .setMapStoreConfig(mapStoreConfig);

        return config;
    }
}
