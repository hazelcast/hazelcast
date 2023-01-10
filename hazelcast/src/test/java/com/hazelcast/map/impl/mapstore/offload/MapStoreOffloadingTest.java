/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.offload;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStoreOffloadingTest extends HazelcastTestSupport {

    @Override
    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    @Test
    public void offloaded_operations_are_retried_and_finish_when_partition_migrated() {
        // configure map
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");
        config.setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "1");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            @Override
            public Integer load(Integer key) {
                sleepSeconds(3);
                return -1;
            }
        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        // create cluster
        TestHazelcastFactory factory = new TestHazelcastFactory(2);
        factory.newHazelcastInstance(config);

        // create client
        HazelcastInstance client = factory.newHazelcastClient();
        IMap map = client.getMap(slowMapName);

        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(map.getAsync(i).toCompletableFuture());
        }

        // start new member to trigger migration
        factory.newHazelcastInstance(config);

        try {
            assertTrueEventually(() -> {
                for (Future future : futures) {
                    assertEquals(-1, future.get());
                }
            });
        } finally {
            factory.terminateAll();
        }
    }

    @Test
    public void store_waits_running_store() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "1");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            ConcurrentHashMap store = new ConcurrentHashMap<>();

            @Override
            public Integer load(Integer key) {
                return (Integer) store.get(key);
            }

            @Override
            public void store(Integer key, Integer value) {
                store.put(key, value);
            }

        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> slowMap = node.getMap(slowMapName);

        Future oldIsNull = slowMap.putAsync(1, 1).toCompletableFuture();
        Future oldIs1 = slowMap.putAsync(1, 2).toCompletableFuture();
        Future oldIs2 = slowMap.putAsync(1, 3).toCompletableFuture();
        Future oldIs3 = slowMap.putAsync(1, 4).toCompletableFuture();
        Future oldIs4 = slowMap.putAsync(1, 5).toCompletableFuture();

        assertTrueEventually(() -> {
            assertEquals(4, oldIs4.get());
            assertEquals(3, oldIs3.get());
            assertEquals(2, oldIs2.get());
            assertEquals(1, oldIs1.get());
            assertNull(oldIsNull.get());
        });
    }

    @Test
    public void load_waits_running_store() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "1");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            ConcurrentMap<Integer, Integer> store = new ConcurrentHashMap<>();

            @Override
            public Integer load(Integer key) {
                return store.get(key);
            }

            @Override
            public void store(Integer key, Integer value) {
                sleepMillis(500);
                store.put(key, value);
            }
        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> slowMap = node.getMap(slowMapName);

        slowMap.setAsync(1, 1);
        assertEquals(1, slowMap.get(1));
    }

    @Test
    public void slow_map_does_not_block_fast_map() {
        Config config = getConfig();

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            @Override
            public Integer load(Integer key) {
                sleepSeconds(1000);
                return 11;
            }
        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> slowMap = node.getMap(slowMapName);
        IMap<Object, Object> fastMap = node.getMap("fastMap");

        int keySetSize = 10_000;

        for (int i = 0; i < keySetSize; i++) {
            slowMap.getAsync(1);
        }

        for (int i = 0; i < keySetSize; i++) {
            fastMap.set(i, i);
        }

        assertTrueEventually(() -> assertEquals(keySetSize, fastMap.size()));
    }

    @Test
    public void setTtl_on_slow_map_store_does_not_block_other_map_operation() {
        Config config = getConfig();

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            @Override
            public Integer load(Integer key) {
                sleepSeconds(1000);
                return 11;
            }
        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> slowMapStoreMap = node.getMap(slowMapName);
        IMap<Object, Object> noMapStoreMap = node.getMap("noMapStoreMap");

        int keySetSize = 10_000;

        Runnable slowMapStoreMapRunnable = () -> {
            for (int i = 0; i < keySetSize; i++) {
                slowMapStoreMap.setTtl(i, 1, TimeUnit.SECONDS);
            }
        };

        Thread thread = new Thread(slowMapStoreMapRunnable);
        thread.start();

        for (int i = 0; i < keySetSize; i++) {
            noMapStoreMap.set(i, 1);
        }

        assertEquals(keySetSize, noMapStoreMap.size());
    }

    @Test
    public void setTtl_on_map_store_backed_map_returns_true_when_old_value_exists() {
        String mapName = "mapName";

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {
            @Override
            public Integer load(Integer key) {
                return (int) System.nanoTime();
            }
        });

        Config config = getConfig();
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(mapName);

        int keySetSize = 1_000;

        for (int i = 0; i < keySetSize; i++) {
            assertTrue(map.setTtl(i, 100, TimeUnit.SECONDS));
        }
    }

    @Test
    public void setTtl_on_map_store_backed_map_returns_false_when_no_old_value() {
        String mapName = "mapName";

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {
        });

        Config config = getConfig();
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(mapName);

        int keySetSize = 1_000;

        for (int i = 0; i < keySetSize; i++) {
            assertFalse(map.setTtl(i, 100, TimeUnit.SECONDS));
        }
    }
}
