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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindOnBackupsTest.writeBehindQueueSize;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindFlushTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehindQueues_flushed_onNodeShutdown() throws Exception {
        int nodeCount = 3;
        String mapName = randomName();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        mapStoreConfig.setImplementation(mapStore).setWriteDelaySeconds(3000);

        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(0).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance member = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = member.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        factory.shutdownAll();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < 1000; i++) {
                    assertEquals(i, mapStore.store.get(i));
                }
            }
        });
    }

    @Test
    public void testWriteBehindQueues_emptied_onBackupNodes() throws Exception {
        int nodeCount = 3;
        String mapName = randomName();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        mapStoreConfig.setImplementation(mapStore).setWriteDelaySeconds(3000);

        Config config = getConfig();
        config.setProperty(ClusterProperty.MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS.getName(), "0");
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance member1 = factory.newHazelcastInstance(config);
        HazelcastInstance member2 = factory.newHazelcastInstance(config);
        HazelcastInstance member3 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = member1.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        map.flush();

        assertWriteBehindQueuesEmpty(mapName, asList(member1, member2, member3));
    }

    @Test
    public void testFlush_shouldNotCause_concurrentStoreOperation() throws Exception {
        int blockStoreOperationSeconds = 5;
        TemporaryBlockerMapStore store = new TemporaryBlockerMapStore(blockStoreOperationSeconds);

        Config config = newMapStoredConfig(store, 2000);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<String, String> map = instance.getMap("default");
        int numberOfPuts = 1000;
        for (int i = 0; i < numberOfPuts; i++) {
            map.put(i + "", i + "");
        }

        map.flush();

        assertEquals("Expecting " + numberOfPuts + " store after flush", numberOfPuts, store.getStoreOperationCount());
    }

    @Test
    public void testWriteBehindQueues_flushed_uponEviction() throws Exception {
        int nodeCount = 3;
        String mapName = randomName();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        mapStoreConfig.setImplementation(mapStore).setWriteDelaySeconds(3000);

        Config config = getConfig();
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);
        HazelcastInstance node3 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = node1.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 1000; i++) {
            map.evict(i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1000, mapStore.countStore.get());
            }
        });
        assertWriteBehindQueuesEmpty(mapName, asList(node1, node2, node3));
    }

    private Config newMapStoredConfig(MapStore store, int writeDelaySeconds) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapStoreConfig.setImplementation(store);

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setMapStoreConfig(mapStoreConfig);

        return config;
    }

    public static void assertWriteBehindQueuesEmpty(final String mapName, final List<HazelcastInstance> nodes) {
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : nodes) {
                assertEquals(0, writeBehindQueueSize(instance, mapName));
            }
        }, 240);
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }
}
