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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindOnBackupsTest.writeBehindQueueSize;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindFlushTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehindQueues_flushed_onNodeShutdown() {
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

        assertTrueEventually(() -> {
            for (int i = 0; i < 1000; i++) {
                assertEquals(i, mapStore.store.get(i));
            }
        });
    }

    @Test
    public void testWriteBehindQueues_emptied_onBackupNodes() {
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
    public void testFlush_shouldNotCause_concurrentStoreOperation() {
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
    public void testFlushAsync_shouldNotBlockAndUltimatelySucceed() {
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

        assertThat(((MapProxyImpl<?, ?>) map).flushAsync())
                .as("should execute asynchronously").isNotDone()
                .as("should ultimately succeed").succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
    }

    @Test
    public void testFlushAsync_shouldFlushNull() throws Exception {
        testFlushAsync_shouldFlushSelectedPartitions(null);
    }

    @Test
    public void testFlushAsync_shouldFlushSelectedPartition() throws Exception {
        testFlushAsync_shouldFlushSelectedPartitions(Set.of(1));
    }

    @Test
    public void testFlushAsync_shouldFlushManyPartitions() throws Exception {
        testFlushAsync_shouldFlushSelectedPartitions(Set.of(1, 4, 9));
    }

    private void testFlushAsync_shouldFlushSelectedPartitions(@Nullable Set<Integer> partitionsToFlush) throws Exception {
        // given
        var store = new MapStoreWriteBehindTest.RecordingMapStore(Integer.MAX_VALUE, Integer.MAX_VALUE);

        // will rely on manual flushing
        Config config = newMapStoredConfig(store, 100000);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<String, String> map = instance.getMap("default");
        int numberOfPuts = 1000;
        for (int i = 0; i < numberOfPuts; i++) {
            map.put(i + "", i + "");
        }

        // when
        ((MapProxyImpl<?, ?>) map).flushAsync(partitionsToFlush).get();

        // then
        if (partitionsToFlush != null) {
            assertThat(store.getStore()).isNotEmpty();
            assertThat(store.getStore().keySet())
                    .as("Should flush only selected partitions")
                    .allMatch(key -> partitionsToFlush.contains(getPartitionId(instance, key)));
        } else {
            assertThat(store.getStore()).containsAllEntriesOf(map);
        }
    }

    @Test
    public void testWriteBehindQueues_flushed_uponEviction() {
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

        assertTrueEventually(() -> assertEquals(1000, mapStore.countStore.get()));
        assertWriteBehindQueuesEmpty(mapName, asList(node1, node2, node3));
    }

    @Test
    public void backupFlush_occursWithDelayAfterPrimaryFlush_ForceOffloadFalse() {
        backupFlush_occursWithDelayAfterPrimaryFlush("false");
    }

    @Test
    public void backupFlush_occursWithDelayAfterPrimaryFlush_ForceOffloadTrue() {
        backupFlush_occursWithDelayAfterPrimaryFlush("true");
    }

    public void backupFlush_occursWithDelayAfterPrimaryFlush(String forceOffload) {
        String mapName = "testMap";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        BlockingStoreMapStore<String, String> mapStore = new BlockingStoreMapStore<>();
        mapStoreConfig.setImplementation(mapStore).setWriteDelaySeconds(Integer.MAX_VALUE);

        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), "2");
        config.setProperty(MapServiceContext.FORCE_OFFLOAD_ALL_OPERATIONS.getName(), forceOffload);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);
        config.getMapConfig(mapName).setBackupCount(1);

        HazelcastInstance[] nodes = factory.newInstances(config, 2);
        HazelcastInstance node1 = nodes[0];
        HazelcastInstance node2 = nodes[1];
        assertClusterSizeEventually(2, nodes);

        IMap<String, String> map = node1.getMap(mapName);
        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put(generateKeyOwnedBy(node2), "value");
        }
        assertThat(mapStore.getStore().size()).isEqualTo(0);

        var flush = spawn(map::flush);
        assertThatCode(() -> flush.get(5, SECONDS)).isInstanceOf(java.util.concurrent.TimeoutException.class);

        node2.getLifecycleService().terminate();
        waitAllForSafeState(node1);
        assertClusterSizeEventually(1, node1);
        mapStore.releaseAll();

        assertThatCode(() -> flush.get(5, SECONDS)).doesNotThrowAnyException();
        map.flush();
        assertThat(mapStore.getStore().size()).isEqualTo(size);

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

    /**
     * MapStore implementation that blocks store operations on the external source
     * until {@code releaseAll()} is invoked.
     */
    private static class BlockingStoreMapStore<K, V> extends MapStoreAdapter<K, V> {

        final Map<Object, Object> store = new ConcurrentHashMap<>();
        final Semaphore storePermit = new Semaphore(0);

        @Override
        public void store(K key, V value) {
            try {
                storePermit.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<K, V> map) {
            try {
                storePermit.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            store.putAll(map);
        }

        public Map<Object, Object> getStore() {
            return store;
        }

        public void releaseAll() {
            storePermit.release(Integer.MAX_VALUE);
        }
    }
}
