package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindMapStoreWithEvictionsTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehind_callEvictBeforePersisting() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = MapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();
        final int numberOfItems = 1000;
        populateMap(map, numberOfItems);
        evictMap(map, numberOfItems);

        assertFinalValueEqualsForEachEntry(map, numberOfItems);
    }

    @Test
    public void testWriteBehind_callEvictBeforePersisting_onSameKey() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = MapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withBackupCount(0)
                .withWriteDelaySeconds(3)
                .withPartitionCount(1)
                .build();
        final int numberOfUpdates = 1000;
        final int key = 0;
        continuouslyUpdateKey(map, numberOfUpdates, key);
        map.evict(0);
        final int expectedLastValue = numberOfUpdates - 1;

        assertFinalValueEquals(expectedLastValue, (Integer) map.get(0));
    }

    @Test
    public void testWriteBehind_callEvictBeforePersisting_onSameKey_thenCallRemove() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = MapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();
        final int numberOfUpdates = 1000;
        final int key = 0;
        continuouslyUpdateKey(map, numberOfUpdates, key);
        map.evict(0);
        final Object previousValue = map.remove(0);
        final int expectedLastValue = numberOfUpdates - 1;

        assertFinalValueEquals(expectedLastValue, (Integer) previousValue);
    }

    @Test
    public void testWriteBehind_callEvictBeforePersisting_onSameKey_thenCallRemoveMultipleTimes() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = MapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();
        final int numberOfUpdates = 1000;
        final int key = 0;
        continuouslyUpdateKey(map, numberOfUpdates, key);
        map.evict(0);
        map.remove(0);
        final Object previousValue = map.remove(0);

        assertNull(null, previousValue);
    }

    private void assertFinalValueEquals(final int expected, final int actual) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, actual);
            }
        }, 20);
    }


    private void populateMap(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }

    private void continuouslyUpdateKey(IMap map, int numberOfUpdates, int key) {
        for (int i = 0; i < numberOfUpdates; i++) {
            map.put(key, i);
        }
    }

    private void evictMap(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.evict(i);
        }
    }

    private void assertFinalValueEqualsForEachEntry(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            assertFinalValueEquals(i, (Integer) map.get(i));
        }
    }


    private static class MapUsingMapStoreBuilder<K, V> {

        private HazelcastInstance[] nodes;

        private int nodeCount;

        private int partitionCount = 271;

        private int backupCount = 1;

        private String mapName = randomMapName("default");

        private MapStore<K, V> mapStore;

        private int writeDelaySeconds = 0;

        private MapUsingMapStoreBuilder() {
        }

        public static <K, V> MapUsingMapStoreBuilder<K, V> create() {
            return new MapUsingMapStoreBuilder<K, V>();
        }


        public MapUsingMapStoreBuilder<K, V> mapName(String mapName) {
            if (mapName == null) {
                throw new IllegalArgumentException("mapName is null");
            }
            this.mapName = mapName;
            return this;
        }

        public MapUsingMapStoreBuilder<K, V> withNodeCount(int nodeCount) {
            if (nodeCount < 1) {
                throw new IllegalArgumentException("nodeCount < 1");
            }
            this.nodeCount = nodeCount;
            return this;
        }

        public MapUsingMapStoreBuilder<K, V> withPartitionCount(int partitionCount) {
            if (partitionCount < 1) {
                throw new IllegalArgumentException("partitionCount < 1");
            }
            this.partitionCount = partitionCount;
            return this;
        }

        public MapUsingMapStoreBuilder<K, V> withBackupCount(int backupCount) {
            if (backupCount < 0) {
                throw new IllegalArgumentException("backupCount < 1");
            }
            this.backupCount = backupCount;
            return this;
        }


        public MapUsingMapStoreBuilder<K, V> withMapStore(MapStore<K, V> mapStore) {
            this.mapStore = mapStore;
            return this;
        }

        public MapUsingMapStoreBuilder<K, V> withWriteDelaySeconds(int writeDelaySeconds) {
            if (writeDelaySeconds < 0) {
                throw new IllegalArgumentException("writeDelaySeconds < 0");
            }
            this.writeDelaySeconds = writeDelaySeconds;
            return this;
        }


        public IMap<K, V> build() {
            if (backupCount > nodeCount - 1) {
                throw new IllegalArgumentException("backupCount > nodeCount - 1");
            }
            final MapStoreConfig mapStoreConfig = new MapStoreConfig();
            mapStoreConfig.setImplementation(mapStore).setWriteDelaySeconds(writeDelaySeconds);

            final Config config = new Config();
            config.getMapConfig(mapName)
                    .setBackupCount(backupCount)
                    .setMapStoreConfig(mapStoreConfig);

            config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
            // nodes.
            final TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(nodeCount);
            nodes = instanceFactory.newInstances(config);
            return nodes[0].getMap(mapName);
        }
    }


    public static class MapStoreWithCounter<K, V> implements MapStore<K, V> {

        protected final Map<K, V> store = new ConcurrentHashMap();

        protected AtomicInteger countStore = new AtomicInteger(0);

        public MapStoreWithCounter() {
        }

        @Override
        public void store(K key, V value) {
            countStore.incrementAndGet();
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<K, V> map) {
            countStore.addAndGet(map.size());
            for (Map.Entry<K, V> kvp : map.entrySet()) {
                store.put(kvp.getKey(), kvp.getValue());
            }
        }

        @Override
        public void delete(K key) {

        }

        @Override
        public void deleteAll(Collection<K> keys) {

        }

        @Override
        public V load(K key) {
            return store.get(key);
        }

        @Override
        public Map<K, V> loadAll(Collection<K> keys) {
            return null;
        }

        @Override
        public Set<K> loadAllKeys() {
            return null;
        }
    }


}
