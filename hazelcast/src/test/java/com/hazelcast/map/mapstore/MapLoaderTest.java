package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.map.mapstore.writebehind.TestMapUsingMapStoreBuilder;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapLoaderTest extends HazelcastTestSupport {

    //https://github.com/hazelcast/hazelcast/issues/1770
    @Test
    public void test1770() throws InterruptedException {
        Config config = new Config();
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl("http://127.0.0.1:8090/mancenter");

        MapConfig mapConfig = new MapConfig("foo");

        final AtomicBoolean loadAllCalled = new AtomicBoolean();
        MapLoader mapLoader = new MapLoader() {
            @Override
            public Object load(Object key) {
                return null;
            }

            @Override
            public Map loadAll(Collection keys) {
                loadAllCalled.set(true);
                return new HashMap();
            }

            @Override
            public Set loadAllKeys() {
                return new HashSet(Arrays.asList(1));
            }
        };
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(mapLoader);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        config.addMapConfig(mapConfig);
        HazelcastInstance hz = createHazelcastInstance(config);
        Map map = hz.getMap(mapConfig.getName());

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse("LoadAll should not have been called", loadAllCalled.get());
            }
        }, 10);
    }

    @Test
    public void testMapLoaderLoadUpdatingIndex() throws Exception {
        final int nodeCount = 3;
        String mapName = randomString();
        SampleIndexableObjectMapLoader loader = new SampleIndexableObjectMapLoader();

        Config config = createMapConfig(mapName, loader);

        NodeBuilder nodeBuilder = new NodeBuilder(nodeCount, config).build();
        HazelcastInstance node = nodeBuilder.getRandomNode();

        IMap<Integer, SampleIndexableObject> map = node.getMap(mapName);
        for (int i = 0; i < 10; i++) {
            map.put(i, new SampleIndexableObject("My-" + i, i));
        }

        final SqlPredicate predicate = new SqlPredicate("name='My-5'");

        assertPredicateResultCorrect(map, predicate);

        map.destroy();
        loader.preloadValues = true;

        node = nodeBuilder.getRandomNode();
        map = node.getMap(mapName);

        assertLoadAllKeysCount(loader, nodeCount);
        assertPredicateResultCorrect(map, predicate);
    }


    @Test
    public void testGetAll_putsLoadedItemsToIMap() throws Exception {
        Integer[] requestedKeys = {1, 2, 3};
        AtomicInteger loadedKeysCounter = new AtomicInteger(0);
        MapStore mapStore = createMapLoader(loadedKeysCounter);

        IMap map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withPartitionCount(1)
                .build();

        Set<Integer> keySet = new HashSet<Integer>(Arrays.asList(requestedKeys));

        map.getAll(keySet);
        map.getAll(keySet);
        map.getAll(keySet);

        assertEquals(requestedKeys.length, loadedKeysCounter.get());
    }

    private MapStore createMapLoader(final AtomicInteger loadAllCounter) {
        return new MapStoreAdapter<Integer, Integer>() {
            @Override
            public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
                loadAllCounter.addAndGet(keys.size());

                Map<Integer, Integer> map = new HashMap<Integer, Integer>();
                for (Integer key : keys) {
                    map.put(key, key);
                }
                return map;
            }

            @Override
            public Integer load(Integer key) {
                loadAllCounter.incrementAndGet();
                return super.load(key);
            }
        };
    }

    private Config createMapConfig(String mapName, SampleIndexableObjectMapLoader loader) {
        MapConfig mapConfig = new MapConfig(mapName);
        List<MapIndexConfig> indexConfigs = mapConfig.getMapIndexConfigs();
        indexConfigs.add(new MapIndexConfig("name", true));

        MapStoreConfig storeConfig = new MapStoreConfig();
        storeConfig.setFactoryImplementation(loader);
        storeConfig.setEnabled(true);
        mapConfig.setMapStoreConfig(storeConfig);

        final Config config = new Config();
        config.addMapConfig(mapConfig);
        return config;
    }

    private void assertLoadAllKeysCount(final SampleIndexableObjectMapLoader loader, final int instanceCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("call-count of loadAllKeys method is problematic", instanceCount, loader.loadAllKeysCallCount.get());
            }
        });
    }

    private void assertPredicateResultCorrect(final IMap<Integer, SampleIndexableObject> map, final SqlPredicate predicate) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final int mapSize = map.size();
                final String message = format("Map size is %d", mapSize);

                Set<Map.Entry<Integer, SampleIndexableObject>> result = map.entrySet(predicate);
                assertEquals(message, 1, result.size());
                assertEquals(message, 5, (int) result.iterator().next().getValue().value);
            }
        });
    }

    private class NodeBuilder {
        private final int nodeCount;
        private final Config config;
        private final Random random = new Random();
        private final TestHazelcastInstanceFactory factory;
        private HazelcastInstance[] nodes;

        public NodeBuilder(int nodeCount, Config config) {
            this.nodeCount = nodeCount;
            this.config = config;
            this.factory = createHazelcastInstanceFactory(nodeCount);
        }

        public NodeBuilder build() {
            nodes = factory.newInstances(config);
            return this;
        }

        public HazelcastInstance getRandomNode() {
            final int nodeIndex = random.nextInt(nodeCount);
            return nodes[nodeIndex];
        }
    }

    public static class SampleIndexableObjectMapLoader
            implements MapLoader<Integer, SampleIndexableObject>, MapStoreFactory<Integer, SampleIndexableObject> {

        private SampleIndexableObject[] values = new SampleIndexableObject[10];
        private Set<Integer> keys = new HashSet<Integer>();
        private AtomicInteger loadAllKeysCallCount = new AtomicInteger(0);

        volatile boolean preloadValues = false;

        public SampleIndexableObjectMapLoader() {
            for (int i = 0; i < 10; i++) {
                keys.add(i);
                values[i] = new SampleIndexableObject("My-" + i, i);
            }
        }

        @Override
        public SampleIndexableObject load(Integer key) {
            if (!preloadValues) return null;
            return values[key];
        }

        @Override
        public Map<Integer, SampleIndexableObject> loadAll(Collection<Integer> keys) {
            if (!preloadValues) return Collections.emptyMap();
            Map<Integer, SampleIndexableObject> data = new HashMap<Integer, SampleIndexableObject>();
            for (Integer key : keys) {
                data.put(key, values[key]);
            }
            return data;
        }

        @Override
        public Set<Integer> loadAllKeys() {
            if (!preloadValues) return Collections.emptySet();

            loadAllKeysCallCount.incrementAndGet();
            return Collections.unmodifiableSet(keys);
        }

        @Override
        public MapLoader<Integer, SampleIndexableObject> newMapStore(String mapName, Properties properties) {
            return this;
        }
    }

    public static class SampleIndexableObject implements Serializable {
        String name;
        Integer value;

        public SampleIndexableObject() {
        }

        public SampleIndexableObject(String name, Integer value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }
    }

}

