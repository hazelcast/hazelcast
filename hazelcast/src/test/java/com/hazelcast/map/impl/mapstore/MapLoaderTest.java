/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.impl.mapstore.writebehind.TestMapUsingMapStoreBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.TestCollectionUtils.setOfValuesBetween;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapLoaderTest extends HazelcastTestSupport {

    @Test
    public void testSenderAndBackupTerminates_AfterInitialLoad() throws InterruptedException {
        String name = randomString();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(name);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new DummyMapLoader());
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(5);
        HazelcastInstance[] instances = instanceFactory.newInstances(config);

        IMap<Object, Object> map = instances[0].getMap(name);
        map.clear();

        HazelcastInstance[] ownerAndReplicas = findOwnerAndReplicas(instances, name);
        ownerAndReplicas[0].getLifecycleService().terminate();
        ownerAndReplicas[1].getLifecycleService().terminate();
        assertClusterSizeEventually(3, ownerAndReplicas[3]);

        map = ownerAndReplicas[3].getMap(name);
        map.loadAll(false);
        assertEquals(DummyMapLoader.DEFAULT_SIZE, map.size());
    }

    private HazelcastInstance[] findOwnerAndReplicas(HazelcastInstance[] instances, String name) {
        Node node = getNode(instances[0]);
        InternalPartitionService partitionService = node.getPartitionService();
        int partitionId = partitionService.getPartitionId(name);
        InternalPartition partition = partitionService.getPartition(partitionId);
        HazelcastInstance[] ownerAndReplicas = new HazelcastInstance[instances.length];
        for (int i = 0; i < instances.length; i++) {
            ownerAndReplicas[i] = getInstanceForAddress(instances, partition.getReplicaAddress(i));
        }
        return ownerAndReplicas;
    }

    @Test
    public void givenSpecificKeysWereReloaded_whenLoadAllIsCalled_thenAllEntriesAreLoadedFromTheStore() {
        String name = randomString();
        int keysInMapStore = 10000;

        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(name);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new DummyMapLoader(keysInMapStore));
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = instanceFactory.newInstances(config);
        IMap<Integer, Integer> map = instances[0].getMap(name);

        //load specific keys
        map.loadAll(setOfValuesBetween(0, keysInMapStore), true);

        //remove everything
        map.clear();

        //assert loadAll with load all entries provided by the mapLoader
        map.loadAll(true);
        assertEquals(keysInMapStore, map.size());
    }

    private HazelcastInstance getInstanceForAddress(HazelcastInstance[] instances, Address address) {
        for (HazelcastInstance instance : instances) {
            Address instanceAddress = instance.getCluster().getLocalMember().getAddress();
            if (address.equals(instanceAddress)) {
                return instance;
            }
        }
        throw new IllegalArgumentException();
    }

    // https://github.com/hazelcast/hazelcast/issues/1770
    @Test
    public void test1770() throws InterruptedException {
        Config config = getConfig();
        config.getManagementCenterConfig().setEnabled(true);
        config.getManagementCenterConfig().setUrl("http://127.0.0.1:8090/mancenter");

        MapConfig mapConfig = config.getMapConfig("foo");

        final AtomicBoolean loadAllCalled = new AtomicBoolean();
        MapLoader<Object, Object> mapLoader = new MapLoader<Object, Object>() {
            @Override
            public Object load(Object key) {
                return null;
            }

            @Override
            public Map<Object, Object> loadAll(Collection keys) {
                loadAllCalled.set(true);
                return new HashMap<Object, Object>();
            }

            @Override
            public Set<Object> loadAllKeys() {
                return new HashSet<Object>(Collections.singletonList(1));
            }
        };
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(mapLoader);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        hz.getMap(mapConfig.getName());

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse("LoadAll should not have been called", loadAllCalled.get());
            }
        }, 10);
    }

    @Test
    public void testMapLoaderLoadUpdatingIndex_noPreload() throws Exception {
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

        SqlPredicate predicate = new SqlPredicate("name='My-5'");
        assertPredicateResultCorrect(map, predicate);
    }

    @Test
    public void testMapLoaderLoadUpdatingIndex_withPreload() throws Exception {
        final int nodeCount = 3;
        String mapName = randomString();
        SampleIndexableObjectMapLoader loader = new SampleIndexableObjectMapLoader();
        loader.preloadValues = true;

        Config config = createMapConfig(mapName, loader);
        NodeBuilder nodeBuilder = new NodeBuilder(nodeCount, config).build();
        HazelcastInstance node = nodeBuilder.getRandomNode();

        IMap<Integer, SampleIndexableObject> map = node.getMap(mapName);
        SqlPredicate predicate = new SqlPredicate("name='My-5'");

        assertLoadAllKeysCount(loader, 1);
        assertPredicateResultCorrect(map, predicate);
    }

    @Test
    public void testGetAll_putsLoadedItemsToIMap() throws Exception {
        Integer[] requestedKeys = {1, 2, 3};
        AtomicInteger loadedKeysCounter = new AtomicInteger(0);
        MapStore<Integer, Integer> mapStore = createMapLoader(loadedKeysCounter);

        IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
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

    @Test(timeout = MINUTE)
    public void testMapCanBeLoaded_whenLoadAllKeysThrowsExceptionFirstTime() throws InterruptedException {
        Config config = getConfig();
        MapLoader failingMapLoader = new FailingMapLoader();
        MapStoreConfig mapStoreConfig = new MapStoreConfig().setImplementation(failingMapLoader);
        MapConfig mapConfig = config.getMapConfig(getClass().getName()).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance[] hz = createHazelcastInstanceFactory(2).newInstances(config, 2);
        IMap map = hz[0].getMap(mapConfig.getName());

        Throwable exception = null;
        try {
            map.get(generateKeyNotOwnedBy(hz[0]));
        } catch (Throwable e) {
            exception = e;
        }
        assertNotNull("Exception wasn't propagated", exception);

        map.loadAll(true);
        assertEquals(1, map.size());
    }

    private MapStore<Integer, Integer> createMapLoader(final AtomicInteger loadAllCounter) {
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
        Config config = getConfig();

        MapConfig mapConfig = config.getMapConfig(mapName);
        List<MapIndexConfig> indexConfigs = mapConfig.getMapIndexConfigs();
        indexConfigs.add(new MapIndexConfig("name", true));

        MapStoreConfig storeConfig = new MapStoreConfig();
        storeConfig.setFactoryImplementation(loader);
        storeConfig.setEnabled(true);
        mapConfig.setMapStoreConfig(storeConfig);

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

    public static class DummyMapLoader implements MapLoader<Integer, Integer> {

        static final int DEFAULT_SIZE = 1000;

        final Map<Integer, Integer> map = new ConcurrentHashMap<Integer, Integer>(DEFAULT_SIZE);

        public DummyMapLoader() {
            this(DEFAULT_SIZE);
        }

        public DummyMapLoader(int size) {
            for (int i = 0; i < size; i++) {
                map.put(i, i);
            }
        }

        @Override
        public Integer load(Integer key) {
            return map.get(key);
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
            for (Integer key : keys) {
                hashMap.put(key, map.get(key));
            }
            return hashMap;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return map.keySet();
        }
    }

    public static class SampleIndexableObjectMapLoader
            implements MapLoader<Integer, SampleIndexableObject>, MapStoreFactory<Integer, SampleIndexableObject> {

        volatile boolean preloadValues = false;

        private SampleIndexableObject[] values = new SampleIndexableObject[10];
        private Set<Integer> keys = new HashSet<Integer>();
        private AtomicInteger loadAllKeysCallCount = new AtomicInteger(0);

        public SampleIndexableObjectMapLoader() {
            for (int i = 0; i < 10; i++) {
                keys.add(i);
                values[i] = new SampleIndexableObject("My-" + i, i);
            }
        }

        @Override
        public SampleIndexableObject load(Integer key) {
            if (!preloadValues) {
                return null;
            }
            return values[key];
        }

        @Override
        public Map<Integer, SampleIndexableObject> loadAll(Collection<Integer> keys) {
            if (!preloadValues) {
                return Collections.emptyMap();
            }
            Map<Integer, SampleIndexableObject> data = new HashMap<Integer, SampleIndexableObject>();
            for (Integer key : keys) {
                data.put(key, values[key]);
            }
            return data;
        }

        @Override
        public Set<Integer> loadAllKeys() {
            if (!preloadValues) {
                return Collections.emptySet();
            }

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

        SampleIndexableObject() {
        }

        SampleIndexableObject(String name, Integer value) {
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

    static class FailingMapLoader extends MapStoreAdapter {

        AtomicBoolean first = new AtomicBoolean(true);

        @Override
        public Set loadAllKeys() {
            if (first.compareAndSet(true, false)) {
                throw new IllegalStateException("Intentional exception");
            }
            return singleton("key");
        }

        @Override
        public Map loadAll(Collection keys) {
            return Collections.singletonMap("key", "value");
        }
    }

    private class NodeBuilder {

        private final int nodeCount;
        private final Config config;
        private final Random random = new Random();
        private final TestHazelcastInstanceFactory factory;
        private HazelcastInstance[] nodes;

        NodeBuilder(int nodeCount, Config config) {
            this.nodeCount = nodeCount;
            this.config = config;
            this.factory = createHazelcastInstanceFactory(nodeCount);
        }

        NodeBuilder build() {
            nodes = factory.newInstances(config);
            return this;
        }

        HazelcastInstance getRandomNode() {
            final int nodeIndex = random.nextInt(nodeCount);
            return nodes[nodeIndex];
        }
    }
}
