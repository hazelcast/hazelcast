/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean batchInvalidationEnabled;

    @Parameterized.Parameters(name = "batchInvalidationEnabled:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    @Test
    public void testBasicUsage() throws Exception {
        int n = 3;
        String mapName = "test";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);

        HazelcastInstance[] instances = factory.newInstances(config);
        IMap<Object, Object> map = instances[0].getMap(mapName);

        int count = 5000;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        for (HazelcastInstance instance : instances) {
            IMap<Object, Object> m = instance.getMap(mapName);
            for (int i = 0; i < count; i++) {
                Assert.assertNotNull(m.get(i));
            }
        }

        for (int i = 0; i < count; i++) {
            map.put(i, i * 2);
        }

        for (HazelcastInstance instance : instances) {
            IMap<Object, Object> m = instance.getMap(mapName);
            for (int i = 0; i < count; i++) {
                Assert.assertNotNull(m.get(i));
            }
        }

        for (HazelcastInstance instance : instances) {
            NearCache nearCache = getNearCache(mapName, instance);
            int size = nearCache.size();
            assertTrue("NearCache size should be > 0 but was " + size, size > 0);
        }

        map.clear();
        for (HazelcastInstance instance : instances) {
            NearCache nearCache = getNearCache(mapName, instance);
            int size = nearCache.size();
            assertEquals("NearCache size should be 0 but was " + size, 0, size);
        }
    }

    protected NearCacheConfig newNearCacheConfig() {
        return new NearCacheConfig();
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED, String.valueOf(batchInvalidationEnabled));
        return config;
    }

    @Test
    public void testNearCacheEvictionByUsingMapClear() throws InterruptedException {
        final Config cfg = getConfig();
        final String mapName = "testNearCacheEviction";
        final NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        cfg.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(cfg);
        final HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(cfg);
        final IMap map1 = hazelcastInstance1.getMap(mapName);
        final IMap map2 = hazelcastInstance2.getMap(mapName);
        final int size = 10;
        //populate map.
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map1.get(i);
            map2.get(i);
        }
        //clear map should trigger near cache eviction.
        map1.clear();
        for (int i = 0; i < size; i++) {
            assertNull(map1.get(i));
        }
    }

    @Test
    public void testNearCacheStats() throws Exception {
        int mapSize = 1000;
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(false));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config);

        // Populate map
        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }

        // Populate near cache
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertTrue(
                String.format("Near cache misses should be > %d but were %d", 400, stats.getOwnedEntryCount()),
                stats.getOwnedEntryCount() > 400
        );
        assertEquals(
                String.format("Near cache misses should be %d but were %d", mapSize, stats.getMisses()),
                mapSize,
                stats.getMisses()
        );

        //make some hits
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        stats = map.getLocalMapStats().getNearCacheStats();
        assertTrue(
                String.format("Near cache hits should be > %d but were %d", 400, stats.getHits()),
                stats.getHits() > 400
        );
        assertTrue(
                String.format("Near cache misses should be > %d but were %d", 400, stats.getMisses()),
                stats.getMisses() > 400
        );
        long hitsAndMisses = stats.getHits() + stats.getMisses();
        assertEquals(
                String.format("Near cache hits + misses should be %s but were %d", mapSize * 2, hitsAndMisses),
                mapSize * 2,
                hitsAndMisses
        );
    }

    @Test
    public void testHeapCostCalculationWhenConcurrentCacheMisses() {
        String mapName = randomName();
        Config config = getConfig();
        NearCacheConfig nearCacheConfig = newNearCacheConfig().setCacheLocalEntries(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig.setInvalidateOnChange(false));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config);

        final IMap<Integer, Integer> map = hazelcastInstance.getMap(mapName);

        int threadCount = 10;
        final int itemCount = 100;

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < itemCount; i++) {
            map.put(i, i);
        }

        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        Runnable task = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < itemCount; i++) {
                    map.get(i);
                }
                countDownLatch.countDown();
            }
        };

        for (int i = 0; i < threadCount; i++) {
            executorService.execute(task);
        }
        assertOpenEventually(countDownLatch);

        for (int i = 0; i < itemCount; i++) {
            map.remove(i);
        }

        assertEquals(0, map.getLocalMapStats().getHeapCost());
    }

    @Test
    public void testNearCacheInvalidationByUsingMapPutAll() {
        int n = 3;
        String mapName = "test";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);

        HazelcastInstance[] instances = factory.newInstances(config);
        IMap<Object, Object> map = instances[0].getMap(mapName);

        int count = 5000;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        //populate the near cache
        for (int i = 0; i < count; i++) {
            map.get(i);
        }

        final NearCache nearCache = getNearCache(mapName, instances[0]);
        assertTrue(nearCache.size() > (count / n - count * 0.1)); //more-or-less (count / no_of_nodes) should be in the near cache now

        Map<Object, Object> invalidationMap = new HashMap<Object, Object>(count);
        for (int i = 0; i < count; i++) {
            invalidationMap.put(i, i);
        }
        map.putAll(invalidationMap); //this should invalidate the near cache

        assertTrueEventually(
                new AssertTask() {
                    @Override
                    public void run() {
                        assertEquals("Invalidation is not working on putAll()", 0, nearCache.size());
                    }
                }
        );
    }

    @Test
    public void testMapContainsKey_withNearCache() {
        int n = 3;
        String mapName = "test";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(true));
        HazelcastInstance instance = createHazelcastInstanceFactory(n).newInstances(config)[0];

        IMap<String, String> map = instance.getMap("mapName");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        map.get("key1");
        map.get("key2");
        map.get("key3");
        assertTrue(map.containsKey("key1"));
        assertFalse(map.containsKey("key5"));

        map.remove("key1");
        assertFalse(map.containsKey("key5"));
        assertTrue(map.containsKey("key2"));
        assertFalse(map.containsKey("key1"));
    }

    @Test
    public void testCacheLocalEntries() {
        int instanceCount = 2;
        int mapSize = 100;
        String mapName = "testCacheLocalEntries";

        Config config = getConfig();
        final NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setInvalidateOnChange(false);
        final MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        HazelcastInstance[] instances = createHazelcastInstanceFactory(instanceCount).newInstances(config);

        IMap<String, String> map = instances[0].getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put("key" + i, "value" + i);
        }

        //warm-up cache
        for (int i = 0; i < mapSize; i++) {
            map.get("key" + i);
        }

        NearCache nearCache = getNearCache(mapName, instances[0]);
        assertEquals(
                String.format("Near cache size should be %d but was %d", mapSize, nearCache.size()),
                mapSize,
                nearCache.size()
        );
    }

    // issue 1570
    @Test
    public void testNullValueNearCache() {
        int n = 2;
        int mapSize = 100;
        String mapName = "testNullValueNearCache";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig());
        HazelcastInstance instance = createHazelcastInstanceFactory(n).newInstances(config)[0];

        IMap<String, String> map = instance.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            assertNull(map.get("key" + i));
        }

        for (int i = 0; i < mapSize; i++) {
            assertNull(map.get("key" + i));
        }

        assertTrue(
                String.format(
                        "NearCache operation count should be < %d but was %d",
                        mapSize * 2,
                        map.getLocalMapStats().getGetOperationCount()
                ),
                map.getLocalMapStats().getGetOperationCount() < mapSize * 2
        );
    }

    @Test
    public void testGetAll() throws Exception {
        int mapSize = 1000;
        int expectedNearCacheHits = 0;
        String mapName = "testGetAllWithNearCache";

        Config config = getConfig();
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(false);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = hazelcastInstanceFactory.newInstances(config);

        warmUpPartitions(instances);

        HazelcastInstance hazelcastInstance = instances[0];
        Node node = TestUtil.getNode(hazelcastInstance);
        InternalPartitionService partitionService = node.getNodeEngine().getPartitionService();

        // Populate map
        IMap<Integer, Integer> map = hazelcastInstance.getMap(mapName);
        HashSet<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
            keys.add(i);
        }

        // Populate near cache
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
            int partitionId = partitionService.getPartitionId(i);
            if (!partitionService.isPartitionOwner(partitionId)) {
                // Map proxy stores non-owned entries (belong to partition which its not owner) inside its near-cache
                expectedNearCacheHits++;
            }
        }

        // Generate near cache hits
        Map<Integer, Integer> allEntries = map.getAll(keys);
        for (int i = 0; i < mapSize; i++) {
            assertEquals(i, (int) allEntries.get(i));
        }

        // Check near cache hits
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(
                String.format("Near cache hits should be %d but were %d", expectedNearCacheHits, stats.getHits()),
                expectedNearCacheHits, stats.getHits());
    }

    @Test
    public void testGetAllIssue1863() throws Exception {
        int mapSize = 1000;
        int expectedNearCacheHits = 1000;
        String mapName = "testGetAllWithNearCacheIssue1863";

        Config config = getConfig();
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = hazelcastInstanceFactory.newInstances(config);

        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        HashSet<Integer> keys = new HashSet<Integer>();

        // Populate near cache with nulls (cache local entries mode on)
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
            keys.add(i);
        }

        // Generate near cache hits
        Map<Integer, Integer> allEntries = map.getAll(keys);
        assertEquals(0, allEntries.size());

        // Check near cache hits
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(
                String.format("Near cache hits should be %d but were %d", expectedNearCacheHits, stats.getHits()),
                expectedNearCacheHits,
                stats.getHits()
        );
    }

    @Test
    public void testGetAsync() throws Exception {
        int mapSize = 1000;
        int expectedNearCacheHits = 400;
        String mapName = "testGetAsyncWithNearCache";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(false));
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        HashSet keys = new HashSet();
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
            keys.add(i);
        }
        //populate near cache
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        for (int i = 0; i < mapSize; i++) {
            final Future<Integer> async = map.getAsync(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertTrue(
                String.format("Near cache hits should be > %d but were %d", expectedNearCacheHits, stats.getHits()),
                stats.getHits() > expectedNearCacheHits
        );
    }

    @Test
    public void testGetAsyncPopulatesNearCache() throws Exception {
        int mapSize = 1000;
        int expectedNearCacheOwnedEntryCount = 400;
        String mapName = "testGetAsyncPopulatesNearCache";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(false));
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);
        final IMap<Object, Object> map = instance1.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < mapSize; i++) {
            Future async = map.getAsync(i);
            async.get();
        }
        //generate near cache hits
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertTrue(
                String.format(
                        "Near cache owned entry count should be > %d but was %d",
                        expectedNearCacheOwnedEntryCount, stats.getOwnedEntryCount()
                ),
                stats.getOwnedEntryCount() > expectedNearCacheOwnedEntryCount
        );
    }

    @Test
    public void testGetAsyncIssue1863() throws Exception {
        int mapSize = 1000;
        int expectedNearCacheHits = 1000;
        String mapName = "testGetAsyncWithNearCacheIssue1863";

        Config config = getConfig();
        final NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        HashSet keys = new HashSet();
        //populate near cache -- cache local entries mode on
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
            keys.add(i);
        }

        for (int i = 0; i < mapSize; i++) {
            final Future<Integer> async = map.getAsync(i);
            assertNull(async.get());
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(
                String.format("Near cache hits should be %d but were %d", expectedNearCacheHits, stats.getHits()),
                expectedNearCacheHits,
                stats.getHits()
        );
    }

    @Test
    public void testAfterLoadAllWithDefinedKeysNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();

        Config config = createNearCachedMapConfigWithMapStoreConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        HashSet<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
            keys.add(i);
        }

        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        map.loadAll(keys, true);

        NearCacheStats nearCacheStats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(0, nearCacheStats.getOwnedEntryCount());
    }

    @Test
    public void testAfterLoadAllNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();

        Config config = createNearCachedMapConfigWithMapStoreConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        map.loadAll(true);

        NearCacheStats nearCacheStats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(0, nearCacheStats.getOwnedEntryCount());
    }

    @Test
    public void testAfterReplaceNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();

        Config config = createNearCachedMapConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        for (int i = 0; i < mapSize; i++) {
            map.replace(i, i, mapSize - 1 - i);
        }

        NearCacheStats nearCacheStats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(0, nearCacheStats.getOwnedEntryCount());
    }

    @Test
    public void testAfterSubmitToKeyWithCallbackNearCacheIsInvalidated() throws InterruptedException {
        int mapSize = 1000;
        String mapName = randomMapName();
        Random random = new Random();

        Config config = createNearCachedMapConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        final CountDownLatch latch = new CountDownLatch(10);
        ExecutionCallback<Integer> callback = new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };

        int randomKey = random.nextInt(mapSize);
        map.submitToKey(randomKey, new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                int currentValue = (Integer) entry.getValue();
                int newValue = currentValue + 1;
                entry.setValue(newValue);
                return newValue;
            }
        }, callback);

        latch.await(3, TimeUnit.SECONDS);
        NearCacheStats nearCacheStats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(mapSize - 1, nearCacheStats.getOwnedEntryCount());
    }

    @Test
    public void testAfterExecuteOnEntriesNearCacheIsInvalidated() {
        int mapSize = 10;
        String mapName = randomMapName();

        Config config = createNearCachedMapConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, SampleObjects.Employee> map = instance.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, new SampleObjects.Employee(i, "", 0, true, 0D));
        }

        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("salary").equal(0);
        map.executeOnEntries(new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                SampleObjects.Employee employee = (SampleObjects.Employee) entry.getValue();
                double currentSalary = employee.getSalary();
                double newSalary = currentSalary + 10;
                employee.setSalary(newSalary);
                return newSalary;
            }
        }, predicate);

        NearCacheStats nearCacheStats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(0, nearCacheStats.getOwnedEntryCount());
    }

    @Test
    public void testNearCacheInvalidation_WithLFU_whenMaxSizeExceeded() throws Exception {
        int mapSize = 2000;
        final int maxSize = 1000;
        final IMap map = getMapConfiguredWithMaxSizeAndPolicy("LFU", maxSize);

        populateMap(map, mapSize);
        pullEntriesToNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                long ownedEntryCount = stats.getOwnedEntryCount();
                triggerNearCacheEviction(map);
                assertTrue("owned entry count " + ownedEntryCount, maxSize > ownedEntryCount);
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_WithLRU_whenMaxSizeExceeded() throws Exception {
        int mapSize = 2000;
        final int maxSize = 1000;
        final IMap map = getMapConfiguredWithMaxSizeAndPolicy("LRU", maxSize);

        populateMap(map, mapSize);
        pullEntriesToNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                long ownedEntryCount = stats.getOwnedEntryCount();
                triggerNearCacheEviction(map);
                assertTrue("owned entry count " + ownedEntryCount, maxSize > ownedEntryCount);
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() throws Exception {
        int mapSize = 2000;
        final int maxSize = 1000;
        final IMap map = getMapConfiguredWithMaxSizeAndPolicy("RANDOM", maxSize);

        populateMap(map, mapSize);
        pullEntriesToNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                long ownedEntryCount = stats.getOwnedEntryCount();
                triggerNearCacheEviction(map);
                assertTrue("owned entry count " + ownedEntryCount, maxSize > ownedEntryCount);
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_WitNone_whenMaxSizeExceeded() throws Exception {
        int mapSize = 2000;
        final int maxSize = 1000;
        final IMap map = getMapConfiguredWithMaxSizeAndPolicy("NONE", maxSize);

        populateMap(map, mapSize);
        pullEntriesToNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                final long ownedEntryCount = stats.getOwnedEntryCount();
                assertEquals(maxSize, ownedEntryCount);
            }
        });
    }

    @Test
    public void testNearCacheGetAsyncTwice() throws ExecutionException, InterruptedException {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        String name = randomName();
        Config config = new Config();
        config.addMapConfig(new MapConfig(name).setNearCacheConfig(nearCacheConfig));
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(name);
        map.getAsync(1).get();
        Thread.sleep(1000);
        assertNull(map.getAsync(1).get());
    }

    /**
     * There is a time-window that an "is-near-cache-evictable" check may return false but
     * in reality near-cache size is bigger than the configured near-cache max-size, this can happen because eviction
     * process is offloaded to a different thread and there is no synchronization exist between the thread
     * that puts the entry to near-cache and the thread which sweeps the entries from near-cache.
     * This method continuously triggers the eviction to bring the near-cache size under the configured max-size.
     * Only needed for testing purposes.
     */
    protected void triggerNearCacheEviction(IMap map) {
        populateMap(map, 1);
        pullEntriesToNearCache(map, 1);
    }

    protected IMap getMapConfiguredWithMaxSizeAndPolicy(String evictionPolicy, int maxSize) {
        String mapName = randomMapName();

        Config config = getConfig();
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setEvictionPolicy(evictionPolicy);
        nearCacheConfig.setMaxSize(maxSize);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        return instance.getMap(mapName);
    }

    /**
     * Near-cache has its own eviction/expiration mechanism,
     * eviction/expiration on IMap should not force any near-cache eviction/expiration.
     * Except a direct call to {@link com.hazelcast.core.IMap#evict(Object)}
     * and {@link com.hazelcast.core.IMap#evictAll()}, those direct calls also evicts near-caches.
     */
    @Test
    public void testNearCacheEntriesNotExpired_afterIMapExpiration() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNearCachedMapConfig(mapName);
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IMap<Integer, Integer> map = instance.getMap(mapName);
        final int mapSize = 3;
        final CountDownLatch latch = new CountDownLatch(mapSize);

        addListener(map, latch);
        populateMapWithExpirableEntries(map, mapSize, 3, TimeUnit.SECONDS);
        pullEntriesToNearCache(map, mapSize);

        int nearCacheSizeBeforeExpiration = ((NearCachedMapProxyImpl) map).getNearCache().size();
        waitUntilEvictionEventsReceived(latch);
        // wait some extra time for possible events.
        sleepSeconds(2);

        int nearCacheSizeAfterExpiration = ((NearCachedMapProxyImpl) map).getNearCache().size();

        assertEquals(nearCacheSizeBeforeExpiration, nearCacheSizeAfterExpiration);
    }

    protected void waitUntilEvictionEventsReceived(CountDownLatch latch) {
        assertOpenEventually(latch);
    }

    protected void addListener(IMap<Integer, Integer> map, final CountDownLatch latch) {
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                latch.countDown();
            }
        });
    }

    protected void pullEntriesToNearCache(IMap<Integer, Integer> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }
    }

    protected void populateMapWithExpirableEntries(IMap<Integer, Integer> map, int mapSize, long ttl, TimeUnit timeunit) {
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i, ttl, timeunit);
        }
    }

    protected void populateMap(IMap<Integer, Integer> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }
    }

    protected void assertNearCacheSize(final int expectedSize, final String mapName, final Map map) {

        final AssertTask assertionTask = new AssertTask() {
            @Override
            public void run() throws Exception {
                final MapProxyImpl mapProxy = (MapProxyImpl) map;
                final MapService mapService = (MapService) mapProxy.getService();
                final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
                final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
                final NearCache nearCache = nearCacheProvider.getOrCreateNearCache(mapName);

                assertEquals(expectedSize, nearCache.size());
            }
        };

        assertTrueEventually(assertionTask);
    }

    protected Config createNearCachedMapConfig(String mapName) {
        final Config config = getConfig();

        final NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);

        final MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return config;
    }

    protected Config createNearCachedMapConfigWithMapStoreConfig(String mapName) {
        Config config = createNearCachedMapConfig(mapName);
        SimpleMapStore store = new SimpleMapStore();
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        return config;
    }

    protected NearCache getNearCache(String mapName, HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = TestUtil.getNode(instance).nodeEngine;
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);

        return service.getMapServiceContext().getNearCacheProvider().getOrCreateNearCache(mapName);
    }

    public static class SimpleMapStore<K, V> extends MapStoreAdapter<K, V> {
        public final Map<K, V> store;
        private boolean loadAllKeys = true;

        public SimpleMapStore() {
            store = new ConcurrentHashMap<K, V>();
        }

        public SimpleMapStore(final Map<K, V> store) {
            this.store = store;
        }

        @Override
        public void delete(final K key) {
            store.remove(key);
        }

        @Override
        public V load(final K key) {
            return store.get(key);
        }

        @Override
        public void store(final K key, final V value) {
            store.put(key, value);
        }

        public Set<K> loadAllKeys() {
            if (loadAllKeys) {
                return store.keySet();
            }
            return null;
        }

        public void setLoadAllKeys(boolean loadAllKeys) {
            this.loadAllKeys = loadAllKeys;
        }

        @Override
        public void storeAll(final Map<K, V> kvMap) {
            store.putAll(kvMap);
        }
    }
}
