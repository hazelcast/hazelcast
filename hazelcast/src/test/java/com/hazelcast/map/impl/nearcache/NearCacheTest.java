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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getPartitionService;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCacheTest extends NearCacheTestSupport {

    @Parameterized.Parameter
    public boolean batchInvalidationEnabled;

    @Parameterized.Parameters(name = "batchInvalidationEnabled:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    @Test
    public void testBasicUsage() {
        int clusterSize = 3;
        int mapSize = 5000;
        final String mapName = "testBasicUsage";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);

        final HazelcastInstance[] instances = factory.newInstances(config);
        IMap<Integer, Integer> map = instances[0].getMap(mapName);

        populateMap(map, mapSize);

        for (HazelcastInstance instance : instances) {
            IMap<Integer, Integer> instanceMap = instance.getMap(mapName);
            for (int i = 0; i < mapSize; i++) {
                assertNotNull(instanceMap.get(i));
            }
        }

        for (int i = 0; i < mapSize; i++) {
            map.put(i, i * 2);
        }

        for (HazelcastInstance instance : instances) {
            IMap<Object, Object> m = instance.getMap(mapName);
            for (int i = 0; i < mapSize; i++) {
                assertNotNull(m.get(i));
            }
        }

        for (HazelcastInstance instance : instances) {
            int nearCacheSize = getNearCache(mapName, instance).size();
            assertTrue("Near Cache size should be > 0 but was " + nearCacheSize, nearCacheSize > 0);
        }

        map.clear();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                NearCache nearCache = getNearCache(mapName, instance);
                int size = nearCache.size();
                assertEquals("Near Cache size should be 0 but was " + size, 0, size);
            }
        });
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), valueOf(batchInvalidationEnabled));
        config.setProperty(NearCache.PROP_EXPIRATION_TASK_INITIAL_DELAY_SECONDS, "0");
        config.setProperty(NearCache.PROP_EXPIRATION_TASK_PERIOD_SECONDS, "1");
        return config;
    }

    @Test
    public void test_whenEmptyMap_thenPopulatedNearCacheShouldReturnNull_neverNULL_OBJECT() {
        int size = 10;
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig());

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        // populate map
        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = 0; i < size; i++) {
            // populate Near Cache
            assertNull(map.get(i));
            // fetch value from Near Cache
            assertNull(map.get(i));
        }
    }

    @Test
    public void test_whenCacheIsFullPutOnSameKeyShouldUpdateValue_withEvictionPolicyIsNONE() {
        int size = 10;
        int maxSize = 5;
        IMap<Integer, Integer> map = getMapConfiguredWithMaxSizeAndPolicy(EvictionPolicy.NONE, maxSize);

        populateMap(map, size);
        populateNearCache(map, size);

        assertEquals(maxSize, getNearCacheSize(map));
        assertEquals(1, map.get(1).intValue());

        map.put(1, 1502);

        assertEquals(1502, map.get(1).intValue());
        assertEquals(1502, map.get(1).intValue());
    }

    @Test
    public void testNearCacheEviction() {
        String mapName = "testNearCacheEviction";

        NearCacheConfig nearCacheConfig = newNearCacheConfigWithEntryCountEviction(EvictionPolicy.LRU, MAX_CACHE_SIZE);

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = hazelcastInstance.getMap(mapName);

        testNearCacheEviction(map, MAX_CACHE_SIZE);
    }

    @Test
    public void testNearCacheEviction_withMapClear() {
        final int size = 10;
        String mapName = "testNearCacheEvictionWithMapClear";

        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config);
        HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config);

        final IMap<Integer, Integer> map1 = hazelcastInstance1.getMap(mapName);
        final IMap<Integer, Integer> map2 = hazelcastInstance2.getMap(mapName);

        // populate map
        populateMap(map1, size);
        // populate Near Caches
        populateNearCache(map1, size);
        populateNearCache(map2, size);

        // clear map should trigger Near Cache eviction
        map1.clear();

        assertTrueEventually(() -> {
            // assert that the Near Cache doesn't return any cached values
            for (int i = 0; i < size; i++) {
                assertNull(map1.get(i));
                assertNull(map2.get(i));
            }
            assertEquals(0, getNearCacheStats(map1).getEvictions());
            assertEquals(0, getNearCacheStats(map2).getEvictions());
        });
    }

    @Test
    public void testNearCacheStats() {
        int mapSize = 1000;
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(false));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config);

        // populate map
        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        populateMap(map, mapSize);

        // populate Near Cache
        populateNearCache(map, mapSize);

        NearCacheStats stats = getNearCacheStats(map);
        long ownedEntryCount = stats.getOwnedEntryCount();
        long misses = stats.getMisses();
        assertTrue(format("Near Cache entry count should be > %d but were %d", 400, ownedEntryCount), ownedEntryCount > 400);
        assertEquals(format("Near Cache misses should be %d but were %d", mapSize, misses), mapSize, misses);

        // make some hits
        populateNearCache(map, mapSize);

        long hits = stats.getHits();
        misses = stats.getMisses();
        long hitsAndMisses = hits + misses;
        assertTrue(format("Near Cache hits should be > %d but were %d", 400, hits), hits > 400);
        assertTrue(format("Near Cache misses should be > %d but were %d", 400, misses), misses > 400);
        assertEquals(format("Near Cache hits + misses should be %s but were %d", mapSize * 2, hitsAndMisses), mapSize * 2,
                hitsAndMisses);
    }

    @Test
    public void testNearCacheMemoryCostCalculation() {
        testNearCacheMemoryCostCalculation(1);
    }

    @Test
    public void testNearCacheMemoryCostCalculation_withConcurrentCacheMisses() {
        testNearCacheMemoryCostCalculation(10);
    }

    private void testNearCacheMemoryCostCalculation(int threadCount) {
        String mapName = randomName();

        Config config = getConfig();
        NearCacheConfig nearCacheConfig = newNearCacheConfig().setCacheLocalEntries(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig.setInvalidateOnChange(false));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = hazelcastInstance.getMap(mapName);
        testNearCacheMemoryCostCalculation(map, true, threadCount);
    }

    @Test
    public void testNearCacheInvalidationByUsingMapPutAll() {
        int clusterSize = 3;
        int mapSize = 5000;
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);

        HazelcastInstance[] instances = factory.newInstances(config);
        final IMap<Integer, Integer> map = instances[0].getMap(mapName);

        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        // more-or-less (count / no_of_nodes) should be in the Near Cache now
        assertTrue(getNearCacheSize(map) > (mapSize / clusterSize - mapSize * 0.1));

        Map<Integer, Integer> invalidationMap = new HashMap<>(mapSize);
        populateMap(invalidationMap, mapSize);

        // this should invalidate the Near Cache
        map.putAll(invalidationMap);

        assertTrueEventually(
                () -> assertEquals("Invalidation is not working on putAll()", 0, getNearCacheSize(map))
        );
    }

    @Test
    public void testNearCacheInvalidationByUsingMapSetAll() {
        int clusterSize = 3;
        int mapSize = 5000;
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);

        HazelcastInstance[] instances = factory.newInstances(config);
        final IMap<Integer, Integer> map = instances[0].getMap(mapName);

        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        // more-or-less (count / no_of_nodes) should be in the Near Cache now
        assertTrue(getNearCacheSize(map) > (mapSize / clusterSize - mapSize * 0.1));

        Map<Integer, Integer> invalidationMap = new HashMap<>(mapSize);
        populateMap(invalidationMap, mapSize);

        // this should invalidate the Near Cache
        map.setAll(invalidationMap);

        assertTrueEventually(
            () -> assertEquals("Invalidation is not working on setAll()", 0, getNearCacheSize(map))
        );
    }

    @Test
    public void testMapContainsKey_withNearCache() {
        int clusterSize = 3;
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(true));
        HazelcastInstance instance = createHazelcastInstanceFactory(clusterSize).newInstances(config)[0];

        IMap<String, String> map = instance.getMap(mapName);
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
        String mapName = "testCacheLocalEntries";

        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setInvalidateOnChange(false);

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        HazelcastInstance[] instances = createHazelcastInstanceFactory(instanceCount).newInstances(config);

        IMap<String, String> map = instances[0].getMap(mapName);
        for (int i = 0; i < MAX_CACHE_SIZE; i++) {
            map.put("key" + i, "value" + i);
        }

        // warm-up cache
        for (int i = 0; i < MAX_CACHE_SIZE; i++) {
            map.get("key" + i);
        }

        int nearCacheSize = getNearCacheSize(map);
        assertEquals(format("Near Cache size should be %d but was %d", MAX_CACHE_SIZE, nearCacheSize),
                MAX_CACHE_SIZE, nearCacheSize);
    }

    // issue 1570
    @Test
    public void testNullValueNearCache() {
        int clusterSize = 2;
        String mapName = "testNullValueNearCache";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig());
        HazelcastInstance instance = createHazelcastInstanceFactory(clusterSize).newInstances(config)[0];

        IMap<String, String> map = instance.getMap(mapName);
        for (int i = 0; i < MAX_CACHE_SIZE; i++) {
            assertNull(map.get("key" + i));
        }

        for (int i = 0; i < MAX_CACHE_SIZE; i++) {
            assertNull(map.get("key" + i));
        }

        LocalMapStats stats = map.getLocalMapStats();
        assertTrue(
                format("Near Cache operation count should be < %d but was %d", MAX_CACHE_SIZE * 2, stats.getGetOperationCount()),
                stats.getGetOperationCount() < MAX_CACHE_SIZE * 2
        );
    }

    @Test
    public void testGetAll() {
        int mapSize = 1000;
        String mapName = "testGetAllWithNearCache";

        Config config = getConfig();
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(false);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = hazelcastInstanceFactory.newInstances(config);

        warmUpPartitions(instances);

        HazelcastInstance hazelcastInstance = instances[0];
        InternalPartitionService partitionService = getPartitionService(hazelcastInstance);

        // populate map
        IMap<Integer, Integer> map = hazelcastInstance.getMap(mapName);
        populateMap(map, mapSize);
        Set<Integer> keys = map.keySet();

        // populate Near Cache
        int expectedHits = 0;
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
            int partitionId = partitionService.getPartitionId(i);
            if (!partitionService.isPartitionOwner(partitionId)) {
                // map proxy stores non-owned entries (belong to partition which its not owner) inside its Near Cache
                expectedHits++;
            }
        }

        // generate Near Cache hits
        Map<Integer, Integer> allEntries = map.getAll(keys);
        for (int i = 0; i < mapSize; i++) {
            assertEquals(i, (int) allEntries.get(i));
        }

        // check Near Cache hits
        long hits = getNearCacheStats(map).getHits();
        assertEquals(format("Near Cache hits should be %d but were %d", expectedHits, hits), expectedHits, hits);
    }

    @Test
    public void testGetAsync() {
        int mapSize = 1000;
        int expectedHits = 400;
        String mapName = "testGetAsyncWithNearCache";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(false));

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        for (int i = 0; i < mapSize; i++) {
            map.getAsync(i);
        }

        long hits = getNearCacheStats(map).getHits();
        assertTrue(format("Near Cache hits should be > %d but were %d", expectedHits, hits), hits > expectedHits);
    }

    @Test
    public void testGetAsyncPopulatesNearCache() throws Exception {
        int mapSize = 1000;
        String mapName = "testGetAsyncPopulatesNearCache";

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig().setInvalidateOnChange(false));

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz = hazelcastInstanceFactory.newHazelcastInstance(config);
        hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = hz.getMap(mapName);
        populateMap(map, mapSize);

        // populate Near Cache
        for (int i = 0; i < mapSize; i++) {
            Future future = map.getAsync(i).toCompletableFuture();
            future.get();
        }
        // generate Near Cache hits
        populateNearCache(map, mapSize);

        long ownedEntryCount = getNearCacheStats(map).getOwnedEntryCount();
        assertTrue(format("Near Cache should be populated but current size is %d", ownedEntryCount), ownedEntryCount > 0);
    }

    @Test
    public void testAfterLoadAllWithDefinedKeysNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();

        Config config = createNearCachedMapConfigWithMapStoreConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        Set<Integer> keys = map.keySet();
        map.loadAll(keys, true);

        assertEquals(0, getNearCacheStats(map).getOwnedEntryCount());
    }

    @Test
    public void testAfterLoadAllNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();

        Config config = createNearCachedMapConfigWithMapStoreConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        map.loadAll(true);

        assertEquals(0, getNearCacheStats(map).getOwnedEntryCount());
    }

    @Test
    public void testAfterSubmitToKeyWithCallbackNearCacheIsInvalidated() throws Exception {
        int mapSize = 1000;
        String mapName = randomMapName();
        Random random = new Random();

        Config config = createNearCachedMapConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        final CountDownLatch latch = new CountDownLatch(10);
        int randomKey = random.nextInt(mapSize);
        map.submitToKey(randomKey,
                entry -> {
                    int currentValue = entry.getValue();
                    int newValue = currentValue + 1;
                    entry.setValue(newValue);
                    return newValue;
                })
            .thenRunAsync(latch::countDown);

        latch.await(3, TimeUnit.SECONDS);

        assertEquals(mapSize - 1, getNearCacheStats(map).getOwnedEntryCount());
    }

    @Test
    public void testAfterExecuteOnEntriesNearCacheIsInvalidated() {
        int mapSize = 10;
        String mapName = randomMapName();

        Config config = createNearCachedMapConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Employee> map = instance.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put(i, new Employee(i, "", 0, true, 0D));
        }

        populateNearCache(map, mapSize);

        EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = e.get("salary").equal(0);
        map.executeOnEntries(
                entry -> {
                    Employee employee = entry.getValue();
                    double currentSalary = employee.getSalary();
                    double newSalary = currentSalary + 10;
                    employee.setSalary(newSalary);
                    return newSalary;
                }, predicate);

        assertEquals(0, getNearCacheStats(map).getOwnedEntryCount());
    }

    @Test
    public void testNearCacheInvalidation_WithLFU_whenMaxSizeExceeded() {
        int mapSize = 2000;
        final int maxSize = 1000;
        final IMap<Integer, Integer> map = getMapConfiguredWithMaxSizeAndPolicy(EvictionPolicy.LFU, maxSize);

        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        assertTrueEventually(() -> {
            triggerNearCacheEviction(map);
            long ownedEntryCount = getNearCacheStats(map).getOwnedEntryCount();
            assertEquals("owned entry count " + ownedEntryCount, maxSize, ownedEntryCount);
        });
    }

    @Test
    public void testNearCacheInvalidation_WithLRU_whenMaxSizeExceeded() {
        int mapSize = 2000;
        final int maxSize = 1000;
        final IMap<Integer, Integer> map = getMapConfiguredWithMaxSizeAndPolicy(EvictionPolicy.LRU, maxSize);

        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        assertTrueEventually(() -> {
            triggerNearCacheEviction(map);
            long ownedEntryCount = getNearCacheStats(map).getOwnedEntryCount();
            assertEquals("owned entry count " + ownedEntryCount, maxSize, ownedEntryCount);
        });
    }

    @Test
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() {
        testNearCacheInvalidation_whenMaxSizeExceeded(EvictionPolicy.RANDOM);
    }

    @Test
    public void testNearCacheInvalidation_WitNone_whenMaxSizeExceeded() {
        testNearCacheInvalidation_whenMaxSizeExceeded(EvictionPolicy.NONE);
    }

    private void testNearCacheInvalidation_whenMaxSizeExceeded(EvictionPolicy evictionPolicy) {
        int mapSize = 2000;
        final int maxSize = 1000;
        final IMap<Integer, Integer> map = getMapConfiguredWithMaxSizeAndPolicy(evictionPolicy, maxSize);

        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        assertTrueEventually(() -> {
            long ownedEntryCount = getNearCacheStats(map).getOwnedEntryCount();
            assertEquals(maxSize, ownedEntryCount);
        });
    }

    private IMap<Integer, Integer> getMapConfiguredWithMaxSizeAndPolicy(EvictionPolicy evictionPolicy, int maxSize) {
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfigWithEntryCountEviction(evictionPolicy, maxSize));

        HazelcastInstance instance = createHazelcastInstance(config);

        return instance.getMap(mapName);
    }

    @Test
    public void testNearCacheGetAsyncTwice() throws Exception {
        String mapName = randomName();

        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        nearCacheConfig.getEvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(10);

        Config config = getConfig();
        config.addMapConfig(new MapConfig(mapName).setNearCacheConfig(nearCacheConfig));
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        map.getAsync(1).toCompletableFuture().get();

        sleepMillis(1000);

        assertNull(map.getAsync(1).toCompletableFuture().get());
    }

    /**
     * Near Cache has its own eviction/expiration mechanism, so eviction/expiration on an IMap
     * should not force any Near Cache eviction/expiration. Exceptions from this rule are direct calls to
     * <ul>
     * <li>{@link IMap#evict(Object)}</li>
     * <li>{@link IMap#evictAll()}</li>
     * </ul>
     */
    @Test
    public void testNearCacheEntriesNotExpired_afterIMapExpiration() {
        int mapSize = 3;
        String mapName = randomMapName();

        Config config = createNearCachedMapConfig(mapName);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(mapName);

        CountDownLatch latch = new CountDownLatch(mapSize);
        addEntryExpiredListener(map, latch);

        populateMapWithExpirableEntries(map, mapSize, 3, TimeUnit.SECONDS);
        populateNearCache(map, mapSize);

        int nearCacheSizeBeforeExpiration = getNearCacheSize(map);
        waitUntilEvictionEventsReceived(latch);
        // wait some extra time for possible events
        sleepSeconds(2);
        int nearCacheSizeAfterExpiration = getNearCacheSize(map);

        NearCacheStats stats = getNearCacheStats(map);
        assertEquals(0, stats.getExpirations());
        assertEquals(0, stats.getEvictions());
        assertEquals(nearCacheSizeBeforeExpiration, nearCacheSizeAfterExpiration);
    }

    @Test
    public void testMapEvictAll_clearsLocalNearCache() {
        int size = 1000;
        final String mapName = randomMapName();
        Config config = createNearCachedMapConfig(mapName);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        populateMap(map, size);
        populateNearCache(map, size);

        map.evictAll();

        assertTrueEventually(() -> {
            assertEquals(0, getNearCacheSize(instance1.getMap(mapName)));
            assertEquals(0, getNearCacheSize(instance2.getMap(mapName)));
            assertEquals(0, getNearCacheSize(instance3.getMap(mapName)));
        });
    }

    @Test
    public void testMapClear_clearsLocalNearCache() {
        int size = 1000;
        final String mapName = randomMapName();
        Config config = createNearCachedMapConfig(mapName);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance3 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        populateMap(map, size);
        populateNearCache(map, size);

        map.clear();

        assertTrueEventually(() -> {
            assertEquals(0, getNearCacheSize(instance1.getMap(mapName)));
            assertEquals(0, getNearCacheSize(instance2.getMap(mapName)));
            assertEquals(0, getNearCacheSize(instance3.getMap(mapName)));
        });
    }

    @Test
    public void testNearCacheTTLRecordsExpired() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig()
                .setTimeToLiveSeconds(MAX_TTL_SECONDS)
                .setInvalidateOnChange(false);

        testNearCacheExpiration(nearCacheConfig);
    }

    @Test
    public void testNearCacheMaxIdleRecordsExpired() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig()
                .setMaxIdleSeconds(MAX_IDLE_SECONDS)
                .setInvalidateOnChange(false);

        testNearCacheExpiration(nearCacheConfig);
    }

    private void testNearCacheExpiration(NearCacheConfig nearCacheConfig) {
        String mapName = randomMapName();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = getConfig();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> mapWithoutNearCache = instance1.getMap(mapName);
        populateMap(mapWithoutNearCache, MAX_CACHE_SIZE);

        Config configWithNearCache = getConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        configWithNearCache.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        HazelcastInstance instance2 = factory.newHazelcastInstance(configWithNearCache);

        IMap<Integer, Integer> nearCachedMap = instance2.getMap(mapName);
        populateNearCache(nearCachedMap, MAX_CACHE_SIZE);

        assertNearCacheExpiration(nearCachedMap, MAX_CACHE_SIZE);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException() {
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setNearCacheConfig(newNearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
        );

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        instance.getMap(mapName);
    }

    @Test
    public void multiple_get_on_non_existing_key_generates_one_miss() {
        String mapName = "test";

        Config config = getConfig();
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);

        config.getMapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap(mapName);

        map.get(1);
        map.get(1);
        map.get(1);

        NearCacheStats nearCacheStats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(1, nearCacheStats.getMisses());
    }

    @Test
    public void smoke_near_cache_population() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        String mapName = "test";
        int mapSize = 1000;

        // 1. create cluster
        Config config = getConfig();
        HazelcastInstance server1 = factory.newHazelcastInstance(config);
        HazelcastInstance server2 = factory.newHazelcastInstance(config);
        HazelcastInstance server3 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(3, server1, server2, server3);

        // 2. populate server side map
        IMap<Integer, Integer> nodeMap = server1.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            nodeMap.put(i, i);
        }

        // 3. add client with Near Cache
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setName(mapName);

        Config nearCachedConfig = getConfig();
        nearCachedConfig.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = factory.newHazelcastInstance(nearCachedConfig);

        // 4. populate Near Cache
        final IMap<Integer, Integer> nearCachedMap = client.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            assertNotNull(nearCachedMap.get(i));
        }

        // 5. assert number of entries in client Near Cache
        assertEquals(mapSize, ((NearCachedMapProxyImpl) nearCachedMap).getNearCache().size());
    }
}
