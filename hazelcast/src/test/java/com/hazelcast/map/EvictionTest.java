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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.partition.Partition;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.PredicateBuilderImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ConfigureParallelRunnerWith;
import com.hazelcast.test.annotation.HeavilyMultiThreadedTestLimiter;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.config.EvictionPolicy.RANDOM;
import static com.hazelcast.map.EvictionMaxSizePolicyTest.setMockRuntimeMemoryInfoAccessor;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.query.SampleTestObjects.Employee;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@ConfigureParallelRunnerWith(HeavilyMultiThreadedTestLimiter.class)
public class EvictionTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean statisticsEnabled;
    @Parameterized.Parameter
    public boolean perEntryStatsEnabled;

    @Parameterized.Parameters(name = "statisticsEnabled:{0}, perEntryStatsEnabled:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true, true},
                {false, true},
                {true, false},
                {false, false},
        });
    }

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule
            = set(PROP_TASK_PERIOD_SECONDS, String.valueOf(1));

    boolean updateRecordAccessTime() {
        return true;
    }

    @Test
    public void testTTL_entryShouldNotBeReachableAfterTTL() {
        final IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 1, SECONDS);

        assertTrueEventually(() -> assertFalse(map.containsKey(1)));
    }

    @Test
    public void testMaxIdle_entryShouldNotBeReachableAfterMaxIdle() {
        final IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 0, SECONDS, 1, SECONDS);

        sleepAtLeastSeconds(1);
        assertTrueEventually(() -> assertFalse(map.containsKey(1)));
    }

    @Test
    public void testMaxIdle_backupEntryShouldNotBeReachableAfterMaxIdle() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance instanceA = factory.newHazelcastInstance(getConfig());
        final HazelcastInstance instanceB = factory.newHazelcastInstance(getConfig());

        final String keyOwnedByInstanceA = generateKeyOwnedBy(instanceA);

        instanceA.getMap("Test")
                .put(keyOwnedByInstanceA, "value0", 0, SECONDS, 3, SECONDS);

        assertTrueEventually(() -> {
            RecordStore owner = getRecordStore(instanceA, keyOwnedByInstanceA);
            RecordStore backup = getRecordStore(instanceB, keyOwnedByInstanceA);
            assertEquals(0, owner.size());
            assertEquals(0, backup.size());
        });
    }

    @Test
    public void testMaxIdle_backupRecordStore_mustBeExpirable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = getConfig()
                .setProperty(PROP_TASK_PERIOD_SECONDS, String.valueOf(MAX_VALUE));
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HazelcastInstance instanceB = factory.newHazelcastInstance(config);

        String keyOwnedByInstanceA = generateKeyOwnedBy(instance);

        IMap<String, String> map = instance.getMap("Test");
        map.put(keyOwnedByInstanceA, "value0", 0, SECONDS, 30, SECONDS);

        assertTrueEventually(() -> {
            RecordStore store = getRecordStore(instanceB, keyOwnedByInstanceA);
            assertNotNull(store);
            assertEquals(1, store.size());
            assertTrue(store.isExpirable());
        });
    }

    private static RecordStore getRecordStore(HazelcastInstance instanceB, String keyOwnedByInstanceA) {
        Partition partition = instanceB.getPartitionService().getPartition(keyOwnedByInstanceA);
        MapService service = getNodeEngineImpl(instanceB).getService(MapService.SERVICE_NAME);
        return service.getMapServiceContext()
                .getPartitionContainer(partition.getPartitionId())
                .getExistingRecordStore("Test");
    }

    @Test
    public void testTTL_zeroIsInfinity() {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 2, SECONDS);
        map.put(1, "value1", 0, SECONDS);

        assertTrue(map.containsKey(1));
    }

    @Test
    public void testMaxIdle_zeroIsInfinity() {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 0, SECONDS, 1, SECONDS);
        map.put(1, "value1", 0, SECONDS, 0, SECONDS);

        assertTrue(map.containsKey(1));
    }

    @Test
    public void testMaxIdle_readThroughOrderedIndex() {
        assumeTrue(statisticsEnabled);
        assumeTrue(updateRecordAccessTime());
        testMaxIdle_readThroughIndex(IndexType.SORTED);
    }

    @Test
    public void testMaxIdle_readThroughUnorderedIndex() {
        assumeTrue(statisticsEnabled);
        assumeTrue(updateRecordAccessTime());
        testMaxIdle_readThroughIndex(IndexType.HASH);
    }

    @Test
    public void testMaxIdle_readThroughBitmapIndex() {
        assumeTrue(statisticsEnabled);
        testMaxIdle_readThroughIndex(IndexType.BITMAP);
    }

    private void testMaxIdle_readThroughIndex(IndexType type) {
        String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig("default").setPerEntryStatsEnabled(true);
        // "disable" the cleaner task
        config.setProperty(PROP_TASK_PERIOD_SECONDS, Integer.toString(MAX_VALUE));

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Employee> map = node.getMap(mapName);
        map.addIndex(type, "city");

        int entryCount = 5;
        Map<Integer, Long> lastAccessTimes = new HashMap<>();
        for (int i = 0; i < entryCount; ++i) {
            String cityName = i % 2 == 0 ? "cityname" : null;

            Employee emp = new Employee(i, "name" + i, cityName, 0, true, i);
            map.put(i, emp, 0L, SECONDS, 60L, SECONDS);
            // we do get to set the last access time
            map.get(i);
            EntryView view = map.getEntryView(i);
            long lastAccessTime = view.getLastAccessTime();
            assertTrue(lastAccessTime > 0);
            lastAccessTimes.put(i, lastAccessTime);
        }

        sleepAtLeastSeconds(1);

        EntryObject entryObject = new PredicateBuilderImpl().getEntryObject();
        Predicate predicateCityNull = entryObject.get("city").isNull();
        Collection<Employee> valuesNullCity = map.values(predicateCityNull);
        Collection<Employee> valuesNotNullCity = map.values(Predicates.equal("city", "cityname"));
        assertEquals(entryCount, valuesNullCity.size() + valuesNotNullCity.size());
        // check that evaluating the predicate didn't update the last access time of the returned records
        for (int i = 0; i < entryCount; ++i) {
            EntryView view = map.getEntryView(i);
            assertNotNull(view);
            long lastAccessTime = view.getLastAccessTime();
            long prevLastAccessTime = lastAccessTimes.get(i);
            assertTrue("lastAccessTime=" + lastAccessTime + ", prevLastAccessTime=" + prevLastAccessTime,
                    lastAccessTime == prevLastAccessTime);
        }
    }

    /**
     * We are defining TTL as time being passed since creation time of an entry.
     */
    @Test
    public void testTTL_appliedFromLastUpdate() {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 1, SECONDS);
        map.put(1, "value1", 2, SECONDS);
        long sleepRef = currentTimeMillis();
        map.put(1, "value2", 300, SECONDS);
        sleepAtMostSeconds(sleepRef, 2);

        assertTrue(map.containsKey(1));
    }

    @Test
    @Category(SlowTest.class)
    public void testTTL_AfterNonTTLUpdate()
            throws ExecutionException, InterruptedException {
        final IMap<Integer, String> map = createSimpleMap();

        makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(map);

        // Prolong 1st round
        map.put(1, "value1");
        assertTtlExpirationCorrectness(map, Long.MAX_VALUE);

        // Prolong 2nd round
        makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(map);
        map.set(1, "value2");
        assertTtlExpirationCorrectness(map, Long.MAX_VALUE);

        // Prolong 3rd round
        makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(map);
        final HashMap<Integer, String> items = new HashMap<>();
        items.put(1, "value3");
        items.put(2, "value1");
        items.put(3, "value1");
        map.putAll(items);
        assertTtlExpirationCorrectness(map, Long.MAX_VALUE);

        // Prolong 4th round
        makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(map);
        map.putAsync(1, "value4").toCompletableFuture().get();
        assertTtlExpirationCorrectness(map, Long.MAX_VALUE);

        // Prolong 5th round
        makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(map);
        map.setAsync(1, "value5").toCompletableFuture().get();
        assertTtlExpirationCorrectness(map, Long.MAX_VALUE);

        // Prolong 6th round
        makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(map);
        map.tryPut(1, "value6", 5, TimeUnit.SECONDS);
        assertTtlExpirationCorrectness(map, Long.MAX_VALUE);

        // Prolong 7th round
        makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(map);
        map.replace(1, "value7");
        assertTtlExpirationCorrectness(map, Long.MAX_VALUE);

        // Prolong 8th round
        makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(map);
        map.replace(1, "value0", "value8");
        assertTtlExpirationCorrectness(map, Long.MAX_VALUE);
    }

    private void makeUpdateWithTTLAndSleepAndAssertTtlCorrectness(IMap<Integer, String> map) {
        long startRef = currentTimeMillis();
        map.put(1, "value0", 10, SECONDS);
        long endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);
    }

    private void sleepAndAssertTtlExpirationCorrectness(IMap<Integer, String> map, long expected, long startRef, long endRef) {
        sleepAtLeastSeconds(3);

        EntryView view = map.getEntryView(1);
        if (expected == Long.MAX_VALUE) {
            assertEquals(expected, view.getExpirationTime());
        } else {
            long actual = MILLISECONDS.toSeconds(view.getExpirationTime() - startRef);
            long delta = (1 + MILLISECONDS.toSeconds(endRef - startRef));
            assertEquals(expected, actual, delta);
        }
    }

    private void assertTtlExpirationCorrectness(IMap<Integer, String> map, long expected) {
        EntryView view = map.getEntryView(1);
        assertEquals(expected, view.getExpirationTime());
        assertTrue(map.containsKey(1));
    }

    @Test
    public void testGetEntryView_withTTL() {
        final IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value", 1, SECONDS);

        assertTrueEventually(() -> assertNull(map.getEntryView(1)));
    }

    @Test
    public void testIssue455ZeroTTLShouldPreventEviction() {
        MapConfig mapConfig = newMapConfig("testIssue455ZeroTTLShouldPreventEviction")
                .setNearCacheConfig(new NearCacheConfig());
        Config config = getConfig().setClusterName("testIssue455ZeroTTLShouldPreventEviction")
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance h = factory.newHazelcastInstance(config);
        IMap<String, String> map = h.getMap("testIssue455ZeroTTLShouldPreventEviction");
        map.put("key", "value", 1, SECONDS);
        map.put("key", "value2", 0, SECONDS);
        sleepAtLeastSeconds(2);
        assertEquals("value2", map.get("key"));
    }

    @Test
    public void testIssue585ZeroTTLShouldPreventEvictionWithSet() {
        MapConfig mapConfig = newMapConfig("testIssue585ZeroTTLShouldPreventEvictionWithSet")
                .setNearCacheConfig(new NearCacheConfig());
        Config config = getConfig().setClusterName("testIssue585ZeroTTLShouldPreventEvictionWithSet")
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance h = factory.newHazelcastInstance(config);
        IMap<String, String> map = h.getMap("testIssue585ZeroTTLShouldPreventEvictionWithSet");
        map.set("key", "value", 1, SECONDS);
        map.set("key", "value2", 0, SECONDS);
        sleepAtLeastSeconds(2);
        assertEquals("value2", map.get("key"));
    }

    @Test
    public void testIssue304EvictionDespitePut() {
        String mapName = "testIssue304EvictionDespitePut";
        MapConfig mapConfig = newMapConfig(mapName)
                .setMaxIdleSeconds(10);
        Config config = getConfig().addMapConfig(mapConfig);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        IMap<String, Long> map = hazelcastInstance.getMap(mapName);

        String key = "key";

        map.put(key, currentTimeMillis());
        long expirationTimeMillis1 = map.getEntryView(key).getExpirationTime();

        sleepAtLeastSeconds(1);
        map.put(key, currentTimeMillis());

        long expirationTimeMillis2 = map.getEntryView(key).getExpirationTime();

        assertTrue("before: " + expirationTimeMillis1 + ", after: " + expirationTimeMillis2,
                expirationTimeMillis2 - expirationTimeMillis1 >= 1000);
    }

    // current eviction check period is 1 second
    // about 30000 records can be put in one second, so the size should be adapted
    @Test
    public void testEvictionSpeedTest() throws InterruptedException {
        final int clusterSize = 3;
        final int size = 10000;
        final String mapName = "testEvictionSpeedTest";

        MapConfig mapConfig = newMapConfig(mapName);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(size);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);

        final IMap firstMap = instances[0].getMap(mapName);
        final CountDownLatch latch = new CountDownLatch(clusterSize);
        final AtomicBoolean success = new AtomicBoolean(true);
        new Thread(() -> {
            sleepAtLeastSeconds(1);
            while (latch.getCount() != 0) {
                int mapSize = firstMap.size();
                if (mapSize > (size * clusterSize + size * clusterSize * 10 / 100)) {
                    success.set(false);
                    break;
                }
                sleepAtLeastSeconds(1);
            }
        }).start();

        for (int i = 0; i < clusterSize; i++) {
            final IMap<String, Integer> map = instances[i].getMap(mapName);
            new Thread(() -> {
                for (int j = 0; j < size; j++) {
                    map.put(clusterSize + "-" + j, j);
                }
                latch.countDown();
            }).start();
        }

        assertTrue(latch.await(10, TimeUnit.MINUTES));
        assertTrue(success.get());
    }

    @Test
    public void testEvictionSpeedTestPerPartition() {
        final int clusterSize = 2;
        final int size = 100;
        final String mapName = "testEvictionSpeedTestPerPartition";

        MapConfig mapConfig = newMapConfig(mapName);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_PARTITION)
                .setSize(size);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);

        final IMap firstMap = instances[0].getMap(mapName);
        final int partitionCount = instances[0].getPartitionService().getPartitions().size();
        final CountDownLatch latch = new CountDownLatch(clusterSize);
        final AtomicBoolean error = new AtomicBoolean(false);
        new Thread(() -> {
            sleepAtLeastSeconds(1);
            while (latch.getCount() != 0) {
                if (firstMap.size() > (size * partitionCount * 1.2)) {
                    error.set(true);
                }
                sleepAtLeastSeconds(1);
            }
        }).start();

        for (int i = 0; i < clusterSize; i++) {
            final IMap<String, Integer> map = instances[i].getMap(mapName);
            new Thread(() -> {
                for (int j = 0; j < 10000; j++) {
                    map.put(clusterSize + "-" + j, j);
                }
                latch.countDown();
            }).start();
        }

        assertOpenEventually(latch);
        assertFalse("map was not evicted properly!", error.get());
    }

    @Test
    public void testEvictionPerPartition() {
        int clusterSize = 2;
        int size = 10;
        String mapName = "testEvictionPerPartition";

        MapConfig mapConfig = newMapConfig(mapName);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_PARTITION)
                .setSize(size);
        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1")
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);
        int partitionCount = instances[0].getPartitionService().getPartitions().size();
        int insertCount = size * partitionCount * 2;
        Map<Integer, Integer> map = instances[0].getMap(mapName);
        for (int i = 0; i < insertCount; i++) {
            map.put(i, i);
        }
        int mapSize = map.size();
        String message = format("mapSize : %d should be <= max-size : %d ", mapSize, size);
        assertTrue(message, mapSize <= size);
    }

    @Test
    public void testEvictionLRU_statisticsDisabled() {
        int clusterSize = 2;
        int size = 100000;
        String mapName = randomMapName("_testEvictionLRU_statisticsDisabled_");

        MapConfig mapConfig = newMapConfig(mapName)
                .setStatisticsEnabled(false);

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_PARTITION)
                .setSize(size);

        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1")
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);
        IMap<Object, Object> map = instances[0].getMap(mapName);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            if (i < size / 2) {
                map.get(i);
            }
        }
        // give some time to eviction thread run
        sleepAtLeastSeconds(3);

        int recentlyUsedEvicted = 0;
        for (int i = 0; i < size / 2; i++) {
            if (map.get(i) == null) {
                recentlyUsedEvicted++;
            }
        }
        assertEquals(0, recentlyUsedEvicted);
    }

    @Test
    public void testEvictionLFU() {
        testEvictionLFUInternal(false);
    }

    @Test
    public void testEvictionLFU_statisticsDisabled() {
        testEvictionLFUInternal(true);
    }

    /**
     * This test is only testing occurrence of LFU eviction.
     */
    private void testEvictionLFUInternal(boolean disableStats) {
        int mapMaxSize = 10000;
        String mapName = randomMapName();

        MapConfig mapConfig = newMapConfig(mapName)
                .setStatisticsEnabled(disableStats);

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(mapMaxSize);

        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);

        for (int i = 0; i < 2 * mapMaxSize; i++) {
            map.put(i, i);
        }

        int mapSize = map.size();
        assertTrue("Eviction did not work, map size " + mapSize + " should be smaller than allowed max size = " + mapMaxSize,
                mapSize < mapMaxSize);
    }

    @Test
    public void testEvictionLFU2() {
        int size = 10000;
        String mapName = randomMapName("testEvictionLFU2");

        MapConfig mapConfig = newMapConfig(mapName);

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(size);

        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1")
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            if (i < 100 || i >= size - 100) {
                map.get(i);
            }
        }
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 100; j++) {
                assertNotNull(map.get(j));
            }
            for (int j = size - 100; j < size; j++) {
                assertNotNull(map.get(j));
            }
        }
    }

    @Test(timeout = 5 * 60 * 1000)
    public void testMapRecordEviction() {
        String mapName = randomMapName();
        final int size = 100;

        final AtomicInteger entryEvictedEventCount = new AtomicInteger(0);
        EntryListenerConfig entryListenerConfig = new EntryListenerConfig()
                .setLocal(true)
                .setImplementation(new EntryAdapter() {
                    public void entryExpired(EntryEvent event) {
                        entryEvictedEventCount.incrementAndGet();
                    }
                });

        MapConfig mapConfig = newMapConfig(mapName)
                .setTimeToLiveSeconds(1)
                .addEntryListenerConfig(entryListenerConfig);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        // wait until eviction is complete
        assertSizeEventually(0, map);
        assertTrueEventually(() -> assertEquals(size, entryEvictedEventCount.get()));
    }

    @Test
    public void testMapRecordIdleEviction() {
        String mapName = randomMapName("testMapRecordIdleEviction");
        int maxIdleSeconds = 1;
        int size = 100;

        MapConfig mapConfig = newMapConfig(mapName)
                .setMaxIdleSeconds(maxIdleSeconds);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(mapName);

        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        assertSizeEventually(0, map);
    }

    @Test
    public void testZeroResetsTTL() throws Exception {
        MapConfig mapConfig = newMapConfig("testZeroResetsTTL")
                .setTimeToLiveSeconds(5);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap<Object, Object> map = instance.getMap("testZeroResetsTTL");
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, false);

        map.put(1, 1);
        map.put(2, 2);
        map.put(1, 2, 0, SECONDS);

        latch.await(10, SECONDS);
        assertTrueEventually(() -> {
            assertNull(map.get(2));
            assertEquals(2, map.get(1));
        });
    }

    @Test
    @Category(NightlyTest.class)
    public void expired_entries_removed_after_migration() {
        int numOfEntries = 10;
        String name = "expired_entries_removed_after_migration";

        MapConfig mapConfig = newMapConfig(name)
                .setMaxIdleSeconds(20);
        Config config = getConfig()
                .setProperty(PROP_TASK_PERIOD_SECONDS, "1")
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2")
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = node1.getMap(name);

        final CountDownLatch latch = new CountDownLatch(numOfEntries);
        map.addEntryListener(new EntryAdapter() {
            @Override
            public void entryExpired(EntryEvent event) {
                latch.countDown();
            }
        }, false);

        for (int i = 0; i < numOfEntries; ++i) {
            map.put(i, i);
        }

        // data migration will be done to new node
        factory.newHazelcastInstance(config);

        assertOpenEventually(latch);
        assertSizeEventually(0, map);
    }

    /**
     * Background task {@link com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask}
     * should sweep expired records eventually.
     */
    @Test
    @Category(NightlyTest.class)
    public void testMapPutTTLWithListener() {
        int putCount = 100;
        IMap<Integer, Integer> map = createSimpleMap();

        final CountDownLatch latch = new CountDownLatch(putCount);
        map.addEntryListener(new EntryAdapter() {
            @Override
            public void entryExpired(final EntryEvent event) {
                latch.countDown();
            }
        }, true);

        int ttl = (int) (Math.random() * 5000);
        for (int j = 0; j < putCount; j++) {
            map.put(j, j, ttl, TimeUnit.MILLISECONDS);
        }

        // wait until eviction is complete
        assertOpenEventually(latch, 100);
    }

    /**
     * Test for issue 614
     */
    @Test
    public void testContainsKeyShouldDelayEviction() {
        assumeTrue(statisticsEnabled);

        String mapName = randomMapName();
        int waitSeconds = 2;

        MapConfig mapConfig = newMapConfig(mapName)
                .setMaxIdleSeconds(30).setPerEntryStatsEnabled(true);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap(mapName);
        map.put(1, 1);

        sleepAtLeastSeconds(waitSeconds);

        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long lastAccessTime = entryView.getLastAccessTime();

        // 1. shift lastAccessTime
        assertTrue(map.containsKey(1));

        entryView = map.getEntryView(1);
        long lastAccessTimeAfterContainsOperation = entryView.getLastAccessTime();

        // 2. expecting lastAccessTime to be shifted by containsKey operation
        long diffSecs = TimeUnit.MILLISECONDS.toSeconds(lastAccessTimeAfterContainsOperation - lastAccessTime);

        // 3. so there should be a diff at least waitSeconds
        String failureMessage = format("Diff seconds %d, wait seconds %d", diffSecs, waitSeconds);
        assertTrue(failureMessage, diffSecs >= waitSeconds);
    }

    @Test
    public void testIssue1085EvictionBackup() {
        String mapName = randomMapName();
        int entryCount = 10;

        MapConfig mapConfig = newMapConfig(mapName)
                .setTimeToLiveSeconds(3);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(config);
        IMap<Integer, Integer> map = instances[0].getMap(mapName);

        final CountDownLatch latch = new CountDownLatch(entryCount);
        map.addEntryListener((EntryExpiredListener<Integer, Integer>) event -> latch.countDown(), false);

        // put some sample data
        for (int i = 0; i < entryCount; i++) {
            map.put(i, i);
        }
        // wait until eviction is complete
        assertOpenEventually(latch);
        assertSizeEventually(0, map);
        assertHeapCostsZeroEventually(mapName, instances);
    }

    private void assertHeapCostsZeroEventually(final String mapName, final HazelcastInstance... nodes) {
        assertTrueEventually(() -> {
            for (HazelcastInstance node : nodes) {
                long heapCostOfNode = node.getMap(mapName).getLocalMapStats().getHeapCost();
                assertEquals(0L, heapCostOfNode);
            }
        });
    }

    /**
     * Test for the issue 537.
     * Eviction event is fired for an object already removed
     */
    @Test
    public void testEvictionAfterRemove() {
        IMap<Object, Object> map = createSimpleMap();

        final AtomicInteger count = new AtomicInteger(0);
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
                count.incrementAndGet();
            }
        }, true);

        // TTL is 2 seconds
        map.put(1, 1, 2, SECONDS);

        final int expected = (map.remove(1) == null ? 1 : 0);
        assertTrueEventually(() -> assertEquals(expected, count.get()));
    }

    @Test
    public void testEvictionPerNode_sweepsBackupPartitions() {
        // cluster size should be at least 2 since we are testing a scenario with backups
        int clusterSize = 2;
        int maxSize = 1000;
        String mapName = randomMapName();

        Config config = newConfig(mapName, maxSize, MaxSizePolicy.PER_NODE);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);

        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        // over fill map with (10 * maxSize) items
        for (int i = 0; i < 1; i++) {
            map.put(i, i);
        }

        assertBackupsSweptOnAllNodes(mapName, maxSize, instances);
    }

    @Test
    public void testEviction_increasingEntrySize() {
        int maxSizeMB = 50;
        String mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(ClusterProperty.MAP_EVICTION_BATCH_SIZE.getName(), "2");

        MapConfig mapConfig = config.getMapConfig(mapName);
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setComparator((o1, o2) -> 0)
                .setMaxSizePolicy(MaxSizePolicy.USED_HEAP_SIZE)
                .setSize(maxSizeMB);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, byte[]> map = instance.getMap(mapName);

        int perIterationIncrementBytes = 2048;
        long maxObservedHeapCost = 0;
        for (int i = 0; i < 1000; i++) {
            int payloadSizeBytes = i * perIterationIncrementBytes;
            map.put(i, new byte[payloadSizeBytes]);
            maxObservedHeapCost = max(maxObservedHeapCost, map.getLocalMapStats().getHeapCost());
        }

        double toleranceFactor = 1.1d;
        long maxAllowedHeapCost = (long) (MemoryUnit.MEGABYTES.toBytes(maxSizeMB) * toleranceFactor);
        long minAllowedHeapCost = (long) (MemoryUnit.MEGABYTES.toBytes(maxSizeMB) / toleranceFactor);

        assertBetween("Maximum cost", maxObservedHeapCost, minAllowedHeapCost, maxAllowedHeapCost);
    }

    private void assertBackupsSweptOnAllNodes(String mapName, int maxSize, HazelcastInstance[] instances) {
        for (HazelcastInstance instance : instances) {
            IMap<Integer, Integer> map = instance.getMap(mapName);

            long backupEntryCount = map.getLocalMapStats().getBackupEntryCount();
            long ownedEntryCount = map.getLocalMapStats().getOwnedEntryCount();

            // entry count = (owned + backup)
            // on one node, entry count should be smaller than (2 * maxSize)
            assertTrue(2 * maxSize > ownedEntryCount + backupEntryCount);
        }
    }

    /**
     * Test for the issue 2659.
     * Eviction event is fired for an object already removed.
     */
    @Test
    public void testEvictionForNanosTTL() {
        final IMap<String, String> map = createSimpleMap();
        map.put("foo", "bar", 1, TimeUnit.NANOSECONDS);

        assertTrueEventually(() -> assertNull(map.get("foo")), 30);
    }

    @Test
    public void testOnExpiredKeys_getAll() {
        IMap<Integer, Integer> map = getMapPopulatedWithExpirableKeys();
        Set<Integer> keys = Collections.singleton(1);
        assertTrueEventually(() -> assertEquals(0, map.getAll(keys).size()));
    }

    @Test
    public void testOnExpiredKeys_values() {
        IMap<Integer, Integer> map = getMapPopulatedWithExpirableKeys();
        assertTrueEventually(() -> assertEquals(0, map.values().size()));
    }

    @Test
    public void testOnExpiredKeys_keySet() {
        IMap<Integer, Integer> map = getMapPopulatedWithExpirableKeys();
        assertTrueEventually(() -> assertEquals(0, map.keySet().size()));
    }

    @Test
    public void testOnExpiredKeys_entrySet() {
        IMap<Integer, Integer> map = getMapPopulatedWithExpirableKeys();
        assertTrueEventually(() -> assertEquals(0, map.entrySet().size()));

    }

    @Test
    public void test_get_expiration_from_EntryView() {
        long now = Clock.currentTimeMillis();
        IMap<Integer, Integer> map = createSimpleMap();
        map.put(1, 1, 100, SECONDS);
        EntryView<Integer, Integer> entryView = map.getEntryView(1);
        long expirationTime = entryView.getExpirationTime();

        assertTrue(expirationTime > now);
    }

    private IMap<Integer, Integer> getMapPopulatedWithExpirableKeys() {
        IMap<Integer, Integer> map = createSimpleMap();
        map.put(1, 1, 100, TimeUnit.MILLISECONDS);
        map.put(2, 1, 100, TimeUnit.MILLISECONDS);
        map.put(3, 1, 100, TimeUnit.MILLISECONDS);
        return map;
    }

    @Test
    @Category(NightlyTest.class)
    public void testNumberOfEventsFired_withMaxIdleSeconds_whenReadBackupDataEnabled() {
        int maxIdleSeconds = 1;
        int numberOfEntriesToBeAdded = 1000;

        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch evictedEntryLatch = new CountDownLatch(numberOfEntriesToBeAdded);

        IMap<Integer, Integer> map = createMapWithReadBackupDataEnabled(maxIdleSeconds);

        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryExpired(EntryEvent<Integer, Integer> event) {
                evictedEntryLatch.countDown();
                count.incrementAndGet();
            }
        }, false);

        for (int i = 0; i < numberOfEntriesToBeAdded; i++) {
            map.put(i, i);
        }
        // wait some time for idle expiration
        sleepAtLeastSeconds(2);

        for (int i = 0; i < numberOfEntriesToBeAdded; i++) {
            map.get(i);
        }

        assertOpenEventually(evictedEntryLatch);
        // sleep some seconds to be sure that
        // we did not receive more than expected number of events
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(numberOfEntriesToBeAdded, count.get());
            }
        }, 5);
    }

    private IMap<Integer, Integer> createMapWithReadBackupDataEnabled(int maxIdleSeconds) {
        String mapName = randomMapName();

        MapConfig mapConfig = newMapConfig(mapName)
                .setMaxIdleSeconds(maxIdleSeconds)
                .setReadBackupData(true);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] hazelcastInstances = hazelcastInstanceFactory.newInstances(config);

        return hazelcastInstances[0].getMap(mapName);
    }

    @Test
    @Category(NightlyTest.class)
    public void testBackupExpirationDelay_onPromotedReplica() {
        // cluster size should be at least 2 since we are testing a scenario with backups
        int clusterSize = 2;
        int ttlSeconds = 3;
        int numberOfItemsToBeAdded = 1_000;
        String mapName = randomMapName();

        Config config = newConfigWithTTL(mapName, ttlSeconds)
                // use a long delay for testing purposes
                .setProperty(ClusterProperty.MAP_EXPIRY_DELAY_SECONDS.getName(),
                        String.valueOf(TimeUnit.HOURS.toSeconds(1)));
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);

        IMap<Integer, Integer> map1 = instances[0].getMap(mapName);
        IMap<Integer, Integer> map2 = instances[1].getMap(mapName);

        for (int i = 0; i < numberOfItemsToBeAdded; i++) {
            map1.put(i, i);
        }

        //instances[0].shutdown();

        sleepAtLeastSeconds(3);

        // force entries to expire by touching each one
        for (int i = 0; i < numberOfItemsToBeAdded; i++) {
            map2.get(i);
        }

        assertSizeEventually(0, map2);
    }

    @Test
    public void testExpiration_onReplicatedPartition() {
        String mapName = randomMapName();
        Config config = getConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance initialNode = factory.newHazelcastInstance(config);
        IMap<String, Integer> map = initialNode.getMap(mapName);

        final CountDownLatch evictedEntryCounterLatch = new CountDownLatch(1);
        map.addEntryListener((EntryExpiredListener<String, Integer>) event -> evictedEntryCounterLatch.countDown(), false);

        String key = getClass().getCanonicalName();

        // 1. put a key to expire
        map.put(key, 1, 3, SECONDS);

        // 2. wait for expiration on owner node
        assertOpenEventually(evictedEntryCounterLatch, 20);

        HazelcastInstance joinerNode = factory.newHazelcastInstance(config);
        waitAllForSafeState(factory.getAllHazelcastInstances());

        // 3. shutdown owner
        initialNode.shutdown();

        // 4. key should be expired on new owner
        assertExpirationOccurredOnJoinerNode(mapName, key, joinerNode);
    }

    @Test
    @Category(NightlyTest.class)
    public void testExpiration_onBackupPartitions_whenPuttingWithTTL() {
        String mapName = randomMapName();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        HazelcastInstance[] nodes = factory.newInstances(config);
        IMap<Integer, Integer> map = nodes[0].getMap(mapName);

        // 1. put keys with TTL
        for (int i = 0; i < 100; i++) {
            map.put(i, i, 10, SECONDS);
        }

        // 2. shutdown one node (since we want to see previous backup partitions as owners)
        nodes[1].shutdown();

        // 3. background task should sweep all keys
        assertSizeEventually(0, map, 20);
    }

    private void assertExpirationOccurredOnJoinerNode(String mapName, String key, HazelcastInstance joinerNode) {
        IMap<String, Integer> newNodeMap = joinerNode.getMap(mapName);
        Integer value = newNodeMap.get(key);

        assertNull("value of expired key should be null on a replicated partition", value);
    }

    @Test
    public void testGetAll_doesNotShiftLastUpdateTimeOfEntry() {
        IMap<Integer, Integer> map = createSimpleMap();

        int key = 1;
        map.put(key, 0, 1, TimeUnit.MINUTES);

        EntryView<Integer, Integer> entryView = map.getEntryView(key);
        long lastUpdateTimeBeforeGetAll = entryView.getLastUpdateTime();

        Set<Integer> keys = Collections.singleton(key);
        map.getAll(keys);

        entryView = map.getEntryView(key);
        long lastUpdateTimeAfterGetAll = entryView.getLastUpdateTime();

        assertEquals("getAll should not shift lastUpdateTime of the entry",
                lastUpdateTimeBeforeGetAll, lastUpdateTimeAfterGetAll);
    }

    @Test
    public void testRandomEvictionPolicyWorks() {
        int maxSize = 300;

        MapConfig mapConfig = newMapConfig("test");

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(RANDOM)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(maxSize);

        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap("test");

        for (int i = 0; i < 500; i++) {
            map.put(i, i);
        }

        int size = map.size();
        String message = "map-size should be smaller than"
                + " max-size but found [map-size = %d and max-size = %d]";
        assertTrue(format(message, size, maxSize), size <= maxSize);
    }

    @Test
    public void testLastAddedKey_notEvicted() {
        MapConfig mapConfig = newMapConfig("test");

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizePolicy(MaxSizePolicy.PER_PARTITION)
                .setSize(1);

        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1")
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap("test");

        final AtomicReference<Integer> evictedKey = new AtomicReference<>(null);
        map.addEntryListener(
                (EntryEvictedListener<Integer, Integer>) event -> evictedKey.set(event.getKey()), false);

        map.put(1, 1);
        map.put(2, 1);

        final Integer expected = 1;
        assertTrueEventually(
                () -> assertEquals("Eviction impl. cannot evict latest added key 2", expected, evictedKey.get()));
    }

    /**
     * Eviction of last added key can only be triggered with one of heap based max-size-policies.
     */
    @Test
    public void testLastAddedKey_canBeEvicted_whenFreeHeapNeeded() {
        MapConfig mapConfig = newMapConfig("test");

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizePolicy(MaxSizePolicy.FREE_HEAP_PERCENTAGE)
                .setSize(90);

        // don't use getConfig(), this test is OSS specific
        Config config = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1")
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap("test");

        final AtomicReference<Integer> evictedKey = new AtomicReference<>(null);
        map.addEntryListener(
                (EntryEvictedListener<Integer, Integer>) event -> evictedKey.set(event.getKey()), false);

        // 1. make available free-heap-percentage 10. availableFree = maxMemoryMB - (totalMemoryMB - freeMemoryMB)
        // free-heap-percentage = availableFree/maxMemoryMB
        int totalMemoryMB = 90;
        int freeMemoryMB = 0;
        int maxMemoryMB = 100;
        setMockRuntimeMemoryInfoAccessor(map, totalMemoryMB, freeMemoryMB, maxMemoryMB);

        // 2. this `put` should trigger eviction because we used 90% heap already
        // and max used-heap-percentage was set 10% in mapConfig
        map.put(1, 1);

        final Integer expected = 1;
        assertTrueEventually(
                () -> assertEquals("Eviction impl. should evict latest added key when heap based max-size-policy is used",
                        expected, evictedKey.get()));
    }

    private <K, V> IMap<K, V> createSimpleMap() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        return instance.getMap(randomMapName());
    }

    protected MapConfig newMapConfig(String mapName) {
        return new MapConfig(mapName).setStatisticsEnabled(statisticsEnabled);
    }

    private Config newConfig(String mapName, int maxSize, MaxSizePolicy maxSizePolicy) {
        MapConfig mapConfig = newMapConfig(mapName + "*");

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(maxSizePolicy)
                .setSize(maxSize);

        return getConfig()
                .addMapConfig(mapConfig);
    }

    private Config newConfigWithTTL(String mapName, int ttlSeconds) {
        MapConfig mapConfig = newMapConfig(mapName + "*")
                .setTimeToLiveSeconds(ttlSeconds);

        return getConfig()
                .addMapConfig(mapConfig);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getMapConfig("default")
                .setStatisticsEnabled(statisticsEnabled)
                .setPerEntryStatsEnabled(perEntryStatsEnabled);
        return config;
    }
}
