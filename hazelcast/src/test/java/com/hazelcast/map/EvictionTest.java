/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
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
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.internal.util.Clock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.RANDOM;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.map.EvictionMaxSizePolicyTest.setMockRuntimeMemoryInfoAccessor;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.query.SampleTestObjects.Employee;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("deprecation")
public class EvictionTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule
            = set(PROP_TASK_PERIOD_SECONDS, String.valueOf(1));

    @Test
    public void testTTL_entryShouldNotBeReachableAfterTTL() {
        final IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 1, SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(map.containsKey(1));
            }
        });
    }

    @Test
    public void testMaxIdle_entryShouldNotBeReachableAfterMaxIdle() {
        final IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 0, SECONDS, 1, SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(map.containsKey(1));
            }
        });
    }

    @Test
    public void testMaxIdle_backupEntryShouldNotBeReachableAfterMaxIdle() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance instanceA = factory.newHazelcastInstance(getConfig());
        final HazelcastInstance instanceB = factory.newHazelcastInstance(getConfig());

        final String keyOwnedByInstanceA = generateKeyOwnedBy(instanceA);

        instanceA.getMap("Test")
                .put(keyOwnedByInstanceA, "value0", 0, SECONDS, 3, SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RecordStore recordStore = getRecordStore(instanceB, keyOwnedByInstanceA);
                assertEquals(0, recordStore.size());
            }
        });
    }

    @Test
    public void testMaxIdle_backupRecordStore_mustBeExpirable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        final HazelcastInstance instanceB = factory.newHazelcastInstance(getConfig());

        final String keyOwnedByInstanceA = generateKeyOwnedBy(instance);

        IMap<String, String> map = instance.getMap("Test");
        map.put(keyOwnedByInstanceA, "value0", 0, SECONDS, 30, SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                RecordStore store = getRecordStore(instanceB, keyOwnedByInstanceA);
                assertEquals(1, store.size());
                assertEquals(true, store.isExpirable());
            }
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
        testMaxIdle_readThroughIndex(true);
    }

    @Test
    public void testMaxIdle_readThroughUnorderedIndex() {
        testMaxIdle_readThroughIndex(false);
    }

    private void testMaxIdle_readThroughIndex(boolean ordered) {
        String mapName = randomMapName();

        Config config = getConfig();
        // make the cleaner task as aggressive as possible
        config.setProperty(PROP_TASK_PERIOD_SECONDS, "1");

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Employee> map = node.getMap(mapName);
        map.addIndex("city", ordered);

        for (int i = 0; i < 5; ++i) {
            String cityName = i % 2 == 0 ? "cityname" : null;

            Employee emp = new Employee(i, "name" + i, cityName, 0, true, i);
            map.put(i, emp, 0L, SECONDS, 14L, SECONDS);
        }

        EntryObject entryObject = new PredicateBuilderImpl().getEntryObject();
        Predicate predicateCityNull = entryObject.get("city").isNull();
        // Touch the map entry though the index to make sure expiration cleaning task
        // doesn't evict it.
        assertTrueAllTheTime(() -> {
            assertTrue(map.containsKey(0));
            Collection<Employee> valuesNullCity = map.values(predicateCityNull);
            assertEquals(2, valuesNullCity.size());

            Collection<Employee> valuesNotNullCity = map.values(Predicates.equal("city", "cityname"));
            assertEquals(3, valuesNotNullCity.size());
        }, 30);
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
    public void testTTL_prolongationAfterNonTTLUpdate()
            throws ExecutionException, InterruptedException {
        final IMap<Integer, String> map = createSimpleMap();

        long startRef = currentTimeMillis();
        map.put(1, "value0", 10, SECONDS);
        long endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Prolong 1st round
        startRef = currentTimeMillis();
        map.put(1, "value1");
        endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Prolong 2nd round
        startRef = currentTimeMillis();
        map.set(1, "value2");
        endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Prolong 3rd round
        final HashMap<Integer, String> items = new HashMap<Integer, String>();
        items.put(1, "value3");
        items.put(2, "value1");
        items.put(3, "value1");

        startRef = currentTimeMillis();
        map.putAll(items);
        endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Prolong 4th round
        startRef = currentTimeMillis();
        map.putAsync(1, "value4").get();
        endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Prolong 5th round
        startRef = currentTimeMillis();
        map.setAsync(1, "value5").get();
        endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Prolong 6th round
        startRef = currentTimeMillis();
        map.tryPut(1, "value6", 5, TimeUnit.SECONDS);
        endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Prolong 7th round
        startRef = currentTimeMillis();
        map.replace(1, "value7");
        endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Prolong 8th round
        startRef = currentTimeMillis();
        map.replace(1, "value7", "value8");
        endRef = currentTimeMillis();
        sleepAndAssertTtlExpirationCorrectness(map, 10, startRef, endRef);

        // Confirm expiration
        sleepAtLeastSeconds(10);
        assertFalse(map.containsKey(1));
    }

    private void sleepAndAssertTtlExpirationCorrectness(IMap<Integer, String> map, long expected, long startRef, long endRef) {
        sleepAtLeastSeconds(3);

        EntryView view = map.getEntryView(1);
        long actual = MILLISECONDS.toSeconds(view.getExpirationTime() - startRef);
        long delta = (1 + MILLISECONDS.toSeconds(endRef - startRef));
        assertEquals(expected, actual, delta);
        assertTrue(map.containsKey(1));
    }

    @Test
    public void testGetEntryView_withTTL() {
        final IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value", 1, SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map.getEntryView(1));
            }
        });
    }

    @Test
    public void testIssue455ZeroTTLShouldPreventEviction() {
        GroupConfig groupConfig = new GroupConfig()
                .setName("testIssue455ZeroTTLShouldPreventEviction");
        MapConfig mapConfig = newMapConfig("testIssue455ZeroTTLShouldPreventEviction")
                .setNearCacheConfig(new NearCacheConfig());
        Config config = getConfig()
                .setGroupConfig(groupConfig)
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
        GroupConfig groupConfig = new GroupConfig()
                .setName("testIssue585ZeroTTLShouldPreventEvictionWithSet");
        MapConfig mapConfig = newMapConfig("testIssue585ZeroTTLShouldPreventEvictionWithSet")
                .setNearCacheConfig(new NearCacheConfig());
        Config config = getConfig()
                .setGroupConfig(groupConfig)
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
    public void testIssue585SetWithoutTTL() {
        final IMap<String, String> map = createSimpleMap();
        final String key = "key";

        map.set(key, "value", 5, SECONDS);
        // this `set` operation should not affect existing TTL, so "key" should be expired after 1 second
        map.set(key, "value2");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull("Key should be expired after 1 seconds", map.get(key));
            }
        });
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

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE)
                .setSize(size);
        MapConfig mapConfig = newMapConfig(mapName)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizeConfig(maxSizeConfig);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);

        final IMap firstMap = instances[0].getMap(mapName);
        final CountDownLatch latch = new CountDownLatch(clusterSize);
        final AtomicBoolean success = new AtomicBoolean(true);
        new Thread() {
            @Override
            public void run() {
                sleepAtLeastSeconds(1);
                while (latch.getCount() != 0) {
                    int mapSize = firstMap.size();
                    if (mapSize > (size * clusterSize + size * clusterSize * 10 / 100)) {
                        success.set(false);
                        break;
                    }
                    sleepAtLeastSeconds(1);
                }
            }
        }.start();

        for (int i = 0; i < clusterSize; i++) {
            final IMap<String, Integer> map = instances[i].getMap(mapName);
            new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < size; j++) {
                        map.put(clusterSize + "-" + j, j);
                    }
                    latch.countDown();
                }
            }.start();
        }

        assertTrue(latch.await(10, TimeUnit.MINUTES));
        assertTrue(success.get());
    }

    @Test
    public void testEvictionSpeedTestPerPartition() {
        final int clusterSize = 2;
        final int size = 100;
        final String mapName = "testEvictionSpeedTestPerPartition";

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setMaxSizePolicy(PER_PARTITION)
                .setSize(size);
        MapConfig mapConfig = newMapConfig(mapName)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizeConfig(maxSizeConfig);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);

        final IMap firstMap = instances[0].getMap(mapName);
        final int partitionCount = instances[0].getPartitionService().getPartitions().size();
        final CountDownLatch latch = new CountDownLatch(clusterSize);
        final AtomicBoolean error = new AtomicBoolean(false);
        new Thread() {
            @Override
            public void run() {
                sleepAtLeastSeconds(1);
                while (latch.getCount() != 0) {
                    if (firstMap.size() > (size * partitionCount * 1.2)) {
                        error.set(true);
                    }
                    sleepAtLeastSeconds(1);
                }
            }
        }.start();

        for (int i = 0; i < clusterSize; i++) {
            final IMap<String, Integer> map = instances[i].getMap(mapName);
            new Thread() {
                public void run() {
                    for (int j = 0; j < 10000; j++) {
                        map.put(clusterSize + "-" + j, j);
                    }
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);
        assertFalse("map was not evicted properly!", error.get());
    }

    @Test
    public void testEvictionPerPartition() {
        int clusterSize = 2;
        int size = 10;
        String mapName = "testEvictionPerPartition";

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setMaxSizePolicy(PER_PARTITION)
                .setSize(size);
        MapConfig mapConfig = newMapConfig(mapName)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizeConfig(maxSizeConfig);
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
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

        MaxSizeConfig max = new MaxSizeConfig()
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE)
                .setSize(size);
        MapConfig mapConfig = newMapConfig(mapName)
                .setStatisticsEnabled(false)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizeConfig(max);
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
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

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setMaxSizePolicy(PER_NODE)
                .setSize(mapMaxSize);
        MapConfig mapConfig = newMapConfig(mapName)
                .setStatisticsEnabled(disableStats)
                .setEvictionPolicy(LFU)
                .setMaxSizeConfig(maxSizeConfig);
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

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE)
                .setSize(size);
        MapConfig mapConfig = newMapConfig(mapName)
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizeConfig(maxSizeConfig);
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
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

    @Test
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
        assertSizeEventually(0, map, 300);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(size, entryEvictedEventCount.get());
            }
        }, 300);
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
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(map.get(2));
                assertEquals(2, map.get(1));
            }
        });
    }

    @Test
    @Category(NightlyTest.class)
    public void expired_entries_removed_after_migration() {
        int numOfEntries = 1000;
        String name = "expired_entries_removed_after_migration";

        MapConfig mapConfig = newMapConfig(name)
                .setMaxIdleSeconds(20);
        Config config = getConfig()
                .setProperty(PROP_TASK_PERIOD_SECONDS, "1")
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
        assertOpenEventually(latch, TimeUnit.MINUTES.toSeconds(10));
    }

    /**
     * Test for issue 614
     */
    @Test
    public void testContainsKeyShouldDelayEviction() {
        String mapName = randomMapName();
        int waitSeconds = 2;

        MapConfig mapConfig = newMapConfig(mapName)
                .setMaxIdleSeconds(30);
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
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance node : nodes) {
                    long heapCostOfNode = node.getMap(mapName).getLocalMapStats().getHeapCost();
                    assertEquals(0L, heapCostOfNode);
                }
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
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(expected, count.get());
            }
        });
    }

    @Test
    public void testEvictionPerNode_sweepsBackupPartitions() {
        // cluster size should be at least 2 since we are testing a scenario with backups
        int clusterSize = 2;
        int maxSize = 1000;
        String mapName = randomMapName();

        Config config = newConfig(mapName, maxSize, MaxSizeConfig.MaxSizePolicy.PER_NODE);

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

        Config config = newConfig(mapName, maxSizeMB, MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(GroupProperty.MAP_EVICTION_BATCH_SIZE.getName(), "2");

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

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(map.get("foo"));
            }
        }, 30);
    }

    @Test
    public void testOnExpiredKeys_getAll() {
        IMap<Integer, Integer> map = getMapWithExpiredKeys();
        Set<Integer> keys = Collections.singleton(1);
        Map<Integer, Integer> all = map.getAll(keys);

        assertEquals(0, all.size());
    }

    @Test
    public void testOnExpiredKeys_values() {
        IMap<Integer, Integer> map = getMapWithExpiredKeys();
        Collection<Integer> values = map.values();

        assertEquals(0, values.size());
    }

    @Test
    public void testOnExpiredKeys_keySet() {
        IMap<Integer, Integer> map = getMapWithExpiredKeys();
        Set<Integer> keySet = map.keySet();

        assertEquals(0, keySet.size());
    }

    @Test
    public void testOnExpiredKeys_entrySet() {
        IMap<Integer, Integer> map = getMapWithExpiredKeys();
        Set<Map.Entry<Integer, Integer>> entries = map.entrySet();

        assertEquals(0, entries.size());
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

    private IMap<Integer, Integer> getMapWithExpiredKeys() {
        IMap<Integer, Integer> map = createSimpleMap();
        map.put(1, 1, 100, TimeUnit.MILLISECONDS);
        map.put(2, 1, 100, TimeUnit.MILLISECONDS);
        map.put(3, 1, 100, TimeUnit.MILLISECONDS);
        sleepAtLeastSeconds(2);
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

        assertOpenEventually(evictedEntryLatch, 600);
        // sleep some seconds to be sure that
        // we did not receive more than expected number of events
        sleepAtLeastSeconds(10);
        assertEquals(numberOfEntriesToBeAdded, count.get());
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
        int numberOfItemsToBeAdded = 1000;
        String mapName = randomMapName();

        Config config = newConfigWithTTL(mapName, ttlSeconds)
                // use a long delay for testing purposes
                .setProperty(GroupProperty.MAP_EXPIRY_DELAY_SECONDS.getName(), String.valueOf(TimeUnit.HOURS.toSeconds(1)));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = factory.newInstances(config);

        IMap<Integer, Integer> map1 = instances[0].getMap(mapName);
        IMap<Integer, Integer> map2 = instances[1].getMap(mapName);

        for (int i = 0; i < numberOfItemsToBeAdded; i++) {
            map1.put(i, i);
        }

        instances[0].shutdown();

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
        assertOpenEventually(evictedEntryCounterLatch, 240);

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
        HazelcastInstance[] nodes = factory.newInstances(getConfig());
        IMap<Integer, Integer> map = nodes[0].getMap(mapName);

        // 1. put keys with TTL
        for (int i = 0; i < 60; i++) {
            map.put(i, i, 5, SECONDS);
        }

        // 2. shutdown one node (since we want to see previous backup partitions as owners)
        nodes[1].shutdown();

        // 3. background task should sweep all keys
        assertSizeEventually(0, map, 240);
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

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(maxSize)
                .setMaxSizePolicy(PER_NODE);
        MapConfig mapConfig = newMapConfig("test")
                .setEvictionPolicy(RANDOM)
                .setMaxSizeConfig(maxSizeConfig);
        Config config = getConfig()
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap("test");

        for (int i = 0; i < 500; i++) {
            map.put(i, i);
        }

        int size = map.size();
        String message = "map-size should be smaller than max-size but found [map-size = %d and max-size = %d]";
        assertTrue(format(message, size, maxSize), size <= maxSize);
    }

    @Test
    public void testLastAddedKey_notEvicted() {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(1)
                .setMaxSizePolicy(PER_PARTITION);
        MapConfig mapConfig = newMapConfig("test")
                .setEvictionPolicy(LFU)
                .setMaxSizeConfig(maxSizeConfig);
        Config config = getConfig()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap("test");

        final AtomicReference<Integer> evictedKey = new AtomicReference<Integer>(null);
        map.addEntryListener(new EntryEvictedListener<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                evictedKey.set(event.getKey());
            }
        }, false);

        map.put(1, 1);
        map.put(2, 1);

        final Integer expected = 1;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Eviction impl. cannot evict latest added key 2", expected, evictedKey.get());
            }
        });
    }

    /**
     * Eviction of last added key can only be triggered with one of heap based max-size-policies.
     */
    @Test
    public void testLastAddedKey_canBeEvicted_whenFreeHeapNeeded() {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(90)
                .setMaxSizePolicy(FREE_HEAP_PERCENTAGE);
        MapConfig mapConfig = newMapConfig("test")
                .setEvictionPolicy(LFU)
                .setMaxSizeConfig(maxSizeConfig);
        // don't use getConfig(), this test is OSS specific
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap("test");

        final AtomicReference<Integer> evictedKey = new AtomicReference<Integer>(null);
        map.addEntryListener(new EntryEvictedListener<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                evictedKey.set(event.getKey());
            }
        }, false);

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
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Eviction impl. should evict latest added key when heap based max-size-policy is used",
                        expected, evictedKey.get());
            }
        });
    }

    private <K, V> IMap<K, V> createSimpleMap() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        return instance.getMap(randomMapName());
    }

    protected MapConfig newMapConfig(String mapName) {
        return new MapConfig(mapName);
    }

    private Config newConfig(String mapName, int maxSize, MaxSizeConfig.MaxSizePolicy maxSizePolicy) {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig(maxSize, maxSizePolicy);

        MapConfig mapConfig = newMapConfig(mapName + "*")
                .setMaxSizeConfig(maxSizeConfig)
                .setEvictionPolicy(EvictionPolicy.LRU);

        return getConfig()
                .addMapConfig(mapConfig);
    }

    private Config newConfigWithTTL(String mapName, int ttlSeconds) {
        MapConfig mapConfig = newMapConfig(mapName + "*")
                .setTimeToLiveSeconds(ttlSeconds);

        return getConfig()
                .addMapConfig(mapConfig);
    }
}
