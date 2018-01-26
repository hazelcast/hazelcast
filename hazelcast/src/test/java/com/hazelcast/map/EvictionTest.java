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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EvictionTest extends HazelcastTestSupport {

    private Config newConfig(String mapName, int maxSize, MaxSizeConfig.MaxSizePolicy maxSizePolicy) {
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName + "*");
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig(maxSize, maxSizePolicy);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setMinEvictionCheckMillis(0);
        config.addMapConfig(mapConfig);

        return config;
    }

    private Config newConfigWithTTL(String mapName, int ttlSeconds) {
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName + "*");
        mapConfig.setTimeToLiveSeconds(ttlSeconds);
        config.addMapConfig(mapConfig);
        return config;
    }

    @Test
    public void testTTL_entryShouldNotBeReachableAfterTTL() throws Exception {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 1, TimeUnit.SECONDS);
        sleepSeconds(1);

        assertFalse(map.containsKey(1));
    }

    @Test
    public void testTTL_zeroIsInfinity() throws Exception {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 2, TimeUnit.SECONDS);
        map.put(1, "value1", 0, TimeUnit.SECONDS);
        sleepSeconds(3);

        assertTrue(map.containsKey(1));
    }

    /**
     * We are defining TTL as time being passed since creation time of an entry.
     */
    @Test
    public void testTTL_appliedFromLastUpdate() throws Exception {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 1, TimeUnit.SECONDS);
        map.put(1, "value1", 2, TimeUnit.SECONDS);
        map.put(1, "value2", 300, TimeUnit.SECONDS);
        sleepSeconds(2);

        assertTrue(map.containsKey(1));
    }

    @Test
    public void testGetEntryView_withTTL() throws Exception {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value", 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        EntryView<Integer, String> entryView = map.getEntryView(1);

        assertNull(entryView);
    }

    @Test
    public void testIssue455ZeroTTLShouldPreventEviction() throws InterruptedException {
        Config config = getConfig();
        config.getGroupConfig().setName("testIssue455ZeroTTLShouldPreventEviction");
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        HazelcastInstance h = factory.newHazelcastInstance(config);
        IMap<String, String> map = h.getMap("testIssue455ZeroTTLShouldPreventEviction");
        map.put("key", "value", 1, TimeUnit.SECONDS);
        map.put("key", "value2", 0, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals("value2", map.get("key"));
    }

    @Test
    public void testIssue585ZeroTTLShouldPreventEvictionWithSet() throws InterruptedException {
        Config config = getConfig();
        config.getGroupConfig().setName("testIssue585ZeroTTLShouldPreventEvictionWithSet");
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        HazelcastInstance h = factory.newHazelcastInstance(config);
        IMap<String, String> map = h.getMap("testIssue585ZeroTTLShouldPreventEvictionWithSet");
        map.set("key", "value", 1, TimeUnit.SECONDS);
        map.set("key", "value2", 0, TimeUnit.SECONDS);
        sleepSeconds(2);
        assertEquals("value2", map.get("key"));
    }

    @Test
    public void testIssue585SetWithoutTTL() throws InterruptedException {
        final IMap<String, String> map = createSimpleMap();

        final String key = "key";

        map.set(key, "value", 5, TimeUnit.SECONDS);
        // this `set` operation should not affect existing ttl.
        // so "key" should be expired after 1 seconds.
        map.set(key, "value2");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull("Key should be expired after 1 seconds", map.get(key));
            }
        });
    }

    @Test
    public void testIssue304EvictionDespitePut() throws InterruptedException {
        Config config = getConfig();
        config.getGroupConfig().setName("testIssue304EvictionDespitePut");
        MapConfig mapConfig = config.getMapConfig("testIssue304EvictionDespitePut");
        mapConfig.setMaxIdleSeconds(5);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        IMap<String, Long> map = hazelcastInstance.getMap("testIssue304EvictionDespitePut");
        final AtomicInteger evictCount = new AtomicInteger(0);
        map.addEntryListener(new EntryAdapter<String, Long>() {
            public void entryEvicted(EntryEvent<String, Long> event) {
                evictCount.incrementAndGet();
            }
        }, true);

        String key = "key";
        for (int i = 0; i < 5; i++) {
            map.put(key, System.currentTimeMillis());
            sleepMillis(500);
        }
        assertEquals(evictCount.get(), 0);
        assertNotNull(map.get(key));
    }

    // current eviction check period is 1 second.
    // about 30000 records can be put in one second, so the size should be adapted
    @Test
    public void testEvictionSpeedTest() throws InterruptedException {
        final int k = 3;
        final int size = 10000;
        final CountDownLatch latch = new CountDownLatch(k);
        final String mapName = "testEvictionSpeedTest";
        Config config = getConfig();
        final MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setEvictionPercentage(25);
        final MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        maxSizeConfig.setSize(size);
        mapConfig.setMaxSizeConfig(maxSizeConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final AtomicBoolean success = new AtomicBoolean(true);

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                    while (latch.getCount() != 0) {
                        try {
                            int mapSize = map.size();
                            if (mapSize > (size * k + size * k * 10 / 100)) {
                                success.set(false);
                                break;
                            }
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        for (int i = 0; i < k; i++) {
            final IMap<String, Integer> map = instances[i].getMap(mapName);
            new Thread() {
                public void run() {
                    for (int j = 0; j < size; j++) {
                        map.put(k + "-" + j, j);
                    }
                    latch.countDown();
                }
            }.start();
        }

        assertTrue(latch.await(10, TimeUnit.MINUTES));
        assertTrue(success.get());
    }

    @Test
    public void testEvictionSpeedTestPerPartition() throws InterruptedException {
        final int k = 2;
        final int size = 100;
        final CountDownLatch latch = new CountDownLatch(k);
        final String mapName = "testEvictionSpeedTestPerPartition";
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setEvictionPercentage(25);
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(PER_PARTITION);
        maxSizeConfig.setSize(size);
        mapConfig.setMaxSizeConfig(maxSizeConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final int pNum = instances[0].getPartitionService().getPartitions().size();
        final AtomicBoolean error = new AtomicBoolean(false);

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                    while (latch.getCount() != 0) {
                        try {
                            int msize = map.size();
                            if (msize > (size * pNum * 1.2)) {
                                error.set(true);
                            }
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        for (int i = 0; i < k; i++) {
            final IMap<String, Integer> map = instances[i].getMap(mapName);
            new Thread() {
                public void run() {
                    for (int j = 0; j < 10000; j++) {
                        map.put(k + "-" + j, j);
                    }
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);
        assertFalse("map was not evicted properly!", error.get());
    }

    @Test
    public void testEvictionPerPartition() throws InterruptedException {
        final int k = 2;
        final int size = 10;
        final String mapName = "testEvictionPerPartition";
        Config cfg = getConfig();
        cfg.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        final MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(EvictionPolicy.LRU);
        mc.setEvictionPercentage(50);
        mc.setMinEvictionCheckMillis(0);
        final MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(PER_PARTITION);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        final int pNum = instances[0].getPartitionService().getPartitions().size();
        int insertCount = size * pNum * 2;
        final Map<Integer, Integer> map = instances[0].getMap(mapName);
        for (int i = 0; i < insertCount; i++) {
            map.put(i, i);
        }
        int mapSize = map.size();
        String message = format("mapSize : %d should be <= max-size : %d ", mapSize, size);
        assertTrue(message, mapSize <= size);
    }

    @Test
    public void testEvictionLRU_statisticsDisabled() {
        final int nodeCount = 2;
        final int size = 100000;
        final String mapName = randomMapName("_testEvictionLRU_statisticsDisabled_");

        Config cfg = getConfig();
        cfg.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setStatisticsEnabled(false);
        mc.setEvictionPolicy(EvictionPolicy.LRU);
        mc.setEvictionPercentage(10);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        IMap<Object, Object> map = instances[0].getMap(mapName);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            if (i < size / 2) {
                map.get(i);
            }
        }
        // give some time to eviction thread run.
        sleepSeconds(3);

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
    protected void testEvictionLFUInternal(boolean disableStats) {
        int mapMaxSize = 10000;
        String mapName = randomMapName();

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(PER_NODE);
        msc.setSize(mapMaxSize);

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setStatisticsEnabled(disableStats);
        mapConfig.setEvictionPolicy(LFU);
        mapConfig.setMinEvictionCheckMillis(0);
        mapConfig.setMaxSizeConfig(msc);

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
        final int size = 10000;
        final String mapName = randomMapName("testEvictionLFU2");

        Config cfg = getConfig();
        cfg.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(EvictionPolicy.LFU);
        mc.setEvictionPercentage(90);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        HazelcastInstance node = createHazelcastInstance();
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
    public void testMapRecordEviction() throws InterruptedException {
        final String mapName = randomMapName();
        final int size = 100;
        final AtomicInteger entryEvictedEventCount = new AtomicInteger(0);
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setTimeToLiveSeconds(1);
        mapConfig.addEntryListenerConfig(new EntryListenerConfig().setImplementation(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                entryEvictedEventCount.incrementAndGet();
            }
        }).setLocal(true));

        HazelcastInstance instance = createHazelcastInstance(config);

        final IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //wait until eviction is complete
        assertSizeEventually(0, map, 300);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(size, entryEvictedEventCount.get());
            }
        }, 300);
    }

    @Test
    public void testMapRecordIdleEviction() throws InterruptedException {
        final String mapName = randomMapName("testMapRecordIdleEviction");
        final int maxIdleSeconds = 1;
        final int size = 100;
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setMaxIdleSeconds(maxIdleSeconds);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(mapName);

        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        assertSizeEventually(0, map);
    }

    @Test
    public void testZeroResetsTTL() throws InterruptedException {
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig("testZeroResetsTTL");
        mapConfig.setTimeToLiveSeconds(5);
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
        map.put(1, 2, 0, TimeUnit.SECONDS);
        latch.await(10, TimeUnit.SECONDS);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map.get(2));
                assertEquals(2, map.get(1));
            }
        });
    }

    @Test
    @Category(NightlyTest.class)
    public void testMapRecordIdleEvictionOnMigration() {
        final String name = "testMapRecordIdleEvictionOnMigration";

        Config cfg = getConfig();
        cfg.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        MapConfig mapConfig = cfg.getMapConfig(name);
        int maxIdleSeconds = 30;
        int size = 100;
        final int nsize = size / 5;
        mapConfig.setMaxIdleSeconds(maxIdleSeconds);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        final IMap<Integer, Integer> map = instance1.getMap(name);
        final CountDownLatch latch = new CountDownLatch(size - nsize);
        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, false);

        // put sample data
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        // wait until some time that is close to eviction
        sleepSeconds(maxIdleSeconds - 5);

        // touch the ones you dont want to be evicted.
        for (int i = 0; i < nsize; i++) {
            map.get(i);
        }

        factory.newHazelcastInstance(cfg);
        factory.newHazelcastInstance(cfg);

        //wait until eviction is complete
        assertOpenEventually(latch, 240);

        assertSizeEventually(nsize, map);
    }

    /**
     * Background task {@link com.hazelcast.map.impl.eviction.ExpirationManager.ClearExpiredRecordsTask}
     * should sweep expired records eventually.
     */
    @Test
    @Category(NightlyTest.class)
    public void testMapPutTTLWithListener() throws InterruptedException {
        final int putCount = 100;
        final CountDownLatch latch = new CountDownLatch(putCount);
        IMap<Integer, Integer> map = createSimpleMap();

        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(final EntryEvent event) {
                latch.countDown();
            }
        }, true);

        final int ttl = (int) (Math.random() * 5000);

        for (int j = 0; j < putCount; j++) {
            map.put(j, j, ttl, TimeUnit.MILLISECONDS);
        }

        // wait until eviction is complete.
        assertOpenEventually(latch, TimeUnit.MINUTES.toSeconds(10));
    }

    /**
     * Test for issue 614
     */
    @Test
    public void testContainsKeyShouldDelayEviction() throws InterruptedException {
        String mapName = randomMapName();
        final int waitSeconds = 2;

        Config config = getConfig();
        config.getMapConfig(mapName).setMaxIdleSeconds(30);

        HazelcastInstance instance = createHazelcastInstance(config);

        final IMap<Object, Object> map = instance.getMap(mapName);
        map.put(1, 1);

        sleepSeconds(waitSeconds);

        EntryView<Object, Object> entryView = map.getEntryView(1);
        final long lastAccessTime = entryView.getLastAccessTime();

        //1. Shift lastAccessTime.
        map.containsKey(1);

        entryView = map.getEntryView(1);
        final long lastAccessTimeAfterContainsOperation = entryView.getLastAccessTime();

        //2. Expecting lastAccessTime to be shifted by containsKey operation.
        final long diffSecs = TimeUnit.MILLISECONDS.toSeconds(lastAccessTimeAfterContainsOperation - lastAccessTime);

        //3. So there should be a diff at least waitSeconds.
        final String failureMessage = format("Diff seconds %d, wait seconds %d", diffSecs, waitSeconds);
        assertTrue(failureMessage, diffSecs >= waitSeconds);
    }

    @Test
    public void testIssue1085EvictionBackup() throws InterruptedException {
        final String mapName = randomMapName();
        int entryCount = 10;
        Config config = getConfig();
        config.getMapConfig(mapName).setTimeToLiveSeconds(3);
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(config);

        final CountDownLatch latch = new CountDownLatch(entryCount);

        final IMap<Integer, Integer> map = instances[0].getMap(mapName);
        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                super.entryEvicted(event);
                latch.countDown();
            }
        }, false);
        // put some sample data
        for (int i = 0; i < entryCount; i++) {
            map.put(i, i);
        }
        //wait until eviction is complete
        assertOpenEventually(latch);
        assertSizeEventually(0, map);
        assertHeapCostsZeroEventually(mapName, instances);
    }

    private void assertHeapCostsZeroEventually(final String mapName, final HazelcastInstance... nodes) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance node : nodes) {
                    final long heapCostOfNode = node.getMap(mapName).getLocalMapStats().getHeapCost();
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
    public void testEvictionAfterRemove() throws InterruptedException {
        IMap<Object, Object> map = createSimpleMap();
        final AtomicInteger count = new AtomicInteger(0);
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
                count.incrementAndGet();
            }
        }, true);
        // ttl is 2 seconds.
        map.put(1, 1, 2, TimeUnit.SECONDS);
        final int expected = (map.remove(1) == null ? 1 : 0);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, count.get());
            }
        });
    }

    @Test
    public void testEvictionPerNode_sweepsBackupPartitions() {
        final int maxSize = 1000;
        // node count should be at least 2 since we are testing a scenario on backups.
        final int nodeCount = 2;
        final String mapName = randomMapName();
        Config config = newConfig(mapName, maxSize, MaxSizeConfig.MaxSizePolicy.PER_NODE);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        IMap<Integer, Integer> map = instances[0].getMap(mapName);
        // over fill map with (10 * maxSize) items.
        for (int i = 0; i < 1; i++) {
            map.put(i, i);
        }

        assertBackupsSweptOnAllNodes(mapName, maxSize, instances);
    }

    private void assertBackupsSweptOnAllNodes(String mapName, int maxSize, HazelcastInstance[] instances) {
        for (HazelcastInstance instance : instances) {
            final IMap<Integer, Integer> map = instance.getMap(mapName);

            final long backupEntryCount = map.getLocalMapStats().getBackupEntryCount();
            final long ownedEntryCount = map.getLocalMapStats().getOwnedEntryCount();

            // entry count = (owned + backup).
            // On one node, entry count should be smaller than (2 * maxSize).
            assertTrue(2 * maxSize > ownedEntryCount + backupEntryCount);
        }
    }

    /**
     * Test for the issue 2659.
     * Eviction event is fired for an object already removed
     */
    @Test
    public void testEvictionForNanosTTL() throws InterruptedException {
        final IMap<String, String> map = createSimpleMap();
        map.put("foo", "bar", 1, TimeUnit.NANOSECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(map.get("foo"));
            }
        }, 30);
    }

    @Test
    public void testOnExpiredKeys_getAll() throws Exception {
        final IMap<Integer, Integer> map = getMapWithExpiredKeys();
        final Set<Integer> keys = Collections.singleton(1);
        final Map<Integer, Integer> all = map.getAll(keys);

        assertEquals(0, all.size());
    }

    @Test
    public void testOnExpiredKeys_values() throws Exception {
        final IMap<Integer, Integer> map = getMapWithExpiredKeys();
        final Collection<Integer> values = map.values();

        assertEquals(0, values.size());
    }

    @Test
    public void testOnExpiredKeys_keySet() throws Exception {
        final IMap<Integer, Integer> map = getMapWithExpiredKeys();
        final Set<Integer> keySet = map.keySet();

        assertEquals(0, keySet.size());
    }

    @Test
    public void testOnExpiredKeys_entrySet() throws Exception {
        final IMap<Integer, Integer> map = getMapWithExpiredKeys();
        final Set<Map.Entry<Integer, Integer>> entries = map.entrySet();

        assertEquals(0, entries.size());
    }

    @Test
    public void test_get_expiration_from_EntryView() throws Exception {
        final long now = Clock.currentTimeMillis();
        IMap<Integer, Integer> map = createSimpleMap();
        map.put(1, 1, 100, TimeUnit.SECONDS);
        final EntryView<Integer, Integer> entryView = map.getEntryView(1);
        final long expirationTime = entryView.getExpirationTime();

        assertTrue(expirationTime > now);
    }

    private IMap<Integer, Integer> getMapWithExpiredKeys() {
        IMap<Integer, Integer> map = createSimpleMap();
        map.put(1, 1, 100, TimeUnit.MILLISECONDS);
        map.put(2, 1, 100, TimeUnit.MILLISECONDS);
        map.put(3, 1, 100, TimeUnit.MILLISECONDS);
        sleepSeconds(1);
        return map;
    }

    @Test
    @Category(NightlyTest.class)
    public void testNumberOfEventsFired_withMaxIdleSeconds_whenReadBackupDataEnabled() throws Exception {
        final int maxIdleSeconds = 1;
        final int numberOfEntriesToBeAdded = 1000;

        final AtomicInteger count = new AtomicInteger(0);

        final CountDownLatch evictedEntryLatch = new CountDownLatch(numberOfEntriesToBeAdded);

        IMap<Integer, Integer> map = createMapWithReadBackupDataEnabled(maxIdleSeconds);

        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                evictedEntryLatch.countDown();
                count.incrementAndGet();
            }
        }, false);

        for (int i = 0; i < numberOfEntriesToBeAdded; i++) {
            map.put(i, i);
        }
        // wait some time for idle expiration.
        sleepSeconds(2);

        for (int i = 0; i < numberOfEntriesToBeAdded; i++) {
            map.get(i);
        }

        assertOpenEventually(evictedEntryLatch, 600);
        // sleep some seconds to be sure that
        // we did not receive more than expected number of events.
        sleepSeconds(10);
        assertEquals(numberOfEntriesToBeAdded, count.get());
    }

    private IMap<Integer, Integer> createMapWithReadBackupDataEnabled(int maxIdleSeconds) {
        final String mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setMaxIdleSeconds(maxIdleSeconds).setReadBackupData(true);

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] hazelcastInstances = hazelcastInstanceFactory.newInstances(config);

        return hazelcastInstances[0].getMap(mapName);
    }

    private <K, V> IMap<K, V> createSimpleMap() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        return instance.getMap(randomMapName());
    }

    @Test
    @Category(NightlyTest.class)
    public void testBackupExpirationDelay_onPromotedReplica() throws Exception {
        final int numberOfItemsToBeAdded = 1000;

        // node count should be at least 2 since we are testing a scenario on backups.
        final int nodeCount = 2;
        final int ttlSeconds = 3;
        final String mapName = randomMapName();

        Config config = newConfigWithTTL(mapName, ttlSeconds);
        // use a long delay for testing purposes.
        config.setProperty(GroupProperty.MAP_EXPIRY_DELAY_SECONDS.getName(), String.valueOf(TimeUnit.HOURS.toSeconds(1)));

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final IMap<Integer, Integer> map1 = instances[0].getMap(mapName);
        final IMap<Integer, Integer> map2 = instances[1].getMap(mapName);

        for (int i = 0; i < numberOfItemsToBeAdded; i++) {
            map1.put(i, i);
        }

        instances[0].shutdown();

        sleepSeconds(3);

        // Force entries to expire by touching each one.
        for (int i = 0; i < numberOfItemsToBeAdded; i++) {
            map2.get(i);
        }

        assertSizeEventually(0, map2);
    }

    @Test
    public void testExpiration_onReplicatedPartition() throws Exception {
        final CountDownLatch evictedEntryCounterLatch = new CountDownLatch(1);
        String mapName = randomMapName();
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance initialNode = factory.newHazelcastInstance(config);
        IMap<String, Integer> map = initialNode.getMap(mapName);
        map.addEntryListener(new EntryAdapter<String, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<String, Integer> event) {
                evictedEntryCounterLatch.countDown();
            }
        }, false);

        final String key = getClass().getCanonicalName();

        // 1. put a key to expire.
        map.put(key, 1, 3, TimeUnit.SECONDS);

        final HazelcastInstance joinerNode = factory.newHazelcastInstance(config);

        // 2. Wait for expiration on owner node.
        assertOpenEventually(evictedEntryCounterLatch, 240);

        // 3. Shutdown owner.
        initialNode.shutdown();

        // 4. Key should be expired on new owner.
        assertExpirationOccuredOnJoinerNode(mapName, key, joinerNode);
    }

    @Test
    @Category(NightlyTest.class)
    public void testExpiration_onBackupPartitions_whenPuttingWithTTL() throws Exception {
        String mapName = randomMapName();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] nodes = factory.newInstances(getConfig());
        IMap<Integer, Integer> map = nodes[0].getMap(mapName);

        // 1. put keys with TTL.
        for (int i = 0; i < 60; i++) {
            map.put(i, i, 5, TimeUnit.SECONDS);
        }

        // 2. Shutdown one node.
        // Since we want to see previous backup partitions as owners.
        nodes[1].shutdown();

        // 3. Background task should sweep all keys.
        assertSizeEventually(0, map, 240);
    }

    private void assertExpirationOccuredOnJoinerNode(String mapName, String key, HazelcastInstance joinerNode) {
        final IMap<String, Integer> newNodeMap = joinerNode.getMap(mapName);
        final Integer value = newNodeMap.get(key);

        assertNull("value of expired key should be null on a replicated partition", value);
    }

    @Test
    public void testGetAll_doesNotShiftLastUpdateTimeOfEntry() throws Exception {
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
    public void testRandomEvictionPolicyWorks() throws Exception {
        Config config = getConfig();
        int maxSize = 300;
        config.getMapConfig("test").setEvictionPolicy(RANDOM).getMaxSizeConfig().setSize(maxSize).setMaxSizePolicy(PER_NODE);

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
    public void testLastAddedKey_notEvicted() throws Exception {
        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.getMapConfig("test").setEvictionPolicy(LFU).getMaxSizeConfig().setSize(1).setMaxSizePolicy(PER_PARTITION);

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
            public void run() throws Exception {
                assertEquals("Eviction impl. cannot evict latest added key 2", expected, evictedKey.get());
            }
        });
    }

    /**
     * Eviction of last added key can only be triggered with one of heap based max-size-policies.
     */
    @Test
    public void testLastAddedKey_canBeEvicted_whenFreeHeapNeeded() {
        // don't use getConfig(), this test is OSS specific
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.getMapConfig("test")
                .setEvictionPolicy(LFU)
                .getMaxSizeConfig()
                .setSize(90)
                .setMaxSizePolicy(FREE_HEAP_PERCENTAGE);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap("test");

        final AtomicReference<Integer> evictedKey = new AtomicReference<Integer>(null);
        map.addEntryListener(new EntryEvictedListener<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                evictedKey.set(event.getKey());
            }
        }, false);

        // 1. Make available free-heap-percentage 10. availableFree = maxMemoryMB - (totalMemoryMB - freeMemoryMB)
        // free-heap-percentage = availableFree/maxMemoryMB;
        int totalMemoryMB = 90;
        int freeMemoryMB = 0;
        int maxMemoryMB = 100;
        setMockRuntimeMemoryInfoAccessor(map, totalMemoryMB, freeMemoryMB, maxMemoryMB);

        // 2. This `put` should trigger eviction because we used 90% heap already.
        // And max used-heap-percentage was set 10% in map-config.
        map.put(1, 1);

        final Integer expected = 1;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Eviction impl. should evict latest added key when heap based max-size-policy is used",
                        expected, evictedKey.get());
            }
        });
    }
}
