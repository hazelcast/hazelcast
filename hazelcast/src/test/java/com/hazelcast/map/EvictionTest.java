/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.impl.DefaultRecordStore;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EvictionTest extends HazelcastTestSupport {

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTTL_entryShouldNotBeReachableAfterTTL() throws Exception {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 1, TimeUnit.SECONDS);
        sleepSeconds(1);

        assertFalse(map.containsKey(1));
    }

    @Test
    public void testTTL_affectedByUpdates() throws Exception {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0", 2, TimeUnit.SECONDS);
        map.put(1, "value1", 100, TimeUnit.SECONDS);
        sleepSeconds(3);

        assertTrue(map.containsKey(1));
    }

    /**
     * We are defining TTL as time being passed since creation time of an entry.
     */
    @Test
    public void testTTL_appliedFromCreationTime() throws Exception {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value0");
        sleepSeconds(2);
        map.put(1, "value1", 2, TimeUnit.SECONDS);

        assertFalse(map.containsKey(1));
    }

    @Test
    public void testGetEntryView_withTTL() throws Exception {
        IMap<Integer, String> map = createSimpleMap();

        map.put(1, "value", 1, TimeUnit.SECONDS);
        sleepSeconds(2);

        EntryView<Integer, String> entryView = map.getEntryView(1);

        assertNull(entryView);
    }

    /*
       github issue 455
    */
    @Test
    public void testIssue455ZeroTTLShouldPreventEviction() throws InterruptedException {
        Config config = new Config();
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

    /*
       github issue 585
    */
    @Test
    public void testIssue585ZeroTTLShouldPreventEvictionWithSet() throws InterruptedException {
        Config config = new Config();
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

    /*
       github issue 585
    */
    @Test
    public void testIssue585SetWithoutTTL() throws InterruptedException {
        Config config = new Config();
        config.getGroupConfig().setName("testIssue585ZeroTTLShouldPreventEvictionWithSet");
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        HazelcastInstance h = factory.newHazelcastInstance(config);
        final IMap<String, String> map = h.getMap("testIssue585ZeroTTLShouldPreventEvictionWithSet");
        map.set("key", "value", 1, TimeUnit.SECONDS);
        map.set("key", "value2");
        assertSizeEventually(0, map);
    }

    /*
       github issue 304
    */
    @Test
    public void testIssue304EvictionDespitePut() throws InterruptedException {
        Config c = new Config();
        c.getGroupConfig().setName("testIssue304EvictionDespitePut");
        final HashMap<String, MapConfig> mapConfigs = new HashMap<String, MapConfig>();
        final MapConfig value = new MapConfig();
        value.setMaxIdleSeconds(5);
        mapConfigs.put("default", value);
        c.setMapConfigs(mapConfigs);
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(c);

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
    // about 30.000 records can be put in one second
    // so the size should be adapted
    @Test
    public void testEvictionSpeedTest() throws InterruptedException {
        final int k = 3;
        final int size = 10000;
        final CountDownLatch latch = new CountDownLatch(k);
        final String mapName = "testEvictionSpeedTest";
        Config cfg = new Config();
        final MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(EvictionPolicy.LRU);
        mc.setEvictionPercentage(25);
        final MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        final AtomicBoolean success = new AtomicBoolean(true);

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                    while (latch.getCount() != 0) {
                        try {
                            int msize = map.size();
                            if (msize > (size * k + size * k * 10 / 100)) {
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
            final IMap map = instances[i].getMap(mapName);
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
        Config cfg = new Config();
        final MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(EvictionPolicy.LRU);
        mc.setEvictionPercentage(25);
        final MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_PARTITION);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        final int pnum = instances[0].getPartitionService().getPartitions().size();
        final AtomicBoolean error = new AtomicBoolean(false);

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                    while (latch.getCount() != 0) {
                        try {
                            int msize = map.size();
                            if (msize > (size * pnum * 1.2)) {
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
            final IMap map = instances[i].getMap(mapName);
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
        Config cfg = new Config();
        cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");
        final MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(EvictionPolicy.LRU);
        mc.setEvictionPercentage(50);
        final MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_PARTITION);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        final int pnum = instances[0].getPartitionService().getPartitions().size();
        int insertCount = size * pnum * 2;
        final Map map = instances[0].getMap(mapName);
        for (int i = 0; i < insertCount; i++) {
            if (i == insertCount - 1) {
                sleepMillis(1100);
            }
            map.put(i, i);
        }
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(map.size() < size);
            }
        });
    }

    @Test
    public void testEvictionLRU_sweepsLeastRecentlyUseds() {
        final int nodeCount = 2;
        final int perNodeMaxSize = 1000;

        final String mapName = randomMapName();
        Config cfg = new Config();
        cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(EvictionPolicy.LRU);
        mc.setEvictionPercentage(20);
        mc.setMinEvictionCheckMillis(0L);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(perNodeMaxSize);
        mc.setMaxSizeConfig(msc);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        IMap<Object, Object> map = instances[0].getMap(mapName);

        // 1. Use only first half of entries by getting them.
        int recentlyUsedEvicted = 0;
        for (int i = 0; i < perNodeMaxSize / 2; i++) {
            map.put(i, i);
            map.get(i);
        }
        // 2. Over fill map to trigger eviction.
        for (int i = perNodeMaxSize / 2; i < 5 * perNodeMaxSize; i++) {
            map.put(i, i);
        }
        // 3. These entries should not be evicted.
        for (int i = 0; i < perNodeMaxSize / 2; i++) {
            if (map.get(i) == null) {
                recentlyUsedEvicted++;
            }
        }
        assertEquals(0, recentlyUsedEvicted);
    }

    @Test
    public void testEvictionLRU_statisticsDisabled() {
        final int nodeCount = 2;
        final int size = 100000;
        final String mapName = randomMapName("_testEvictionLRU_statisticsDisabled_");

        Config cfg = new Config();
        cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");
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
    public void testEvictionLFU_statisticsDisabled() {
        final String mapName = randomMapName("_testEvictionLFU_statisticsDisabled_");
        final int instanceCount = 1;
        final int size = 10000;

        Config cfg = new Config();
        cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setStatisticsEnabled(false);
        mc.setEvictionPolicy(EvictionPolicy.LFU);
        mc.setEvictionPercentage(20);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(instanceCount);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        final int atLeastShouldEvict = size / 40;
        final CountDownLatch latch = new CountDownLatch(atLeastShouldEvict);
        IMap<Object, Object> map = instances[0].getMap(mapName);
        map.addLocalEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        });
        // these are frequently used entries.
        for (int i = 0; i < size / 2; i++) {
            map.put(i, i);
            map.get(i);
        }
        // expecting these entries to be evicted.
        for (int i = size / 2; i < size; i++) {
            map.put(i, i);
        }
        assertOpenEventually(latch, 120);
        assertFalse("No eviction!?!?!?", map.size() == size);
        // these entries should exist in map after evicting LFU.
        for (int i = 0; i < size / 2; i++) {
            assertNotNull(map.get(i));
        }
    }

    @Test
    public void testEvictionLFU() {
        final String mapName = "testEvictionLFU_" + randomString();
        final int instanceCount = 1;
        final int size = 10000;
        Config cfg = new Config();
        cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(EvictionPolicy.LFU);
        mc.setEvictionPercentage(20);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(instanceCount);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        final int atLeastShouldEvict = size / 40;
        final CountDownLatch latch = new CountDownLatch(atLeastShouldEvict);
        IMap<Object, Object> map = instances[0].getMap(mapName);
        map.addLocalEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        });
        // these are frequently used entries.
        for (int i = 0; i < size / 2; i++) {
            map.put(i, i);
            map.get(i);
        }
        // expecting these entries to be evicted.
        for (int i = size / 2; i < size; i++) {
            map.put(i, i);
        }
        assertOpenEventually(latch, 120);
        assertFalse("No eviction!?!?!?", map.size() == size);
        // these entries should exist in map after evicting LFU.
        for (int i = 0; i < size / 2; i++) {
            assertNotNull(map.get(i));
        }
    }

    @Test
    public void testEvictionLFU2() {
        try {
            final int k = 2;
            final int size = 10000;
            final String mapName = randomMapName("testEvictionLFU2");
            Config cfg = new Config();
            cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");
            MapConfig mc = cfg.getMapConfig(mapName);
            mc.setEvictionPolicy(EvictionPolicy.LFU);
            mc.setEvictionPercentage(90);
            MaxSizeConfig msc = new MaxSizeConfig();
            msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
            msc.setSize(size);
            mc.setMaxSizeConfig(msc);
            TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
            final HazelcastInstance[] instances = factory.newInstances(cfg);
            IMap<Object, Object> map = instances[0].getMap(mapName);
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
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMapRecordEviction() throws InterruptedException {
        final String mapName = randomMapName();
        final int size = 100;
        final AtomicInteger entryEvictedEventCount = new AtomicInteger(0);
        final Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setTimeToLiveSeconds(1);
        mc.addEntryListenerConfig(new EntryListenerConfig().setImplementation(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                entryEvictedEventCount.incrementAndGet();
            }
        }).setLocal(true));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(cfg);

        final IMap map = instance.getMap(mapName);
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
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setMaxIdleSeconds(maxIdleSeconds);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(cfg);
        final IMap map = instance.getMap(mapName);

        //populate map.
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        assertSizeEventually(0, map);
    }

    @Test
    public void testZeroResetsTTL() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testZeroResetsTTL");
        int ttl = 5;
        mc.setTimeToLiveSeconds(ttl);
        HazelcastInstance instance = createHazelcastInstance(cfg);
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
        Config cfg = new Config();
        cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1");
        final String name = "testMapRecordIdleEvictionOnMigration";
        MapConfig mc = cfg.getMapConfig(name);
        int maxIdleSeconds = 30;
        int size = 100;
        final int nsize = size / 5;
        mc.setMaxIdleSeconds(maxIdleSeconds);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        final IMap map = instance1.getMap(name);
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

        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = factory.newHazelcastInstance(cfg);

        //wait until eviction is complete
        assertOpenEventually(latch, 240);

        assertSizeEventually(nsize, map);
    }

    @Test
    public void testMapPutTTLWithListener() throws InterruptedException {
        final HazelcastInstance instance = createHazelcastInstance();

        final int putCount = 100;
        final CountDownLatch latch = new CountDownLatch(putCount);
        final IMap map = instance.getMap("testMapPutTTLWithListener");

        final AtomicBoolean error = new AtomicBoolean(false);
        final Set<Long> times = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(final EntryEvent event) {
                latch.countDown();
            }
        }, true);

        int ttl = (int) (Math.random() * 3000);
        for (int j = 0; j < putCount; j++) {
            map.put(j, j, ttl, TimeUnit.MILLISECONDS);
        }

        // wait until eviction is completed.
        assertOpenEventually(latch);
    }

    /**
     * Test for issue 614
     *
     * @throws InterruptedException
     */
    @Test
    public void testContainsKeyShouldDelayEviction() throws InterruptedException {
        Config cfg = new Config();
        String mapname = "testContainsKeyShouldDelayEviction";
        cfg.getMapConfig(mapname).setMaxIdleSeconds(3);
        HazelcastInstance instance = createHazelcastInstance(cfg);
        final IMap<Object, Object> map = instance.getMap(mapname);
        map.put(1, 1);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(map.containsKey(1));
            }
        }, 5);
    }

    @Test
    public void testIssue1085EvictionBackup() throws InterruptedException {
        final String mapName = randomMapName();
        int entryCount = 10;
        Config config = new Config();
        config.getMapConfig(mapName).setTimeToLiveSeconds(3);
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(config);

        final CountDownLatch latch = new CountDownLatch(entryCount);

        final IMap map = instances[0].getMap(mapName);
        map.addEntryListener(new EntryAdapter() {
            @Override
            public void entryEvicted(EntryEvent event) {
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
     *
     * @throws Exception
     */
    @Test
    public void testEvictionAfterRemove() throws InterruptedException {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");
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

        final Config config = newConfig(mapName, maxSize, MaxSizeConfig.MaxSizePolicy.PER_NODE);

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

    private static Config newConfig(String mapName, int maxSize, MaxSizeConfig.MaxSizePolicy maxSizePolicy) {
        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig(mapName + "*");
        final MaxSizeConfig maxSizeConfig = new MaxSizeConfig(maxSize, maxSizePolicy);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setMinEvictionCheckMillis(0);
        config.addMapConfig(mapConfig);

        return config;
    }


    private static Config newConfigWithExpiration(String mapName, int maxIdleSeconds) {
        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig(mapName + "*");
        mapConfig.setMaxIdleSeconds(maxIdleSeconds);
        config.addMapConfig(mapConfig);
        return config;
    }


    /**
     * Test for the issue 2659.
     * Eviction event is fired for an object already removed
     *
     * @throws Exception
     */
    @Test
    public void testEvictionForNanosTTL() throws InterruptedException {
        final IMap<String, String> map = createHazelcastInstance().getMap(randomMapName());
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
        final String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, Integer> map = instance.getMap(mapName);
        map.put(1, 1, 100, TimeUnit.MILLISECONDS);
        final EntryView<Integer, Integer> entryView = map.getEntryView(1);
        final long expirationTime = entryView.getExpirationTime();

        assertTrue(expirationTime > now);
    }

    private IMap<Integer, Integer> getMapWithExpiredKeys() {
        final String mapName = randomMapName();
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, Integer> map = instance.getMap(mapName);
        map.put(1, 1, 100, TimeUnit.MILLISECONDS);
        map.put(2, 1, 100, TimeUnit.MILLISECONDS);
        map.put(3, 1, 100, TimeUnit.MILLISECONDS);
        sleepSeconds(1);
        return map;
    }

    @Test
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

        assertOpenEventually(evictedEntryLatch);
        // sleep some seconds to be sure that
        // we did not receive more than expected number of events.
        sleepSeconds(10);
        assertEquals(numberOfEntriesToBeAdded, count.get());
    }

    private IMap<Integer, Integer> createMapWithReadBackupDataEnabled(int maxIdleSeconds) {
        final String mapName = randomMapName();

        Config config = new Config();
        config.getMapConfig(mapName).setMaxIdleSeconds(maxIdleSeconds).setReadBackupData(true);

        TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] hazelcastInstances = hazelcastInstanceFactory.newInstances(config);

        return hazelcastInstances[0].getMap(mapName);
    }

    private IMap<Integer, String> createSimpleMap() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();
        return hazelcastInstance.getMap(randomMapName());
    }

    @Test
    public void testBackupExpirationDelay_notAffectExpiration_onOwnerPartitions() throws Exception {
        final int numberOfItemsToBeAdded = 1000;
        final int expectedEntryCountAfterExpirationOnOwnerPartitions = 0;

        testExpirationDelay(expectedEntryCountAfterExpirationOnOwnerPartitions, numberOfItemsToBeAdded, false);
    }

    @Test
    public void testBackupExpirationDelay_preventsSweepOfEntries_onBackupPartitions() throws Exception {
        final int numberOfItemsToBeAdded = 1000;
        final int expectedEntryCountAfterExpirationOnBackupPartitions = numberOfItemsToBeAdded;

        testExpirationDelay(expectedEntryCountAfterExpirationOnBackupPartitions, numberOfItemsToBeAdded, true);
    }

    private void testExpirationDelay(final int expectedEntryCountAfterExpiration, final int numberOfItemsToBeAdded, final boolean backup) {
        // node count should be at least 2 since we are testing a scenario on backups.
        final int nodeCount = 2;
        final int maxIdleSeconds = 1;
        final String mapName = randomMapName();

        final Config config = newConfigWithExpiration(mapName, maxIdleSeconds);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        final IMap<Integer, Integer> map1 = instances[0].getMap(mapName);
        final IMap<Integer, Integer> map2 = instances[1].getMap(mapName);

        for (int i = 0; i < numberOfItemsToBeAdded; i++) {
            map1.put(i, i);
        }
        // 1. Wait for idle expiration.
        sleepSeconds(1);

        assertTrueEventually(new AssertTask() {

            @Override
            public void run() throws Exception {
                // 2. On backups expiration has 10 seconds delay. So entries on backups should be there.
                // but on owners they should be expired.
                final long now = Clock.currentTimeMillis();

                final int notExpiredEntryCountOnNode1 = getNotExpiredEntryCount(map1, now, backup);
                final int notExpiredEntryCountOnNode2 = getNotExpiredEntryCount(map2, now, backup);

                assertEquals(expectedEntryCountAfterExpiration,
                        notExpiredEntryCountOnNode1 + notExpiredEntryCountOnNode2);
            }
        });


    }


    private int getNotExpiredEntryCount(IMap map, long now, boolean backup) {
        int count = 0;
        final MapProxyImpl mapProxy = (MapProxyImpl) map;
        final MapService mapService = (MapService) mapProxy.getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            final Address owner = partitionService.getPartitionOwner(i);
            if (!nodeEngine.getThisAddress().equals(owner) && backup
                    || nodeEngine.getThisAddress().equals(owner) && !backup) {
                final PartitionContainer container = mapServiceContext.getPartitionContainer(i);
                if (container == null) {
                    continue;
                }
                final RecordStore recordStore = container.getRecordStore(map.getName());
                final DefaultRecordStore defaultRecordStore = (DefaultRecordStore) recordStore;
                final Iterator<Record> iterator = defaultRecordStore.iterator(now, true);
                while (iterator.hasNext()) {
                    iterator.next();
                    count++;
                }
            }
        }
        return count;
    }

}
