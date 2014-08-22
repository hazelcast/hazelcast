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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    /**
     * Test for the issue 477.
     * Updates should also update the TTL
     *
     * @throws Exception
     */
    @Test
    public void testMapPutWithTTL() throws Exception {
        int n = 1;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        IMap<Integer, String> map = factory.newHazelcastInstance(null).getMap("testMapPutWithTTL");
        map.put(1, "value0", 100, TimeUnit.MILLISECONDS);
        assertEquals(true, map.containsKey(1));
        Thread.sleep(2500);
        assertEquals(false, map.containsKey(1));
        map.put(1, "value1", 10, TimeUnit.SECONDS);
        assertEquals(true, map.containsKey(1));
        Thread.sleep(5000);
        assertEquals(true, map.containsKey(1));
        map.put(1, "value2", 10, TimeUnit.SECONDS);
        Thread.sleep(2000);
        assertEquals(true, map.containsKey(1));
        map.put(1, "value3", 10, TimeUnit.SECONDS);
        assertEquals(true, map.containsKey(1));
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
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
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
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
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
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
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
        System.out.println(map.size());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(map.size() < size);
            }
        });
    }


    @Test
    public void testEvictionLRU() {
        final int k = 2;
        final int size = 10000;

        final String mapName = "testEvictionLRU";
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mc.setEvictionPercentage(10);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        IMap<Object, Object> map = instances[0].getMap(mapName);
        sleepSeconds(1);

        for (int i = size / 2; i < size; i++) {
            map.put(i, i);
        }
        sleepSeconds(2);
        for (int i = 0; i < size / 2; i++) {
            map.put(i, i);
        }
        sleepSeconds(1);

        int recentlyUsedEvicted = 0;
        for (int i = 0; i < size / 2; i++) {
            if (map.get(i) == null) {
                recentlyUsedEvicted++;
            }
        }
        assertTrue(recentlyUsedEvicted == 0);
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
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
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
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LFU);
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
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LFU);
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
            mc.setEvictionPolicy(MapConfig.EvictionPolicy.LFU);
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
        final int size = 1000;
        final CountDownLatch latch = new CountDownLatch(size);

        final Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setTimeToLiveSeconds(1);
        mc.addEntryListenerConfig(new EntryListenerConfig().setImplementation(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }).setLocal(true));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(cfg);

        final IMap map = instances[0].getMap(mapName);
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //wait until eviction is complete
        assertOpenEventually("Can not complete eviction map size is " + map.size(), latch, 300);
        assertSizeEventually(0, map);
    }

    @Test(timeout = 120000)
    public void testMapRecordIdleEviction() throws InterruptedException {
        final String mapName = randomMapName("testMapRecordIdleEviction");
        final int maxIdleSeconds = 5;
        final int size = 1000;
        final int expectedEntryCountAfterIdleEviction = size / 100;
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setMaxIdleSeconds(maxIdleSeconds);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(cfg);
        final IMap map = instance.getMap(mapName);
        final CountDownLatch latch = new CountDownLatch(size - expectedEntryCountAfterIdleEviction);
        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, false);
        //populate map.
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //use some entries.
        for (; ; ) {
            for (int i = 0; i < expectedEntryCountAfterIdleEviction; i++) {
                map.get(i);
            }
            if (latch.getCount() == 0) {
                break;
            }
        }
        assertSizeEventually(expectedEntryCountAfterIdleEviction, map);
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
        // fill map with (2 * maxSize) items.
        for (int i = 0; i < 2 * maxSize; i++) {
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
        mapConfig.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mapConfig.setMinEvictionCheckMillis(0);
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


}
