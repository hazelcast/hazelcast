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
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

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
        Thread.sleep(5000);
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
        Thread.sleep(2000);
        assertEquals("value2", map.get("key"));
        h.shutdown();
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
        Thread.sleep(2000);
        assertEquals("value2", map.get("key"));
        h.shutdown();
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
        IMap<String, String> map = h.getMap("testIssue585ZeroTTLShouldPreventEvictionWithSet");
        map.set("key", "value", 1, TimeUnit.SECONDS);
        map.set("key", "value2");
        Thread.sleep(2000);
        assertEquals(0, map.size());
        h.shutdown();
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
        value.setMaxIdleSeconds(3);
        mapConfigs.put("default", value);
        c.setMapConfigs(mapConfigs);
        final Properties properties = new Properties();
        properties.setProperty("hazelcast.map.cleanup.delay.seconds", "1"); // we need faster cleanups
        c.setProperties(properties);
        int n = 1;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(c);

        IMap<String, Long> map = hazelcastInstance.getMap("testIssue304EvictionDespitePutMap");
        final AtomicInteger evictCount = new AtomicInteger(0);
        map.addEntryListener(new EntryAdapter<String, Long>() {
            public void entryEvicted(EntryEvent<String, Long> event) {
                evictCount.incrementAndGet();
            }
        }, true);


        String key = "key";
        for (int i = 0; i < 5; i++) {
            map.put(key, System.currentTimeMillis());
            Thread.sleep(1000);
        }
        assertEquals(evictCount.get(), 0);
        assertNotNull(map.get(key));
        hazelcastInstance.shutdown();
    }

    @Test
    public void testMapWideEviction() throws InterruptedException {
        int size = 10000;

        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapWideEviction");
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mc.setEvictionPercentage(25);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);
        int n = 3;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance[] instances = factory.newInstances(cfg);

        IMap map = instances[0].getMap("testMapWideEviction");
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        Thread.sleep(1200);

        assertTrue(map.size() <= (size * n * (100 - mc.getEvictionPercentage()) / 100));
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
        int insertCount = size * pnum * 2;
        Map map = instances[0].getMap(mapName);
        for (int i = 0; i < insertCount; i++) {
            map.put(i, i);
        }
        Thread.sleep(2000);
        assertTrue(map.size() < size * pnum * (100 - mc.getEvictionPercentage()) / 100);
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
    public void testEvictionLFU() {
        try {
            final int k = 1;
            final int size = 10000;

            final String mapName = "testEvictionLFU";
            Config cfg = new Config();
            MapConfig mc = cfg.getMapConfig(mapName);
            mc.setEvictionPolicy(MapConfig.EvictionPolicy.LFU);
            mc.setEvictionPercentage(20);
            MaxSizeConfig msc = new MaxSizeConfig();
            msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE);
            msc.setSize(size);
            mc.setMaxSizeConfig(msc);

            TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(k);
            final HazelcastInstance[] instances = factory.newInstances(cfg);
            IMap<Object, Object> map = instances[0].getMap(mapName);

            for (int i = 0; i < size / 2; i++) {
                map.put(i, i);
                map.get(i);
            }
            Thread.sleep(1000);
            for (int i = size / 2; i < size; i++) {
                map.put(i, i);
            }

            Thread.sleep(3000);

            Assert.assertFalse("No eviction!?!?!?", map.size() == size);
            boolean isFrequentlyUsedEvicted = false;
            for (int i = 0; i < size / 2; i++) {
                if (map.get(i) == null) {
                    isFrequentlyUsedEvicted = true;
                    break;
                }
            }
            Assert.assertFalse(isFrequentlyUsedEvicted);
            instances[0].shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testEvictionLFU2() {
        try {
            final int k = 2;
            final int size = 10000;
            final String mapName = "testEvictionLFU2";
            Config cfg = new Config();
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
    @Category(ProblematicTest.class)
    public void testMapRecordEviction() throws InterruptedException {
        int size = 1000;
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapRecordEviction");
        mc.setTimeToLiveSeconds(1);
        final CountDownLatch latch = new CountDownLatch(size);
        mc.addEntryListenerConfig(new EntryListenerConfig().setImplementation(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }).setLocal(true));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(cfg);

        IMap map = instances[0].getMap("testMapRecordEviction");
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //wait until eviction is complete
        assertOpenEventually(latch);
        assertEquals(0, map.size());
    }

    @Test
    public void testMapRecordIdleEviction() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapRecordIdleEviction");
        int maxIdleSeconds = 8;
        int size = 1000;
        final int nsize = size / 10;
        mc.setMaxIdleSeconds(maxIdleSeconds);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        final IMap map = instances[0].getMap("testMapRecordIdleEviction");
        final CountDownLatch latch = new CountDownLatch(size - nsize);
        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, false);

        final Thread thread = new Thread(new Runnable() {
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        for (int i = 0; i < nsize; i++) {
                            map.get(i);
                        }
                        Thread.sleep(100);
                    } catch (HazelcastInstanceNotActiveException e) {
                        return;
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
        thread.start();

        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        assertTrue(latch.await(1, TimeUnit.MINUTES));
        assertEquals(nsize, map.size());

        thread.interrupt();
        thread.join(5000);
    }

    @Test
    public void testZeroResetsTTL() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testZeroResetsTTL");
        int ttl = 5;
        mc.setTimeToLiveSeconds(ttl);
       HazelcastInstance instance = createHazelcastInstance(cfg);
        IMap<Object, Object> map = instance.getMap("testZeroResetsTTL");
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
        assertNull(map.get(2));
        assertEquals(2, map.get(1));
    }

    @Test
    public void testMapRecordIdleEvictionOnMigration() throws InterruptedException {
        Config cfg = new Config();
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
        Thread.sleep((maxIdleSeconds - 5) * 1000);

        // touch the ones you dont want to be evicted.
        for (int i = 0; i < nsize; i++) {
            map.get(i);
        }

        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = factory.newHazelcastInstance(cfg);

        //wait until eviction is complete
        assertOpenEventually(latch);

        assertEquals("not all idle values evicted!",nsize, map.size());

    }

    @Test
    public void testMapPutTTLWithListener() throws InterruptedException {
        Config cfg = new Config();
        final HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(cfg);
        warmUpPartitions(instances);

        final int k = 10;
        final int putCount = 1000;
        final CountDownLatch latch = new CountDownLatch(k * putCount);
        final IMap map = instances[0].getMap("testMapPutTTLWithListener");

        final AtomicBoolean error = new AtomicBoolean(false);
        final Set<Long> times = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(final EntryEvent event) {
                final Long expectedEvictionTime = (Long) (event.getOldValue());
                long timeDifference = System.currentTimeMillis() - expectedEvictionTime;
                if (timeDifference > 5000) {
                    error.set(true);
                    times.add(timeDifference);
                }
                latch.countDown();
            }
        }, true);

        for (int i = 0; i < k; i++) {
            final int threadId = i;
            int ttl = (int) (Math.random() * 5000 + 3000);
            for (int j = 0; j < putCount; j++) {
                final long expectedEvictionTime = ttl + System.currentTimeMillis();
                map.put(j + putCount * threadId, expectedEvictionTime, ttl, TimeUnit.MILLISECONDS);
            }
        }

        // wait until eviction is completed.
        assertOpenEventually(latch);
        assertFalse("Some evictions took more than 5 seconds! -> late eviction count:" + times.size(), error.get());
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
        IMap<Object, Object> map = instance.getMap(mapname);
        map.put(1, 1);
        for (int i = 0; i < 20; i++) {
            assertTrue(map.containsKey(1));
            Thread.sleep(500);
        }
    }

    @Test
    public void testIssue1085EvictionBackup() throws InterruptedException {
        Config config = new Config();
        config.getMapConfig("testIssue1085EvictionBackup").setTimeToLiveSeconds(3);

        HazelcastInstance[] instances = createHazelcastInstanceFactory(3).newInstances(config);

        int size = 1000;
        final CountDownLatch latch = new CountDownLatch(size);

        final IMap map = instances[0].getMap("testIssue1085EvictionBackup");
        map.addEntryListener(new EntryAdapter() {
            @Override
            public void entryEvicted(EntryEvent event) {
                super.entryEvicted(event);
                latch.countDown();
            }
        }, false);

        // put some sample data
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        // shutdown instances so we can check eviction happens in case backup process
        instances[1].shutdown();
        instances[2].shutdown();

        //wait until eviction is complete
        assertOpenEventually(latch);

        assertEquals("not all values evicted!",0,map.size());
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
        map.put(1, 1, 1, TimeUnit.SECONDS);
        map.put(2, 2, 1, TimeUnit.SECONDS);
        map.remove(1);
        Thread.sleep(2000);
        assertEquals(1, count.get());
    }

    /** Tests If Map operations operate as if eviction is done
     *  while eviction process is ongoing
     */
    @Test
    public void testEvictedValueOperations() throws InterruptedException {
        int size = 1000;
        Config cfg = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        final CountDownLatch latch = new CountDownLatch(size);

        IMap map = instances[0].getMap("testMapWideEviction");
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
                latch.countDown();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, true);

        for (int i = 0; i < size; i++) {
            map.put(i, i, 1, TimeUnit.SECONDS);
        }
        Thread.sleep(1000);

        for (int i = size - 1; i > -1; i--) {
            assertNull("value is not null :" + i, map.get(i));
        }

        assertTrue(map.size() == 0);
        assertTrue("all entries are evicted! test is invalid!", latch.getCount() > 0);


    }
}
