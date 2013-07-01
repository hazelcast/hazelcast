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

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
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
        h.getLifecycleService().shutdown();
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
        map.addEntryListener(new EntryListener<String, Long>() {
            public void entryAdded(EntryEvent<String, Long> event) {
            }

            public void entryRemoved(EntryEvent<String, Long> event) {
            }

            public void entryUpdated(EntryEvent<String, Long> event) {
            }

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
        hazelcastInstance.getLifecycleService().shutdown();
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
    public void testEvictionSpeedTest() {
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

        try {
            Assert.assertEquals(latch.await(10, TimeUnit.MINUTES), true);
            Assert.assertTrue(success.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testEvictionSpeedTestPerPartition() {
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

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                    while (latch.getCount() != 0) {
                        try {
                            int msize = map.size();
                            assertTrue(msize <= (size * pnum + size * pnum * 10 / 100));
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
                    for (int j = 0; j < 100000; j++) {
                        map.put(k + "-" + j, j);
                    }
                    latch.countDown();
                }
            }.start();
        }

        try {
            Assert.assertEquals(latch.await(10, TimeUnit.MINUTES), true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testEvictionPerPartition() throws InterruptedException {
        final int k = 2;
        final int size = 10;
        final CountDownLatch latch = new CountDownLatch(k);
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

        try {
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
            Thread.sleep(1000);

            for (int i = size / 2; i < size; i++) {
                map.put(i, i);
            }
            Thread.sleep(2000);
            for (int i = 0; i < size / 2; i++) {
                map.put(i, i);
            }
            Thread.sleep(1000);

            int recentlyUsedEvicted = 0;
            for (int i = 0; i < size / 2; i++) {
                if (map.get(i) == null) {
                    recentlyUsedEvicted++;
                }
            }
            Assert.assertTrue(recentlyUsedEvicted == 0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
            instances[0].getLifecycleService().shutdown();
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
    public void testMapRecordEviction() throws InterruptedException {
        int size = 100000;
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
        assertTrue(latch.await(10, TimeUnit.SECONDS));
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
        map.addEntryListener(new EntryAdapter(){
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        },false);

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
        latch.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(nsize, map.size());

        thread.interrupt();
        thread.join(5000);
    }

    @Test
    public void testZeroResetsTTL() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testZeroResetsTTL");
        int ttl = 5;
        mc.setTimeToLiveSeconds(ttl);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(cfg);
        IMap<Object, Object> map = instance.getMap("testZeroResetsTTL");
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter(){
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        },false);

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
        MapConfig mc = cfg.getMapConfig("testMapRecordIdleEvictionOnMigration2");
        int maxIdleSeconds = 10;
        int size = 100;
        final int nsize = size / 5;
        mc.setMaxIdleSeconds(maxIdleSeconds);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        final IMap map = instance1.getMap("testMapRecordIdleEvictionOnMigration2");
        final CountDownLatch latch = new CountDownLatch(size - nsize);
        map.addEntryListener(new EntryAdapter(){
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        },false);

        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        final Thread thread = new Thread(new Runnable() {
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        for (int i = 0; i < nsize; i++) {
                            map.get(i);
                        }
                        Thread.sleep(1000);
                    } catch (HazelcastInstanceNotActiveException e) {
                        return;
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
        thread.start();
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = factory.newHazelcastInstance(cfg);

        latch.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(nsize, map.size());

        thread.interrupt();
        thread.join(5000);
    }

    @Test
    public void testMapPutTtlWithListener() throws InterruptedException {
        Config cfg = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance[] instances = factory.newInstances(cfg);
        warmUpPartitions(instances);

        final int k = 10;
        final int putCount = 10000;
        final CountDownLatch latch = new CountDownLatch(k * putCount);
        final IMap map = instances[0].getMap("testMapEvictionTtlWithListener");

        final ExecutorService ex = Executors.newFixedThreadPool(k * 2);
        final AtomicBoolean error = new AtomicBoolean(false);
        final Set<Long> times = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

        map.addEntryListener(new EntryAdapter() {
            public void entryEvicted(final EntryEvent event) {
                ex.execute(new Runnable() {
                    public void run() {
                        final Long expectedEvictionTime = (Long) (event.getOldValue());
                        long timeDifference = System.currentTimeMillis() - expectedEvictionTime;
                        if (timeDifference > 5000) {
                            error.set(true);
                            times.add(timeDifference);
                        }
                        latch.countDown();
                    }
                });
            }
        }, true);

        for (int i = 0; i < k; i++) {
            final int threadId = i;
            ex.execute(new Runnable() {
                public void run() {
                    int ttl = (int) (Math.random() * 5000 + 3000);
                    for (int j = 0; j < putCount; j++) {
                        final long expectedEvictionTime = ttl + System.currentTimeMillis();
                        map.put(j + putCount * threadId, expectedEvictionTime, ttl, TimeUnit.MILLISECONDS);
                    }
                }
            });
        }

        try {
            assertTrue(latch.await(1, TimeUnit.MINUTES));
            assertFalse("Some evictions took more than 3 seconds! -> " + times, error.get());
        } finally {
            ex.shutdownNow();
        }
    }
}
