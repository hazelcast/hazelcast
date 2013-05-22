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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.*;
import com.hazelcast.test.RandomBlockJUnit4ClassRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@RunWith(RandomBlockJUnit4ClassRunner.class)
public class EvictionTest {

    @Before
    @After
    public void shutdown() {
        Hazelcast.shutdownAll();
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
        HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, n);

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

        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
        final AtomicBoolean success = new AtomicBoolean(true);

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                    while (latch.getCount() != 0) {
                        try {
                            int msize = map.size();
                            if(msize > (size * k + size * k * 10 / 100)) {
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
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
        final int pnum = instances[0].getPartitionService().getPartitions().size();

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                    while (latch.getCount() != 0) {
                        try {
                            int msize = map.size();
//                            System.out.println("size:"+msize +" max"+(size*pnum)+ " target:" + (size * pnum * (100 - mc.getEvictionPercentage()) / 100));
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
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
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

            final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
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
            Hazelcast.shutdownAll();
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

            final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
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

            System.out.println("size:" + map.size());
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

            final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
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
        int nsize = 500;
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapRecordEviction");
        mc.setTimeToLiveSeconds(1);

        HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, 2);

        IMap map = instances[0].getMap("testMapRecordEviction");
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < nsize; i++) {
            map.put(size + i, size + i, 1000, TimeUnit.SECONDS);
        }
        for (int i = 0; i < nsize; i++) {
            map.put(i, i, 1, TimeUnit.SECONDS);
        }
        Thread.sleep(2000);

        assertEquals(nsize, map.size());
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMapRecordIdleEviction() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapRecordIdleEviction");
        int maxIdleSeconds = 8;
        int size = 100;
        final int nsize = size / 10;
        mc.setMaxIdleSeconds(maxIdleSeconds);
        HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, 2);
        final IMap map = instances[0].getMap("testMapRecordIdleEviction");
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        for (int i = 0; i < nsize; i++) {
                            map.get(i);
                        }
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }).start();

        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        System.out.println("sleeping");
        Thread.sleep(maxIdleSeconds * 1000 + 5000);

//        for (Object key : map.keySet()) {
//            System.out.println("key:"+key + " value:"+map.get(key));
//        }
//        Assert.assertEquals(0, map.size());
        Assert.assertEquals(nsize, map.size());
        Hazelcast.shutdownAll();
    }

    @Test
    public void testZeroResetsTTL() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testZeroResetsTTL");
        int ttl = 3;
        mc.setTimeToLiveSeconds(ttl);
        StaticNodeFactory factory = new StaticNodeFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(cfg);
        IMap<Object, Object> map = instance.getMap("testZeroResetsTTL");
        map.put(1,1);
        map.put(2,2);
        map.put(1,2, 0, TimeUnit.SECONDS);
        Thread.sleep((ttl + 1) * 1000);
        assertNull(map.get(2));
        assertEquals(2, map.get(1));
    }

    @Test
    public void testMapRecordIdleEvictionOnMigration() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapRecordIdleEvictionOnMigration");
        int maxIdleSeconds = 5;
        int size = 1000;
        mc.setMaxIdleSeconds(maxIdleSeconds);

        StaticNodeFactory factory = new StaticNodeFactory(3);

        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        final IMap map = instance1.getMap("testMapRecordIdleEvictionOnMigration");

        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = factory.newHazelcastInstance(cfg);

        Thread.sleep(maxIdleSeconds * 1000 + 3000);

        Assert.assertEquals(0, map.size());
    }

    @Test
    public void testMapRecordIdleEvictionOnMigration2() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapRecordIdleEvictionOnMigration2");
        int maxIdleSeconds = 5;
        int size = 1000;
        final int nsize = size / 5;
        mc.setMaxIdleSeconds(maxIdleSeconds);
        StaticNodeFactory factory = new StaticNodeFactory(3);

        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        final IMap map = instance1.getMap("testMapRecordIdleEvictionOnMigration2");

        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        for (int i = 0; i < nsize; i++) {
                            map.get(i);
                        }
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }).start();
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = factory.newHazelcastInstance(cfg);

        Thread.sleep(1000);


        Thread.sleep(maxIdleSeconds * 1000 + 5000);

        Assert.assertEquals(nsize, map.size());
        Hazelcast.shutdownAll();
    }



    @Test
    public void testMapPutTtlWithListener() throws InterruptedException {
        Config cfg = new Config();
        HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, 2);
        int k = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(k);

        final IMap map = instances[0].getMap("testMapEvictionTtlWithListener");
        map.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent event) {
            }

            public void entryRemoved(EntryEvent event) {
            }

            public void entryUpdated(EntryEvent event) {
            }

            public void entryEvicted(EntryEvent event) {
                long timeDifference = System.currentTimeMillis() - (Long) (event.getOldValue());
                assertTrue(timeDifference < 3000);
            }
        }, true);

        for (int i = 0; i < k; i++) {
            final int threadId = i;
            new Thread() {
                public void run() {
                    int ttl = (int) (Math.random() * 5000 + 3000);
                    for (int j = 0; j < 10000; j++) {
                        map.put(j + 10000 * threadId, ttl + System.currentTimeMillis(), ttl, TimeUnit.MILLISECONDS);
                    }
                    countDownLatch.countDown();
                }
            }.start();
        }
        countDownLatch.await();
    }


}
