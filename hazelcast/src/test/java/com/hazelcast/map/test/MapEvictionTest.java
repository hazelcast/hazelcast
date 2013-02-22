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

package com.hazelcast.map.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapEvictionTest {

    @Before
    @After
    public void shutdown() {
        Hazelcast.shutdownAll();
    }

    //TODO eviction can not keep up with puts
    //P.S. when there is 1 milliseconds between puts, test does not fail
    @Test
    public void testEvictionSpeedTest() {
        final int k = 3;
        final int size = 1700;
        final CountDownLatch latch = new CountDownLatch(k);
        final String mapName = "testEvictionSpeedTest";
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mc.setEvictionPercentage(25);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_JVM);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
//        final HazelcastInstance[] instances = new HazelcastInstance[k];
        instances[0] = Hazelcast.newHazelcastInstance(cfg);
        instances[1] = Hazelcast.newHazelcastInstance(cfg);
        instances[2] = Hazelcast.newHazelcastInstance(cfg);

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                while (latch.getCount() != 0) {
                    try {
//                        System.out.println("Checking");
//                        System.out.println("Actual Size " + map.size());
//                        System.out.println("Max Eviction Size " + size * k);
//                        System.out.println("Fault tolerant Size " + (size * k + size * k * 20 / 100));
                        assertTrue(map.size() <= (size * k + size * k * 20 / 100));
                        Thread.sleep(4000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

            }
        }.start();

        for (int i = 0; i < k; i++) {
            final IMap map = instances[i].getMap(mapName);
            new Thread() {
                public void run() {
                    for (int j = 0; j < 100000; j++) {
                        map.put(j + k * 1000000, j);
//                        try {
//                            Thread.sleep(1);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
                    }
//                    System.out.println("done");
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
    public void TestEvictionLRU() {
        final int k = 1;
        final int size = 2000;

        final String mapName = "TestEvictionLRU";
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mc.setEvictionPercentage(25);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_JVM);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
        IMap<Object, Object> map = instances[0].getMap(mapName);

        for (int i = size / 2; i < size; i++) {
            map.put(i, i);
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < size / 2; i++) {
            map.put(i, i);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        boolean isRecentlyUsedEvicted = false;
        for (int i = 0; i < size / 2; i++) {
            if (map.get(i) == null) {
                isRecentlyUsedEvicted = true;
                break;
            }
        }
        if (isRecentlyUsedEvicted) {
            for (int i = size / 2; i < size; i++) {
                Assert.assertEquals(null, map.get(i));
            }
        }
        instances[0].getLifecycleService().shutdown();

    }

    @Test
    public void TestEvictionLFU() {
        final int k = 1;
        final int size = 2000;

        final String mapName = "TestEvictionLFU";
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig(mapName);
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LFU);
        mc.setEvictionPercentage(25);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_JVM);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);

        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
        IMap<Object, Object> map = instances[0].getMap(mapName);

        for (int i = 0; i < size / 2; i++) {
            map.put(i, i);
            map.put(i, i + 1);
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = size / 2; i < size; i++) {
            map.put(i, i);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(map.size());
        Assert.assertFalse("No eviction!?!?!?", map.size() == size);
        boolean isLeastFrequentlyUsedEvicted = false;
        for (int i = size / 2; i < size; i++) {
            if (map.get(i) == null) {
                isLeastFrequentlyUsedEvicted = true;
                break;
            }
        }
        if (isLeastFrequentlyUsedEvicted) {
            for (int i = 0; i < size / 2; i++) {
                Assert.assertEquals(null, map.get(i));
            }
        }
        instances[0].getLifecycleService().shutdown();

    }

    @Test
    public void testMapWideEviction() throws InterruptedException {
        int size = 10000;

        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapWideEviction");
        mc.setEvictionPolicy(MapConfig.EvictionPolicy.LRU);
        mc.setEvictionPercentage(10);
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_JVM);
        msc.setSize(size);
        mc.setMaxSizeConfig(msc);
        HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, 2);

        IMap map = instances[0].getMap("testMapWideEviction");
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        Thread.sleep(2000);

        assertTrue((map.size() / 2) <= (size * (100 - mc.getEvictionPercentage()) / 100));
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMapRecordEviction() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapRecordEviction");
        mc.setTimeToLiveSeconds(1);

        HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, 2);

        IMap map = instances[0].getMap("testMapRecordEviction");
        int size = 10000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        int nsize = 20;
        for (int i = 0; i < nsize; i++) {
            map.put(size + i, size + i, 1000, TimeUnit.SECONDS);
        }
        for (int i = 0; i < nsize; i++) {
            map.put(i, i, 1, TimeUnit.SECONDS);
        }
        Thread.sleep(3000);

        assertEquals(map.size(), nsize);
        Hazelcast.shutdownAll();
    }

    // todo below test fails!!!
    @Test
    public void testMapRecordIdleEviction() throws InterruptedException {
        Config cfg = new Config();
        MapConfig mc = cfg.getMapConfig("testMapRecordIdleEviction");
        mc.setMaxIdleSeconds(6);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(cfg);

        IMap map = instance.getMap("testMapRecordIdleEviction");
        int size = 100;
        long cur = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        long lasts = System.currentTimeMillis() - cur;
        System.out.println("lasts:" + lasts);
        Thread.sleep(5000 - lasts);
        int nsize = 20;
        for (int i = 0; i < nsize; i++) {
            map.get(size - i);
        }
        Thread.sleep(3000);
        Assert.assertEquals(nsize, map.size());
        for (int i = 0; i < 3000; i++) {
            System.out.println("turn:" + i + " size:" + nsize + ":" + map.size());
            Thread.sleep(1000);

        }
//        instances[0].getLifecycleService().shutdown();
//        Hazelcast.shutdownAll();
    }

    @Test
    public void testMapPutTtl() throws InterruptedException {

        Config cfg = new Config();
        HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, 2);
        final IMap map = instances[0].getMap("testMapEvictionTtl");

        for (int i = 0; i < 5; i++) {

            int ttl = (int) (Math.random() * 5000);
            for (int j = 0; j < 100000; j++) {
                map.put(j, j, ttl, TimeUnit.MILLISECONDS);
            }
            Thread.sleep(ttl + 2000);
            Assert.assertEquals(0, map.size());
        }
    }

    //TODO this test also fails. TTL evictions is late more than 2 seconds
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
                long timeDifference = System.currentTimeMillis() - (Long) (event.getValue());
                Assert.assertTrue(2000 > timeDifference && timeDifference >= 0);
            }
        }, true);

        for (int i = 0; i < k; i++) {
            final int threadId = i;
            new Thread() {
                public void run() {
                    int ttl = (int) (Math.random() * 5000 + 1000);
                    for (int j = 0; j < 10000; j++) {
                        map.put(j + 10000 * threadId, ttl + System.currentTimeMillis(), ttl, TimeUnit.MILLISECONDS);
                    }
                    countDownLatch.countDown();
                }
            }.start();
        }
        countDownLatch.await();
        Thread.sleep(7000);
    }


}
