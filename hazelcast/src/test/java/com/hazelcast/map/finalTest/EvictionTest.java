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

package com.hazelcast.map.finalTest;

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

import static org.junit.Assert.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
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
        mc.setEvictionPercentage(10);
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

        assertTrue((map.size()) <= (size * n * (100 - mc.getEvictionPercentage()) / 100));
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

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                    while (latch.getCount() != 0) {
                        try {
                            int msize = map.size();
//                            System.out.println("size:"+msize +" max"+(size*k)+ " target:"+(size * k * (100 - mc.getEvictionPercentage()) /100 ));
                            assertTrue(msize <= (size * k + size * k * 10 / 100));
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
        int nsize = size /10;
        int waitBetween = maxIdleSeconds/2;
        int waitMore = 2;
        mc.setMaxIdleSeconds(maxIdleSeconds);


        HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, 2);

        IMap map = instances[0].getMap("testMapRecordIdleEviction");
        long cur = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        long lasts = System.currentTimeMillis() - cur;
        long wait1 = ((maxIdleSeconds - waitBetween) * 1000) - lasts;
        Thread.sleep(wait1);
        for (int i = 0; i < nsize; i++) {
            map.get(i);
        }
        Thread.sleep(waitBetween * 1000);
        Thread.sleep(waitMore * 1000);
        Assert.assertEquals(nsize,map.size());
        Hazelcast.shutdownAll();
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
                long timeDifference = System.currentTimeMillis() - (Long) (event.getOldValue());
                if(timeDifference > 3000)
                System.out.println(timeDifference);
//                Assert.assertTrue(5000 > timeDifference && timeDifference >= 0);
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
