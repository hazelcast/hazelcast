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
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.Clock;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class EvictionTest {

    static final Config cfg = new Config();
    static final int instanceCount = 2;
    static final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, instanceCount);
    static final Random rand = new Random(Clock.currentTimeMillis());


    private HazelcastInstance getInstance() {
        return instances[rand.nextInt(instanceCount)];
    }

    //TODO eviction can not keep up with puts
    //P.S. when there is 1 milliseconds between puts, test does not fail
    @Test
    public void testEvictionSpeedTest() {
        final int k = 3;
        final int size = 2000;
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

        Hazelcast.shutdownAll();

        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(cfg, k);
//        final HazelcastInstance[] instances = new HazelcastInstance[k];
//        instances[0] = Hazelcast.newHazelcastInstance(cfg);
//        instances[1] = Hazelcast.newHazelcastInstance(cfg);
//        instances[2] = Hazelcast.newHazelcastInstance(cfg);

        new Thread() {
            final IMap map = instances[0].getMap(mapName);

            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                while (latch.getCount() != 0) {
                    try {
//                        System.out.println("Checking");
//                        System.out.println("Actual Size " + map.size());
//                        System.out.println("Max Eviction Size " + size * k);
//                        System.out.println("Fault tolerant Size " + (size * k + size * k * 20 / 100));
                        System.out.println("size:"+ map.size());
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

}
