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
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class MemoryLeakTest extends HazelcastTestSupport {

    @Before
    public void gc() {
        Runtime.getRuntime().gc();
    }

    @Test
    public void testShutdownAllMemoryLeak() throws Exception {
        long usedMemoryInit = getUsedMemoryAsMB();
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance[] instances = factory.newInstances(config);
        IMap map1 = instances[0].getMap("default");
        final int size = 10000;
        for (int i = 0; i < size; i++) {
            map1.put(i, new byte[10000]);
        }
        final ExecutorService es = Executors.newFixedThreadPool(4);
        final CountDownLatch latch = new CountDownLatch(4);
        for (int a = 0; a < 4; a++) {
            final int t = a;
            es.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < size; i++) {
                        instances[t].getMap("default").get(i);
                    }
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        es.shutdown();
        assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        Hazelcast.shutdownAll();
        waitForGC(10 + usedMemoryInit, 200);
    }

    @Test
    public void testTTLAndMemoryLeak() throws Exception {
        long usedMemoryInit = getUsedMemoryAsMB();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setTimeToLiveSeconds(15);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance[] instances = factory.newInstances(config);
        IMap map1 = instances[0].getMap("default");

        final int size = 10000;
        for (int i = 0; i < size; i++) {
            map1.put(i, new byte[10000]);
        }
        long usedMemoryStart = getUsedMemoryAsMB();
        assertTrue("UsedMemoryStart: " + usedMemoryStart, usedMemoryStart > 200);
        final ExecutorService es = Executors.newFixedThreadPool(4);
        final CountDownLatch latch = new CountDownLatch(4);
        for (int a = 0; a < 4; a++) {
            final int t = a;
            es.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < size; i++) {
                        instances[t].getMap("default").get(i);
                    }
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        es.shutdown();
        assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        waitForGC(25 + usedMemoryInit, 200);
    }

    @Test
    public void testTTLAndMemoryLeak2() throws Exception {
        long usedMemoryInit = getUsedMemoryAsMB();
        Config config = new Config();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance[] instances = factory.newInstances(config);
        IMap map1 = instances[0].getMap("default");

        final int size = 10000;
        for (int i = 0; i < size; i++) {
            map1.put(i, new byte[10000], 15, TimeUnit.SECONDS);
        }
        long usedMemoryStart = getUsedMemoryAsMB();
        assertTrue("UsedMemoryStart: " + usedMemoryStart, usedMemoryStart > 200);
        final ExecutorService es = Executors.newFixedThreadPool(4);
        final CountDownLatch latch = new CountDownLatch(4);
        for (int a = 0; a < 4; a++) {
            final int t = a;
            es.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < size; i++) {
                        instances[t].getMap("default").get(i);
                    }
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        es.shutdown();
        assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        waitForGC(25 + usedMemoryInit, 200);
    }

    @Test
    public void testMaxIdleAndMemoryLeak() throws Exception {
        Runtime.getRuntime().gc();
        long usedMemoryInit = getUsedMemoryAsMB();
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setMaxIdleSeconds(15);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        final HazelcastInstance[] instances = factory.newInstances(config);
        IMap map1 = instances[0].getMap("default");

        final int size = 10000;
        for (int i = 0; i < size; i++) {
            map1.put(i, new byte[10000]);
        }
        long usedMemoryStart = getUsedMemoryAsMB();
        assertTrue("UsedMemoryStart: " + usedMemoryStart, usedMemoryStart > 200);
        final ExecutorService es = Executors.newFixedThreadPool(4);
        final CountDownLatch latch = new CountDownLatch(4);
        for (int a = 0; a < 4; a++) {
            final int t = a;
            es.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < size; i++) {
                        instances[t].getMap("default").get(i);
                    }
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        es.shutdown();
        assertTrue(es.awaitTermination(5, TimeUnit.SECONDS));
        waitForGC(25 + usedMemoryInit, 200);
    }

    private static long getUsedMemoryAsMB() {
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        return (total - free) / 1024 / 1024;
    }

    private static void waitForGC(long limit, int maxSeconds) throws InterruptedException {
        if (getUsedMemoryAsMB() < limit) {
            return;
        }
        for (int i = 0; i < maxSeconds; i++) {
            sleep(1000);
            Runtime.getRuntime().gc();
            if (getUsedMemoryAsMB() < limit) {
                return;
            }
        }
        fail(String.format("UsedMemory now: %s but expected max: %s", getUsedMemoryAsMB(), limit));
    }
}
