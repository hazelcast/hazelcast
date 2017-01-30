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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Verifies how well Hazelcast is able to deal with a cluster where members
 * are joining and leaving all the time, so partitions are moving.
 *
 * In this cluster, partitioned calls are made and therefor it happens
 * frequently that calls are sent to the wrong machine.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class RepartitioningStressTest extends HazelcastTestSupport {

    private static final int INITIAL_MEMBER_COUNT = 5;
    private static final int THREAD_COUNT = 10;
    private static final int DURATION_SECONDS = 120;

    private BlockingQueue<HazelcastInstance> queue = new LinkedBlockingQueue<HazelcastInstance>();

    private TestHazelcastInstanceFactory instanceFactory;
    private Config config;
    private HazelcastInstance hz;

    private RestartThread restartThread;

    @Before
    public void setUp() {
        Hazelcast.shutdownAll();

        instanceFactory = createHazelcastInstanceFactory(10000);
        config = new Config();
        config.getGroupConfig().setName(generateRandomString(10));
        MapConfig mapConfig = new MapConfig("map");
        //mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);
        hz = createHazelcastInstance();

        for (int i = 0; i < INITIAL_MEMBER_COUNT; i++) {
            queue.add(createHazelcastInstance());
        }

        restartThread = new RestartThread();
    }

    @After
    public void tearDown() throws InterruptedException {
        restartThread.stopAndJoin();
        Hazelcast.shutdownAll();
    }

    @Override
    public HazelcastInstance createHazelcastInstance() {
        return instanceFactory.newHazelcastInstance(config);
    }

    @Test
    @Ignore(value = "https://github.com/hazelcast/hazelcast/issues/3683")
    public void callWithBackups() throws Exception {
        int itemCount = 10000;
        ConcurrentMap<Integer, Integer> map = hz.getMap("map");

        for (int i = 0; i < itemCount; i++) {
            map.put(i, 0);
        }

        restartThread.start();

        UpdateThread[] testThreads = new UpdateThread[THREAD_COUNT];
        for (int i = 0; i < testThreads.length; i++) {
            testThreads[i] = new UpdateThread(i, itemCount, map);
            testThreads[i].start();
        }

        sleepSeconds(DURATION_SECONDS);

        for (TestThread thread : testThreads) {
            thread.join(TimeUnit.MINUTES.toMillis(1));
            thread.assertDiedPeacefully();
        }

        int[] expectedValues = new int[itemCount];
        for (UpdateThread t : testThreads) {
            for (int i = 0; i < itemCount; i++) {
                expectedValues[i] += t.values[i];
            }
        }

        for (int i = 0; i < itemCount; i++) {
            int expected = expectedValues[i];
            int found = map.get(i);
            assertEquals("value not the same", expected, found);
        }
    }

    @Test
    public void callWithoutBackups() throws Exception {
        final int itemCount = 10000;
        final Map<Integer, Integer> map = hz.getMap("map");

        for (int i = 0; i < itemCount; i++) {
            map.put(i, i);
        }

        restartThread = new RestartThread();
        restartThread.start();

        TestThread[] testThreads = new TestThread[THREAD_COUNT];
        for (int i = 0; i < testThreads.length; i++) {
            testThreads[i] = new TestThread("GetThread-" + i) {
                @Override
                void doRun() {
                    long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SECONDS);

                    Random random = new Random();
                    while (true) {
                        int key = random.nextInt(itemCount);
                        assertEquals(new Integer(key), map.get(key));
                        if (System.currentTimeMillis() > endTime) {
                            break;
                        }
                    }
                }
            };
            testThreads[i].start();
        }

        sleepSeconds(DURATION_SECONDS);

        for (TestThread thread : testThreads) {
            thread.join(TimeUnit.MINUTES.toMillis(1));
            thread.assertDiedPeacefully();
        }
    }

    private abstract class TestThread extends Thread {

        private volatile Throwable throwable;

        protected TestThread(String name) {
            super(name);
        }

        @Override
        public final void run() {
            try {
                doRun();
            } catch (Throwable t) {
                throwable = t;
                t.printStackTrace();
            }
        }

        abstract void doRun();

        void assertDiedPeacefully() {
            assertFalse(isAlive());

            if (throwable != null) {
                throwable.printStackTrace();
                fail(getName() + " failed with an exception: " + throwable.getMessage());
            }
        }
    }

    public class RestartThread extends Thread {

        private volatile boolean stopRequested;

        @Override
        public void run() {
            while (!stopRequested) {
                try {
                    sleepSeconds(10);
                    HazelcastInstance hz = queue.take();
                    hz.shutdown();
                    if (!Thread.interrupted()) {
                        queue.add(createHazelcastInstance());
                    }
                } catch (InterruptedException ignored) {
                }
            }
        }

        public void stopAndJoin() throws InterruptedException {
            stopRequested = true;
            interrupt();
            join();
        }
    }

    private class UpdateThread extends RepartitioningStressTest.TestThread {

        private final int itemCount;
        private final ConcurrentMap<Integer, Integer> map;
        private final int[] values;

        UpdateThread(int id, int itemCount, ConcurrentMap<Integer, Integer> map) {
            super("Thread-" + id);
            this.itemCount = itemCount;
            this.map = map;
            this.values = new int[itemCount];
        }

        @Override
        void doRun() {
            long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SECONDS);

            Random random = new Random();
            while (true) {
                int key = random.nextInt(itemCount);
                int increment = random.nextInt(100);
                values[key] += increment;

                while (true) {
                    Integer value = map.get(key);
                    if (value == null) {
                        value = 0;
                    }
                    if (map.replace(key, value, value + increment)) {
                        break;
                    }
                }

                if (System.currentTimeMillis() > endTime) {
                    break;
                }
            }
        }
    }
}
