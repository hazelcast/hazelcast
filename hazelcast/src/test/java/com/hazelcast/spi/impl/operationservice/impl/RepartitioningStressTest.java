/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Verifies how well Hazelcast is able to deal with a cluster where members are joining and leaving all the time, so partitions
 * are moving.
 * <p>
 * In this cluster, partitioned calls are made and therefor it happens frequently that calls are sent to the wrong machine.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class RepartitioningStressTest extends HazelcastTestSupport {

    private static final int DUPLICATE_OPS_TOLERANCE = 10;

    private static final int INITIAL_MEMBER_COUNT = 5;
    private static final int THREAD_COUNT = 10;
    private static final int DURATION_SECONDS = 120;

    private BlockingQueue<HazelcastInstance> queue = new LinkedBlockingQueue<HazelcastInstance>();

    private TestHazelcastInstanceFactory instanceFactory;
    private Config config;
    private HazelcastInstance hz;
    private final AtomicLong updateCounter = new AtomicLong();
    private final AtomicLong updateCounterInListener = new AtomicLong();

    private RestartThread restartThread;

    @Before
    public void setUp() {
        Hazelcast.shutdownAll();

        instanceFactory = createHazelcastInstanceFactory(10000);
        config = new Config();
        config.setClusterName(generateRandomString(10));
        MapConfig mapConfig = new MapConfig("map");
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
    public void replaceUpdatesAtLeastOnce() throws Exception {
        int itemCount = 10000;
        IMap<Integer, Integer> map = hz.getMap("map");

        for (int i = 0; i < itemCount; i++) {
            map.put(i, 0);
        }
        map.addEntryListener((EntryUpdatedListener<Integer, Integer>) event -> updateCounterInListener.incrementAndGet(), true);

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

        assertEqualsWithDuplicatesTolerance("Unexpected count of updates seen in listener", updateCounter.get(),
                updateCounterInListener.get());

        int[] expectedValues = new int[itemCount];
        for (UpdateThread t : testThreads) {
            for (int i = 0; i < itemCount; i++) {
                expectedValues[i] += t.values[i].get();
            }
        }

        for (int i = 0; i < itemCount; i++) {
            assertEqualsWithDuplicatesTolerance("Unexpected value for key " + i, expectedValues[i], map.get(i));
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

    private void assertEqualsWithDuplicatesTolerance(String msg, long expected, long actual) {
        if (actual < expected || actual > expected + DUPLICATE_OPS_TOLERANCE) {
            fail(String.format("%s, expected: %d, actual %d, tolerance for duplicates: %d", msg, expected,
                    actual, DUPLICATE_OPS_TOLERANCE));
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
        private final IMap<Integer, Integer> map;
        private final AtomicInteger[] values;

        UpdateThread(int id, int itemCount, IMap<Integer, Integer> map) {
            super("Thread-" + id);
            this.itemCount = itemCount;
            this.map = map;
            this.values = new AtomicInteger[itemCount];
            for (int i = 0; i < itemCount; i++) {
                this.values[i] = new AtomicInteger(0);
            }
        }

        @Override
        void doRun() {
            long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SECONDS);

            Random random = new Random();
            int key = random.nextInt(itemCount);
            do {
                Integer value = map.get(key);

                if (map.replace(key, value, value + 1)) {
                    values[key].incrementAndGet();
                    updateCounter.incrementAndGet();
                    key = random.nextInt(itemCount);
                }
            } while (System.currentTimeMillis() < endTime);
        }
    }
}
