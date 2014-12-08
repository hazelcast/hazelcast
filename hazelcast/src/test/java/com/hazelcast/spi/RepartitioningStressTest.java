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

package com.hazelcast.spi;

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

import java.io.IOException;
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
 * A test that verifies how well Hazelcast is able to deal with a cluster where
 * members are joining an leaving all the time, so partitions are moving. On this
 * cluster, partitioned calls are made and therefor it happens frequently that calls
 * are send to the wrong machine.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class RepartitioningStressTest extends HazelcastTestSupport {

    private BlockingQueue<HazelcastInstance> queue = new LinkedBlockingQueue<HazelcastInstance>();
    private HazelcastInstance hz;
    private TestHazelcastInstanceFactory instanceFactory;

    private final static long DURATION_SECONDS = 120;
    private final static int THREAD_COUNT = 10;
    private Config config;

    @After
    public  void tearDown() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp() {
        Hazelcast.shutdownAll();

        instanceFactory = createHazelcastInstanceFactory(10000);
        config = new Config();
        MapConfig mapConfig = new MapConfig("map");
        //mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);
        hz = createHazelcastInstance();

        for (int k = 0; k < INITIAL_MEMBER_COUNT(); k++) {
            queue.add(createHazelcastInstance());
        }
    }

    private int INITIAL_MEMBER_COUNT() {
        return 5;
    }

    public HazelcastInstance createHazelcastInstance() {
        return instanceFactory.newHazelcastInstance(config);
    }

    @Test
    @Ignore // https://github.com/hazelcast/hazelcast/issues/3683
    public void callWithBackups() throws InterruptedException {
        final ConcurrentMap<Integer, Integer> map = hz.getMap("map");
        final int itemCount = 10000;

        for (int k = 0; k < itemCount; k++) {
            map.put(k, 0);
        }

        RestartThread restartThread = new RestartThread();
        restartThread.start();

        UpdateThread[] testThreads = new UpdateThread[THREAD_COUNT];
        for (int l = 0; l < testThreads.length; l++) {
            testThreads[l] = new UpdateThread(l, itemCount, map);
            testThreads[l].start();
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(DURATION_SECONDS));

        for (TestThread t : testThreads) {
            t.join(TimeUnit.MINUTES.toMillis(1));
            t.assertDiedPeacefully();
        }

        int[] expectedValues = new int[itemCount];
        for (UpdateThread t : testThreads) {
            for (int k = 0; k < itemCount; k++) {
                expectedValues[k] += t.values[k];
            }
        }

        for (int k = 0; k < itemCount; k++) {
            int expected = expectedValues[k];
            int found = map.get(k);
            assertEquals("value not the same", expected, found);
        }

        restartThread.stop = true;
    }

    @Test
    public void callWithoutBackups() throws InterruptedException {
        final Map<Integer, Integer> map = hz.getMap("map");
        final int itemCount = 10000;
        for (int k = 0; k < itemCount; k++) {
            map.put(k, k);
        }

        RestartThread restartThread = new RestartThread();
        restartThread.start();

        TestThread[] testThreads = new TestThread[THREAD_COUNT];
        for (int k = 0; k < testThreads.length; k++) {
            testThreads[k] = new TestThread("GetThread-" + k) {
                @Override
                void doRun() {
                    long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SECONDS);

                    Random random = new Random();
                    for (; ; ) {
                        int key = random.nextInt(itemCount);
                        assertEquals(new Integer(key), map.get(key));
                        if (System.currentTimeMillis() > endTime) {
                            break;
                        }
                    }
                }
            };
            testThreads[k].start();
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(DURATION_SECONDS));

        for (TestThread t : testThreads) {
            t.join(TimeUnit.MINUTES.toMillis(1));
            t.assertDiedPeacefully();
        }

        restartThread.stop = true;
    }

    private abstract class TestThread extends Thread {
        private volatile Throwable t;

        protected TestThread(String name) {
            super(name);
        }

        @Override
        public final void run() {
            try {
                doRun();
            } catch (Throwable t) {
                this.t = t;
                t.printStackTrace();
            }
        }

        abstract void doRun();

        public void assertDiedPeacefully() {
            assertFalse(isAlive());

            if (t != null) {
                t.printStackTrace();
                fail(getName() + " failed with an exception:" + t.getMessage());
            }
        }
    }

    public class RestartThread extends Thread {

        private volatile boolean stop;

        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(10000);
                    HazelcastInstance hz = queue.take();
                    hz.shutdown();
                    queue.add(createHazelcastInstance());
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private class UpdateThread extends RepartitioningStressTest.TestThread {
        private final int itemCount;
        private final ConcurrentMap<Integer, Integer> map;
        private final int[] values;

        public UpdateThread(int l, int itemCount, ConcurrentMap<Integer, Integer> map) {
            super("Thread-" + l);
            this.itemCount = itemCount;
            this.map = map;
            this.values = new int[itemCount];
        }

        @Override
        void doRun() {

            long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SECONDS);

            Random random = new Random();
            for (; ; ) {
                int key = random.nextInt(itemCount);
                int increment = random.nextInt(100);
                values[key] += increment;

                for (; ; ) {
                    Integer value = map.get(key);
                    if (value == null) value = 0;
                    if(map.replace(key, value,value + increment)){
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
