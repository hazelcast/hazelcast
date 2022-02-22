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

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertJoinable;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapTryLockConcurrentTests {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test
    public void concurrent_MapTryLockTest() throws InterruptedException {
        concurrent_MapTryLock(false);
    }

    @Test
    public void concurrent_MapTryLockTimeOutTest() throws InterruptedException {
        concurrent_MapTryLock(true);
    }

    private void concurrent_MapTryLock(boolean withTimeOut) throws InterruptedException {
        final int maxThreads = 8;
        final IMap<String, Integer> map = client.getMap(randomString());
        final String upKey = "upKey";
        final String downKey = "downKey";

        map.put(upKey, 0);
        map.put(downKey, 0);

        Thread[] threads = new Thread[maxThreads];
        for (int i = 0; i < threads.length; i++) {

            Thread t;
            if (withTimeOut) {
                t = new MapTryLockTimeOutThread(map, upKey, downKey);
            } else {
                t = new MapTryLockThread(map, upKey, downKey);
            }
            t.start();
            threads[i] = t;
        }

        assertJoinable(threads);

        int upTotal = map.get(upKey);
        int downTotal = map.get(downKey);

        assertTrue("concurrent access to locked code caused wrong total", upTotal + downTotal == 0);
    }

    static class MapTryLockThread extends TestHelper {

        MapTryLockThread(IMap map, String upKey, String downKey) {
            super(map, upKey, downKey);
        }

        public void doRun() throws Exception {
            if (map.tryLock(upKey)) {
                try {
                    if (map.tryLock(downKey)) {
                        try {
                            work();
                        } finally {
                            map.unlock(downKey);
                        }
                    }
                } finally {
                    map.unlock(upKey);
                }
            }
        }
    }

    static class MapTryLockTimeOutThread extends TestHelper {

        MapTryLockTimeOutThread(IMap map, String upKey, String downKey) {
            super(map, upKey, downKey);
        }

        public void doRun() throws Exception {
            if (map.tryLock(upKey, 1, TimeUnit.MILLISECONDS)) {
                try {
                    if (map.tryLock(downKey, 1, TimeUnit.MILLISECONDS)) {
                        try {
                            work();
                        } finally {
                            map.unlock(downKey);
                        }
                    }
                } finally {
                    map.unlock(upKey);
                }
            }
        }
    }

    abstract static class TestHelper extends Thread {

        protected static final int ITERATIONS = 1000 * 10;

        protected final Random random = new Random();
        protected final IMap<String, Integer> map;
        protected final String upKey;
        protected final String downKey;

        TestHelper(IMap map, String upKey, String downKey) {
            this.map = map;
            this.upKey = upKey;
            this.downKey = downKey;
        }

        public void run() {
            try {
                for (int i = 0; i < ITERATIONS; i++) {
                    doRun();
                }
            } catch (Exception e) {
                throw new RuntimeException("Test Thread crashed with ", e);
            }
        }

        abstract void doRun() throws Exception;

        public void work() {
            int upTotal = map.get(upKey);
            int downTotal = map.get(downKey);

            int dif = random.nextInt(1000);
            upTotal += dif;
            downTotal -= dif;

            map.put(upKey, upTotal);
            map.put(downKey, downTotal);
        }
    }
}
