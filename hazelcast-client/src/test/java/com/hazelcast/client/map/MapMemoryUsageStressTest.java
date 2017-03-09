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

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//https://github.com/hazelcast/hazelcast/issues/2138
@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapMemoryUsageStressTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private final ILogger logger = Logger.getLogger(MapMemoryUsageStressTest.class);
    private HazelcastInstance client;

    @Before
    public void launchHazelcastServer() {
        Config config = getConfig();
        hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void shutdownHazelcastServer() {
        hazelcastFactory.terminateAll();
    }

    @Test(timeout = 30 * 60 * 1000)
    public void voidCacher() throws Exception {
        final AtomicInteger counter = new AtomicInteger(200000);
        final AtomicInteger errors = new AtomicInteger();
        Thread[] threads = new Thread[8];
        for (int k = 0; k < threads.length; k++) {
            StressThread stressThread = new StressThread(counter, errors);
            threads[k] = stressThread;
            stressThread.start();
        }

        assertJoinable(TimeUnit.MINUTES.toSeconds(30), threads);
        assertEquals(0, errors.get());
        assertTrue(counter.get() <= 0);
    }

    private class StressThread extends Thread {
        private final AtomicInteger counter;
        private final AtomicInteger errors;

        public StressThread(AtomicInteger counter, AtomicInteger errors) {
            this.counter = counter;
            this.errors = errors;
        }

        @Override
        public void run() {
            try {
                for (; ; ) {
                    int index = counter.decrementAndGet();
                    if (index <= 0) {
                        return;
                    }

                    IMap<Object, Object> map = client.getMap("juka" + index);
                    map.set("aaaa", "bbbb");
                    map.clear();
                    map.destroy();

                    if (index % 1000 == 0) {
                        logger.info("At: " + index);
                    }
                }
            } catch (Throwable t) {
                errors.incrementAndGet();
                t.printStackTrace();
            }
        }
    }
}
