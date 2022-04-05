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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;

/**
 * Test for issue https://github.com/hazelcast/hazelcast/issues/2138
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapMemoryUsageStressTest extends HazelcastTestSupport {

    private final ILogger logger = Logger.getLogger(MapMemoryUsageStressTest.class);
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @Before
    public void launchHazelcastServer() {
        hazelcastFactory.newHazelcastInstance(getConfig());
        client = hazelcastFactory.newHazelcastClient(getClientConfig());
    }

    @After
    public void shutdownHazelcastServer() {
        hazelcastFactory.terminateAll();
    }

    @Test(timeout = 30 * 60 * 1000)
    public void voidCacher() {
        AtomicInteger counter = new AtomicInteger(200000);
        AtomicInteger errors = new AtomicInteger();
        List<String> errorList = synchronizedList(new LinkedList<String>());

        Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            StressThread stressThread = new StressThread(counter, errors, errorList);
            threads[i] = stressThread;
            stressThread.start();
        }

        assertJoinable(MINUTES.toSeconds(30), threads);
        assertEqualsStringFormat("Expected %d errors, but got %d (" + errorList + ")", 0, errors.get());
        assertTrue(format("Expected the counter to be <= 0, but was %d", counter.get()), counter.get() <= 0);
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private class StressThread extends Thread {

        private final AtomicInteger counter;
        private final AtomicInteger errors;
        private final List<String> errorList;

        StressThread(AtomicInteger counter, AtomicInteger errors, List<String> errorList) {
            this.counter = counter;
            this.errors = errors;
            this.errorList = errorList;
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
                errorList.add(t.getClass().getSimpleName() + ": " + t.getMessage());
                t.printStackTrace();
            }
        }
    }
}
