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

package com.hazelcast.client.flakeidgen.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class FlakeIdGenerator_ClientBackpressureTest {

    private static final ILogger LOGGER = Logger.getLogger(FlakeIdGenerator_ClientBackpressureTest.class);

    private TestHazelcastFactory factory;
    private HazelcastInstance client;

    public void before(ClientConfig config) {
        factory = new TestHazelcastFactory(1);
        factory.newHazelcastInstance();
        client = factory.newHazelcastClient(config);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void backpressureTest() {
        int batchSize = 100000;
        before(new ClientConfig().addFlakeIdGeneratorConfig(new ClientFlakeIdGeneratorConfig("gen")
                .setPrefetchCount(batchSize)));

        final FlakeIdGenerator generator = client.getFlakeIdGenerator("gen");

        // One call to newId takes little more than 1.5second worth of IDs. Maximum allowed time is 15 seconds.
        // So 10 calls should be immediate, the 11th call should cause blocking.
        // But getting the IDs also takes some time, so the test is a little heuristic. It expects some blocking
        // after 13th call.
        long testStart = System.nanoTime();
        for (int i = 1; i <= 15; i++) {
            LOGGER.info("Iteration " + i + ", elapsed since test start: " + NANOSECONDS.toMillis(System.nanoTime() - testStart) + "ms");
            long start = System.nanoTime();
            generator.newId();
            long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - start);
            LOGGER.info("newId call took " + elapsedMs + "ms");
            if (i < 10) {
                assertTrue("elapsedMs=" + elapsedMs + ", i=" + i, elapsedMs < 200);
            } else if (i >= 13) {
                assertTrue("elapsedMs=" + elapsedMs + ", i=" + i, elapsedMs > 1200);
            }

            start = System.nanoTime();
            for (int j = 1; j < batchSize; j++) {
                generator.newId();
            }
            elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue("elapsedMs=" + elapsedMs, elapsedMs < 200);
        }

        /*
        This is typical output by the test:
            Iteration 1, elapsed since test start: 0ms
            newId call took 5ms
            Iteration 2, elapsed since test start: 15ms
            newId call took 1ms
            Iteration 3, elapsed since test start: 19ms
            newId call took 1ms
            Iteration 4, elapsed since test start: 24ms
            newId call took 1ms
            Iteration 5, elapsed since test start: 28ms
            newId call took 2ms
            Iteration 6, elapsed since test start: 32ms
            newId call took 1ms
            Iteration 7, elapsed since test start: 36ms
            newId call took 1ms
            Iteration 8, elapsed since test start: 39ms
            newId call took 1ms
            Iteration 9, elapsed since test start: 43ms
            newId call took 2ms
            Iteration 10, elapsed since test start: 47ms
            newId call took 590ms
            Iteration 11, elapsed since test start: 642ms
            newId call took 1558ms
            Iteration 12, elapsed since test start: 2206ms
            newId call took 1556ms
            Iteration 13, elapsed since test start: 3765ms
            newId call took 1560ms
            Iteration 14, elapsed since test start: 5331ms
            newId call took 1557ms
            Iteration 15, elapsed since test start: 6894ms
            newId call took 1558ms
         */
    }
}
