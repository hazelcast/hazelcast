/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.ALLOWED_FUTURE_MILLIS;
import static com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy.BITS_SEQUENCE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public abstract class FlakeIdGenerator_AbstractBackpressureTest {

    public static final int BATCH_SIZE = 100000;
    private static final ILogger LOGGER = Logger.getLogger(FlakeIdGenerator_MemberBackpressureTest.class);
    public HazelcastInstance instance;

    @Test
    public void backpressureTest() {
        int batchSize = 100000;

        final FlakeIdGenerator generator = instance.getFlakeIdGenerator("gen");

        long testStart = System.nanoTime();
        long allowedHiccupMillis = 2000;
        int idsPerMs = 1 << BITS_SEQUENCE;
        for (int i = 1; i <= 15; i++) {
            generator.newId();
            long elapsedMs = NANOSECONDS.toMillis(System.nanoTime() - testStart);
            long minimumRequiredMs = Math.max(0, i * batchSize / idsPerMs - ALLOWED_FUTURE_MILLIS);
            long maximumAllowedMs = minimumRequiredMs + allowedHiccupMillis;
            String msg = "Iteration " + i + ", elapsed: " + NANOSECONDS.toMillis(System.nanoTime() - testStart) + "ms, "
                    + "minimum: " + minimumRequiredMs + ", maximum: " + maximumAllowedMs;
            LOGGER.info(msg);
            assertTrue(msg, elapsedMs >= minimumRequiredMs && elapsedMs <= maximumAllowedMs);

            // drain remainder of the IDs from the batch
            for (int j = 1; j < batchSize; j++) {
                generator.newId();
            }
        }

        /*
        This is the typical output of the test:
            Iteration 1, elapsed: 6ms, minimum: 0, maximum: 2000
            Iteration 2, elapsed: 14ms, minimum: 0, maximum: 2000
            Iteration 3, elapsed: 20ms, minimum: 0, maximum: 2000
            Iteration 4, elapsed: 25ms, minimum: 0, maximum: 2000
            Iteration 5, elapsed: 29ms, minimum: 0, maximum: 2000
            Iteration 6, elapsed: 33ms, minimum: 0, maximum: 2000
            Iteration 7, elapsed: 40ms, minimum: 0, maximum: 2000
            Iteration 8, elapsed: 44ms, minimum: 0, maximum: 2000
            Iteration 9, elapsed: 48ms, minimum: 0, maximum: 2000
            Iteration 10, elapsed: 632ms, minimum: 625, maximum: 2625
            Iteration 11, elapsed: 2193ms, minimum: 2187, maximum: 4187
            Iteration 12, elapsed: 3755ms, minimum: 3750, maximum: 5750
            Iteration 13, elapsed: 5319ms, minimum: 5312, maximum: 7312
            Iteration 14, elapsed: 6881ms, minimum: 6875, maximum: 8875
            Iteration 15, elapsed: 8443ms, minimum: 8437, maximum: 10437
         */
    }
}
