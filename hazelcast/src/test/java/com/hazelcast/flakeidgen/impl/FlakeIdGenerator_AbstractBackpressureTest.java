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

package com.hazelcast.flakeidgen.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_ALLOWED_FUTURE_MILLIS;
import static com.hazelcast.config.FlakeIdGeneratorConfig.DEFAULT_BITS_SEQUENCE;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public abstract class FlakeIdGenerator_AbstractBackpressureTest {

    protected static final int BATCH_SIZE = 100000;
    private static final ILogger LOGGER = Logger.getLogger(FlakeIdGenerator_MemberBackpressureTest.class);
    private static final long CTM_IMPRECISION = 50; // imprecision of the value returned from currentTimeMillis()
    public HazelcastInstance instance;

    @Test
    public void backpressureTest() {
        final FlakeIdGenerator generator = instance.getFlakeIdGenerator("gen");

        long testStartNanos = Timer.nanos();
        long allowedHiccupMillis = 2000;
        for (int i = 1; i <= 15; i++) {
            generator.newId();
            long elapsedMs = Timer.millisElapsed(testStartNanos);
            long minimumRequiredMs = Math.max(0, (i * BATCH_SIZE >> DEFAULT_BITS_SEQUENCE) - DEFAULT_ALLOWED_FUTURE_MILLIS - CTM_IMPRECISION);
            long maximumAllowedMs = minimumRequiredMs + allowedHiccupMillis;
            String msg = "Iteration " + i + ", elapsed: " + Timer.millisElapsed(testStartNanos) + "ms, "
                    + "minimum: " + minimumRequiredMs + ", maximum: " + maximumAllowedMs;
            LOGGER.info(msg);
            assertTrue(msg, elapsedMs >= minimumRequiredMs && elapsedMs <= maximumAllowedMs);

            // drain remainder of the IDs from the batch
            for (int j = 1; j < BATCH_SIZE; j++) {
                generator.newId();
            }
        }

        /*
        This is the typical output of the test:
            Iteration 1, elapsed: 0ms, minimum: 0, maximum: 2000
            Iteration 2, elapsed: 1ms, minimum: 0, maximum: 2000
            Iteration 3, elapsed: 3ms, minimum: 0, maximum: 2000
            Iteration 4, elapsed: 4ms, minimum: 0, maximum: 2000
            Iteration 5, elapsed: 5ms, minimum: 0, maximum: 2000
            Iteration 6, elapsed: 6ms, minimum: 0, maximum: 2000
            Iteration 7, elapsed: 8ms, minimum: 0, maximum: 2000
            Iteration 8, elapsed: 9ms, minimum: 0, maximum: 2000
            Iteration 9, elapsed: 10ms, minimum: 0, maximum: 2000
            Iteration 10, elapsed: 621ms, minimum: 575, maximum: 2575
            Iteration 11, elapsed: 2183ms, minimum: 2137, maximum: 4137
            Iteration 12, elapsed: 3746ms, minimum: 3700, maximum: 5700
            Iteration 13, elapsed: 5307ms, minimum: 5262, maximum: 7262
            Iteration 14, elapsed: 6871ms, minimum: 6825, maximum: 8825
            Iteration 15, elapsed: 8433ms, minimum: 8387, maximum: 10387
         */
    }
}
