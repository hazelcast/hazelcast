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

package com.hazelcast.jet.core;

import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.util.WatermarkPolicyUtil.limitingTimestampAndWallClockLag;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class WatermarkPolicies_limitingTimestampAndWallClockLag {

    private static final long TIMESTAMP_LAG = 3;
    private static final long WALL_CLOCK_LAG = 10;
    private long currTimeMs;
    private WatermarkPolicy p = limitingTimestampAndWallClockLag(TIMESTAMP_LAG, WALL_CLOCK_LAG, () -> currTimeMs);

    @Test
    public void when_outOfOrderEvents_then_monotonicWm() {
        // Given
        currTimeMs = -1_000_000;

        // When, Then
        assertEquals(10 - TIMESTAMP_LAG, p.reportEvent(10));
        assertEquals(10 - TIMESTAMP_LAG, p.reportEvent(9));
        assertEquals(10 - TIMESTAMP_LAG, p.reportEvent(8));
        assertEquals(10 - TIMESTAMP_LAG, p.reportEvent(7)); // late event
        assertEquals(11 - TIMESTAMP_LAG, p.reportEvent(11));
    }

    @Test
    public void when_eventsStop_then_wmFollowsWallClock() {
        for (currTimeMs = 100; currTimeMs < 110; currTimeMs++) {
            assertEquals(currTimeMs - WALL_CLOCK_LAG, p.reportEvent(0));
        }
    }

    @Test
    public void when_noEventEver_then_wmFollowsWallClock() {
        for (currTimeMs = 100; currTimeMs < 110; currTimeMs++) {
            assertEquals(currTimeMs - WALL_CLOCK_LAG, p.getCurrentWatermark());
        }
    }
}
