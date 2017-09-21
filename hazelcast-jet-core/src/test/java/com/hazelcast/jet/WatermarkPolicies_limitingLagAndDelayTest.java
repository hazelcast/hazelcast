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

package com.hazelcast.jet;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.util.WatermarkPolicyUtil.limitingLagAndDelay;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WatermarkPolicies_limitingLagAndDelayTest {

    private static final int MAX_RETAIN_MS = 8;
    private static final int TIMESTAMP_LAG = 8;

    private long time;
    private WatermarkPolicy p = limitingLagAndDelay(TIMESTAMP_LAG, MAX_RETAIN_MS, 8, () -> time);

    @Test
    public void when_outOfOrderEvents_then_monotonicWm() {
        assertWm(2, 10);
        assertWm(2, 9);
        assertWm(2, 8);
        assertWm(2, 7);
        assertWm(3, 11);
    }

    @Test
    public void when_clockIncreasing_then_eventuallyCatchUp() {
        assertWm(2, 10);
        time = 1;
        assertWm(3, 11);
        time = 2;
        assertWm(4, 12);
        time = 3;
        assertWm(5, 13);
        time = 4;
        assertEquals(5, p.getCurrentWatermark());
        time = 8;
        assertEquals(10, p.getCurrentWatermark());
        time = 9;
        assertEquals(11, p.getCurrentWatermark());
        time = 10;
        assertEquals(12, p.getCurrentWatermark());
        time = 11;
        assertEquals(13, p.getCurrentWatermark());
    }

    private void assertWm(long expected, long timestamp) {
        assertEquals(expected, p.reportEvent(timestamp));
        assertEquals(expected, p.getCurrentWatermark());
    }
}
