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

import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByMinStep;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class WatermarkEmissionPolicyTest {

    private static final int MIN_STEP = 2;
    private WatermarkEmissionPolicy p;
    private long lastEmittedWm = Long.MIN_VALUE;

    @Test
    public void test_throttleByMinStep() {
        p = emitByMinStep(MIN_STEP);
        assertWm(2, 2);
        assertWm(3, 2);
        assertWm(4, 4);
        assertWm(6, 6);
        assertWm(9, 9);
        assertWm(10, 9);
        assertWm(11, 11);
    }

    @Test
    public void test_throttleByFrame_noMaxStep() {
        p = emitByFrame(tumblingWinPolicy(3), Long.MAX_VALUE);
        assertWm(2, 0);
        assertWm(3, 3);
        assertWm(4, 3);
        assertWm(5, 3);
        assertWm(6, 6);
        assertWm(13, 12);
        assertWm(14, 12);
        assertWm(15, 15);
    }

    @Test
    public void test_throttleByFrame_maxStepCloseToMax() {
        // this tests the possible overflow when calculating with maxStep
        p = emitByFrame(tumblingWinPolicy(3), Long.MAX_VALUE);
        assertWm(2, 0);
        assertWm(3, 3);
        assertWm(4, 3);
        assertWm(5, 3);
        assertWm(6, 6);
        assertWm(13, 12);
        assertWm(14, 12);
        assertWm(15, 15);
    }

    @Test
    public void test_throttleByFrame_withMaxStep() {
        // this tests the possible overflow when calculating with maxStep
        p = emitByFrame(tumblingWinPolicy(5), 2);
        assertWm(2, 2);
        assertWm(3, 2);
        assertWm(4, 4);
        assertWm(5, 5);
        assertWm(6, 6);
        assertWm(13, 12);
        assertWm(14, 14);
        assertWm(15, 15);
    }

    private void assertWm(long currentWm, long expectedToEmit) {
        long newWm = p.throttleWm(currentWm, lastEmittedWm);
        lastEmittedWm = Math.max(lastEmittedWm, newWm);
        assertEquals(expectedToEmit, lastEmittedWm);
    }
}
