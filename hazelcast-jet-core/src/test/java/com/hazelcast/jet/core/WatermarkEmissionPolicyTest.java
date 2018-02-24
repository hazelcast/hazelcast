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

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByMinStep;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class WatermarkEmissionPolicyTest {

    private static final int MIN_STEP = 2;
    private WatermarkEmissionPolicy p;
    private long lastEmittedWm = Long.MIN_VALUE;

    @Test
    public void when_wmIncreasing_then_throttleByMinStep() {
        p = emitByMinStep(MIN_STEP);
        assertWm(2, true);
        assertWm(3, false);
        assertWm(4, true);
        assertWm(6, true);
        assertWm(9, true);
        assertWm(10, false);
        assertWm(11, true);
    }

    @Test
    public void when_wmIncreasing_then_throttleByFrame() {
        p = emitByFrame(tumblingWinPolicy(3));
        assertWm(Long.MIN_VALUE, false);
        assertWm(2, true);
        assertWm(3, true);
        assertWm(4, false);
        assertWm(5, false);
        assertWm(6, true);
        assertWm(13, true);
        assertWm(14, false);
        assertWm(15, true);
    }

    private void assertWm(long currentWm, boolean expectedToEmit) {
        if (p.shouldEmit(currentWm, lastEmittedWm)) {
            assertTrue(expectedToEmit);
            lastEmittedWm = currentWm;
        } else {
            assertFalse(expectedToEmit);
        }
    }
}
