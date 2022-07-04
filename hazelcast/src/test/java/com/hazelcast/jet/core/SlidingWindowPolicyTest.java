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

package com.hazelcast.jet.core;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SlidingWindowPolicyTest {

    private SlidingWindowPolicy definition;

    @Before
    public void setup() {
    }

    @Test
    public void when_noOffset() {
        definition = new SlidingWindowPolicy(4, 0, 10);
        assertFrameTs(-5, -8, -4);
        assertFrameTs(-4, -4, 0);
        assertFrameTs(-3, -4, 0);
        assertFrameTs(-2, -4, 0);
        assertFrameTs(-1, -4, 0);
        assertFrameTs(0, 0, 4);
        assertFrameTs(1, 0, 4);
        assertFrameTs(2, 0, 4);
        assertFrameTs(3, 0, 4);
        assertFrameTs(4, 4, 8);
        assertFrameTs(5, 4, 8);
        assertFrameTs(6, 4, 8);
        assertFrameTs(7, 4, 8);
        assertFrameTs(8, 8, 12);
    }

    @Test
    public void when_offset1() {
        definition = new SlidingWindowPolicy(4, 1, 10);

        assertFrameTs(-4, -7, -3);
        assertFrameTs(-3, -3, 1);
        assertFrameTs(-2, -3, 1);
        assertFrameTs(-1, -3, 1);
        assertFrameTs(0, -3, 1);
        assertFrameTs(1, 1, 5);
        assertFrameTs(2, 1, 5);
        assertFrameTs(3, 1, 5);
        assertFrameTs(4, 1, 5);
        assertFrameTs(5, 5, 9);
        assertFrameTs(6, 5, 9);
        assertFrameTs(7, 5, 9);
        assertFrameTs(8, 5, 9);
        assertFrameTs(9, 9, 13);
    }

    @Test
    public void when_offset2() {
        definition = new SlidingWindowPolicy(4, 2, 10);

        assertFrameTs(-4, -6, -2);
        assertFrameTs(-3, -6, -2);
        assertFrameTs(-2, -2, 2);
        assertFrameTs(-1, -2, 2);
        assertFrameTs(0, -2, 2);
        assertFrameTs(1, -2, 2);
        assertFrameTs(2, 2, 6);
        assertFrameTs(3, 2, 6);
        assertFrameTs(4, 2, 6);
        assertFrameTs(5, 2, 6);
        assertFrameTs(6, 6, 10);
        assertFrameTs(7, 6, 10);
        assertFrameTs(8, 6, 10);
        assertFrameTs(9, 6, 10);
    }

    @Test
    public void when_frameLength3() {
        definition = new SlidingWindowPolicy(3, 0, 10);
        assertEquals(Long.MIN_VALUE, definition.floorFrameTs(Long.MIN_VALUE));
    }

    @Test
    public void when_floorOutOfRange_then_minValue() {
        definition = new SlidingWindowPolicy(4, 3, 10);
        assertEquals(Long.MIN_VALUE, definition.floorFrameTs(Long.MIN_VALUE + 2));
        assertEquals(Long.MAX_VALUE, definition.floorFrameTs(Long.MAX_VALUE));
    }

    @Test
    public void when_higherOutOfRange_then_maxValue() {
        definition = new SlidingWindowPolicy(4, 2, 10);
        assertEquals(Long.MAX_VALUE, definition.higherFrameTs(Long.MAX_VALUE - 1));
        assertEquals(Long.MIN_VALUE + 2, definition.higherFrameTs(Long.MIN_VALUE));
    }

    private void assertFrameTs(long timestamp, long expectedFloor, long expectedHigher) {
        assertEquals(expectedFloor, definition.floorFrameTs(timestamp));
        assertEquals(expectedHigher, definition.higherFrameTs(timestamp));
    }

    @Test
    public void test_tumblingWindowDef() {
        definition = tumblingWinPolicy(123L);
        assertEquals(123L, definition.frameSize());
        assertEquals(123L, definition.windowSize());
        assertEquals(0, definition.frameOffset());
    }

    @Test
    public void test_toTumblingByFrame() {
        definition = slidingWinPolicy(1000, 100);
        definition = definition.toTumblingByFrame();
        assertEquals(100, definition.windowSize());
        assertEquals(100, definition.frameSize());
    }

    @Test
    public void test_withOffset() {
        definition = slidingWinPolicy(1000, 100);
        assertEquals(0, definition.frameOffset());
        definition = definition.withOffset(10);
        assertEquals(10, definition.frameOffset());
    }
}
