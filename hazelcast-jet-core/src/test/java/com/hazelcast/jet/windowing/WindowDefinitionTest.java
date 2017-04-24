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

package com.hazelcast.jet.windowing;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WindowDefinitionTest {

    private WindowDefinition definition;

    @Before
    public void setup() {
    }

    @Test
    public void when_noOffset() throws Exception {
        definition = new WindowDefinition(4, 0, 10);
        assertSeq(-5, -8, -4);
        assertSeq(-4, -4, 0);
        assertSeq(-3, -4, 0);
        assertSeq(-2, -4, 0);
        assertSeq(-1, -4, 0);
        assertSeq(0, 0, 4);
        assertSeq(1, 0, 4);
        assertSeq(2, 0, 4);
        assertSeq(3, 0, 4);
        assertSeq(4, 4, 8);
        assertSeq(5, 4, 8);
        assertSeq(6, 4, 8);
        assertSeq(7, 4, 8);
        assertSeq(8, 8, 12);
    }

    @Test
    public void when_offset1() throws Exception {
        definition = new WindowDefinition(4, 1, 10);

        assertSeq(-4, -7, -3);
        assertSeq(-3, -3, 1);
        assertSeq(-2, -3, 1);
        assertSeq(-1, -3, 1);
        assertSeq(0, -3, 1);
        assertSeq(1, 1, 5);
        assertSeq(2, 1, 5);
        assertSeq(3, 1, 5);
        assertSeq(4, 1, 5);
        assertSeq(5, 5, 9);
        assertSeq(6, 5, 9);
        assertSeq(7, 5, 9);
        assertSeq(8, 5, 9);
        assertSeq(9, 9, 13);
    }

    @Test
    public void when_offset2() throws Exception {
        definition = new WindowDefinition(4, 2, 10);

        assertSeq(-4, -6, -2);
        assertSeq(-3, -6, -2);
        assertSeq(-2, -2, 2);
        assertSeq(-1, -2, 2);
        assertSeq(0, -2, 2);
        assertSeq(1, -2, 2);
        assertSeq(2, 2, 6);
        assertSeq(3, 2, 6);
        assertSeq(4, 2, 6);
        assertSeq(5, 2, 6);
        assertSeq(6, 6, 10);
        assertSeq(7, 6, 10);
        assertSeq(8, 6, 10);
        assertSeq(9, 6, 10);
    }

    @Test
    public void when_frameLength3() {
        definition = new WindowDefinition(3, 0, 10);
        assertEquals(Long.MIN_VALUE, definition.floorFrameSeq(Long.MIN_VALUE));
    }

    @Test
    public void when_floorOutOfRange_then_minValue() {
        definition = new WindowDefinition(4, 3, 10);
        assertEquals(Long.MIN_VALUE, definition.floorFrameSeq(Long.MIN_VALUE + 2));
        assertEquals(Long.MAX_VALUE, definition.floorFrameSeq(Long.MAX_VALUE));
    }

    @Test
    public void when_higherOutOfRange_then_maxValue() {
        definition = new WindowDefinition(4, 2, 10);
        assertEquals(Long.MAX_VALUE, definition.higherFrameSeq(Long.MAX_VALUE - 1));
        assertEquals(Long.MIN_VALUE + 2, definition.higherFrameSeq(Long.MIN_VALUE));
    }

    private void assertSeq(long seq, long floor, long higher) {
        assertEquals(floor, definition.floorFrameSeq(seq));
        assertEquals(higher, definition.higherFrameSeq(seq));
    }

}
