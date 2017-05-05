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

package com.hazelcast.jet.impl.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class TimestampHistoryTest {

    private TimestampHistory histo;

    @Before
    public void setup() {
        histo = new TimestampHistory(6, 3);
    }

    @Test
    public void when_clockIncreasingByOne() {
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(2, 2, Long.MIN_VALUE);
        validateSample(3, 3, Long.MIN_VALUE);
        validateSample(4, 4, Long.MIN_VALUE);
        validateSample(5, 5, Long.MIN_VALUE);
        validateSample(6, 6, Long.MIN_VALUE);
        validateSample(7, 7, 2);
        validateSample(8, 8, 2);
        validateSample(9, 8, 4);
        validateSample(10, 10, 4);
        validateSample(11, 10, 6);
        validateSample(12, 10, 6);
        validateSample(13, 10, 8);
    }

    @Test
    public void when_negativeClockIncreasingByOne() {
        validateSample(-10, 1, Long.MIN_VALUE);
        validateSample(-9, 2, Long.MIN_VALUE);
        validateSample(-8, 3, Long.MIN_VALUE);
        validateSample(-7, 4, Long.MIN_VALUE);
        validateSample(-6, 5, Long.MIN_VALUE);
        validateSample(-5, 6, Long.MIN_VALUE);
        validateSample(-4, 7, 2);
        validateSample(-3, 8, 2);
        validateSample(-2, 9, 4);
        validateSample(-1, 9, 4);
        validateSample(0, 9, 6);
        validateSample(1, 9, 6);
    }

    @Test
    public void when_clockIncreasingByTwoStopAndResume() {
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(3, 2, Long.MIN_VALUE);
        validateSample(5, 3, Long.MIN_VALUE);
        validateSample(5, 4, Long.MIN_VALUE);
        validateSample(5, 5, Long.MIN_VALUE);
        validateSample(5, 6, Long.MIN_VALUE);
        validateSample(5, 7, Long.MIN_VALUE);
        validateSample(7, 8, 1);
    }

    @Test
    public void when_clockIncreasingByTwo() {
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(3, 2, Long.MIN_VALUE);
        validateSample(5, 3, Long.MIN_VALUE);
        validateSample(7, 4, 1);
        validateSample(9, 5, 2);
    }

    @Test
    public void when_clockIncreasingByThree() {
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(4, 2, Long.MIN_VALUE);
        validateSample(7, 3, 1);
        validateSample(10, 4, 2);
    }

    @Test
    public void when_clockIncreasingByFour() {
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(5, 2, Long.MIN_VALUE);
        validateSample(9, 3, 1);
        validateSample(13, 4, 2);
        validateSample(17, 5, 3);
    }

    @Test
    public void when_clockIncreasingByFive() {
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(6, 2, Long.MIN_VALUE);
        validateSample(11, 3, 2);
        validateSample(16, 4, 2);
        validateSample(17, 4, 3);
        validateSample(18, 4, 3);
        validateSample(19, 4, 3);
        validateSample(20, 4, 3);
        validateSample(21, 4, 4);
    }

    @Test
    public void when_clockIncreasingBySix() {
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(7, 2, 1);
        validateSample(13, 3, 2);
        validateSample(19, 4, 3);
    }

    @Test
    public void when_clockIncreasingBySeven() {
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(8, 2, 1);
        validateSample(15, 3, 2);
        validateSample(22, 4, 3);
    }

    @Test
    public void when_historySize1() {
        histo = new TimestampHistory(3, 1);
        validateSample(0, 0, Long.MIN_VALUE);
        validateSample(1, 1, Long.MIN_VALUE);
        validateSample(2, 2, Long.MIN_VALUE);
        validateSample(3, 3, 2);
    }

    @Test
    public void when_minValuePunc_then_minValue() {
        for (int i = 0; i < 100; i++) {
            validateSample(i, Long.MIN_VALUE, Long.MIN_VALUE);
        }
    }

    private void validateSample(long now, long sampleVal, long expectedResult) {
        assertEquals(expectedResult, histo.sample(now, sampleVal));
    }
}
