/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.submitjob.clientside.upload;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class SubmitJobPartCalculatorTotalPartsTest {

    @Parameter
    public long jarSize;

    @Parameter(value = 1)
    public long partSize;

    @Parameter(value = 2)
    public int totalParts;

    @Parameters (name = "{index}: jarSize - {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[] {
                // {jarSize,partSize,totalParts}
                new Object[] {0, 10_000_000, 0},
                new Object[] {1, 10_000_000, 1},
                new Object[] {3_500, 1_000, 4},
                new Object[] {30_720, 10_000_000, 1},
                new Object[] {999_9999, 10_000_000, 1},
                new Object[] {10_000_000, 10_000_000, 1},
                new Object[] {10_000_001, 10_000_000, 2},
                new Object[] {100_999_999, 10_000_000, 11},
                new Object[] {100_999_999_001L, 1_000, 101_000_000},
        });
    }

    @Test
    public void calculateTotalParts() {
        SubmitJobPartCalculator submitJobPartCalculator = new SubmitJobPartCalculator();

        int calculatedTotalParts = submitJobPartCalculator.calculateTotalParts(jarSize, partSize);
        assertEquals(totalParts, calculatedTotalParts);
    }
}
