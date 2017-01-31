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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.util.HashUtil;
import com.hazelcast.util.collection.IntHashSet;
import org.HdrHistogram.Histogram;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class HyperLogLogImplTest {

    private static final int PRIME_PRECISION = 25;

    @Parameterized.Parameters()
    public static Collection<Integer[]> params() {
        return Arrays.asList(new Integer[][]{
                {11, 10000000}, {12, 10000000}, {13, 10000000},
                {14, 10000000}, {15, 10000000}, {16, 10000000}
        });
    }

    @Parameterized.Parameter()
    public int precision;

    @Parameterized.Parameter(1)
    public int runLength;

    private HyperLogLog hyperLogLog;

    @Before
    public void setup() {
        hyperLogLog = new HyperLogLogImpl(precision, PRIME_PRECISION);
    }

    @Test
    public void add() {
        hyperLogLog.add(1000L);
        assertEquals(1L, hyperLogLog.estimate());
    }

    @Test
    public void addAll() {
        hyperLogLog.addAll(new long[]{1L, 1L, 2000L, 3000, 40000L});
        assertEquals(4L, hyperLogLog.estimate());
    }

    /**
     * - Add up-to runLength() random numbers on both a Set and a HyperLogLog encoder.
     * - Sample the actual count, and the estimate respectively every 100 operations.
     * - Compute the error rate, of the measurements and store it in a histogram.
     * - Assert that the 99th percentile of the histogram is less than the expected max error,
     * which is the result of std error (1.04 / sqrt(m)) + 3%.
     * (2% is the typical accuracy, but tests on the implementation showed up rare occurrences of 3%)
     */
    @Test
    public void testEstimateErrorRateForBigCardinalities() {
        double stdError = (1.04 / Math.sqrt(1 << precision)) * 100;
        double maxError = Math.ceil(stdError + 3.0);

        IntHashSet actualCount = new IntHashSet(runLength, -1);
        Random random = new Random();
        Histogram histogram = new Histogram(5);
        ByteBuffer bb = ByteBuffer.allocate(4);

        int sampleStep = 100;
        long expected;
        long actual;

        for (int i = 1; i <= runLength; i++) {
            int toCount = random.nextInt();
            actualCount.add(toCount);

            bb.clear();
            bb.putInt(toCount);
            hyperLogLog.add(HashUtil.MurmurHash3_x64_64(bb.array(), 0, bb.array().length));

            if (i % sampleStep == 0) {
                expected = actualCount.size();
                actual = hyperLogLog.estimate();
                double errorPct = ((actual * 100.0) / expected) - 100;
                histogram.recordValue(Math.abs((long) (errorPct * 100)));
            }
        }

        double errorPerc99 = histogram.getValueAtPercentile(99) / 100.0;
        if (errorPerc99 > maxError) {
            fail("For P=" + precision + ", Expected max error=" + maxError + "%."
                    + " Actual error: " + errorPerc99 + "%.");
        }
    }
}
