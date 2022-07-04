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

package com.hazelcast.cardinality.impl.hyperloglog.impl;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.IntHashSet;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.HdrHistogram.Histogram;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Random;

import static com.hazelcast.internal.util.JVMUtil.upcast;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class HyperLogLogImplTest {

    private static final int DEFAULT_RUN_LENGTH = 10000000;

    @Parameters(name = "precision:{0}, errorRange:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {11, 7.0f},
                {12, 5.5f},
                {13, 3.5f},
                {14, 3.0f},
                {15, 2.5f},
                {16, 2.0f},
        });
    }

    @Parameter
    public int precision;

    @Parameter(value = 1)
    public double errorRange;

    private HyperLogLog hyperLogLog;

    @Before
    public void setup() {
        hyperLogLog = new HyperLogLogImpl(precision);
    }

    @Test
    public void add() {
        hyperLogLog.add(1000L);
        assertEquals(1L, hyperLogLog.estimate());
    }

    @Test
    public void addAll() {
        hyperLogLog.addAll(new long[]{1L, 1L, 2000L, 3000L, 40000L});
        assertEquals(4L, hyperLogLog.estimate());
    }

    /**
     * <ul>
     * <li>Adds up to {@link #DEFAULT_RUN_LENGTH} random numbers on both a Set and a HyperLogLog encoder.</li>
     * <li>Samples the actual count, and the estimate respectively every 100 operations.</li>
     * <li>Computes the error rate, of the measurements and store it in a histogram.</li>
     * <li>Asserts that the 99th percentile of the histogram is less than the expected max error,
     * which is the result of std error (1.04 / sqrt(m)) + [2.0, 6.5]% (2% is the typical accuracy,
     * but tests with a lower precision need a higher error range).</li>
     * </ul>
     */
    @Test
    public void testEstimateErrorRateForBigCardinalities() {
        double stdError = (1.04f / Math.sqrt(1 << precision)) * 100;
        double maxError = Math.ceil(stdError + errorRange);

        IntHashSet actualCount = new IntHashSet(DEFAULT_RUN_LENGTH, -1);
        Random random = new Random();
        Histogram histogram = new Histogram(5);
        ByteBuffer bb = ByteBuffer.allocate(4);

        int sampleStep = 100;
        long expected;
        long actual;

        for (int i = 1; i <= DEFAULT_RUN_LENGTH; i++) {
            int toCount = random.nextInt();
            actualCount.add(toCount);

            upcast(bb).clear();
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
            fail("For P=" + precision + ": Expected max error=" + maxError + "%. Actual error=" + errorPerc99 + "%.");
        }
    }
}
