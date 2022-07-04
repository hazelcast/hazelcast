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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.collection.IntHashSet;
import org.HdrHistogram.Histogram;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Random;

import static com.hazelcast.internal.util.JVMUtil.upcast;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class HyperLogLogEncoderAbstractTest {

    private HyperLogLogEncoder encoder;

    public abstract int runLength();

    public abstract int precision();

    public abstract HyperLogLogEncoder createStore();

    @Before
    public void setup() {
        encoder = createStore();
    }

    @Test
    public void add() {
        assertTrue(encoder.add(1000L));
        assertEquals(1L, encoder.estimate());
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
        double stdError = (1.04 / Math.sqrt(1 << precision())) * 100;
        double maxError = Math.ceil(stdError + 3.0);

        IntHashSet actualCount = new IntHashSet(runLength(), -1);
        Random random = new Random();
        Histogram histogram = new Histogram(5);
        ByteBuffer bb = ByteBuffer.allocate(4);

        int sampleStep = 100;
        long expected;
        long actual;

        for (int i = 1; i <= runLength(); i++) {
            int toCount = random.nextInt();
            actualCount.add(toCount);

            upcast(bb).clear();
            bb.putInt(toCount);
            encoder.add(HashUtil.MurmurHash3_x64_64(bb.array(), 0, bb.array().length));

            if (i % sampleStep == 0) {
                expected = actualCount.size();
                actual = encoder.estimate();
                double errorPct = ((actual * 100.0) / expected) - 100;
                histogram.recordValue(Math.abs((long) (errorPct * 100)));
            }
        }

        double errorPerc99 = histogram.getValueAtPercentile(99) / 100.0;
        if (errorPerc99 > maxError) {
            fail("For P=" + precision() + ", max error=" + maxError + "% expected."
                    + " Error: " + errorPerc99 + "%.");
        }
    }

    HyperLogLogEncoder getEncoder() {
        return encoder;
    }
}
