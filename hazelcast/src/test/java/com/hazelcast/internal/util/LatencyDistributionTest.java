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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LatencyDistributionTest {

    @Test
    public void accuracyTest() {
        LatencyDistribution d = new LatencyDistribution();
        d.recordNanos(TimeUnit.MICROSECONDS.toNanos(10));

        ThreadLocalRandom random = ThreadLocalRandom.current();
        long expected = 0;
        for (int k = 0; k < 100000; k++) {
            long value = random.nextInt(2000000000);
            expected += value;
            d.recordNanos(value);
        }

        long actual = 0;
        for (int bucket = 0; bucket < d.bucketCount(); bucket++) {
            actual += LatencyDistribution.bucketMaxUs(bucket) * d.bucket(bucket) * 1000;
        }

        double accuracyFactor = (actual * 1.0d / expected);
        assertTrue("accuracyFactor = " + accuracyFactor, accuracyFactor < 1.37);
    }

    @Test
    public void bucketMaxUs() {
        assertEquals(1, LatencyDistribution.bucketMaxUs(0));
        assertEquals(3, LatencyDistribution.bucketMaxUs(1));
        assertEquals(7, LatencyDistribution.bucketMaxUs(2));
        assertEquals(15, LatencyDistribution.bucketMaxUs(3));
        assertEquals(31, LatencyDistribution.bucketMaxUs(4));
        assertEquals(63, LatencyDistribution.bucketMaxUs(5));
    }

    @Test
    public void bucketMinUs() {
        assertEquals(0, LatencyDistribution.bucketMinUs(0));
        assertEquals(2, LatencyDistribution.bucketMinUs(1));
        assertEquals(4, LatencyDistribution.bucketMinUs(2));
        assertEquals(8, LatencyDistribution.bucketMinUs(3));
        assertEquals(16, LatencyDistribution.bucketMinUs(4));
        assertEquals(32, LatencyDistribution.bucketMinUs(5));
    }

    @Test
    public void recordNanos() {
        recordNanos(0, 0);
        recordNanos(200, 0);

        recordNanos(TimeUnit.MICROSECONDS.toNanos(1), 0);

        recordNanos(TimeUnit.MICROSECONDS.toNanos(2), 1);
        recordNanos(TimeUnit.MICROSECONDS.toNanos(3), 1);

        recordNanos(TimeUnit.MICROSECONDS.toNanos(4), 2);
        recordNanos(TimeUnit.MICROSECONDS.toNanos(5), 2);
        recordNanos(TimeUnit.MICROSECONDS.toNanos(6), 2);

        recordNanos(TimeUnit.MICROSECONDS.toNanos(8), 3);
        recordNanos(TimeUnit.MICROSECONDS.toNanos(11), 3);
        recordNanos(TimeUnit.MICROSECONDS.toNanos(12), 3);

        recordNanos(TimeUnit.MICROSECONDS.toNanos(23), 4);
        recordNanos(TimeUnit.MICROSECONDS.toNanos(24), 4);

        recordNanos(TimeUnit.MICROSECONDS.toNanos(33), 5);
    }

    private void recordNanos(long latencyNs, int bucket) {
        LatencyDistribution d = new LatencyDistribution();
        d.recordNanos(latencyNs);

        assertEquals(1, d.bucket(bucket));
        assertEquals(1, d.count());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(latencyNs), d.maxMicros());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(latencyNs), d.totalMicros());
    }

    @Test
    public void recordNanos_whenMax() {
        LatencyDistribution d = new LatencyDistribution();
        d.recordNanos(Long.MAX_VALUE);

        assertEquals(1, d.bucket(LatencyDistribution.BUCKET_COUNT - 1));
        assertEquals(1, d.count());
        assertEquals(Integer.MAX_VALUE, d.maxMicros());
        assertEquals(Integer.MAX_VALUE, d.totalMicros());
    }

    @Test
    public void recordNanos_whenNegative() {
        LatencyDistribution d = new LatencyDistribution();
        d.recordNanos(-10);

        assertEquals(1, d.bucket(0));
        assertEquals(1, d.count());
        assertEquals(0, d.maxMicros());
        assertEquals(0, d.totalMicros());
    }

    @Test
    public void usToBucketIndex() {
        assertEquals(0, LatencyDistribution.usToBucketIndex(0));
        assertEquals(0, LatencyDistribution.usToBucketIndex(1));

        assertEquals(1, LatencyDistribution.usToBucketIndex(2));
        assertEquals(1, LatencyDistribution.usToBucketIndex(3));

        assertEquals(2, LatencyDistribution.usToBucketIndex(4));
        assertEquals(2, LatencyDistribution.usToBucketIndex(5));
        assertEquals(2, LatencyDistribution.usToBucketIndex(6));
        assertEquals(2, LatencyDistribution.usToBucketIndex(7));
        assertEquals(3, LatencyDistribution.usToBucketIndex(8));
        assertEquals(3, LatencyDistribution.usToBucketIndex(11));
        assertEquals(3, LatencyDistribution.usToBucketIndex(12));

        assertEquals(4, LatencyDistribution.usToBucketIndex(16));
        assertEquals(4, LatencyDistribution.usToBucketIndex(17));
        assertEquals(4, LatencyDistribution.usToBucketIndex(23));
        assertEquals(4, LatencyDistribution.usToBucketIndex(24));
    }
}
