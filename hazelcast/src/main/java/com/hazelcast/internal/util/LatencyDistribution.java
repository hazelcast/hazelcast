/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * A latency distribution
 */
public final class LatencyDistribution {

    public static final int BUCKET_COUNT = 32;

    @SuppressFBWarnings("MS_MUTABLE_ARRAY")
    public static final String[] LATENCY_KEYS;

    static {
        LATENCY_KEYS = new String[BUCKET_COUNT];
        for (int k = 0; k < BUCKET_COUNT; k++) {
            LATENCY_KEYS[k] = bucketMinUs(k) + ".." + (bucketMaxUs(k)) + "us";
        }
    }

    private static final AtomicLongFieldUpdater<LatencyDistribution> COUNT
            = newUpdater(LatencyDistribution.class, "count");
    private static final AtomicLongFieldUpdater<LatencyDistribution> TOTAL_MICROS
            = newUpdater(LatencyDistribution.class, "totalMicros");
    private static final AtomicLongFieldUpdater<LatencyDistribution> MAX_MICROS
            = newUpdater(LatencyDistribution.class, "maxMicros");

    private final AtomicLongArray buckets = new AtomicLongArray(BUCKET_COUNT);

    private volatile long count;
    private volatile long maxMicros;
    private volatile long totalMicros;

    public int bucketCount() {
        return BUCKET_COUNT;
    }

    public long bucket(int bucket) {
        return buckets.get(bucket);
    }

    /**
     * The maximum value that can be placed in a bucket.
     */
    public static long bucketMaxUs(int bucket) {
        return bucket == 0 ? 0 : (1L << bucket) - 1;
    }

    /**
     * The minimum value that can be placed in a bucket.
     */
    public static long bucketMinUs(int bucket) {
        return bucket == 0 ? 0 : bucketMaxUs(bucket - 1) + 1;
    }

    public long count() {
        return count;
    }

    public long maxMicros() {
        return maxMicros;
    }

    public long totalMicros() {
        return totalMicros;
    }

    public long avgMicros() {
        return count == 0 ? 0 : totalMicros / count;
    }

    public void done(long startNanos) {
        recordNanos(System.nanoTime() - startNanos);
    }

    public void recordNanos(long durationNanos) {
        // nano clock is not guaranteed to be monotonic. So lets record it as zero so we can at least count.
        if (durationNanos < 0) {
            durationNanos = 0;
        }

        long d = NANOSECONDS.toMicros(durationNanos);
        int durationMicros = d > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) d;

        COUNT.addAndGet(this, 1);
        TOTAL_MICROS.addAndGet(this, durationMicros);

        for (; ; ) {
            long currentMax = maxMicros;
            if (durationMicros <= currentMax) {
                break;
            }

            if (MAX_MICROS.compareAndSet(this, currentMax, durationMicros)) {
                break;
            }
        }

        try {
            buckets.incrementAndGet(usToBucketIndex(durationMicros));
        } catch (RuntimeException e) {
            throw new RuntimeException("duration nanos:" + durationNanos, e);
        }
    }

    public static int usToBucketIndex(int us) {
        if (us < 2) {
            return 0;
        }

        int nextPow = BUCKET_COUNT - Integer.numberOfLeadingZeros(us);
        int prevPow = nextPow - 1;
        int middle = (1 << nextPow) - (1 << (prevPow - 1));
        return us < middle ? prevPow : nextPow;
    }
}
