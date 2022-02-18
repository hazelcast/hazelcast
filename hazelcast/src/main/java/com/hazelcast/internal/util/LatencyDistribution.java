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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * Contains latency distribution logic.
 * <p>
 * <h2>How do we calculate latency-range distribution?</h2>
 * <p>
 * We have an array of buckets and each bucket has a matching latency
 * range with its index. Min latency is zero and max latency is {@link  Integer#MAX_VALUE}.
 * Index is an integer value and its bits represent an index of {@link #BUCKET_COUNT}
 * array. Sign bit is not used. So we have 31 bits to use.
 * <p>
 * Bits: 000 0000 0000 0000 0000 0000 0000 0000
 * <p>
 * Indexes: 0-1-2-...-16-...30
 * <p>
 * Each bucket has a minimum latency matching with index's power of 2:
 * <p>
 * 2<sup>0</sup>, 2<sup>1</sup>, 2<sup>2</sup>,...2<sup>16</sup>... 2<sup>30</sup>
 * <p>
 * <h3>Range boundaries calculation</h3>
 * Min latency = 1 << bucketIndex
 * <p>
 * Max latency = minLatencyOfNextBucketIndex - 1
 * <p>
 * So we will have a range distribution like this in the end:
 * <pre>
 * Ranges per bucket index:
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-++
 * | Index |   Latency Range                |
 * +-------+--------------------------------+
 * | 0     |   0..1us                       |
 * +-------+--------------------------------+
 * | 1     |   2..3us                       |
 * +-------+--------------------------------+
 * | 2     |   4..7us                       |
 * +-------+--------------------------------+
 * | ...   |   ...                          |
 * +-------+--------------------------------+
 * | ...   |   ...                          |
 * +-------+--------------------------------+
 * | 29    |   536870912..1073741823us      |
 * +-------+--------------------------------+
 * | 30    |   1073741824..2147483647us     |
 * +-------+--------------------------------+
 * </pre>
 */
public final class LatencyDistribution {

    public static final int BUCKET_COUNT = 31;

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
    static int bucketMaxUs(int bucket) {
        return bucketMinUs(bucket + 1) - 1;
    }

    /**
     * The minimum value that can be placed in a bucket.
     */
    static int bucketMinUs(int bucket) {
        return bucket == 0 ? 0 : 1 << bucket;
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
        // nano clock is not guaranteed to be monotonic. So
        // lets record it as zero, so we can at least count.
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

    static int usToBucketIndex(int us) {
        return Math.max(0, BUCKET_COUNT - Integer.numberOfLeadingZeros(us));
    }
}
