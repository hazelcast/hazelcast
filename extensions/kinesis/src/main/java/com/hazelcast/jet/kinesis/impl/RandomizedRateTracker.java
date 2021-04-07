/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis.impl;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Kinesis Data Streams impose various limits on their operations (details
 * <a href="https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html">here</a>).
 * For example the <em>GetRecords</em> operation is allowed only 5 times per
 * second, per shard. This rate could be enforced, for example, with fixed
 * delays of 200ms between starting two consecutive such operations, but then
 * we risk having all processors starting the operation at the same time on
 * all shards. It would be much better to spread out the parallel operations on
 * the shards over the time period.
 * <p>
 * This class does exactly that. It takes a time period, it breaks it up into
 * N random parts and helps with walking through them in a round-robin fashion.
 */
public class RandomizedRateTracker {

    private final long[] parts;
    private int index;

    public RandomizedRateTracker(long duration, int n) {
        parts = init(duration, n);
    }

    public long next() {
        long part = parts[index++];
        if (index == parts.length) {
            index = 0;
        }
        return part;
    }

    private static long[] init(long total, int n) {
        long[] parts = new long[n];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long base = total / n;
        long remaining = total;
        for (int i = 0; i < n - 1; i++) {
            long part = random.nextLong(base / 2, 3 * base / 2);
            remaining -= part;
            parts[i] = part;
        }
        parts[n - 1] = remaining;
        return parts;
    }

    @Override
    public String toString() {
        return Arrays.stream(parts).boxed().map(Object::toString).collect(Collectors.joining(", "));
    }
}
