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

package com.hazelcast.spi.impl.sequence;

import com.hazelcast.internal.util.ConcurrencyDetection;

import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.internal.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.QuickMath.modPowerOfTwo;

/**
 * A {@link CallIdSequence} that provides backpressure by taking
 * the number of in-flight operations into account when before creating a new call-id.
 * <p>
 * It is possible to temporarily create more concurrent invocations than the declared capacity due to:
 * <ul>
 * <li>system operations</li>
 * <li>the racy nature of checking if space is available and getting the next sequence. </li>
 * </ul>
 * The latter cause is not a problem since the capacity is exceeded temporarily and it isn't sustainable.
 * So perhaps there are a few threads that at the same time see that the there is space and do a next.
 */
public abstract class AbstractCallIdSequence implements CallIdSequence {
    private static final int INDEX_HEAD = 7;
    private static final int INDEX_TAIL = 15;
    private static final int MAX_CONCURRENT_CALLS = Integer.getInteger("hazelcast.concurrent.invocations.max", 10);
    private static final int MOD = 8;

    // instead of using 2 AtomicLongs, we use an array if width of 3 cache lines to prevent any false sharing.
    private final AtomicLongArray longs = new AtomicLongArray(3 * CACHE_LINE_LENGTH / LONG_SIZE_IN_BYTES);

    private final int maxConcurrentInvocations;
    private final ConcurrencyDetection concurrencyDetection;

    public AbstractCallIdSequence(int maxConcurrentInvocations, ConcurrencyDetection concurrencyDetection) {
        checkPositive("maxConcurrentInvocations", maxConcurrentInvocations);

        this.concurrencyDetection = concurrencyDetection;
        this.maxConcurrentInvocations = maxConcurrentInvocations;
    }

    @Override
    public long next() {
        if (!hasSpace()) {
            handleNoSpaceLeft();
        }
        return forceNext();
    }

    protected abstract void handleNoSpaceLeft();

    @Override
    public long getLastCallId() {
        return longs.get(INDEX_HEAD);
    }

    @Override
    public int getMaxConcurrentInvocations() {
        return maxConcurrentInvocations;
    }

    @Override
    public void complete() {
        long newTail = longs.incrementAndGet(INDEX_TAIL);
        assert newTail <= longs.get(INDEX_HEAD);
    }

    public long forceNext() {
        long l = longs.incrementAndGet(INDEX_HEAD);
        // we don't want to check for every call, so we'll check 1 in 8 calls. If there is sufficient concurrency
        // one of the calls will trigger the onDetected.
        if (modPowerOfTwo(l, MOD) == 0 && concurrentInvocations() > MAX_CONCURRENT_CALLS) {
            concurrencyDetection.onDetected();
        }
        return l;
    }

    long getTail() {
        return longs.get(INDEX_TAIL);
    }

    protected boolean hasSpace() {
        return concurrentInvocations() < maxConcurrentInvocations;
    }

    public long concurrentInvocations() {
        return longs.get(INDEX_HEAD) - longs.get(INDEX_TAIL);
    }
}
