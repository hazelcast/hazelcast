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

package com.hazelcast.spi.impl.sequence;

import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A {@link CallIdSequence} that provides backpressure by taking
 * the number of in-flight operations into account when before creating a new call-id.
 * <p>
 * It is possible to temporarily create more concurrent invocations than the declared capacity due to:
 * <ul>
 *     <li>system operations</li>
 *     <li>the racy nature of checking if space is available and getting the next sequence. </li>
 * </ul>
 * The latter cause is not a problem since the capacity is exceeded temporarily and it isn't sustainable.
 * So perhaps there are a few threads that at the same time see that the there is space and do a next.
 * But any following invocation needs to wait till there is is capacity.
 */
public final class CallIdSequenceWithBackpressure implements CallIdSequence {
    static final int MAX_DELAY_MS = 500;
    private static final IdleStrategy IDLER = new BackoffIdleStrategy(
            0, 0, MILLISECONDS.toNanos(1), MILLISECONDS.toNanos(MAX_DELAY_MS));
    private static final int INDEX_HEAD = 7;
    private static final int INDEX_TAIL = 15;

    // instead of using 2 AtomicLongs, we use an array if width of 3 cache lines to prevent any false sharing.
    private final AtomicLongArray longs = new AtomicLongArray(3 * CACHE_LINE_LENGTH / LONG_SIZE_IN_BYTES);

    private final int maxConcurrentInvocations;
    private final long backoffTimeoutNanos;

    public CallIdSequenceWithBackpressure(int maxConcurrentInvocations, long backoffTimeoutMs) {
        this.maxConcurrentInvocations = maxConcurrentInvocations;
        this.backoffTimeoutNanos = MILLISECONDS.toNanos(backoffTimeoutMs);
    }

    @Override
    public long getLastCallId() {
        return longs.get(INDEX_HEAD);
    }

    @Override
    public int getMaxConcurrentInvocations() {
        return maxConcurrentInvocations;
    }

    @Override
    public long next() throws TimeoutException {
        if (!hasSpace()) {
            waitForSpace();
        }
        return forceNext();
    }

    @Override
    public void complete() {
        long newTail = longs.incrementAndGet(INDEX_TAIL);
        assert newTail <= longs.get(INDEX_HEAD);
    }

    public long forceNext() {
        return longs.incrementAndGet(INDEX_HEAD);
    }

    long getTail() {
        return longs.get(INDEX_TAIL);
    }

    private boolean hasSpace() {
        return longs.get(INDEX_HEAD) - longs.get(INDEX_TAIL) < maxConcurrentInvocations;
    }

    private void waitForSpace() throws TimeoutException {
        long deadline = System.nanoTime() + backoffTimeoutNanos;
        for (long idleCount = 0; ; idleCount++) {
            if (System.nanoTime() >= deadline) {
                throw new TimeoutException(String.format("Timed out trying to acquire another call ID."
                        + " maxConcurrentInvocations = %d, backoffTimeout = %d", maxConcurrentInvocations,
                        NANOSECONDS.toMillis(backoffTimeoutNanos)));
            }
            IDLER.idle(idleCount);
            if (hasSpace()) {
                return;
            }
        }
    }
}
