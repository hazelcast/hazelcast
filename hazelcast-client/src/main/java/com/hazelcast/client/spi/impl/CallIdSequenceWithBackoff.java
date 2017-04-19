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

package com.hazelcast.client.spi.impl;

import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A {@link com.hazelcast.spi.impl.operationservice.impl.CallIdSequence} that provided backpressure by taking
 * the number in flight operations into account when a call-id needs to be determined.
 *
 * It is possible to temporary exceed the capacity:
 * - due to system operations
 * - due to racy nature of checking if space is available and getting the next sequence.
 */
public final class CallIdSequenceWithBackoff extends CallIdSequence {
    private static final int MAX_DELAY_MS = 500;
    private static final IdleStrategy IDLER = new BackoffIdleStrategy(
            0, 0, MILLISECONDS.toNanos(1), MILLISECONDS.toNanos(MAX_DELAY_MS));

    private static final int INDEX_HEAD = 7;
    private static final int INDEX_TAIL = INDEX_HEAD + CACHE_LINE_LENGTH / LONG_SIZE_IN_BYTES;

    // instead of using two AtomicLongs, we use an array of width of 3 cache lines to prevent any false sharing
    private final AtomicLongArray longs = new AtomicLongArray(3 * CACHE_LINE_LENGTH / LONG_SIZE_IN_BYTES);

    private final int maxConcurrentInvocations;
    private final long backoffTimeoutMs;

    public CallIdSequenceWithBackoff(int maxConcurrentInvocations, long backofftimeoutMs) {
        this.maxConcurrentInvocations = maxConcurrentInvocations;
        this.backoffTimeoutMs = backofftimeoutMs;
    }

    @Override
    public long next() {
        if (!hasSpace()) {
            waitForSpace();
        }

        return longs.incrementAndGet(INDEX_HEAD);
    }

    private void waitForSpace() {
        if (backoffTimeoutMs <= 0) {
            throw new HazelcastOverloadException("maxConcurrentInvocations: "
                    + maxConcurrentInvocations + " is reached");
        }
        long deadline = System.currentTimeMillis() + backoffTimeoutMs;
        for (long idleCount = 0; ; idleCount++) {
            IDLER.idle(idleCount);
            if (hasSpace()) {
                return;
            }
            if (System.currentTimeMillis() >= deadline) {
                throw new HazelcastOverloadException(String.format("Timed out trying to acquire another call ID."
                        + " maxConcurrentInvocations = %d, backoffTimeout = %d", maxConcurrentInvocations, backoffTimeoutMs));
            }
        }
    }

    @Override
    public long forceNext() {
        return longs.incrementAndGet(INDEX_HEAD);
    }

    private boolean hasSpace() {
        return longs.get(INDEX_HEAD) - longs.get(INDEX_TAIL) < maxConcurrentInvocations;
    }

    @Override
    public void complete() {
        longs.incrementAndGet(INDEX_TAIL);
    }
}
