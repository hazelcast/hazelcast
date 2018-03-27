/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.util.Preconditions.checkPositive;

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
 */
public abstract class AbstractCallIdSequence implements CallIdSequence {
    private static final int INDEX_HEAD = 7;
    private static final int INDEX_TAIL = 15;

    // instead of using 2 AtomicLongs, we use an array if width of 3 cache lines to prevent any false sharing.
    private final AtomicLongArray longs = new AtomicLongArray(3 * CACHE_LINE_LENGTH / LONG_SIZE_IN_BYTES);

    private final int maxConcurrentInvocations;

    public AbstractCallIdSequence(int maxConcurrentInvocations) {
        checkPositive(maxConcurrentInvocations,
                "maxConcurrentInvocations should be a positive number. maxConcurrentInvocations=" + maxConcurrentInvocations);

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
        return longs.incrementAndGet(INDEX_HEAD);
    }

    long getTail() {
        return longs.get(INDEX_TAIL);
    }

    protected boolean hasSpace() {
        return longs.get(INDEX_HEAD) - longs.get(INDEX_TAIL) < maxConcurrentInvocations;
    }

}
