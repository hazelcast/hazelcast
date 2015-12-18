package com.hazelcast.client.spi.impl;

/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastOverloadException;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;


public abstract class CallIdSequence {

    // just for testing purposes.
    abstract long getLastCallId();

    /**
     * Returns the maximum concurrent invocations supported. Integer.MAX_VALUE means there is no max.
     *
     * @return the maximum concurrent invocation.
     */
    public abstract int getMaxConcurrentInvocations();

    /**
     * Creates the next call-id.
     * <p/>
     *
     * @return the generated callId.
     * @throws AssertionError if invocation.op.callId not equals 0
     */
    public abstract long next();

    public abstract void complete();

    public static final class CallIdSequenceWithoutBackpressure extends CallIdSequence {

        private static final AtomicLongFieldUpdater<CallIdSequenceWithoutBackpressure> HEAD
                = AtomicLongFieldUpdater.newUpdater(CallIdSequenceWithoutBackpressure.class, "head");

        private volatile long head;

        @Override
        public long getLastCallId() {
            return head;
        }

        @Override
        public int getMaxConcurrentInvocations() {
            return Integer.MAX_VALUE;
        }

        @Override
        public long next() {
            return HEAD.incrementAndGet(this);
        }

        @Override
        public void complete() {
            //no-op
        }
    }

    /**
     * A {@link com.hazelcast.spi.impl.operationservice.impl.CallIdSequence} that provided backpressure by taking
     * the number in flight operations into account when a call-id needs to be determined.
     *
     * It is possible to temporary exceed the capacity:
     * - due to system operations
     * - due to racy nature of checking if space is available and getting the next sequence.
     *
     * The last cause is not a problem since the capacity is exceeded temporary and it isn't sustainable. So perhaps
     * there are a few threads that at the same time see that the there is space and do a next. But any following
     * invocation needs to wait till there is is capacity.
     */
    public static final class CallIdSequenceWithBackpressure extends CallIdSequence {
        static final int MAX_DELAY_MS = 500;
        private static final int INDEX_HEAD = 7;
        private static final int INDEX_TAIL = 15;

        // instead of using 2 AtomicLongs, we use an array if width of 3 cache lines to prevent any false sharing.
        private final AtomicLongArray longs = new AtomicLongArray(3 * CACHE_LINE_LENGTH / LONG_SIZE_IN_BYTES);

        private final int maxConcurrentInvocations;
        private final long backoffTimeoutMs;

        public CallIdSequenceWithBackpressure(int maxConcurrentInvocations, long backoffTimeoutMs) {
            this.maxConcurrentInvocations = maxConcurrentInvocations;
            this.backoffTimeoutMs = backoffTimeoutMs;
        }

        @Override
        public long getLastCallId() {
            return longs.get(INDEX_HEAD);
        }

        long getTail() {
            return longs.get(INDEX_TAIL);
        }

        @Override
        public int getMaxConcurrentInvocations() {
            return maxConcurrentInvocations;
        }

        @Override
        public long next() {
            if (!hasSpace()) {
                waitForSpace();
            }

            // we need to generate a sequence, no matter if the invocation is going to skip registration.
            // because this information is used to determine the number of in flight invocations
            return longs.incrementAndGet(INDEX_HEAD);
        }

        private boolean hasSpace() {
            return longs.get(INDEX_HEAD) - longs.get(INDEX_TAIL) < maxConcurrentInvocations;
        }

        private void waitForSpace() {
            long remainingTimeoutMs = backoffTimeoutMs;
            boolean restoreInterrupt = false;
            try {
                long delayMs = 1;
                for (; ; ) {
                    long startMs = System.currentTimeMillis();
                    restoreInterrupt = sleep(delayMs);
                    long durationMs = System.currentTimeMillis() - startMs;
                    remainingTimeoutMs -= durationMs;

                    if (hasSpace()) {
                        return;
                    }

                    if (remainingTimeoutMs <= 0) {
                        throw new HazelcastOverloadException("Failed to get a callId for invocation: ");
                    }

                    delayMs = nextDelay(remainingTimeoutMs, delayMs);
                    remainingTimeoutMs -= delayMs;
                }
            } finally {
                if (restoreInterrupt) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        static boolean sleep(long delayMs) {
            try {
                Thread.sleep(delayMs);
                return false;
            } catch (InterruptedException e) {
                return true;
            }
        }

        /**
         * Calculates the next delay using an exponential increase in time until either the MAX_DELAY_MS is reached or
         * till the remainingTimeoutMs is reached.
         */
        static long nextDelay(long remainingTimeoutMs, long delayMs) {
            delayMs *= 2;

            if (delayMs > MAX_DELAY_MS) {
                delayMs = MAX_DELAY_MS;
            }

            if (delayMs > remainingTimeoutMs) {
                delayMs = remainingTimeoutMs;
            }

            return delayMs;
        }

        @Override
        public void complete() {
            long newTail = longs.incrementAndGet(INDEX_TAIL);
        }
    }
}
