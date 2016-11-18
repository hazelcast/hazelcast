/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Responsible for generating invocation callIds.
 * <p>
 * It is very important that for each {@link #next(Invocation)}, there is a matching {@link #complete()}.
 * If they don't match, the number of concurrent invocations will grow/shrink without bound over time.
 * This can lead to OOME or deadlock.
 * <p>
 * When backpressure is enabled and there are too many concurrent invocations, calls of {@link #next(Invocation)}
 * will block using a spin-loop with exponential backoff.
 * <p/>
 * Currently a single CallIdSequence is used for all partitions, so there is contention. Also one partition
 * can cause problems in other partition if a lot of invocations are created for that partition. Then other
 * partitions can't make as many invocations because a single callIdSequence is being used.
 * <p/>
 * In the future we could add a CallIdSequence per partition or using some 'concurrency level'
 * and do a mod based on the partition-id. The advantage is that you reduce contention and improve isolation,
 * at the expense of:
 * <ol>
 * <li>increased complexity</li>
 * <li>not always being able to fully utilize the number of invocations.</li>
 * </ol>
 */
abstract class CallIdSequence {

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
     *
     * @param invocation the Invocation to create a call-id for.
     * @return the generated callId.
     * @throws AssertionError if invocation.op.callId not equals 0
     */
    public abstract long next(Invocation invocation);

    /** Not idempotent: must be called exactly once per invocation. */
    public abstract void complete();

    static final class CallIdSequenceWithoutBackpressure extends CallIdSequence {

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
        public long next(Invocation invocation) {
            assert invocation.op.getCallId() == 0 : "callId should be null:" + invocation;

            return HEAD.incrementAndGet(this);
        }

        @Override
        public void complete() {
            //no-op
        }
    }

    /**
     * A {@link com.hazelcast.spi.impl.operationservice.impl.CallIdSequence} that provides backpressure by taking
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
    static final class CallIdSequenceWithBackpressure extends CallIdSequence {
        static final int MAX_DELAY_MS = 500;
        private static final IdleStrategy IDLER = new BackoffIdleStrategy(
                0, 0, MILLISECONDS.toNanos(1), MILLISECONDS.toNanos(MAX_DELAY_MS));
        private static final int INDEX_HEAD = 7;
        private static final int INDEX_TAIL = 15;

        // instead of using 2 AtomicLongs, we use an array if width of 3 cache lines to prevent any false sharing.
        private final AtomicLongArray longs = new AtomicLongArray(3 * CACHE_LINE_LENGTH / LONG_SIZE_IN_BYTES);

        private final int maxConcurrentInvocations;
        private final long backoffTimeoutMs;

        CallIdSequenceWithBackpressure(int maxConcurrentInvocations, long backoffTimeoutMs) {
            this.maxConcurrentInvocations = maxConcurrentInvocations;
            this.backoffTimeoutMs = backoffTimeoutMs;
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
        public long next(Invocation invocation) {
            final Operation op = invocation.op;
            assert op.getCallId() == 0 : "callId should be zero:" + invocation;

            if (!op.isUrgent() && !hasSpace()) {
                waitForSpace(invocation);
            }
            // must generate a sequence even if the invocation is going to skip registration
            // because this information is used to determine the number of in-flight invocations
            return next();
        }

        @Override
        public void complete() {
            long newTail = longs.incrementAndGet(INDEX_TAIL);
            assert newTail <= longs.get(INDEX_HEAD);
        }

        long getTail() {
            return longs.get(INDEX_TAIL);
        }

        private long next() {
            return longs.incrementAndGet(INDEX_HEAD);
        }

        private boolean hasSpace() {
            return longs.get(INDEX_HEAD) - longs.get(INDEX_TAIL) < maxConcurrentInvocations;
        }

        private void waitForSpace(Invocation invocation) {
            long deadline = System.currentTimeMillis() + backoffTimeoutMs;
            for (long idleCount = 0; ; idleCount++) {
                IDLER.idle(idleCount);

                if (hasSpace()) {
                    return;
                }

                if (System.currentTimeMillis() >= deadline) {
                    throw new HazelcastOverloadException("Failed to get a callId for invocation: " + invocation);
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
    }
}
