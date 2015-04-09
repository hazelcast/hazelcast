package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.spi.BackupAwareOperation;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.spi.Operation.CALL_ID_LOCAL_SKIPPED;

/**
 * Responsible for generating callIds.
 * <p/>
 * It is very important that for each {@link #next(Invocation)}, there is a matching {@link #complete(Invocation)}.
 * If this doesn't match, than either the number of concurrent invocations can grow or shrink over time. This can lead
 * to OOME or deadlock.
 * <p/>
 * The CallIdSequence provides back pressure if enabled and there are too many concurrent invocations. When this happens,
 * an exponential backoff policy is applied.
 * <p/>
 * Currently a single CallIdSequence is used for all partitions; so there is contention. Also one partition can cause problems
 * in other partition if a lot of invocations are created for that partition. Then other partitions can't make as many invocations
 * because a single callIdSequence is being used.
 * <p/>
 * In the future we could add a CallIdSequence per partition or using some 'concurrency level' and do a mod based on the
 * partition-id. The advantage is that you reduce contention and improved isolation, at the expensive of:
 * <ol>
 * <li>increased complexity</li>
 * <li>not always being able to fully utilize the number of invocations.</li>
 * </ol>
 */
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
     * {@link com.hazelcast.spi.Operation#CALL_ID_LOCAL_SKIPPED} is returned if local registration in
     * the InvocationRegistry can be skipped.
     *
     * @param invocation the Invocation to create a call-id for.
     * @return the generated callId.
     * @throws AssertionError if invocation.op.callId not equals 0
     */
    public abstract long next(Invocation invocation);

    public abstract void complete(Invocation invocation);

    /**
     * Not every call needs to be registered. A call that is local and has no backups, doesn't need to be registered
     * since there will not be a remote machine sending a response back to the invocation.
     *
     * @param invocation
     * @return true if registration is required, false otherwise.
     */
    protected boolean skipRegistration(Invocation invocation) {
        if (invocation.remote) {
            return false;
        }

        if (invocation.op instanceof BackupAwareOperation) {
            return false;
        }

        return true;
    }

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
        public long next(Invocation invocation) {
            assert invocation.op.getCallId() == 0 : "callId should be null:" + invocation;

            if (skipRegistration(invocation)) {
                return CALL_ID_LOCAL_SKIPPED;
            }

            return HEAD.incrementAndGet(this);
        }

        @Override
        public void complete(Invocation invocation) {
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
        private static final int MAX_DELAY_MS = 500;
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
        public long next(Invocation invocation) {
            assert invocation.op.getCallId() == 0 : "callId should be null:" + invocation;

            if (!invocation.op.isUrgent()) {
                if (!hasSpace()) {
                    waitForSpace(invocation);
                }
            }

            // we need to generate a sequence, no matter if the invocation is going to skip registration.
            // because this information is used to determine the number of in flight invocations
            long callId = next();

            if (skipRegistration(invocation)) {
                return CALL_ID_LOCAL_SKIPPED;
            } else {
                return callId;
            }
        }

        private long next() {
            return longs.incrementAndGet(INDEX_HEAD);
        }

        private boolean hasSpace() {
            return longs.get(INDEX_HEAD) - longs.get(INDEX_TAIL) < maxConcurrentInvocations;
        }

        private void waitForSpace(Invocation invocation) {
            long remainingTimeoutMs = backoffTimeoutMs;
            boolean restoreInterrupt = false;
            try {
                long delayMs = 1;
                for (; ; ) {
                    long startMs = System.currentTimeMillis();
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        restoreInterrupt = true;
                    }
                    long durationMs = System.currentTimeMillis() - startMs;
                    remainingTimeoutMs -= durationMs;

                    if (hasSpace()) {
                        return;
                    }

                    if (remainingTimeoutMs <= 0) {
                        throw new HazelcastOverloadException("Failed to get a callId for invocation: " + invocation);
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

        /**
         * Calculates the next delay using an exponential increase in time until either the MAX_DELAY_MS is reached or
         * till the remainingTimeoutMs is reached.
         */
        private long nextDelay(long remainingTimeoutMs, long delayMs) {
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
        public void complete(Invocation invocation) {
            if (invocation.op.getCallId() == 0) {
                return;
            }

            long newTail = longs.incrementAndGet(INDEX_TAIL);
            assert newTail <= longs.get(INDEX_HEAD);
        }
    }
}
