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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedLongSupplier;
import com.hazelcast.jet.impl.util.TimestampHistory;

import javax.annotation.Nonnull;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Utility class with factories of several useful watermark policies.
 */
public final class WatermarkPolicies {

    private static final int DEFAULT_NUM_STORED_SAMPLES = 16;

    private WatermarkPolicies() {
    }

    private abstract static class WatermarkPolicyBase implements WatermarkPolicy {

        private long wm = Long.MIN_VALUE;

        long makeWmAtLeast(long proposedWm) {
            wm = max(wm, proposedWm);
            return wm;
        }

        long advanceWmBy(long amount) {
            wm += amount;
            return wm;
        }

        @Override
        public long getCurrentWatermark() {
            return wm;
        }
    }

    /**
     * Maintains watermark that lags behind the top observed timestamp by the
     * given amount. In the case of a stream lull the watermark does not
     * advance towards the top observed timestamp and remains behind it
     * indefinitely.
     *
     * @param lag the desired difference between the top observed timestamp
     *            and the watermark
     */
    @Nonnull
    public static WatermarkPolicy withFixedLag(long lag) {
        checkNotNegative(lag, "lag must not be negative");

        return new WatermarkPolicyBase() {
            @Override
            public long reportEvent(long timestamp) {
                return makeWmAtLeast(timestamp - lag);
            }
        };
    }

    /**
     * Maintains watermark that lags behind the top observed timestamp by at
     * most the given amount and is additionally guaranteed to reach the
     * timestamp of any given event within {@code maxDelayMs} after observing
     * it.
     *
     * @param lag upper bound on the difference between the top observed timestamp and the
     *               watermark
     * @param maxDelayMs upper bound (in milliseconds) on how long it can take for the
     *                   watermark to reach any observed event's timestamp
     */
    @Nonnull
    public static WatermarkPolicy limitingLagAndDelay(long lag, long maxDelayMs) {
        return limitingLagAndDelay(
                lag, MILLISECONDS.toNanos(maxDelayMs), DEFAULT_NUM_STORED_SAMPLES, System::nanoTime);
    }

    @Nonnull
    static WatermarkPolicy limitingLagAndDelay(
            long maxLag, long maxRetainNanos, int numStoredSamples, DistributedLongSupplier nanoClock
    ) {
        return new WatermarkPolicyBase() {

            private long topTs = Long.MIN_VALUE;
            private final TimestampHistory history = new TimestampHistory(maxRetainNanos, numStoredSamples);

            @Override
            public long reportEvent(long timestamp) {
                topTs = Math.max(timestamp, topTs);
                return applyMaxRetain(timestamp - maxLag);
            }

            @Override
            public long getCurrentWatermark() {
                return applyMaxRetain(super.getCurrentWatermark());
            }

            private long applyMaxRetain(long wm) {
                return makeWmAtLeast(Math.max(wm, history.sample(nanoClock.getAsLong(), topTs)));
            }
        };
    }

    /**
     * Maintains watermark that lags behind the top timestamp by at most
     * {@code timestampLag} and behind wall-clock time by at most {@code
     * wallClockLag}. It assumes that the event timestamp is in milliseconds
     * since Unix epoch and will use that fact to correlate it with wall-clock
     * time acquired from the underlying OS. Note that wall-clock time is
     * non-monotonic and sudden jumps that may occur in it will cause temporary
     * disruptions in the functioning of this policy.
     * <p>
     * In most cases the {@link #limitingLagAndLull(long, long)
     * limitingLagAndLull} policy should be preferred; this is a backup option
     * for cases where some substreams may never see an event.
     *
     * @param timestampLag maximum difference between the top observed timestamp
     *                     and the watermark
     * @param wallClockLag maximum difference between the current value of
     *                     {@code System.currentTimeMillis} and the watermark
     */
    @Nonnull
    public static WatermarkPolicy limitingTimestampAndWallClockLag(long timestampLag, long wallClockLag) {
        return limitingTimestampAndWallClockLag(timestampLag, wallClockLag, System::currentTimeMillis);
    }

    @Nonnull
    static WatermarkPolicy limitingTimestampAndWallClockLag(
            long timestampLag, long wallClockLag, DistributedLongSupplier wallClock
    ) {
        checkNotNegative(timestampLag, "timestampLag must not be negative");
        checkNotNegative(wallClockLag, "wallClockLag must not be negative");

        return new WatermarkPolicyBase() {

            @Override
            public long reportEvent(long timestamp) {
                updateFromWallClock();
                return makeWmAtLeast(timestamp - timestampLag);
            }

            @Override
            public long getCurrentWatermark() {
                return updateFromWallClock();
            }

            private long updateFromWallClock() {
                return makeWmAtLeast(wallClock.getAsLong() - wallClockLag);
            }
        };
    }

    /**
     * Maintains watermark that lags behind the top timestamp by the amount
     * specified with {@code lag}. Assumes that the event timestamp is given
     * in milliseconds and will use that fact to correlate it with the passage
     * of system time. There is no requirement on any specific point of origin
     * for the timestamp, i.e., the zero value can denote any point in time as
     * long as it is fixed.
     * <p>
     * When the defined {@code maxLullMs} period elapses without observing more
     * events, watermark will start advancing in lockstep with system time
     * acquired from the underlying OS's monotonic clock.
     * <p>
     * If no event is ever observed, watermark will advance from the initial
     * value of {@code Long.MIN_VALUE}. Therefore this policy can be used only
     * when there is a guarantee that each substream will emit at least one
     * event that will initialize the timestamp. Otherwise the empty substream
     * will hold back the processing of all other substreams by keeping the
     * watermark below any realistic value.
     *
     * @param lag the desired difference between the top observed timestamp
     *               and the watermark
     * @param maxLullMs maximum duration of a lull period before starting to
     *                  advance watermark with system time
     */
    @Nonnull
    public static WatermarkPolicy limitingLagAndLull(long lag, long maxLullMs) {
        return limitingLagAndLull(lag, maxLullMs, System::nanoTime);
    }

    @Nonnull
    static WatermarkPolicy limitingLagAndLull(long lag, long maxLullMs, DistributedLongSupplier nanoClock) {
        checkNotNegative(lag, "lag must not be negative");
        checkNotNegative(maxLullMs, "maxLullMs must not be negative");

        return new WatermarkPolicyBase() {

            private long maxLullAt = Long.MIN_VALUE;

            @Override
            public long reportEvent(long timestamp) {
                maxLullAt = monotonicTimeMillis() + maxLullMs;
                return makeWmAtLeast(timestamp - lag);
            }

            @Override
            public long getCurrentWatermark() {
                long now = monotonicTimeMillis();
                ensureInitialized(now);
                long millisPastMaxLull = max(0, now - maxLullAt);
                maxLullAt += millisPastMaxLull;
                return advanceWmBy(millisPastMaxLull);
            }

            private void ensureInitialized(long now) {
                if (maxLullAt == Long.MIN_VALUE) {
                    maxLullAt = now + maxLullMs;
                }
            }

            private long monotonicTimeMillis() {
                return NANOSECONDS.toMillis(nanoClock.getAsLong());
            }
        };
    }
}
