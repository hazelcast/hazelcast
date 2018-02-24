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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.core.WatermarkPolicies;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.function.DistributedLongSupplier;

import javax.annotation.Nonnull;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Util and test support classes for {@link WatermarkPolicies}
 */
public final class WatermarkPolicyUtil {

    private WatermarkPolicyUtil() {
    }

    @Nonnull
    public static WatermarkPolicy limitingTimestampAndWallClockLag(
            long timestampLag, long wallClockLag, DistributedLongSupplier wallClock
    ) {
        checkNotNegative(timestampLag, "timestampLag must not be negative");
        checkNotNegative(wallClockLag, "wallClockLag must not be negative");

        return new WatermarkPolicyBase() {

            @Override
            public long reportEvent(long timestamp) {
                long wm = updateFromWallClock();
                if (timestamp > Long.MIN_VALUE + timestampLag) {
                    wm = makeWmAtLeast(timestamp - timestampLag);
                }
                return wm;
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

    @Nonnull
    public static WatermarkPolicy limitingLagAndLull(
            long lag, long maxLullMs, DistributedLongSupplier nanoClock
    ) {
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
                long currentWm = super.getCurrentWatermark();
                return currentWm > Long.MIN_VALUE ? advanceWmBy(millisPastMaxLull) : currentWm;
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

    @Nonnull
    public static WatermarkPolicy limitingLagAndDelay(
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

    public abstract static class WatermarkPolicyBase implements WatermarkPolicy {

        private long wm = Long.MIN_VALUE;

        protected long makeWmAtLeast(long proposedWm) {
            wm = max(wm, proposedWm);
            return wm;
        }

        protected long advanceWmBy(long amount) {
            wm += amount;
            return wm;
        }

        @Override
        public long getCurrentWatermark() {
            return wm;
        }
    }
}
