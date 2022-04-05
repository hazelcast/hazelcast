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

package com.hazelcast.jet.core;

import com.hazelcast.function.SupplierEx;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static java.lang.Math.max;

/**
 * This object tracks and determines the current {@link Watermark} given the
 * event timestamps as they occur for a single input stream. Typically the
 * watermark will be advanced with a {@link #limitingLag(long) fixed lag}
 * behind the top observed timestamp so far.
 * <p>
 * This object is used by source processors to determine the current
 * watermark. The processor may choose to create several of these objects
 * to track each source partition separately and each processor will also
 * have their own instance. The implementation does not need to be thread-safe.
 *
 * @see EventTimePolicy
 * @see EventTimeMapper
 *
 * @since Jet 3.0
 */
public interface WatermarkPolicy {

    /**
     * Called to report the observation of an event with the given timestamp.
     * The next call to {@link #getCurrentWatermark()} should reflect this.
     *
     * @param timestamp event's timestamp
     */
    void reportEvent(long timestamp);

    /**
     * Called to get the current watermark. The watermark may advance based
     * just on the passage of time.
     */
    long getCurrentWatermark();

    /**
     * Maintains a watermark that lags behind the top observed timestamp by the
     * given amount.
     * <p>
     * <strong>Note:</strong> if Jet stops receiving events at some point (e.g.,
     * at the end of a business day), the watermark will stop advancing and
     * stay behind the most recent events. Jet will not output the results of
     * aggregating these events until it starts receiving events again (e.g.,
     * at the start of the next business day).
     *
     * @param lag the desired difference between the top observed timestamp
     *            and the watermark
     */
    @Nonnull
    static SupplierEx<WatermarkPolicy> limitingLag(long lag) {
        checkNotNegative(lag, "lag must not be negative");

        return () -> new WatermarkPolicy() {
            private long wm = Long.MIN_VALUE;

            @Override
            public void reportEvent(long timestamp) {
                // avoid overflow
                if (timestamp >= Long.MIN_VALUE + lag) {
                    wm = max(wm, timestamp - lag);
                }
            }

            @Override
            public long getCurrentWatermark() {
                return wm;
            }
        };
    }

    /**
     * Maintains a watermark that lags behind the real time by the given
     * amount. Doesn't consider the event timestamp at all.
     *
     * @param lag the desired difference between the top observed timestamp
     *            and the watermark
     * @since Jet 4.3
     */
    @Nonnull
    static SupplierEx<WatermarkPolicy> limitingRealTimeLag(long lag) {
        checkNotNegative(lag, "lag must not be negative");

        return () -> new WatermarkPolicy() {
            @Override
            public void reportEvent(long timestamp) {
            }

            @Override
            public long getCurrentWatermark() {
                return System.currentTimeMillis() - lag;
            }
        };
    }
}
