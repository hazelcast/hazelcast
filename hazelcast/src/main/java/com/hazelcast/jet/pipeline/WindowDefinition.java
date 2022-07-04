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

package com.hazelcast.jet.pipeline;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * The definition of the window for a windowed aggregation operation. To obtain
 * a window definition, use the factory methods provided in this interface.
 *
 * @since Jet 3.0
 */
public abstract class WindowDefinition implements Serializable {

    private long earlyResultPeriodMs;

    /**
     * Returns the {@linkplain #setEarlyResultsPeriod early results period} for
     * this window definition. A return value of zero means that the stage
     * won't emit early window results.
     *
     * @since Jet 3.1
     */
    public long earlyResultsPeriod() {
        return earlyResultPeriodMs;
    }

    /**
     * Sets the period in milliseconds at which the windowed aggregation
     * stage will emit partial results of all the windows that contain some
     * data, but the watermark hasn't yet advanced enough to close them and
     * emit the final results.
     * <p>
     * Consider this example: we're collecting a 1-minute tumbling window of
     * stock exchange data. The results we're getting pertain to the minute
     * that just elapsed, but we'd also like to detect any sudden changes
     * within the running minute. We can set the early results period to
     * 1000 ms and get an update every second for the window that's currently
     * being filled with data.
     * <p>
     * Note that, for a sliding window, there will be many incomplete windows
     * that contain some data and you'll get the early results for all of them.
     * Similarly, if you configure a high-enough {@code maxLag} for the event
     * timestamps, there can be more than one tumbling/session window with
     * early results.
     * <p>
     * The default value is zero, which means "don't emit early results".
     *
     * @param earlyResultPeriodMs the period in milliseconds from one start of
     *                           the emission of early results to the next one
     * @return {@code this}
     *
     * @since Jet 3.1
     */
    public WindowDefinition setEarlyResultsPeriod(long earlyResultPeriodMs) {
        this.earlyResultPeriodMs = earlyResultPeriodMs;
        return this;
    }

    /**
     * Returns a sliding window definition with the given parameters.
     * In a sliding window, the incoming items are grouped by fixed-size
     * and overlapping intervals where the windows "slide" by the given size.
     * <p>
     * For example, given the timestamps
     * {@code [0, 1, 2, 3, 4, 5, 6]}, {@code windowSize} of 4 and {@code @slideBy} of 2,
     * the timestamps would be grouped into the following windows:
     *
     * <pre>
     *     [0, 1], [0, 1, 2, 3], [2, 3, 4, 5], [4, 5, 6], [6]
     * </pre>
     *
     * A sliding window where window size and slide by are the same is equivalent to a
     * tumbling window.
     * <p>
     * Find more information see the Hazelcast Jet Reference Manual section
     * Sliding and Tumbling Window.
     *
     * @param windowSize the size of the window in the items' timestamp unit (typically milliseconds)
     * @param slideBy the size of the sliding step. Window size must be multiple of this number.
     */
    @Nonnull
    public static SlidingWindowDefinition sliding(long windowSize, long slideBy) {
        return new SlidingWindowDefinition(windowSize, slideBy);
    }

    /**
     * Returns a tumbling window definition with the given parameters.
     * In a tumbling window the incoming items are grouped by
     * fixed size, contiguous and non-overlapping intervals.
     * <p>
     * For example, given the timestamps
     * {@code [0, 1, 2, 3, 4, 5, 6]} and a {@code windowSize} of 2, the timestamps
     * would be grouped into the following windows:
     *
     * <pre>
     *     [0, 1], [2, 3], [4, 5], [6]
     * </pre>
     *
     * @param windowSize the size of the window in the items' timestamp unit (typically milliseconds)
     *
     */
    @Nonnull
    public static SlidingWindowDefinition tumbling(long windowSize) {
        return new SlidingWindowDefinition(windowSize, windowSize);
    }

    /**
     * Returns a window definition that aggregates events into session windows.
     * In a session window, the items are grouped into variable size,
     * non-overlapping windows which are formed by periods of activity followed
     * by inactivity. A session window is kept open as long as the gap
     * between the events remains less than the given {@code sessionTimeout} and
     * is closed when the gap exceeds the timeout.
     * <p>
     * For example, given the timestamps
     * {@code [0, 1, 4, 8, 9, 10, 15]} and a {@code sessionTimeout} of 2, the timestamps
     * would be grouped into the following windows:
     *
     * <pre>
     *     [0, 1], [4], [8, 9, 10], [15]
     * </pre>
     * @param sessionTimeout the upper bound on the difference between any two
     *                       consecutive timestamps in a window, given in the
     *                       items' timestamp unit (typically milliseconds)
     */
    @Nonnull
    public static SessionWindowDefinition session(long sessionTimeout) {
        return new SessionWindowDefinition(sessionTimeout);
    }
}
