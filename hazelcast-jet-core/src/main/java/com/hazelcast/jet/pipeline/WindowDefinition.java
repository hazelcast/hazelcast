/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

/**
 * The definition of the window for a windowed aggregation operation. The
 * enum {@link WindowKind} enumerates the kinds of window that Jet supports.
 * To obtain a window definition, use the factory methods provided in this
 * interface.
 */
public interface WindowDefinition {

    /**
     * Enumerates the kinds of window that Jet supports.
     */
    enum WindowKind {
        /**
         * A sliding window "slides" along the time axis in discrete steps. You can
         * specify the size and the sliding step. The size of the window must be
         * divisible by the sliding step.
         */
        SLIDING,
        /**
         * The session window captures bursts of events delimited by periods of
         * quiescence. You can specify the duration of the quiet period that causes
         * the window to close.
         */
        SESSION
    }

    /**
     * Returns what kind of window this definition describes.
     */
    @Nonnull
    WindowKind kind();

    /**
     * Returns this window definition downcast to the type determined through
     * type inference at the call site. It will be an unchecked downcast and
     * may fail at runtime with a {@code ClassCastException}.
     *
     * @param <W> The target type of the downcast
     * @return this object, downcast into the inferred type
     */
    @Nonnull
    <W extends WindowDefinition> W downcast();

    /**
     * Returns the optimal watermark stride for this window definition.
     * Watermarks that are more spaced out are better for performance, but they
     * hurt the responsiveness of a windowed pipeline stage. The Planner will
     * determine the actual stride, which may be an integer fraction of the
     * value returned here.
     */
    long preferredWatermarkStride();

    /**
     * Returns a {@link WindowKind#SLIDING sliding} window definition with the
     * given parameters.
     * <p>
     * Find more information in the Hazelcast Jet Reference Manual, Sliding and
     * Tumbling Window.
     *
     * @param windowSize the size of the window (size of the range of the timestamps it covers)
     * @param slideBy the size of the sliding step. Window size must be multiple of this number.
     */
    @Nonnull
    static SlidingWindowDef sliding(long windowSize, long slideBy) {
        return new SlidingWindowDef(windowSize, slideBy);
    }

    /**
     * Returns a tumbling window definition with the given parameters. Tumbling
     * window is a special case of {@link WindowKind#SLIDING sliding} where the
     * slide is equal to window size.
     *
     * @param windowSize the size of the window (size of the range of the timestamps it covers)
     */
    @Nonnull
    static SlidingWindowDef tumbling(long windowSize) {
        return new SlidingWindowDef(windowSize, windowSize);
    }

    /**
     * Returns a window definition that aggregates events into session windows.
     * Events and windows under different grouping keys are treated
     * independently.
     * <p>
     * The functioning of session windows is easiest to explain in terms of the
     * <em>event interval</em>: the range {@code [timestamp, timestamp +
     * sessionTimeout)}. Initially an event causes a new session window to be
     * created, covering exactly the event interval. A following event under
     * the same key belongs to this window iff its interval overlaps it. The
     * window is extended to cover the entire interval of the new event. The
     * event may happen to belong to two existing windows if its interval
     * bridges the gap between them; in that case they are combined into one.
     *
     * <h4>Behavior when changing session timeout on job update</h4>
     *
     * It is allowed to change session timeout in an updated pipeline. Windows
     * are stored in the snapshot with the end time equal to the time of the
     * latest event + session timeout. A new event after the update will be
     * merged into the old window using the new timeout. This will cause that
     * the windows after the update will have varying timeouts until all
     * windows from before the update are emitted.
     * <p>
     * For example: say {@code E(n)} is an event with timestamp {@code n} and
     * {@code W(m, n)} is a window with {@code startTime=m} and {@code
     * endTime=n}. Session timeout is 10. We receive {@code E(50)}, we'll store
     * it in a window {@code W(50, 60)}. Then, job is updated and session
     * timeout changes to 20. If we then receive {@code E(45)}, we'll handle it
     * as merging of the restored {@code W(50, 60)} and of {@code W(45, 65)},
     * created from the new event and new timeout. It will result in {@code
     * W(45, 65)}. Thus, the actual session timeout in this window will be 15.
     *
     * @param sessionTimeout the exclusive upper bound on the difference between any two
     *                       successive timestamps included in a window.
     */
    @Nonnull
    static SessionWindowDef session(long sessionTimeout) {
        return new SessionWindowDef(sessionTimeout);
    }
}
