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

import com.hazelcast.internal.util.Preconditions;

import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.impl.util.Util.addClamped;
import static com.hazelcast.jet.impl.util.Util.subtractClamped;
import static com.hazelcast.jet.impl.util.Util.sumHadOverflow;
import static java.lang.Math.floorMod;

/**
 * Contains parameters that define a sliding/tumbling window over which Jet
 * will apply an aggregate function. Internally, Jet computes the window
 * by maintaining <em>frames</em> of size equal to the sliding step. It
 * treats the frame as a "unit range" of timestamps which cannot be further
 * divided and immediately applies the accumulating function to the items
 * belonging to the same frame. This allows Jet to let go of the individual
 * items' data, saving memory. The user-visible consequences of this are
 * that the configured window length must be an integer multiple of the
 * sliding step and that the memory requirements scale with the ratio
 * between window size and the sliding step.
 * <p>
 * A frame is labelled with its timestamp, which is the first timestamp
 * value beyond the range covered by the frame. That timestamp denotes the
 * exact moment on the event timeline where the frame was closed.
 *
 * @since Jet 3.0
 */
public class SlidingWindowPolicy implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long frameSize;
    private final long frameOffset;
    private final long windowSize;

    SlidingWindowPolicy(long frameSize, long frameOffset, long framesPerWindow) {
        checkPositive(frameSize, "frameLength must be positive");
        checkNotNegative(frameOffset, "frameOffset must not be negative");
        checkTrue(frameOffset < frameSize, "frameOffset must be less than frameSize, offset="
                + frameOffset + ", size=" + frameSize);
        checkPositive(framesPerWindow, "framesPerWindow must be positive");

        this.frameSize = frameSize;
        this.frameOffset = frameOffset;
        this.windowSize = frameSize * framesPerWindow;
    }

    /**
     * Returns the length of the frame (equal to the sliding step).
     */
    public long frameSize() {
        return frameSize;
    }

    /**
     * Returns the frame offset. For example, with {@code frameLength = 10} and
     * {@code frameOffset = 5} the frames will start at 5, 15, 25...
     */
    public long frameOffset() {
        return frameOffset;
    }

    /**
     * Returns the length of the window (the size of the timestamp range it covers).
     * It is an integer multiple of {@link #frameSize()}.
     */
    public long windowSize() {
        return windowSize;
    }

    /**
     * Tells whether this definition describes a tumbling window. Tumbling
     * window is a special case of sliding window whose sliding step is equal
     * to its size.
     */
    public boolean isTumbling() {
        return windowSize == frameSize;
    }

    /**
     * Returns the highest frame timestamp less than or equal to the given
     * timestamp. If there is no such {@code long} value, returns {@code
     * Long.MIN_VALUE}.
     */
    public long floorFrameTs(long timestamp) {
        return subtractClamped(timestamp, floorMod(
                (timestamp >= Long.MIN_VALUE + frameOffset ? timestamp : timestamp + frameSize) - frameOffset,
                frameSize
        ));
    }

    /**
     * Returns the lowest frame timestamp greater than the given timestamp. If
     * there is no such {@code long} value, returns {@code Long.MAX_VALUE}.
     */
    public long higherFrameTs(long timestamp) {
        long tsPlusFrame = timestamp + frameSize;
        return sumHadOverflow(timestamp, frameSize, tsPlusFrame)
                ? addClamped(floorFrameTs(timestamp), frameSize)
                : floorFrameTs(tsPlusFrame);
    }

    /**
     * Returns a new window definition where all the frames are shifted by the
     * given offset. More formally, it specifies the value of the lowest
     * non-negative frame timestamp.
     * <p>
     * Given a tumbling window of {@code windowLength = 4}, with no offset the
     * windows would cover the timestamps {@code ..., [-4, 0), [0..4), ...}
     * With {@code offset = 2} they will cover {@code ..., [-2, 2), [2..6),
     * ...}
     */
    public SlidingWindowPolicy withOffset(long offset) {
        return new SlidingWindowPolicy(frameSize, offset, windowSize / frameSize);
    }

    /**
     * Converts this definition to one defining a tumbling window of the
     * same length as this definition's frame.
     */
    public SlidingWindowPolicy toTumblingByFrame() {
        return new SlidingWindowPolicy(frameSize, frameOffset, 1);
    }

    /**
     * Returns the definition of a sliding window of length {@code
     * windowSize} that slides by {@code slideBy}. Given {@code
     * windowSize = 4} and {@code slideBy = 2}, the generated windows would
     * cover timestamps {@code ..., [-2, 2), [0..4), [2..6), [4..8), [6..10),
     * ...}
     * <p>
     * Since the window will be computed internally by maintaining {@link
     * SlidingWindowPolicy frames} of size equal to the sliding step, the
     * configured window length must be an integer multiple of the sliding
     * step.
     *
     * @param windowSize the length of the window, must be a multiple of {@code slideBy}
     * @param slideBy the amount to slide the window by
     */
    public static SlidingWindowPolicy slidingWinPolicy(long windowSize, long slideBy) {
        Preconditions.checkPositive(windowSize, "windowSize must be >= 1");
        Preconditions.checkPositive(slideBy, "slideBy must be >= 1");
        Preconditions.checkTrue(windowSize % slideBy == 0, "windowSize must be an integer multiple of slideBy");
        return new SlidingWindowPolicy(slideBy, 0, windowSize / slideBy);
    }

    /**
     * Returns the definition of a tumbling window of length {@code
     * windowSize}. The tumbling window is a special case of the sliding
     * window with {@code slideBy = windowSize}. Given {@code
     * windowSize = 4}, the generated windows would cover timestamps {@code
     * ..., [-4, 0), [0..4), [4..8), ...}
     */
    public static SlidingWindowPolicy tumblingWinPolicy(long windowSize) {
        return slidingWinPolicy(windowSize, windowSize);
    }
}
