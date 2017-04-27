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

package com.hazelcast.jet.windowing;

import com.hazelcast.util.Preconditions;

import java.io.Serializable;

import static com.hazelcast.jet.impl.util.Util.addClamped;
import static com.hazelcast.jet.impl.util.Util.subtractClamped;
import static com.hazelcast.jet.impl.util.Util.sumHadOverflow;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Math.floorMod;

/**
 * Contains parameters that define a sliding/tumbling window over which Jet
 * will apply an aggregate function. Internally, Jet computes the window
 * by maintaining <em>frames</em> of size equal to the sliding step. It
 * treats the frame as a "unit range" of event seqs which cannot be further
 * divided and immediately applies the aggregate function to the items
 * belonging to the same frame. This allows Jet to let go of the individual
 * items' data, saving memory. The user-visible consequences of this are
 * that the configured window length must be an integer multiple of the
 * sliding step and that the memory requirements scale with the ratio
 * between window size and the sliding step.
 * <p>
 * A frame is labelled with its {@code frameSeq}, which is the first
 * {@code eventSeq} beyond the range covered by the frame. In other words,
 * it is the starting {@code eventSeq} of the next frame, or, in event-time
 * language, the "closing time" of the frame.
 */
public class WindowDefinition implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long frameLength;
    private final long frameOffset;
    private final long windowLength;

    WindowDefinition(long frameLength, long frameOffset, long framesPerWindow) {
        checkPositive(frameLength, "frameLength must be positive");
        checkNotNegative(frameOffset, "frameOffset must not be negative");
        checkTrue(frameOffset < frameLength, "frameOffset must be less than frameLength");
        checkPositive(framesPerWindow, "framesPerWindow must be positive");

        this.frameLength = frameLength;
        this.frameOffset = frameOffset;
        this.windowLength = frameLength * framesPerWindow;
    }

    /**
     * Returns the length of the frame (equal to the sliding step).
     */
    public long frameLength() {
        return frameLength;
    }

    /**
     * Returns the frame offset. For example, if {@code frameLength = 10} and
     * {@code frameOffset = 5}, then frames will start at 5, 15, 25...
     */
    public long frameOffset() {
        return frameOffset;
    }

    /**
     * Returns the length of the window in terms of {@code eventSeq}. It is an
     * integer multiple of {@link #frameLength()}.
     */
    public long windowLength() {
        return windowLength;
    }

    /**
     * Returns {@code true}, if this is a tumbling window.
     */
    public boolean isTumbling() {
        return windowLength == frameLength;
    }

    /**
     * Returns a new window definition where all the frames are shifted by the
     * given offset. More formally, it specifies the value of the lowest
     * non-negative {@code frameSeq}.
     * <p>
     * Given a tumbling window of {@code windowLength = 4}, with no offset the
     * windows would cover the event seqs {@code ..., [-4, 0), [0..4), ...}
     * With {@code offset = 2} they will cover the seqs {@code ..., [-2, 2),
     * [2..6), ...}
     */
    public WindowDefinition withOffset(long offset) {
        return new WindowDefinition(frameLength, offset, windowLength / frameLength);
    }

    /**
     * Returns the highest {@code frameSeq} less than or equal to the given
     * {@code eventSeq}. If there is no such {@code long} value, returns {@code
     * Long.MIN_VALUE}.
     */
    long floorFrameSeq(long eventSeq) {
        return subtractClamped(eventSeq, floorMod(
                (eventSeq >= Long.MIN_VALUE + frameOffset ? eventSeq : eventSeq + frameLength) - frameOffset,
                frameLength
        ));
    }

    /**
     * Returns the lowest {@code frameSeq} greater than the given {@code
     * eventSeq}. If there is no such value, returns {@code Long.MAX_VALUE}.
     */
    long higherFrameSeq(long eventSeq) {
        long seqPlusFrame = eventSeq + frameLength;
        return sumHadOverflow(eventSeq, frameLength, seqPlusFrame)
                ? addClamped(floorFrameSeq(eventSeq), frameLength)
                : floorFrameSeq(seqPlusFrame);
    }

    /**
     * Returns the definition of a sliding window of length {@code
     * windowLength} that slides by {@code slideBy}. Given {@code
     * windowLength = 4} and {@code slideBy = 2}, the generated windows would
     * cover event seqs {@code ..., [-2, 2), [0..4), [2..6), [4..8), [6..10),
     * ...}
     * <p>
     * Since the window will be computed internally by maintaining {@link
     * WindowDefinition frames} of size equal to the sliding step, the
     * configured window length must be an integer multiple of the sliding
     * step.
     *
     * @param windowLength the length of the window, must be a multiple of {@code slideBy}
     * @param slideBy the amount to slide the window by
     */
    public static WindowDefinition slidingWindowDef(long windowLength, long slideBy) {
        Preconditions.checkTrue(windowLength % slideBy == 0, "windowLength must be a multiple of slideBy");
        return new WindowDefinition(slideBy, 0, windowLength / slideBy);
    }

    /**
     * Returns a new tumbling window of length {@code windowLength}. The
     * tumbling window is a special case of the sliding window with {@code
     * slideBy = windowLength}. Given {@code windowLength = 4}, the generated
     * windows would cover event seqs {@code ..., [-4, 0), [0..4), [4..8), ...}
     */
    public static WindowDefinition tumblingWindowDef(long windowLength) {
        return slidingWindowDef(windowLength, windowLength);
    }
}
