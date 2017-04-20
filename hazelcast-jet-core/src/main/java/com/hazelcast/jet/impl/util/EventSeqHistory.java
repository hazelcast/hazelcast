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

package com.hazelcast.jet.impl.util;

import java.util.Arrays;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Helper class to implement the logic needed to enforce the maximum
 * retention time of events in a windowing stream processor. To use this
 * class, call {@link #sample(long, long) sample(now, currValue)}
 * at regular intervals with the current system time and the top observed
 * {@code eventSeq} so far, and interpret the returned value as the minimum
 * value of punctuation that should be/have been emitted. The current time
 * should be obtained from {@code System.nanoTime()} because, unlike
 * {@code System.currentTimeMillis()}, its source is a monotonic clock.
 * <p>
 * This class maintains a circular FIFO buffer of samples acquired over the
 * period starting at {@code maxRetain} time units ago and extending to the
 * present. The period is divided into {@code numStoredSamples} equally-sized
 * intervals and each such interval is mapped to a slot in the buffer.
 * A given {@code sample()} call maps the supplied timestamp to a slot in
 * the buffer, possibly remapping the slots to more recent intervals
 * (thereby automatically discarding the old intervals), updates that slot,
 * and returns the value of the oldest ("head") slot after remapping. This
 * slot will contain a sample acquired at most {@code maxRetain} time units
 * ago.
 * <p>
 * <strong>NOTE:</strong> this class is implemented in terms of some
 * assumptions on the mode of usage:
 * <ol><li>
 *     {@code maxRetain} is expected to be much larger than {@code
 *     numStoredSamples} and uses an integer size of the sample interval.
 *     Therefore the supplied {@code maxRetain} is rounded down to the nearest
 *     multiple of {@code numStoredSamples}.
 * </li><li>
 *     {@link #sample(long, long) sample()} method is expected to be called
 *     with monotonically increasing timestamps and samples and therefore
 *     {@code sample()} always updates the "tail" slot, corresponding to the
 *     most recent time interval. If called with a timestamp less than the
 *     highest timestamp used so far, it will still update the tail slot.
 * </li></ol>
 */
public class EventSeqHistory {

    private final long[] samples;
    private final long sampleInterval;

    private int tailIndex;
    private long advanceAt = Long.MIN_VALUE;

    /**
     * @param maxRetain the length of the period over which to keep the {@code sample} history
     * @param numStoredSamples the number of remembered historical {@code sample} values
     */
    public EventSeqHistory(long maxRetain, int numStoredSamples) {
        checkPositive(numStoredSamples, "numStoredSamples must be at least one");
        samples = new long[numStoredSamples + 1];
        sampleInterval = maxRetain / numStoredSamples;
        checkPositive(sampleInterval, "maxRetain must be at least as much as numStoredSamples");
        Arrays.fill(samples, Long.MIN_VALUE);
    }

    /**
     * Called to report a new sample along with the timestamp when it was taken.
     * Returns the sample that best matches the point in time {@code maxRetain}
     * units ago, or {@link Long#MIN_VALUE} if sampling started less than
     * {@code maxRetain} time units ago.
     *
     * @param now current system time; must not be less than the time used in the previous
     *            call
     * @param value the current value of the tracked quantity
     */
    public long sample(long now, long value) {
        int i = 0;
        for (; advanceAt <= now && i < samples.length; advanceAt += sampleInterval, i++) {
            int oldTailIdx = tailIndex;
            tailIndex = nextTailIndex();
            // Initialize the next tail with the previous tail value.
            // If sample() wasn't called for longer than the sample interval,
            // this is the best estimate we have for the slot.
            samples[tailIndex] = samples[oldTailIdx];
        }

        if (i == samples.length) {
            advanceAt = now + sampleInterval;
        }

        samples[tailIndex] = value;
        // `samples` is a circular buffer which is always full,
        // so `samples[nextTailIndex()]` is the head value (the oldest one)
        return samples[nextTailIndex()];
    }

    private int nextTailIndex() {
        return tailIndex + 1 < samples.length ? tailIndex + 1 : 0;
    }
}
