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

package com.hazelcast.jet.core;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * A policy object that decides when when the watermark has advanced
 * enough to emit a new watermark item.
 */
@FunctionalInterface
public interface WatermarkEmissionPolicy extends Serializable {

    /**
     * Decides whether a watermark item with the supplied {@code currentWm}
     * value should be emitted, given the last emitted value {@code
     * lastEmittedWm}.
     */
    boolean shouldEmit(long currentWm, long lastEmittedWm);

    /**
     * Returns a policy that ensures that each emitted watermark has a higher
     * timestamp than the last one. This protects the basic invariant of
     * watermark items (that their timestamps are strictly increasing), but
     * doesn't perform any throttling. Since the timestamps are typically quite
     * dense (in milliseconds), this emission policy will pass through many
     * watermark items that have no useful effect in terms of updating the
     * state of accumulating vertices. It is useful primarily in testing
     * scenarios or some specific cases where it is known that no watermark
     * throttling is needed.
     */
    @Nonnull
    static WatermarkEmissionPolicy suppressDuplicates() {
        return (currentWm, lastEmittedWm) -> currentWm > lastEmittedWm;
    }

    /**
     * Returns a watermark emission policy that ensures that each emitted
     * watermark's value is at least {@code minStep} more than the previous
     * one. This is a general, scenario-agnostic throttling policy.
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByMinStep(long minStep) {
        checkPositive(minStep, "minStep");
        return (currentWm, lastEmittedWm) -> currentWm >= lastEmittedWm + minStep;
    }

    /**
     * Returns a watermark emission policy that ensures that the value of
     * the emitted watermark belongs to a frame higher than the previous
     * watermark's frame, as per the supplied {@code WindowDefinition}. This
     * emission policy should be employed to drive a downstream processor that
     * computes a sliding/tumbling window
     * ({@link com.hazelcast.jet.core.processor.Processors#accumulateByFrameP
     * accumulateByFrame()} or
     * {@link com.hazelcast.jet.core.processor.Processors#aggregateToSlidingWindowP
     * aggregateToSlidingWindow()}).
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByFrame(SlidingWindowPolicy wDef) {
        return (currentWm, lastEmittedWm) -> wDef.floorFrameTs(currentWm) > lastEmittedWm;
    }

    /**
     * Javadoc pending
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByFrame(long frameSize) {
        return emitByFrame(SlidingWindowPolicy.slidingWinPolicy(frameSize, frameSize));
    }
}
