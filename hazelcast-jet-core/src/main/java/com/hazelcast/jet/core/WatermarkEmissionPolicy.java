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

package com.hazelcast.jet.core;

import com.hazelcast.jet.core.processor.Processors;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * A policy object that decides when the watermark has advanced enough to
 * require emitting a new watermark item. For example, a sliding/tumbling
 * window processor doesn't need to observe more than one watermark item
 * per frame.
 */
@FunctionalInterface
public interface WatermarkEmissionPolicy extends Serializable {

    /**
     * The null-object. Its method throws an exception.
     */
    WatermarkEmissionPolicy NULL_EMIT_POLICY = (currentWm, lastEmittedWm) -> {
        throw new UnsupportedOperationException("Tried to use the NULL watermark emission policy");
    };

    /**
     * Decides which watermark to emit based on the supplied {@code currentWm}
     * value and {@code lastEmittedWm}. We expect the {@code currentWm >
     * lastEmittedWm}.
     */
    long throttleWm(long currentWm, long lastEmittedWm);

    /**
     * Returns a policy that does no throttling: emits each watermark. Since the
     * timestamps are typically quite dense (in milliseconds), this emission
     * policy will pass through many watermark items that have no useful effect
     * in terms of updating the state of accumulating vertices. It is useful
     * primarily in testing scenarios or some specific cases where it is known
     * that no watermark throttling is needed.
     */
    @Nonnull
    static WatermarkEmissionPolicy noThrottling() {
        return (currentWm, lastEmittedWm) -> currentWm;
    }

    /**
     * Returns a watermark emission policy that ensures that each emitted
     * watermark's value is at least {@code minStep} more than the previous
     * one. This is a general, scenario-agnostic throttling policy.
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByMinStep(long minStep) {
        checkPositive(minStep, "minStep should be > 0");
        return (currentWm, lastEmittedWm) -> lastEmittedWm + minStep <= currentWm ? currentWm : lastEmittedWm;
    }

    /**
     * Returns a watermark emission policy that ensures that the value of
     * the emitted watermark belongs to a frame higher than the previous
     * watermark's frame, as per the supplied {@code WindowDefinition}. This
     * emission policy should be employed to drive a downstream processor that
     * computes a sliding/tumbling window
     * ({@link Processors#accumulateByFrameP} or
     * {@link Processors#aggregateToSlidingWindowP}).
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByFrame(SlidingWindowPolicy wDef) {
        return (currentWm, lastEmittedWm) -> Math.max(wDef.floorFrameTs(currentWm), lastEmittedWm);
    }
}
