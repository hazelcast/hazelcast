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

import static com.hazelcast.jet.impl.util.Util.subtractClamped;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.lang.Math.floorMod;

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
     * value and {@code lastEmittedWm}. It asserts that {@code currentWm >
     * lastEmittedWm}. If it returns a value smaller than or equal to {@code
     * lastEmittedWm}, it means no new watermark will be emitted.
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
     * Returns a policy that doesn't emit watermarks. Use it in pipelines that
     * contain timestamped stages, but no windowing stage.
     */
    @Nonnull
    static WatermarkEmissionPolicy noWatermarks() {
        return (currentWm, lastEmittedWm) -> Long.MIN_VALUE;
    }

    /**
     * Returns a watermark emission policy that ensures that each emitted
     * watermark's value is at least {@code minStep} more than the previous
     * one. This is a general, scenario-agnostic throttling policy.
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByMinStep(long minStep) {
        checkPositive(minStep, "minStep must be >= 1");
        return (currentWm, lastEmittedWm) -> lastEmittedWm + minStep <= currentWm ? currentWm : lastEmittedWm;
    }

    /**
     * Returns a watermark emission policy that will restrict the watermarks to
     * be at the frame boundary defined by {@code wDef}. Additionally ensures
     * that two watermarks are at most {@code maxStep} apart, even if the frame
     * size is larger.
     * <p>
     * This emission policy should be employed to drive a downstream processor
     * that computes a sliding/tumbling window ({@link
     * Processors#accumulateByFrameP} or {@link
     * Processors#aggregateToSlidingWindowP}).
     *
     * @param wDef policy to define frames
     * @param maxStep the maximum distance between two watermarks
     */
    @Nonnull
    static WatermarkEmissionPolicy emitByFrame(SlidingWindowPolicy wDef, long maxStep) {
        checkPositive(maxStep, "maxStep must be >= 1");
        if (maxStep == 1) {
            return noThrottling();
        }
        if (maxStep == Long.MAX_VALUE) {
            return (currentWm, lastEmittedWm) -> wDef.floorFrameTs(currentWm);
        }
        return (currentWm, lastEmittedWm) -> {
            assert currentWm > lastEmittedWm : "currentWm (" + currentWm + ") <= lastEmittedWm(" + lastEmittedWm + ')';
            long currentWmAlignedToFrame = wDef.floorFrameTs(currentWm);
            // Implement the step not as a distance from last watermark, but rather as the closest integer
            // product of step smaller than currentWm. This is to ensure that all processors emit aligned
            // WMs, if they use the same emission policy.
            long currentWmAlignedToStep = subtractClamped(currentWm, floorMod(currentWm, maxStep));
            return Math.max(currentWmAlignedToFrame, currentWmAlignedToStep);
        };
    }
}
