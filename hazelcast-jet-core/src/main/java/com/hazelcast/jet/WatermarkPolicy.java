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

package com.hazelcast.jet;

/**
 * A policy object that decides on the watermark in a single data
 * (sub)stream. The timestamp of every observed item should be reported
 * to this object and it will respond with the current value of the
 * watermark. Watermark may also advance in the absence of observed
 * events; {@link #getCurrentWatermark()} can be called at any
 * time to see this change.
 */
public interface WatermarkPolicy {

    /**
     * Called to report the observation of an event with the given timestamp.
     * Returns the watermark that should be (or have been) emitted before
     * the event.
     *
     * @param timestamp event's timestamp
     * @return the watermark value. May be {@code Long.MIN_VALUE} if there is
     *         insufficient information to determine any watermark (e.g., no events
     *         observed)
     */
    long reportEvent(long timestamp);

    /**
     * Called to get the current watermark in the absence of an observed
     * event. The watermark may advance just based on the passage of time.
     */
    long getCurrentWatermark();

    /**
     * Returns a new watermark policy that throttles this policy's output
     * by suppressing the advancement of watermark by less than the supplied
     * {@code minStep}. The throttling policy will ignore any watermark
     * returned from this policy that is less than {@code minStep} ahead of
     * the top watermark returned from the throttling policy.
     */
    default WatermarkPolicy throttleByMinStep(long minStep) {
        return new WatermarkPolicy() {

            private long nextWm = Long.MIN_VALUE;
            private long currWm = Long.MIN_VALUE;

            @Override
            public long reportEvent(long timestamp) {
                long newWm = WatermarkPolicy.this.reportEvent(timestamp);
                return advanceThrottled(newWm);
            }

            @Override
            public long getCurrentWatermark() {
                long newWm = WatermarkPolicy.this.getCurrentWatermark();
                return advanceThrottled(newWm);
            }

            private long advanceThrottled(long newWm) {
                if (newWm < nextWm) {
                    return currWm;
                }
                nextWm = newWm + minStep;
                currWm = newWm;
                return newWm;
            }
        };
    }

    /**
     * Returns a new watermark policy that throttles this policy's output by
     * adjusting it to its {@link WindowDefinition#floorFrameTs(long)
     * floorFrameTs} as returned from the supplied {@code WindowDefinition}.
     * This throttling policy should be employed to drive a downstream
     * processor that computes a sliding/tumbling window
     * ({@link com.hazelcast.jet.processor.Processors#accumulateByFrame(
     *      com.hazelcast.jet.function.DistributedFunction,
     *      com.hazelcast.jet.function.DistributedToLongFunction,
     *      TimestampKind, WindowDefinition, AggregateOperation)
     * accumulateByFrame()} or
     * {@link com.hazelcast.jet.processor.Processors#aggregateToSlidingWindow(
     *      com.hazelcast.jet.function.DistributedFunction,
     *      com.hazelcast.jet.function.DistributedToLongFunction,
     *      TimestampKind, WindowDefinition, AggregateOperation)
     * aggregateToSlidingWindow()}).
     */
    default WatermarkPolicy throttleByFrame(WindowDefinition winDef) {
        return new WatermarkPolicy() {
            private long lastWm = Long.MIN_VALUE;

            @Override
            public long reportEvent(long timestamp) {
                return advanceThrottled(WatermarkPolicy.this.reportEvent(timestamp));
            }

            @Override
            public long getCurrentWatermark() {
                return advanceThrottled(WatermarkPolicy.this.getCurrentWatermark());
            }

            private long advanceThrottled(long proposedWm) {
                return proposedWm == lastWm
                        ? lastWm
                        : (lastWm = winDef.floorFrameTs(proposedWm));
            }
        };
    }
}
