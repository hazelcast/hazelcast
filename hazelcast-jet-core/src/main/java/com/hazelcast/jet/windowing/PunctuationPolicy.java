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

/**
 * A policy object that decides on the punctuation in a single data
 * (sub)stream. The timestamp of every observed item should be reported
 * to this object and it will respond with the current value of the
 * punctuation. Punctuation may also advance in the absence of observed
 * events; {@link #getCurrentPunctuation()} can be called at any
 * time to see this change.
 */
public interface PunctuationPolicy {

    /**
     * Called to report the observation of an event with the given timestamp.
     * Returns the punctuation that should be (or have been) emitted before
     * the event.
     *
     * @param timestamp event's timestamp
     * @return the punctuation value. May be {@code Long.MIN_VALUE} if there is
     *         insufficient information to determine any punctuation (e.g., no events
     *         observed)
     */
    long reportEvent(long timestamp);

    /**
     * Called to get the current punctuation in the absence of an observed
     * event. The punctuation may advance just based on the passage of time.
     */
    long getCurrentPunctuation();

    /**
     * Returns a new punctuation policy that throttles this policy's output
     * by suppressing the advancement of punctuation by less than the supplied
     * {@code minStep}. The throttling policy will ignore any punctuation
     * returned from this policy that is less than {@code minStep} ahead of
     * the top punctuation returned from the throttling policy.
     */
    default PunctuationPolicy throttleByMinStep(long minStep) {
        return new PunctuationPolicy() {

            private long nextPunc = Long.MIN_VALUE;
            private long currPunc = Long.MIN_VALUE;

            @Override
            public long reportEvent(long timestamp) {
                long newPunc = PunctuationPolicy.this.reportEvent(timestamp);
                return advanceThrottled(newPunc);
            }

            @Override
            public long getCurrentPunctuation() {
                long newPunc = PunctuationPolicy.this.getCurrentPunctuation();
                return advanceThrottled(newPunc);
            }

            private long advanceThrottled(long newPunc) {
                if (newPunc < nextPunc) {
                    return currPunc;
                }
                nextPunc = newPunc + minStep;
                currPunc = newPunc;
                return newPunc;
            }
        };
    }

    /**
     * Returns a new punctuation policy that throttles this policy's output by
     * adjusting it to its {@link WindowDefinition#floorFrameTs(long)
     * floorFrameTs} as returned from the supplied {@code WindowDefinition}.
     * This throttling policy should be employed to drive a downstream
     * processor that computes a sliding/tumbling window
     * ({@link WindowingProcessors#slidingWindowStage1(
     *      com.hazelcast.jet.function.DistributedFunction,
     *      com.hazelcast.jet.function.DistributedToLongFunction,
     *      WindowDefinition, WindowOperation)
     * slidingWindowStage1} or
     * {@link WindowingProcessors#slidingWindowSingleStage(
     *      com.hazelcast.jet.function.DistributedFunction,
     *      com.hazelcast.jet.function.DistributedToLongFunction,
     *      WindowDefinition, WindowOperation)
     * slidingWindowSingleStage}).
     */
    default PunctuationPolicy throttleByFrame(WindowDefinition winDef) {
        return new PunctuationPolicy() {
            private long lastPunc = Long.MIN_VALUE;

            @Override
            public long reportEvent(long timestamp) {
                return advanceThrottled(PunctuationPolicy.this.reportEvent(timestamp));
            }

            @Override
            public long getCurrentPunctuation() {
                return advanceThrottled(PunctuationPolicy.this.getCurrentPunctuation());
            }

            private long advanceThrottled(long proposedPunc) {
                return proposedPunc == lastPunc
                        ? lastPunc
                        : (lastPunc = winDef.floorFrameTs(proposedPunc));
            }
        };
    }
}
