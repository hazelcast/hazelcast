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

package com.hazelcast.jet.retry;

import com.hazelcast.jet.retry.impl.IntervalFunctions;

import java.io.Serializable;

/**
 * Function that computes the wait time necessary for each subsequent retry
 * attempt. The input is the sequence number of the attempt (1 = first failed
 * attempt, 2 = second failed attempt and so on), the output is the wait time in
 * milliseconds.
 *
 * @since Jet 4.3
 */
@FunctionalInterface
public interface IntervalFunction extends Serializable {

    /**
     * Creates an {@code IntervalFunction} which returns a fixed interval in
     * milliseconds.
     */
    static IntervalFunction constant(long intervalMs) {
        return IntervalFunctions.constant(intervalMs);
    }

    /**
     * Creates an {@code IntervalFunction} which starts from the specified wait
     * interval, on the first attempt, and for each subsequent attempt uses
     * a longer interval, equal to the previous wait duration multiplied by the
     * provided scaling factor (so for example: 1, 2, 4, 8, ...).
     */
    static IntervalFunction exponentialBackoff(long intervalMillis, double multiplier) {
        return IntervalFunctions.exponentialBackoff(intervalMillis, multiplier);
    }

    /**
     * Creates an {@code IntervalFunction} which starts from the specified wait
     * interval, on the first attempt, and for each subsequent attempt uses
     * a longer interval, equal to the previous wait duration multiplied by the
     * provided scaling factor (so for example: 1, 2, 4, 8, ...). The wait
     * interval increases only until a certain maximum value, ie. is capped. All
     * subsequent wait intervals returned will be equal to the cap.
     */
    static IntervalFunction exponentialBackoffWithCap(long intervalMillis, double multiplier, long capMillis) {
        return IntervalFunctions.exponentialBackoffWithCap(intervalMillis, multiplier, capMillis);
    }

    /**
     * Returns the wait time required before the specified attempt. In this
     * context attempt no. 1 means the first attempt, so it's not an index and
     * zero is not an allowed input.
     */
    long waitAfterAttempt(int attempt);
}
