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

package com.hazelcast.test.jitter;

import java.text.DateFormat;
import java.util.Date;

import static com.hazelcast.test.jitter.JitterRule.LONG_HICCUP_THRESHOLD;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class Slot {

    private final long startInterval;

    private volatile long accumulatedHiccupsNanos;
    private volatile long maxPauseNanos;
    private volatile int pausesOverThreshold;

    Slot(long startInterval) {
        this.startInterval = startInterval;
    }

    long getStartIntervalMillis() {
        return startInterval;
    }

    long getMaxPauseNanos() {
        return maxPauseNanos;
    }

    long getAccumulatedHiccupsNanos() {
        return accumulatedHiccupsNanos;
    }

    void recordHiccup(long hiccupNanos) {
        accumulatedHiccupsNanos += hiccupNanos;
        maxPauseNanos = max(maxPauseNanos, hiccupNanos);
        if (hiccupNanos > LONG_HICCUP_THRESHOLD) {
            pausesOverThreshold++;
        }
    }

    String toHumanFriendly(DateFormat dateFormat) {
        return dateFormat.format(new Date(startInterval))
                + ", accumulated pauses: " + NANOSECONDS.toMillis(accumulatedHiccupsNanos) + " ms"
                + ", max pause: " + NANOSECONDS.toMillis(maxPauseNanos) + " ms"
                + ", pauses over " + NANOSECONDS.toMillis(LONG_HICCUP_THRESHOLD) + " ms: " + pausesOverThreshold;
    }
}
