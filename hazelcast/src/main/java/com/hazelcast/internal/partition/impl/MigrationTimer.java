/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.util.Timer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * the total elapsed time of executions from source to destination endpoints
 */
public class MigrationTimer {
    /**
     * on the latest repartitioning round.
     */
    private final LongAccumulator elapsedNanoseconds = new LongAccumulator(Long::max, 0);

    /**
     * the sum of all previous rounds, except the latest, since the beginning.
     */
    private volatile long lastTotalElapsedNanoseconds;

    protected void markNewRepartition() {
        lastTotalElapsedNanoseconds = getTotalElapsedNanoseconds();
        elapsedNanoseconds.reset();
    }

    protected void calculateElapsed(final long lastRepartitionNanos) {
        elapsedNanoseconds.accumulate(Timer.nanosElapsed(lastRepartitionNanos));
    }

    /**
     * @return nanoseconds
     * @see #elapsedNanoseconds
     */
    public long getElapsedNanoseconds() {
        return elapsedNanoseconds.get();
    }

    /**
     * @return milliseconds
     * @see #elapsedNanoseconds
     */
    public long getElapsedMilliseconds() {
        return TimeUnit.NANOSECONDS.toMillis(getElapsedNanoseconds());
    }

    /**
     * @return the sum of all previous rounds, since the beginning - nanoseconds
     */
    public long getTotalElapsedNanoseconds() {
        return lastTotalElapsedNanoseconds + getElapsedNanoseconds();
    }

    /**
     * @return the sum of all previous rounds, since the beginning - milliseconds
     */
    public long getTotalElapsedMilliseconds() {
        return TimeUnit.NANOSECONDS.toMillis(getTotalElapsedNanoseconds());
    }
}
