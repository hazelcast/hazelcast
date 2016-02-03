/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.concurrent;


import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Math.min;

/**
 * Idling strategy for threads when they have no work to do.
 * <p/>
 * Spin for maxSpins, then
 * {@link Thread#yield()} for maxYields, then
 * {@link LockSupport#parkNanos(long)} on an exponential backoff to maxParkPeriodNs
 */
public class BackoffIdleStrategy implements IdleStrategy {
    private final long yieldThreshold;
    private final long parkThreshold;
    private final long minParkPeriodNs;
    private final long maxParkPeriodNs;
    private final int maxShift;

    /**
     * Create a set of state tracking idle behavior
     *
     * @param maxSpins        to perform before moving to {@link Thread#yield()}
     * @param maxYields       to perform before moving to {@link LockSupport#parkNanos(long)}
     * @param minParkPeriodNs to use when initiating parking
     * @param maxParkPeriodNs to use when parking
     */
    public BackoffIdleStrategy(long maxSpins, long maxYields, long minParkPeriodNs, long maxParkPeriodNs) {
        checkNotNegative(maxSpins, "maxSpins must be positive or zero");
        checkNotNegative(maxYields, "maxYields must be positive or zero");
        checkNotNegative(minParkPeriodNs, "minParkPeriodNs must be positive or zero");
        checkNotNegative(maxParkPeriodNs - minParkPeriodNs,
                "maxParkPeriodNs must be greater than or equal to minParkPeriodNs");
        this.yieldThreshold = maxSpins;
        this.parkThreshold = maxSpins + maxYields;
        this.minParkPeriodNs = minParkPeriodNs;
        this.maxParkPeriodNs = maxParkPeriodNs;
        this.maxShift = numberOfLeadingZeros(minParkPeriodNs) - numberOfLeadingZeros(maxParkPeriodNs);
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean idle(long n) {
        if (n < yieldThreshold) {
            return false;
        }
        if (n < parkThreshold) {
            Thread.yield();
            return false;
        }
        final long parkTime = parkTime(n);
        LockSupport.parkNanos(parkTime);
        return parkTime == maxParkPeriodNs;
    }

    long parkTime(long n) {
        final long proposedShift = n - parkThreshold;
        final long allowedShift = min(maxShift, proposedShift);
        return proposedShift > maxShift ? maxParkPeriodNs
             : proposedShift < maxShift ? minParkPeriodNs << allowedShift
             : min(minParkPeriodNs << allowedShift, maxParkPeriodNs);
    }
}

