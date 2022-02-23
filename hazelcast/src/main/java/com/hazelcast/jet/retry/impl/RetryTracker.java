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

package com.hazelcast.jet.retry.impl;

import com.hazelcast.jet.retry.RetryStrategy;

import java.util.function.LongSupplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Helper class for tracking retry processes defined via {@link RetryStrategy}
 * instances.
 */
public class RetryTracker {

    private final RetryStrategy strategy;
    private final LongSupplier currentTime;

    private int attempt;
    private long nextTryNanoTime;

    public RetryTracker(RetryStrategy strategy) {
        this(strategy, System::nanoTime);
    }

    RetryTracker(RetryStrategy strategy, LongSupplier currentTime) {
        this.strategy = strategy;
        this.currentTime = currentTime;
    }

    public void reset() {
        this.attempt = 0;
        this.nextTryNanoTime = currentTime.getAsLong();
    }

    public boolean needsToWait() {
        return attempt > 0 && currentTime.getAsLong() < nextTryNanoTime;
    }

    public boolean shouldTryAgain() {
        int maxAttempts = strategy.getMaxAttempts();
        return maxAttempts < 0 || maxAttempts > 0 && attempt <= maxAttempts;
    }

    public void attemptFailed() {
        attempt++;

        long waitTimeMs = getNextWaitTimeMs();
        nextTryNanoTime = currentTime.getAsLong() + MILLISECONDS.toNanos(waitTimeMs);
    }

    public long getNextWaitTimeMs() {
        return strategy.getIntervalFunction().waitAfterAttempt(attempt);
    }
}
