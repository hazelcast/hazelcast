/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis.impl;

import com.hazelcast.jet.retry.RetryStrategy;

public class RetryTracker {

    private final RetryStrategy strategy;

    private int attempt;

    public RetryTracker(RetryStrategy strategy) {
        this.strategy = strategy;
    }

    public void reset() {
        this.attempt = 0;
    }

    public void attemptFailed() {
        attempt++;
    }

    public boolean shouldTryAgain() {
        int maxAttempts = strategy.getMaxAttempts();
        return maxAttempts < 0 || maxAttempts > 0 && attempt <= maxAttempts;
    }

    public long getNextWaitTimeMillis() {
        return strategy.getIntervalFunction().waitAfterAttempt(attempt);
    }
}
