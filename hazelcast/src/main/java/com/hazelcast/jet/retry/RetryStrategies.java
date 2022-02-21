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

import com.hazelcast.jet.retry.impl.RetryStrategyImpl;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Collection of factory methods for creating the most frequently used
 * {@link RetryStrategy RetryStrategies}.
 *
 * @since Jet 4.3
 */
public final class RetryStrategies {

    private static final int DEFAULT_MAX_ATTEMPTS = -1;
    private static final long DEFAULT_WAIT_DURATION_MS = SECONDS.toMillis(5);
    private static final IntervalFunction DEFAULT_INTERVAL_FUNCTION = IntervalFunction.constant(DEFAULT_WAIT_DURATION_MS);

    private RetryStrategies() {
    }

    /**
     * Create a strategy which will not retry a failed action.
     */
    public static RetryStrategy never() {
        return new Builder().maxAttempts(0).build();
    }

    /**
     * Create a strategy which will retry failed actions indefinitely and will
     * wait for a fixed amount of time between any two subsequent attempts.
     */
    public static RetryStrategy indefinitely(long intervalMillis) {
        return new Builder().intervalFunction(IntervalFunction.constant(intervalMillis)).build();
    }

    /**
     * Create a builder which can be used for setting up an arbitrarily complex
     * strategy.
     */
    public static Builder custom() {
        return new Builder();
    }

    /**
     * Builder for custom retry strategies.
     */
    public static final class Builder {

        private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
        private IntervalFunction intervalFunction = DEFAULT_INTERVAL_FUNCTION;

        private Builder() {
        }

        /**
         * Sets the maximum number of retry attempts.
         */
        public Builder maxAttempts(int maxRetryAttempts) {
            this.maxAttempts = maxRetryAttempts;
            return this;
        }

        /**
         * Set a function to modify the waiting interval after a failure.
         *
         * @param f Function to modify the interval after a failure
         * @return the RetryConfig.Builder
         */
        public Builder intervalFunction(IntervalFunction f) {
            this.intervalFunction = f;
            return this;
        }

        /**
         * Constructs the actual strategy based on the properties set previously.
         */
        public RetryStrategy build() {
            return new RetryStrategyImpl(maxAttempts, intervalFunction);
        }
    }

}
