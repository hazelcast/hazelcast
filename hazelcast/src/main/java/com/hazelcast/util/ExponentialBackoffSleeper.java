/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.concurrent.TimeUnit;

/**
 * Utility class that allows for polling of state, with an exponentially increasing delay. The
 * {@link Builder#factor exponential factor} can be specified to control how fast the wait time increases between
 * subsequent invocations of {@link #sleep}.
 * <p>
 * Optionally, a {@link Builder#timeoutMs timeout} can be specified to limit the sleep time to a global timeout.
 */
public final class ExponentialBackoffSleeper {
    /**
     * Default exponential factor
     */
    public static final double DEFAULT_FACTOR = 1.3d;
    /**
     * Default initial sleep period in milliseconds
     */
    public static final long DEFAULT_INITIAL_MS = 50;
    /**
     * Default maximum sleep period in milliseconds
     */
    public static final long DEFAULT_MAX_MS = TimeUnit.SECONDS.toMillis(1);

    private final double factor;
    private final long max;
    private final long timeoutTimestamp;
    private long next;

    private ExponentialBackoffSleeper(Builder builder) {
        factor = builder.factor;
        max = builder.maxMs;
        next = builder.initialMs;
        timeoutTimestamp = builder.timeoutMs > 0 ? Clock.currentTimeMillis() + builder.timeoutMs : Long.MAX_VALUE;
    }

    /**
     * @return a new {@link Builder} instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return {@code true} if a {@link Builder#timeoutMs timeout} was specified and more time has elapsed since the
     *         construction of the sleeper instance than the specified timeout.
     */
    public boolean isTimedOut() {
        return Clock.currentTimeMillis() >  timeoutTimestamp;
    }

    /**
     * Sleeps for max(maxMs, initialMs * factor^iteration) milliseconds.
     *
     * @return time in ms actually spent sleeping
     * @throws InterruptedException
     */
    public long sleep() throws InterruptedException {
        long timeToTimeout = timeoutTimestamp - Clock.currentTimeMillis();
        long sleepMs = Math.min(next, timeToTimeout);
        Thread.sleep(sleepMs);

        next = Math.min(max, Math.round(factor * next));
        return sleepMs;
    }

    /**
     * Same as {@link #sleep} but swallows the {@code InterruptedException} if thrown.
     * @return time in ms actually spent sleeping
     */
    public long sleepQuietly() {
        try {
            return sleep();
        } catch (InterruptedException ie) {
            return 0L;
        }
    }

    /**
     * Builder for {@link ExponentialBackoffSleeper}
     */
    public static class Builder {
        private double factor = DEFAULT_FACTOR;
        private long initialMs = DEFAULT_INITIAL_MS;
        private long maxMs = DEFAULT_MAX_MS;
        private long timeoutMs;

        public Builder() {
        }

        public ExponentialBackoffSleeper build() {
            return new ExponentialBackoffSleeper(this);
        }

        /**
         * Sets the exponential factor that controls the increase in sleep time for subsequent calls to {@link #sleep}
         *
         * @param value the factor, must be greater than {@code 1.0}. A value of {@code 1.0} implies no change in sleep.
         * @return the builder
         */
        public Builder factor(double value) {
            if (value < 1) {
                throw new IllegalArgumentException("factor must be greater than or equal to 1, was " + value);
            }
            factor = value;
            return this;
        }

        /**
         * @param value the initial sleep time in milliseconds
         * @return the builder
         */
        public Builder initialMs(long value) {
            if (value <= 0) {
                throw new IllegalArgumentException("initialMs must be greater than 0, was " + value);
            }
            initialMs = value;
            return this;
        }

        /**
         * @param value the maximum sleep time (per call to {@link #sleep} in milliseconds
         * @return the builder
         */
        public Builder maxMs(long value) {
            if (value <= 0) {
                throw new IllegalArgumentException("maxMs must be greater than 0, was " + value);
            }
            maxMs = value;
            return this;
        }

        /**
         * @param value the global timeout in milliseconds, measured from the time of construction of the
         * {@link ExponentialBackoffSleeper}
         *
         * @return the builder
         */
        public Builder timeoutMs(long value) {
            if (value <= 0) {
                throw new IllegalArgumentException("timeoutMs must be greater than 0, was " + value);
            }
            timeoutMs = value;
            return this;
        }
    }
}
