/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

/**
 * Connection Retry Config is controls the period among the retries and when should a client gave up
 * retrying. Exponential behaviour can be chosen or jitter can be added to wait periods.
 */
public class ConnectionRetryConfig {

    private static final int INITIAL_BACKOFF_MILLIS = 1000;
    private static final int MAX_BACKOFF_MILLIS = 30000;
    private static final double JITTER = 0.2;
    private int initialBackoffMillis = INITIAL_BACKOFF_MILLIS;
    private int maxBackoffMillis = MAX_BACKOFF_MILLIS;
    private double multiplier = 2;
    private boolean failOnMaxBackoff = true;
    private double jitter = JITTER;
    private boolean enabled;

    public ConnectionRetryConfig() {
    }

    public ConnectionRetryConfig(ConnectionRetryConfig connectionRetryConfig) {
        initialBackoffMillis = connectionRetryConfig.initialBackoffMillis;
        maxBackoffMillis = connectionRetryConfig.maxBackoffMillis;
        multiplier = connectionRetryConfig.multiplier;
        failOnMaxBackoff = connectionRetryConfig.failOnMaxBackoff;
        jitter = connectionRetryConfig.jitter;
        enabled = connectionRetryConfig.enabled;
    }

    /**
     * how long to wait after the first failure before retrying
     *
     * @return initialBackoffMillis
     */
    public int getInitialBackoffMillis() {
        return initialBackoffMillis;
    }


    /**
     * @param initialBackoffMillis how long to wait after the first failure before retrying
     * @return updated ConnectionRetryConfig
     */
    public ConnectionRetryConfig setInitialBackoffMillis(int initialBackoffMillis) {
        this.initialBackoffMillis = initialBackoffMillis;
        return this;
    }

    /**
     * When backoff reaches this upper bound, it does not increase any more. Behaviour after that changes
     * depending on `failOnMaxBackoff` option
     *
     * @return maxBackoffMillis
     */
    public int getMaxBackoffMillis() {
        return maxBackoffMillis;
    }

    /**
     * When backoff reaches this upper bound, it does not increase any more. Behaviour after that changes
     * depending on `failOnMaxBackoff` option
     *
     * @param maxBackoffMillis upper bound on backoff
     * @return updated ConnectionRetryConfig
     */
    public ConnectionRetryConfig setMaxBackoffMillis(int maxBackoffMillis) {
        this.maxBackoffMillis = maxBackoffMillis;
        return this;
    }

    /**
     * factor with which to multiply backoff after a failed retry
     *
     * @return multiplier
     */
    public double getMultiplier() {
        return multiplier;
    }

    /**
     * @param multiplier factor with which to multiply backoff after a failed retry
     * @return updated ConnectionRetryConfig
     */
    public ConnectionRetryConfig setMultiplier(double multiplier) {
        this.multiplier = multiplier;
        return this;
    }

    /**
     * whether to fail when the max-backoff has reached or continue waiting max-backoff-millis at each iteration
     * When on fail, client shuts down.
     *
     * @return failOnMaxBackoff
     */
    public boolean isFailOnMaxBackoff() {
        return failOnMaxBackoff;
    }

    /**
     * @param failOnMaxBackoff whether to fail when the max-backoff has reached or
     *                         continue waiting max-backoff-millis at each iteration
     * @return updated ConnectionRetryConfig
     */
    public ConnectionRetryConfig setFailOnMaxBackoff(boolean failOnMaxBackoff) {
        this.failOnMaxBackoff = failOnMaxBackoff;
        return this;
    }

    /**
     * by how much to randomize backoffs.
     * At each iteration calculated back-off is randomized via following method
     * Random(-jitter * current_backoff, jitter * current_backoff)
     *
     * @return jitter
     */
    public double getJitter() {
        return jitter;
    }

    /**
     * At each iteration calculated back-off is randomized via following method
     * Random(-jitter * current_backoff, jitter * current_backoff)
     *
     * @param jitter by how much to randomize backoffs
     * @return updated ConnectionRetryConfig
     */
    public ConnectionRetryConfig setJitter(double jitter) {
        this.jitter = jitter;
        return this;
    }


    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionRetryConfig that = (ConnectionRetryConfig) o;

        if (initialBackoffMillis != that.initialBackoffMillis) {
            return false;
        }
        if (maxBackoffMillis != that.maxBackoffMillis) {
            return false;
        }
        if (Double.compare(that.multiplier, multiplier) != 0) {
            return false;
        }
        if (failOnMaxBackoff != that.failOnMaxBackoff) {
            return false;
        }
        if (Double.compare(that.jitter, jitter) != 0) {
            return false;
        }
        return enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = initialBackoffMillis;
        result = 31 * result + maxBackoffMillis;
        temp = Double.doubleToLongBits(multiplier);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (failOnMaxBackoff ? 1 : 0);
        temp = Double.doubleToLongBits(jitter);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (enabled ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConnectionRetryConfig{"
                + "enabled=" + enabled
                + ", initialBackoffMillis=" + initialBackoffMillis
                + ", maxBackoffMillis=" + maxBackoffMillis
                + ", multiplier=" + multiplier
                + ", failOnMaxBackoff=" + failOnMaxBackoff
                + ", jitter=" + jitter
                + '}';
    }
}
