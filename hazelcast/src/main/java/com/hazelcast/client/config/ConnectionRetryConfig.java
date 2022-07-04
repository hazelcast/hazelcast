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

package com.hazelcast.client.config;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Connection Retry Config is controls the period among the retries and when should a client gave up
 * retrying. Exponential behaviour can be chosen or jitter can be added to wait periods.
 */
public class ConnectionRetryConfig {

    /**
     * Default value for the cluster connection timeout.
     */
    public static final int DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS = -1;

    /**
     * Timeout used by the failover client to start trying to
     * connect alternative clusters when the cluster connection
     * timeout is equal to {@link ConnectionRetryConfig#DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS}.
     */
    public static final int FAILOVER_CLIENT_DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS = 120000;

    private static final int INITIAL_BACKOFF_MILLIS = 1000;
    private static final int MAX_BACKOFF_MILLIS = 30000;
    private static final double JITTER = 0;
    private static final double MULTIPLIER = 1.05;
    private int initialBackoffMillis = INITIAL_BACKOFF_MILLIS;
    private int maxBackoffMillis = MAX_BACKOFF_MILLIS;
    private double multiplier = MULTIPLIER;
    private long connectTimeoutMillis = DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS;
    private double jitter = JITTER;

    public ConnectionRetryConfig() {
    }

    public ConnectionRetryConfig(ConnectionRetryConfig connectionRetryConfig) {
        initialBackoffMillis = connectionRetryConfig.initialBackoffMillis;
        maxBackoffMillis = connectionRetryConfig.maxBackoffMillis;
        multiplier = connectionRetryConfig.multiplier;
        connectTimeoutMillis = connectionRetryConfig.connectTimeoutMillis;
        jitter = connectionRetryConfig.jitter;
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
        checkNotNegative(initialBackoffMillis, "Initial backoff must be non-negative!");
        this.initialBackoffMillis = initialBackoffMillis;
        return this;
    }

    /**
     * When backoff reaches this upper bound, it does not increase any more.
     *
     * @return maxBackoffMillis
     */
    public int getMaxBackoffMillis() {
        return maxBackoffMillis;
    }

    /**
     * When backoff reaches this upper bound, it does not increase any more.
     *
     * @param maxBackoffMillis upper bound on backoff
     * @return updated ConnectionRetryConfig
     */
    public ConnectionRetryConfig setMaxBackoffMillis(int maxBackoffMillis) {
        checkNotNegative(maxBackoffMillis, "Max backoff must be non-negative!");
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
        checkTrue(multiplier >= 1.0, "Multiplier must be greater than or equal to 1.0!");
        this.multiplier = multiplier;
        return this;
    }

    /**
     * Timeout value in milliseconds for the client to give up to connect to the current cluster
     * Depending on FailoverConfig, a client can shutdown or start trying on alternative clusters
     * after reaching the timeout. If it is equal to {@code -1}, which is the default value,
     * the client will not stop trying to connect to the cluster. If the failover client is used,
     * for the default value, the client will start trying to connect alternative clusters after
     * {@link ConnectionRetryConfig#FAILOVER_CLIENT_DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS}.
     * For any other value, both the failover and non-failover client will use that as it is.
     *
     * @return clusterConnectTimeoutMillis
     */
    public long getClusterConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    /**
     * @param clusterConnectTimeoutMillis timeout in milliseconds for the client to give up to connect to the current cluster
     *                                    Depending on FailoverConfig, a client can shutdown or start trying on alternative
     *                                    clusters after reaching the timeout. If set to {@code -1}, which is the default
     *                                    value, the client will not stop trying to connect to the cluster. If the
     *                                    failover client is used, for the default value, the client will start trying
     *                                    to connect alternative clusters after
     *                                    {@link ConnectionRetryConfig#FAILOVER_CLIENT_DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS}.
     *                                    For any other value, both the failover and non-failover client will use
     *                                    that as it is.
     *
     * @return updated ConnectionRetryConfig
     */
    public ConnectionRetryConfig setClusterConnectTimeoutMillis(long clusterConnectTimeoutMillis) {
        if (clusterConnectTimeoutMillis < 0 && clusterConnectTimeoutMillis != DEFAULT_CLUSTER_CONNECT_TIMEOUT_MILLIS) {
            throw new IllegalArgumentException("Cluster connect timeout must be non-negative or equal to -1!");
        }
        this.connectTimeoutMillis = clusterConnectTimeoutMillis;
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
        checkTrue(jitter >= 0.0 && jitter <= 1.0, "Jitter must be in range [0.0, 1.0]");
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
        if (connectTimeoutMillis != that.connectTimeoutMillis) {
            return false;
        }
        return Double.compare(that.jitter, jitter) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = initialBackoffMillis;
        result = 31 * result + maxBackoffMillis;
        temp = Double.doubleToLongBits(multiplier);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (connectTimeoutMillis ^ (connectTimeoutMillis >>> 32));
        temp = Double.doubleToLongBits(jitter);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ConnectionRetryConfig{"
                + ", initialBackoffMillis=" + initialBackoffMillis
                + ", maxBackoffMillis=" + maxBackoffMillis
                + ", multiplier=" + multiplier
                + ", clusterConnectTimeoutMillis=" + connectTimeoutMillis
                + ", jitter=" + jitter
                + '}';
    }
}
