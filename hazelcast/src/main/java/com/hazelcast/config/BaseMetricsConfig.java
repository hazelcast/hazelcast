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

package com.hazelcast.config;

import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Base class of configuration options specific to metrics collection.
 *
 * @param <T> The type of the concrete configuration class extending this
 *            base class.
 * @since 4.0
 */
public abstract class BaseMetricsConfig<T extends BaseMetricsConfig> {
    /**
     * Default collection interval for metrics
     */
    private static final int DEFAULT_METRICS_COLLECTION_SECONDS = 5;
    protected boolean enabled = true;
    protected MetricsJmxConfig jmxConfig = new MetricsJmxConfig();
    protected int collectionFrequencySeconds = DEFAULT_METRICS_COLLECTION_SECONDS;

    protected BaseMetricsConfig() {
    }

    protected BaseMetricsConfig(BaseMetricsConfig metricsConfig) {
        this.enabled = metricsConfig.enabled;
        this.jmxConfig = new MetricsJmxConfig(metricsConfig.jmxConfig);
        this.collectionFrequencySeconds = metricsConfig.collectionFrequencySeconds;
    }

    /**
     * Sets whether metrics collection should be enabled for the node. If
     * enabled, Hazelcast Management Center will be able to connect to this
     * member. It's enabled by default.
     * <p>
     * May be overridden by {@link ClusterProperty#METRICS_ENABLED} system property for the member.
     * <p>
     * May be overridden by {@link ClientProperty#METRICS_ENABLED} and
     * {@link ClientProperty#STATISTICS_ENABLED} system properties for the client.
     * <p>
     * When both of them configured for the client {@link ClientProperty#STATISTICS_ENABLED} is ignored.
     */
    @Nonnull
    public T setEnabled(boolean enabled) {
        this.enabled = enabled;
        return (T) this;
    }

    /**
     * Returns if metrics collection is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    @Nonnull
    public T setJmxConfig(MetricsJmxConfig jmxConfig) {
        this.jmxConfig = requireNonNull(jmxConfig, "JMX config must not be null");
        return (T) this;
    }

    @Nonnull
    public MetricsJmxConfig getJmxConfig() {
        return jmxConfig;
    }

    /**
     * Sets the metrics collection frequency in seconds. The same interval is
     * used for collection for Management Center and for JMX publisher. By default,
     * metrics are collected every 5 seconds.
     * <p>
     * May be overridden by {@link ClusterProperty#METRICS_COLLECTION_FREQUENCY} system property for the member.
     * <p>
     * May be overridden by {@link ClientProperty#METRICS_COLLECTION_FREQUENCY} and
     * {@link ClientProperty#STATISTICS_PERIOD_SECONDS} system properties for the client
     * <p>
     * When both of them is configured for the client, {@link ClientProperty#STATISTICS_PERIOD_SECONDS} is ignored.
     */
    @Nonnull
    public T setCollectionFrequencySeconds(int intervalSeconds) {
        Preconditions.checkPositive(intervalSeconds, "collectionFrequencySeconds must be positive");
        this.collectionFrequencySeconds = intervalSeconds;
        return (T) this;
    }

    /**
     * Returns the metrics collection frequency in seconds.
     */
    public int getCollectionFrequencySeconds() {
        return this.collectionFrequencySeconds;
    }
}
