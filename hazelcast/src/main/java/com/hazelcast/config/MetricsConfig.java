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

package com.hazelcast.config;

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * Configuration options specific to metrics collection.
 *
 * @since 4.0
 */
public class MetricsConfig {

    /**
     * Default collection interval for metrics
     */
    public static final int DEFAULT_METRICS_COLLECTION_SECONDS = 5;

    /**
     * Default retention period for metrics.
     */
    public static final int DEFAULT_METRICS_RETENTION_SECONDS = 5;

    private boolean enabled = true;
    private boolean mcEnabled = true;
    private boolean jmxEnabled = true;
    private int retentionSeconds = DEFAULT_METRICS_RETENTION_SECONDS;
    private boolean metricsForDataStructuresEnabled;
    private int intervalSeconds = DEFAULT_METRICS_COLLECTION_SECONDS;
    private ProbeLevel minimumLevel = ProbeLevel.INFO;

    public MetricsConfig() {
    }

    public MetricsConfig(MetricsConfig metricsConfig) {
        this.enabled = metricsConfig.enabled;
        this.mcEnabled = metricsConfig.mcEnabled;
        this.jmxEnabled = metricsConfig.jmxEnabled;
        this.retentionSeconds = metricsConfig.retentionSeconds;
        this.metricsForDataStructuresEnabled = metricsConfig.metricsForDataStructuresEnabled;
        this.intervalSeconds = metricsConfig.intervalSeconds;
        this.minimumLevel = metricsConfig.minimumLevel;
    }

    /**
     * Sets whether metrics collection should be enabled for the node. If
     * enabled, Hazelcast Management Center will be able to connect to this
     * member. It's enabled by default.
     */
    @Nonnull
    public MetricsConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns if metrics collection is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns whether metrics will be exposed to Hazelcast Management
     * Center. If enabled, Hazelcast Management Center will be able to read
     * out the recorder metrics from this member. It's enabled by default.
     * <p/>
     * This configuration acts as a fine-tuning option beyond
     * enabling/disabling the Metrics collection entirely via the {@link #enabled}
     * master switch.
     *
     * @return true if exporting to Hazelcast Management Center is enabled.
     * @see #isEnabled()
     */
    public boolean isMcEnabled() {
        return mcEnabled;
    }

    /**
     * Enables exposing metrics to Hazelcast Management Center. If enabled,
     * Hazelcast Management Center will be able to read out the recorded
     * metrics from this member. It's enabled by default.
     * <p/>
     * This configuration acts as a fine-tuning option beyond
     * enabling/disabling the Metrics collection entirely via the {@link #enabled}
     * master switch.
     *
     * @see #setEnabled(boolean)
     */
    public MetricsConfig setMcEnabled(boolean mcEnabled) {
        this.mcEnabled = mcEnabled;
        return this;
    }

    /**
     * Returns whether metrics will be exposed through JMX MBeans.
     * <p/>
     * This configuration acts as a fine-tuning option beyond
     * enabling/disabling the Metrics collection entirely via the {@link #enabled}
     * master switch.
     */
    public boolean isJmxEnabled() {
        return jmxEnabled;
    }

    /**
     * Enables metrics exposure through JMX. It's enabled by default. Metric
     * values are collected in the {@linkplain #setCollectionIntervalSeconds
     * metric collection interval} and written to a set of MBeans.
     * <p/>
     * This configuration acts as a fine-tuning option beyond
     * enabling/disabling the Metrics collection entirely via the {@link #enabled}
     * master switch.
     */
    public MetricsConfig setJmxEnabled(boolean jmxEnabled) {
        this.jmxEnabled = jmxEnabled;
        return this;
    }

    /**
     * Sets the number of seconds the metrics will be retained on the
     * instance. By default, metrics are retained for 5 seconds (that is for
     * one collection of metrics values, if default {@linkplain
     * #setCollectionIntervalSeconds(int) collection interval} is used). More
     * retention means more heap memory, but allows for longer client hiccups
     * without losing a value (for example to restart the Management Center).
     * <p>
     * This setting applies only to Management Center metrics API. It
     * doesn't affect how metrics are exposed through JMX.
     */
    @Nonnull
    public MetricsConfig setRetentionSeconds(int retentionSeconds) {
        Preconditions.checkPositive(intervalSeconds, "retentionSeconds must be positive");
        this.retentionSeconds = retentionSeconds;
        return this;
    }

    /**
     * Returns the number of seconds the metrics will be retained on the
     * instance.
     */
    public int getRetentionSeconds() {
        return retentionSeconds;
    }

    /**
     * Sets the metrics collection interval in seconds. The same interval is
     * used for collection for Management Center and for JMX publisher. By default,
     * metrics are collected every 5 seconds.
     */
    @Nonnull
    public MetricsConfig setCollectionIntervalSeconds(int intervalSeconds) {
        Preconditions.checkPositive(intervalSeconds, "intervalSeconds must be positive");
        this.intervalSeconds = intervalSeconds;
        return this;
    }

    /**
     * Returns the metrics collection interval.
     */
    public int getCollectionIntervalSeconds() {
        return this.intervalSeconds;
    }

    /**
     * Sets whether statistics for data structures are added to metrics.
     * It's disabled by default.
     * <p/>
     * Note that enabling the data structures metrics also sets {@link #minimumLevel}
     * to {@link ProbeLevel#INFO}.
     *
     * @see #setMinimumLevel(ProbeLevel)
     */
    @Nonnull
    public MetricsConfig setMetricsForDataStructuresEnabled(boolean metricsForDataStructuresEnabled) {
        this.metricsForDataStructuresEnabled = metricsForDataStructuresEnabled;
        return setMinimumLevel(ProbeLevel.INFO);
    }

    /**
     * Returns if statistics for data structures are added to metrics.
     */
    public boolean isMetricsForDataStructuresEnabled() {
        return metricsForDataStructuresEnabled;
    }

    /**
     * Sets the minimum probe level to be collected.
     *
     * @param minimumLevel The minimum level to be collected
     */
    public MetricsConfig setMinimumLevel(ProbeLevel minimumLevel) {
        this.minimumLevel = minimumLevel;
        return this;
    }

    /**
     * Returns the minimum probe level to be collected.
     *
     * @return the minimum probe level
     */
    @Nonnull
    public ProbeLevel getMinimumLevel() {
        return minimumLevel;
    }

    @Override
    @SuppressWarnings("checkstyle:NPathComplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof MetricsConfig)) {
            return false;
        }

        MetricsConfig that = (MetricsConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (mcEnabled != that.mcEnabled) {
            return false;
        }
        if (jmxEnabled != that.jmxEnabled) {
            return false;
        }
        if (retentionSeconds != that.retentionSeconds) {
            return false;
        }
        if (metricsForDataStructuresEnabled != that.metricsForDataStructuresEnabled) {
            return false;
        }
        if (intervalSeconds != that.intervalSeconds) {
            return false;
        }
        return minimumLevel == that.minimumLevel;
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (mcEnabled ? 1 : 0);
        result = 31 * result + (jmxEnabled ? 1 : 0);
        result = 31 * result + retentionSeconds;
        result = 31 * result + (metricsForDataStructuresEnabled ? 1 : 0);
        result = 31 * result + intervalSeconds;
        result = 31 * result + (minimumLevel != null ? minimumLevel.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MetricsConfig{"
                + "enabled=" + enabled
                + ", mcEnabled=" + mcEnabled
                + ", jmxEnabled=" + jmxEnabled
                + ", retentionSeconds=" + retentionSeconds
                + ", metricsForDataStructuresEnabled=" + metricsForDataStructuresEnabled
                + ", intervalSeconds=" + intervalSeconds
                + ", minimumLevel=" + minimumLevel
                + '}';
    }
}
