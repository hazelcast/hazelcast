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

import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;

/**
 * Management Center related metrics configuration.
 *
 * @since 4.0
 */
public class MetricsManagementCenterConfig {

    /**
     * Default retention period for metrics.
     */
    public static final int DEFAULT_METRICS_RETENTION_SECONDS = 5;

    private boolean enabled = true;
    private int retentionSeconds = DEFAULT_METRICS_RETENTION_SECONDS;

    public MetricsManagementCenterConfig() {
    }

    public MetricsManagementCenterConfig(MetricsManagementCenterConfig managementCenterConfig) {
        this.enabled = managementCenterConfig.enabled;
        this.retentionSeconds = managementCenterConfig.retentionSeconds;
    }

    /**
     * Returns whether metrics will be exposed to Hazelcast Management
     * Center. If enabled, Hazelcast Management Center will be able to read
     * out the collected metrics from this member. It's enabled by default.
     * <p>
     * This configuration acts as a fine-tuning option beyond
     * enabling/disabling the Metrics collection entirely via the
     * {@link MetricsConfig#isEnabled()} master switch.
     *
     * @return {@code true} if exposing to Hazelcast Management Center is enabled.
     * @see MetricsConfig#isEnabled()
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables exposing metrics to Hazelcast Management Center. If enabled,
     * Hazelcast Management Center will be able to read out the recorded
     * metrics from this member. It's enabled by default.
     * <p>
     * This configuration acts as a fine-tuning option beyond
     * enabling/disabling the Metrics collection entirely via the {@link #enabled}
     * master switch.
     * <p>
     * May be overridden by {@link ClusterProperty#METRICS_MC_ENABLED}
     * system property.
     *
     * @see #setEnabled(boolean)
     */
    @Nonnull
    public MetricsManagementCenterConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
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
     * Sets the number of seconds the metrics will be retained on the
     * instance. By default, metrics are retained for 5 seconds (that is for
     * one collection of metrics values, if default
     * {@link MetricsConfig#setCollectionFrequencySeconds(int)} interval seconds
     * is used). More retention means more heap memory, but allows for longer
     * client hiccups without losing a value (for example to restart the
     * Management Center).
     * <p>
     * May be overridden by {@link ClusterProperty#METRICS_MC_RETENTION}
     * system property.
     */
    @Nonnull
    public MetricsManagementCenterConfig setRetentionSeconds(int retentionSeconds) {
        this.retentionSeconds = retentionSeconds;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricsManagementCenterConfig)) {
            return false;
        }

        MetricsManagementCenterConfig that = (MetricsManagementCenterConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        return retentionSeconds == that.retentionSeconds;
    }

    @Override
    public final int hashCode() {
        int result = Boolean.hashCode(enabled);
        result = 31 * result + retentionSeconds;
        return result;
    }

    @Override
    public String toString() {
        return "MetricsManagementCenterConfig{"
                + "enabled=" + enabled
                + ", retentionSeconds=" + retentionSeconds
                + '}';
    }
}
