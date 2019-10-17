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
import com.hazelcast.spi.properties.GroupProperty;

import javax.annotation.Nonnull;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

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

    private boolean enabled = true;
    private MetricsManagementCenterConfig managementCenterConfig = new MetricsManagementCenterConfig();
    private MetricsJmxConfig jmxConfig = new MetricsJmxConfig();
    private boolean dataStructureMetricsEnabled;
    private int collectionFrequencySeconds = DEFAULT_METRICS_COLLECTION_SECONDS;
    private ProbeLevel level = ProbeLevel.INFO;

    public MetricsConfig() {
    }

    public MetricsConfig(MetricsConfig metricsConfig) {
        this.enabled = metricsConfig.enabled;
        this.managementCenterConfig = new MetricsManagementCenterConfig(metricsConfig.managementCenterConfig);
        this.jmxConfig = new MetricsJmxConfig(metricsConfig.jmxConfig);
        this.dataStructureMetricsEnabled = metricsConfig.dataStructureMetricsEnabled;
        this.collectionFrequencySeconds = metricsConfig.collectionFrequencySeconds;
        this.level = metricsConfig.level;
    }

    /**
     * Sets whether metrics collection should be enabled for the node. If
     * enabled, Hazelcast Management Center will be able to connect to this
     * member. It's enabled by default.
     * <p/>
     * May be overridden by {@link GroupProperty#METRICS_ENABLED}
     * system property.
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

    @Nonnull
    public MetricsConfig setManagementCenterConfig(MetricsManagementCenterConfig managementCenterConfig) {
        this.managementCenterConfig = requireNonNull(managementCenterConfig, "Management Center config must not be null");
        return this;
    }

    @Nonnull
    public MetricsManagementCenterConfig getManagementCenterConfig() {
        return managementCenterConfig;
    }

    @Nonnull
    public MetricsConfig setJmxConfig(MetricsJmxConfig jmxConfig) {
        this.jmxConfig = requireNonNull(jmxConfig, "JMX config must not be null");
        return this;
    }

    @Nonnull
    public MetricsJmxConfig getJmxConfig() {
        return jmxConfig;
    }

    /**
     * Sets the metrics collection frequency in seconds. The same interval is
     * used for collection for Management Center and for JMX publisher. By default,
     * metrics are collected every 5 seconds.
     * <p/>
     * May be overridden by {@link GroupProperty#METRICS_COLLECTION_FREQUENCY}
     * system property.
     */
    @Nonnull
    public MetricsConfig setCollectionFrequencySeconds(int intervalSeconds) {
        Preconditions.checkPositive(intervalSeconds, "collectionFrequencySeconds must be positive");
        this.collectionFrequencySeconds = intervalSeconds;
        return this;
    }

    /**
     * Returns the metrics collection frequency in seconds.
     */
    public int getCollectionFrequencySeconds() {
        return this.collectionFrequencySeconds;
    }

    /**
     * Sets whether statistics for data structures are added to metrics.
     * It's disabled by default.
     * <p/>
     * Note that enabling the data structures metrics also sets {@link #level}
     * to {@link ProbeLevel#INFO}.
     * <p/>
     * May be overridden by {@link GroupProperty#METRICS_DATASTRUCTURES}
     * system property.
     *
     * @see #setLevel(ProbeLevel)
     */
    @Nonnull
    public MetricsConfig setDataStructureMetricsEnabled(boolean dataStructureMetricsEnabled) {
        this.dataStructureMetricsEnabled = dataStructureMetricsEnabled;
        return setLevel(ProbeLevel.INFO);
    }

    /**
     * Returns if statistics for data structures are added to metrics.
     */
    public boolean isDataStructureMetricsEnabled() {
        return dataStructureMetricsEnabled;
    }

    /**
     * Sets the minimum probe level to be collected.
     * <p/>
     * May be overridden by {@link GroupProperty#METRICS_LEVEL}
     *
     * @param level The minimum level to be collected
     */
    @Nonnull
    public MetricsConfig setLevel(ProbeLevel level) {
        this.level = level;
        return this;
    }

    /**
     * Returns the minimum probe level to be collected.
     *
     * @return the minimum probe level
     */
    @Nonnull
    public ProbeLevel getLevel() {
        return level;
    }

    @Override
    @SuppressWarnings("checkstyle:NPathComplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricsConfig)) {
            return false;
        }

        MetricsConfig that = (MetricsConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (dataStructureMetricsEnabled != that.dataStructureMetricsEnabled) {
            return false;
        }
        if (collectionFrequencySeconds != that.collectionFrequencySeconds) {
            return false;
        }
        if (!Objects.equals(managementCenterConfig, that.managementCenterConfig)) {
            return false;
        }
        if (!Objects.equals(jmxConfig, that.jmxConfig)) {
            return false;
        }
        return level == that.level;
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (managementCenterConfig != null ? managementCenterConfig.hashCode() : 0);
        result = 31 * result + (jmxConfig != null ? jmxConfig.hashCode() : 0);
        result = 31 * result + (dataStructureMetricsEnabled ? 1 : 0);
        result = 31 * result + collectionFrequencySeconds;
        result = 31 * result + (level != null ? level.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MetricsConfig{"
                + "enabled=" + enabled
                + ", managementCenterConfig=" + managementCenterConfig
                + ", jmxConfig=" + jmxConfig
                + ", dataStructureMetricsEnabled=" + dataStructureMetricsEnabled
                + ", collectionFrequencySeconds=" + collectionFrequencySeconds
                + ", level=" + level
                + '}';
    }
}
