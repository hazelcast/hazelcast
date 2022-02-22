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
 * JMX related metrics configuration.
 *
 * @since 4.0
 */
public class MetricsJmxConfig {
    private boolean enabled = true;

    public MetricsJmxConfig() {
    }

    public MetricsJmxConfig(MetricsJmxConfig jmxConfig) {
        this.enabled = jmxConfig.enabled;
    }

    /**
     * Returns whether metrics will be exposed through JMX MBeans. It's
     * enabled by default.
     * <p>
     * This configuration acts as a fine-tuning option beyond
     * enabling/disabling the Metrics collection entirely via the
     * {@link MetricsConfig#isEnabled()} master switch.
     *
     * @return {@code true} if exposing the metrics on JMX is enabled.
     * @see MetricsConfig#isEnabled()
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables metrics exposure through JMX. It's enabled by default. Metric
     * values are collected in the {@link MetricsConfig#getCollectionFrequencySeconds()}
     * metric collection interval} and written to a set of MBeans.
     * <p>
     * This configuration acts as a fine-tuning option beyond
     * enabling/disabling the Metrics collection entirely via the {@link #enabled}
     * master switch.
     * <p>
     * May be overridden by {@link ClusterProperty#METRICS_JMX_ENABLED}
     * system property.
     */
    @Nonnull
    public MetricsJmxConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricsJmxConfig)) {
            return false;
        }

        MetricsJmxConfig that = (MetricsJmxConfig) o;

        return enabled == that.enabled;
    }

    @Override
    public final int hashCode() {
        return Boolean.hashCode(enabled);
    }

    @Override
    public String toString() {
        return "MetricsJmxConfig{"
                + "enabled=" + enabled
                + '}';
    }
}
