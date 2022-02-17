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

import javax.annotation.Nonnull;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Member-side metrics collection configuration.
 *
 * @since 4.0
 */
public class MetricsConfig extends BaseMetricsConfig<MetricsConfig> {

    private MetricsManagementCenterConfig managementCenterConfig = new MetricsManagementCenterConfig();

    public MetricsConfig() {
        super();
    }

    public MetricsConfig(MetricsConfig metricsConfig) {
        super(metricsConfig);
        this.managementCenterConfig = new MetricsManagementCenterConfig(metricsConfig.managementCenterConfig);
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
        if (collectionFrequencySeconds != that.collectionFrequencySeconds) {
            return false;
        }
        if (!Objects.equals(managementCenterConfig, that.managementCenterConfig)) {
            return false;
        }
        return Objects.equals(jmxConfig, that.jmxConfig);
    }

    @Override
    public final int hashCode() {
        int result = Boolean.hashCode(enabled);
        result = 31 * result + (managementCenterConfig != null ? managementCenterConfig.hashCode() : 0);
        result = 31 * result + (jmxConfig != null ? jmxConfig.hashCode() : 0);
        result = 31 * result + collectionFrequencySeconds;
        return result;
    }

    @Override
    public String toString() {
        return "MetricsConfig{"
                + "enabled=" + enabled
                + ", managementCenterConfig=" + managementCenterConfig
                + ", jmxConfig=" + jmxConfig
                + ", collectionFrequencySeconds=" + collectionFrequencySeconds
                + '}';
    }
}
