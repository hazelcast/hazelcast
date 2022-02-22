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

import com.hazelcast.config.BaseMetricsConfig;

import java.util.Objects;

/**
 * Client-side metrics collection configuration.
 *
 * @since 4.0
 */
public class ClientMetricsConfig extends BaseMetricsConfig<ClientMetricsConfig> {
    public ClientMetricsConfig() {
        super();
    }

    public ClientMetricsConfig(ClientMetricsConfig metricsConfig) {
        super(metricsConfig);
    }

    @Override
    @SuppressWarnings("checkstyle:NPathComplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientMetricsConfig)) {
            return false;
        }

        ClientMetricsConfig that = (ClientMetricsConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (collectionFrequencySeconds != that.collectionFrequencySeconds) {
            return false;
        }
        return Objects.equals(jmxConfig, that.jmxConfig);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (jmxConfig != null ? jmxConfig.hashCode() : 0);
        result = 31 * result + collectionFrequencySeconds;
        return result;
    }

    @Override
    public String toString() {
        return "ClientMetricsConfig{"
                + "enabled=" + enabled
                + ", jmxConfig=" + jmxConfig
                + ", collectionFrequencySeconds=" + collectionFrequencySeconds
                + '}';
    }

}
