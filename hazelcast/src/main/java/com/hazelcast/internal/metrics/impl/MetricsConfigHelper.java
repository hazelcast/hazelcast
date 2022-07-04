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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientMetricsConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.config.Config;
import com.hazelcast.config.MetricsConfig;
import com.hazelcast.config.MetricsJmxConfig;
import com.hazelcast.config.MetricsManagementCenterConfig;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;

public final class MetricsConfigHelper {
    private MetricsConfigHelper() {
    }

    /**
     * Overrides the {@link MetricsConfig} in the provided {@link Config}
     * with the metrics system properties if present.
     * See the {@link MetricsConfig} javadoc for the links between metrics
     * configuration fields and the system properties.
     *
     * @param config The configuration
     */
    public static void overrideMemberMetricsConfig(Config config, ILogger logger) {
        MetricsConfig metricsConfig = config.getMetricsConfig();
        MetricsManagementCenterConfig managementCenterConfig = metricsConfig.getManagementCenterConfig();
        MetricsJmxConfig jmxConfig = metricsConfig.getJmxConfig();

        // MetricsConfig.enabled
        tryOverride(ClusterProperty.METRICS_ENABLED, config::getProperty,
                prop -> metricsConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(metricsConfig.isEnabled()), "MetricsConfig.enabled", logger);

        // MetricsManagementCenterConfig.enabled
        tryOverride(ClusterProperty.METRICS_MC_ENABLED, config::getProperty,
                prop -> managementCenterConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(managementCenterConfig.isEnabled()), "MetricsManagementCenterConfig.enabled", logger);

        // MetricsManagementCenterConfig.retentionSeconds
        tryOverride(ClusterProperty.METRICS_MC_RETENTION, config::getProperty,
                prop -> managementCenterConfig.setRetentionSeconds(Integer.parseInt(prop)),
                () -> Integer.toString(managementCenterConfig.getRetentionSeconds()),
                "MetricsManagementCenterConfig.retentionSeconds", logger);

        // MetricsJmxConfig.enabled
        tryOverride(ClusterProperty.METRICS_JMX_ENABLED, config::getProperty,
                prop -> jmxConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(jmxConfig.isEnabled()), "MetricsJmxConfig.enabled", logger);

        // MetricsConfig.collectionFrequencySeconds
        tryOverride(ClusterProperty.METRICS_COLLECTION_FREQUENCY, config::getProperty,
                prop -> metricsConfig.setCollectionFrequencySeconds(Integer.parseInt(prop)),
                () -> Integer.toString(metricsConfig.getCollectionFrequencySeconds()),
                "MetricsConfig.collectionFrequencySeconds", logger);
    }

    /**
     * Overrides the {@link ClientMetricsConfig} in the provided {@link ClientConfig}
     * with the metrics system properties if present.
     * See the {@link ClientMetricsConfig} javadoc for the links between metrics
     * configuration fields and the system properties.
     *
     * @param config The configuration
     */
    public static void overrideClientMetricsConfig(ClientConfig config, ILogger logger) {
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        MetricsJmxConfig jmxConfig = metricsConfig.getJmxConfig();

        //Check old deprecated STATISTICS settings first.
        // MetricsConfig.collectionFrequencySeconds
        tryOverride(ClientProperty.STATISTICS_PERIOD_SECONDS, config::getProperty,
                prop -> metricsConfig.setCollectionFrequencySeconds(Integer.parseInt(prop)),
                () -> Integer.toString(metricsConfig.getCollectionFrequencySeconds()),
                "ClientMetricsConfig.collectionFrequencySeconds", logger);
        // MetricsConfig.enabled
        tryOverride(ClientProperty.STATISTICS_ENABLED, config::getProperty,
                prop -> metricsConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(metricsConfig.isEnabled()), "ClientMetricsConfig.enabled", logger);

        // MetricsConfig.enabled
        tryOverride(ClientProperty.METRICS_ENABLED, config::getProperty,
                prop -> metricsConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(metricsConfig.isEnabled()), "ClientMetricsConfig.enabled", logger);
        // MetricsJmxConfig.enabled
        tryOverride(ClientProperty.METRICS_JMX_ENABLED, config::getProperty,
                prop -> jmxConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(jmxConfig.isEnabled()), "MetricsJmxConfig.enabled", logger);
        // MetricsConfig.collectionFrequencySeconds
        tryOverride(ClientProperty.METRICS_COLLECTION_FREQUENCY, config::getProperty,
                prop -> metricsConfig.setCollectionFrequencySeconds(Integer.parseInt(prop)),
                () -> Integer.toString(metricsConfig.getCollectionFrequencySeconds()),
                "ClientMetricsConfig.collectionFrequencySeconds", logger);
    }

    private static void tryOverride(HazelcastProperty property, Function<String, String> getPropertyValueFn,
                                    Consumer<String> setterFn, Supplier<String> getterFn, String configOverridden,
                                    ILogger logger) {
        String propertyValue = getPropertyValueFn.apply(property.getName());
        try {
            if (propertyValue != null) {
                setterFn.accept(propertyValue);
                logger.info(String.format("Overridden metrics configuration with system property '%s'='%s' -> '%s'='%s'",
                        property, propertyValue, configOverridden, getterFn.get()));
            }
        } catch (Exception ex) {
            logger.warning(String.format("Failed to override metrics configuration with system property '%s'='%s'. Kept "
                    + "'%s'='%s'", property.getName(), propertyValue, configOverridden, getterFn.get()), ex);
        }
    }

    public static ProbeLevel memberMetricsLevel(HazelcastProperties properties, ILogger logger) {
        boolean debugMetrics = properties.getBoolean(ClusterProperty.METRICS_DEBUG);
        ProbeLevel probeLevel = debugMetrics ? DEBUG : INFO;

        if (probeLevel == INFO) {
            logger.fine("Collecting debug metrics and sending to diagnostics is disabled");
        } else {
            logger.info("Collecting debug metrics and sending to diagnostics is enabled");
        }

        return probeLevel;
    }

    public static ProbeLevel clientMetricsLevel(HazelcastProperties properties, ILogger logger) {
        boolean debugMetrics = properties.getBoolean(ClientProperty.METRICS_DEBUG);
        ProbeLevel probeLevel = debugMetrics ? DEBUG : INFO;

        if (probeLevel == INFO) {
            logger.fine("Collecting debug metrics and sending to diagnostics is disabled");
        } else {
            logger.info("Collecting debug metrics and sending to diagnostics is enabled");
        }

        return probeLevel;
    }
}
