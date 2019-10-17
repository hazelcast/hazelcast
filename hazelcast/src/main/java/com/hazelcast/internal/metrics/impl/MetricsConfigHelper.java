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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MetricsConfig;
import com.hazelcast.config.MetricsJmxConfig;
import com.hazelcast.config.MetricsManagementCenterConfig;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.function.Consumer;
import java.util.function.Supplier;

public final class MetricsConfigHelper {
    private static final ILogger LOGGER = Logger.getLogger(MetricsConfigHelper.class);

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
    public static void overrideMetricsConfig(Config config) {
        MetricsConfig metricsConfig = config.getMetricsConfig();
        MetricsManagementCenterConfig managementCenterConfig = metricsConfig.getManagementCenterConfig();
        MetricsJmxConfig jmxConfig = metricsConfig.getJmxConfig();

        // MetricsConfig.enabled
        tryOverride(config, GroupProperty.METRICS_ENABLED, prop -> metricsConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(metricsConfig.isEnabled()), "MetricsConfig.enabled");

        // MetricsManagementCenterConfig.enabled
        tryOverride(config, GroupProperty.METRICS_MC_ENABLED,
                prop -> managementCenterConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(managementCenterConfig.isEnabled()), "MetricsManagementCenterConfig.enabled");

        // MetricsManagementCenterConfig.retentionSeconds
        tryOverride(config, GroupProperty.METRICS_MC_RETENTION,
                prop -> managementCenterConfig.setRetentionSeconds(Integer.parseInt(prop)),
                () -> Integer.toString(managementCenterConfig.getRetentionSeconds()),
                "MetricsManagementCenterConfig.retentionSeconds");

        // MetricsJmxConfig.enabled
        tryOverride(config, GroupProperty.METRICS_JMX_ENABLED, prop -> jmxConfig.setEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(jmxConfig.isEnabled()), "MetricsJmxConfig.enabled");

        // MetricsConfig.collectionFrequencySeconds
        tryOverride(config, GroupProperty.METRICS_COLLECTION_FREQUENCY,
                prop -> metricsConfig.setCollectionFrequencySeconds(Integer.parseInt(prop)),
                () -> Integer.toString(metricsConfig.getCollectionFrequencySeconds()),
                "MetricsConfig.collectionFrequencySeconds");

        // MetricsConfig.dataStructuresEnabled
        tryOverride(config, GroupProperty.METRICS_DATASTRUCTURES,
                prop -> metricsConfig.setDataStructureMetricsEnabled(Boolean.parseBoolean(prop)),
                () -> Boolean.toString(metricsConfig.isDataStructureMetricsEnabled()), "MetricsConfig.dataStructuresEnabled");

        // MetricsConfig.level
        tryOverride(config, GroupProperty.METRICS_LEVEL,
                prop -> metricsConfig.setLevel(ProbeLevel.valueOf(prop)),
                () -> metricsConfig.getLevel().name(), "MetricsConfig.level");
    }

    private static void tryOverride(Config hzConfig, HazelcastProperty property, Consumer<String> setterFn,
                                    Supplier<String> getterFn, String configOverridden) {
        String propertyValue = hzConfig.getProperty(property.getName());
        try {
            if (propertyValue != null) {
                setterFn.accept(propertyValue);
                LOGGER.warning(String.format("Overridden metrics configuration with system property '%s'='%s' -> '%s'='%s'",
                        property, propertyValue, configOverridden, getterFn.get()));
            }
        } catch (Exception ex) {
            LOGGER.warning(String.format("Failed to override metrics configuration with system property '%s'='%s'. Kept "
                    + "'%s'='%s'", property.getName(), propertyValue, configOverridden, getterFn.get()), ex);
        }
    }
}
