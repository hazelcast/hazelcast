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

import com.hazelcast.config.Config;
import com.hazelcast.config.MetricsConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_DISCRIMINATOR_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MetricsPropertiesTest extends HazelcastTestSupport {

    @Rule
    public OverridePropertyRule overrideFrequency =
            OverridePropertyRule.clear(ClusterProperty.METRICS_COLLECTION_FREQUENCY.getName());

    @Rule
    public OverridePropertyRule overrideDebug =
            OverridePropertyRule.clear(ClusterProperty.METRICS_DEBUG.getName());

    @Test
    public void testSystemPropertiesOverrideConfig() {
        // setting non-defaults
        System.setProperty(ClusterProperty.METRICS_ENABLED.getName(), "false");
        System.setProperty(ClusterProperty.METRICS_MC_ENABLED.getName(), "false");
        System.setProperty(ClusterProperty.METRICS_MC_RETENTION.getName(), "42");
        System.setProperty(ClusterProperty.METRICS_JMX_ENABLED.getName(), "false");
        System.setProperty(ClusterProperty.METRICS_COLLECTION_FREQUENCY.getName(), "24");

        HazelcastInstance instance = createHazelcastInstance();
        Config instanceConfig = instance.getConfig();

        MetricsConfig metricsConfig = instanceConfig.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getManagementCenterConfig().isEnabled());
        assertEquals(42, metricsConfig.getManagementCenterConfig().getRetentionSeconds());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(24, metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        MetricsService metricsService = getNodeEngineImpl(instance).getService(MetricsService.SERVICE_NAME);
        assertSame(metricsConfig, metricsService.getConfig());
    }

    @Test
    public void testInvalidSystemPropertiesIgnored() {
        // setting non-defaults
        System.setProperty(ClusterProperty.METRICS_ENABLED.getName(), "invalid");
        System.setProperty(ClusterProperty.METRICS_MC_ENABLED.getName(), "invalid");
        System.setProperty(ClusterProperty.METRICS_MC_RETENTION.getName(), "invalid");
        System.setProperty(ClusterProperty.METRICS_JMX_ENABLED.getName(), "invalid");
        System.setProperty(ClusterProperty.METRICS_COLLECTION_FREQUENCY.getName(), "invalid");

        HazelcastInstance instance = createHazelcastInstance();
        Config instanceConfig = instance.getConfig();

        MetricsConfig defaultConfig = new MetricsConfig();

        // booleans result in false values even though they're "invalid"
        // therefore, all boolean config fields are set to false
        MetricsConfig metricsConfig = instanceConfig.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getManagementCenterConfig().isEnabled());
        assertEquals(defaultConfig.getManagementCenterConfig().getRetentionSeconds(),
                metricsConfig.getManagementCenterConfig().getRetentionSeconds());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(defaultConfig.getCollectionFrequencySeconds(), metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        MetricsService metricsService = getNodeEngineImpl(instance).getService(MetricsService.SERVICE_NAME);
        assertSame(metricsConfig, metricsService.getConfig());
    }

    @Test
    public void testConfigPropertiesOverrideConfig() {
        Config originalConfig = smallInstanceConfig();
        // setting non-defaults
        originalConfig.setProperty(ClusterProperty.METRICS_ENABLED.getName(), "false");
        originalConfig.setProperty(ClusterProperty.METRICS_MC_ENABLED.getName(), "false");
        originalConfig.setProperty(ClusterProperty.METRICS_MC_RETENTION.getName(), "42");
        originalConfig.setProperty(ClusterProperty.METRICS_JMX_ENABLED.getName(), "false");
        originalConfig.setProperty(ClusterProperty.METRICS_COLLECTION_FREQUENCY.getName(), "24");

        HazelcastInstance instance = createHazelcastInstance(originalConfig);
        Config instanceConfig = instance.getConfig();

        MetricsConfig metricsConfig = instanceConfig.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getManagementCenterConfig().isEnabled());
        assertEquals(42, metricsConfig.getManagementCenterConfig().getRetentionSeconds());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(24, metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        MetricsService metricsService = getNodeEngineImpl(instance).getService(MetricsService.SERVICE_NAME);
        assertSame(metricsConfig, metricsService.getConfig());
    }

    @Test
    public void testInvalidConfigPropertiesIgnored() {
        Config originalConfig = smallInstanceConfig();
        // setting non-defaults
        originalConfig.setProperty(ClusterProperty.METRICS_ENABLED.getName(), "invalid");
        originalConfig.setProperty(ClusterProperty.METRICS_MC_ENABLED.getName(), "invalid");
        originalConfig.setProperty(ClusterProperty.METRICS_MC_RETENTION.getName(), "invalid");
        originalConfig.setProperty(ClusterProperty.METRICS_JMX_ENABLED.getName(), "invalid");
        originalConfig.setProperty(ClusterProperty.METRICS_COLLECTION_FREQUENCY.getName(), "invalid");

        HazelcastInstance instance = createHazelcastInstance(originalConfig);
        Config instanceConfig = instance.getConfig();

        MetricsConfig defaultConfig = new MetricsConfig();

        // booleans result in false values even though they're "invalid"
        // therefore, all boolean config fields are set to false
        MetricsConfig metricsConfig = instanceConfig.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getManagementCenterConfig().isEnabled());
        assertEquals(defaultConfig.getManagementCenterConfig().getRetentionSeconds(),
                metricsConfig.getManagementCenterConfig().getRetentionSeconds());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(defaultConfig.getCollectionFrequencySeconds(), metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        MetricsService metricsService = getNodeEngineImpl(instance).getService(MetricsService.SERVICE_NAME);
        assertSame(metricsConfig, metricsService.getConfig());
    }

    @Test
    public void testDebugMetricsSysPropNotSet() {
        HazelcastInstance instance = createHazelcastInstance();
        MetricsRegistry metricsRegistry = getNodeEngineImpl(instance).getMetricsRegistry();

        assertEquals(INFO, metricsRegistry.minimumLevel());
    }

    @Test
    public void testDebugMetricsSysPropDisabled() {
        System.setProperty(ClusterProperty.METRICS_DEBUG.getName(), "false");
        HazelcastInstance instance = createHazelcastInstance();
        MetricsRegistry metricsRegistry = getNodeEngineImpl(instance).getMetricsRegistry();

        assertEquals(INFO, metricsRegistry.minimumLevel());
    }

    @Test
    public void testDebugMetricsSysPropEnabled() {
        System.setProperty(ClusterProperty.METRICS_DEBUG.getName(), "true");
        HazelcastInstance instance = createHazelcastInstance();
        MetricsRegistry metricsRegistry = getNodeEngineImpl(instance).getMetricsRegistry();

        assertEquals(DEBUG, metricsRegistry.minimumLevel());
    }

    @Test
    public void testDataStructureMetricsSysPropNotSet() {
        HazelcastInstance instance = createHazelcastInstance();
        verifyDataStructureMetricPresent(instance, true);
    }

    @Test
    public void testDataStructureMetricsSysPropDisabled() {
        System.setProperty(ClusterProperty.METRICS_DATASTRUCTURES.getName(), "false");
        HazelcastInstance instance = createHazelcastInstance();
        verifyDataStructureMetricPresent(instance, false);
    }

    @Test
    public void testDataStructureMetricsSysPropEnabled() {
        System.setProperty(ClusterProperty.METRICS_DATASTRUCTURES.getName(), "true");
        HazelcastInstance instance = createHazelcastInstance();
        verifyDataStructureMetricPresent(instance, true);
    }

    private void verifyDataStructureMetricPresent(HazelcastInstance instance, boolean shouldContain) {
        CapturingCollector collector = new CapturingCollector();
        instance.getMap("testMap").put("42", "42");

        MetricsRegistry metricsRegistry = getNodeEngineImpl(instance).getMetricsRegistry();
        metricsRegistry.collect(collector);

        MetricDescriptor descriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix(MAP_PREFIX)
                .withDiscriminator(MAP_DISCRIMINATOR_NAME, "testMap")
                .withMetric("putCount")
                .withUnit(COUNT);

        if (shouldContain) {
            assertContains(collector.captures().keySet(), descriptor);
        } else {
            assertNotContains(collector.captures().keySet(), descriptor);
        }
    }
}
