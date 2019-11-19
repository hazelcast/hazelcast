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

package com.hazelcast.client.internal.metrics;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientMetricsConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.MetricsConfig;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMetricsPropertiesTest extends HazelcastTestSupport {
    private TestHazelcastFactory factory;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(smallInstanceConfig());
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testSystemPropertiesOverrideConfig() {
        // setting non-defaults
        System.setProperty(ClientProperty.METRICS_ENABLED.getName(), "false");
        System.setProperty(ClientProperty.METRICS_JMX_ENABLED.getName(), "false");
        System.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "24");

        HazelcastClientProxy client = createClient();
        ClientConfig clientConfig = client.getClientConfig();

        ClientMetricsConfig metricsConfig = clientConfig.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(24, metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        // TODO verify in a subsequent change that the override is actually effective; see MetricsPropertiesTest
    }

    @Test
    public void testInvalidSystemPropertiesIgnored() {
        // setting non-defaults
        System.setProperty(ClientProperty.METRICS_ENABLED.getName(), "invalid");
        System.setProperty(ClientProperty.METRICS_JMX_ENABLED.getName(), "invalid");
        System.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "invalid");

        HazelcastClientProxy client = createClient();
        ClientConfig config = client.getClientConfig();

        ClientMetricsConfig defaultConfig = new ClientMetricsConfig();

        // booleans result in false values even though they're "invalid"
        // therefore, all boolean config fields are set to false
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(defaultConfig.getCollectionFrequencySeconds(), metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        // TODO verify in a subsequent change that the override is actually effective; see MetricsPropertiesTest
    }

    @Test
    public void testConfigPropertiesOverrideConfig() {
        ClientConfig config = new ClientConfig();
        // setting non-defaults
        config.setProperty(ClientProperty.METRICS_ENABLED.getName(), "false");
        config.setProperty(ClientProperty.METRICS_JMX_ENABLED.getName(), "false");
        config.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "24");
        factory.newHazelcastClient(config);

        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(24, metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        // TODO verify in a subsequent change that the override is actually effective; see MetricsPropertiesTest
    }

    @Test
    public void testInvalidConfigPropertiesIgnored() {
        ClientConfig config = new ClientConfig();
        // setting non-defaults
        config.setProperty(ClientProperty.METRICS_ENABLED.getName(), "invalid");
        config.setProperty(ClientProperty.METRICS_JMX_ENABLED.getName(), "invalid");
        config.setProperty(ClientProperty.METRICS_COLLECTION_FREQUENCY.getName(), "invalid");

        factory.newHazelcastClient(config);

        MetricsConfig defaultConfig = new MetricsConfig();

        // booleans result in false values even though they're "invalid"
        // therefore, all boolean config fields are set to false
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(defaultConfig.getCollectionFrequencySeconds(), metricsConfig.getCollectionFrequencySeconds());

        // verify that the overridden config is used
        // TODO verify in a subsequent change that the override is actually effective; see MetricsPropertiesTest
    }

    @Test
    public void testDebugMetricsSysPropNotSet() {
        MetricsRegistry metricsRegistry = createClient().client.getMetricsRegistry();

        assertEquals(INFO, metricsRegistry.minimumLevel());
    }

    @Test
    public void testDebugMetricsSysPropDisabled() {
        System.setProperty(ClientProperty.METRICS_DEBUG.getName(), "false");
        MetricsRegistry metricsRegistry = createClient().client.getMetricsRegistry();

        assertEquals(INFO, metricsRegistry.minimumLevel());
    }

    @Test
    public void testDebugMetricsSysPropEnabled() {
        System.setProperty(ClientProperty.METRICS_DEBUG.getName(), "true");
        MetricsRegistry metricsRegistry = createClient().client.getMetricsRegistry();

        assertEquals(DEBUG, metricsRegistry.minimumLevel());
    }

    private HazelcastClientProxy createClient() {
        return (HazelcastClientProxy) factory.newHazelcastClient();
    }

}
