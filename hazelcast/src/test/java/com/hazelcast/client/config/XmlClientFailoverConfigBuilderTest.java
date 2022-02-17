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

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientFailoverConfigBuilderTest extends AbstractClientFailoverConfigBuilderTest {
    public static final String HAZELCAST_CLIENT_FAILOVER_START_TAG =
            "<hazelcast-client-failover xmlns=\"http://www.hazelcast.com/schema/client-config\">\n";
    public static final String HAZELCAST_CLIENT_FAILOVER_END_TAG = "</hazelcast-client-failover>";

    @Before
    public void init() throws Exception {
        URL schemaResource = XmlClientFailoverConfigBuilderTest.class.
                getClassLoader().getResource("hazelcast-client-failover-sample.xml");
        fullClientConfig = new XmlClientFailoverConfigBuilder(schemaResource).build();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String xml = "<hazelcast>"
                + "<cluster-name>dev</cluster-name>"
                + "</hazelcast>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testExpectsAtLeastOneConfig() {
        String xml = "<hazelcast-client-failover>"
                + "    <clients>"
                + "    </clients>"
                + "</hazelcast-client-failover>";
        Properties properties = new Properties();
        properties.setProperty("try-count", "11");
        ClientFailoverConfig config = buildConfig(xml, properties);
        assertEquals(11, config.getTryCount());
    }

    @Override
    @Test
    public void testVariableReplacementFromProperties() {
        String xml = ""
                + HAZELCAST_CLIENT_FAILOVER_START_TAG
                + "  <clients>"
                + "    <client>hazelcast-client-c1.xml</client>\n"
                + "    <client>hazelcast-client-c2.xml</client>\n"
                + "  </clients>"
                + "  <try-count>${try-count}</try-count>"
                + HAZELCAST_CLIENT_FAILOVER_END_TAG;

        Properties properties = new Properties();
        properties.setProperty("try-count", "11");
        ClientFailoverConfig config = buildConfig(xml, properties);
        assertEquals(11, config.getTryCount());
    }

    @Override
    @Test
    public void testVariableReplacementFromSystemProperties() {
        String xml = ""
                + HAZELCAST_CLIENT_FAILOVER_START_TAG
                + "  <clients>"
                + "    <client>hazelcast-client-c1.xml</client>\n"
                + "    <client>hazelcast-client-c2.xml</client>\n"
                + "  </clients>"
                + "  <try-count>${try-count}</try-count>"
                + HAZELCAST_CLIENT_FAILOVER_END_TAG;

        System.setProperty("try-count", "11");
        ClientFailoverConfig config = buildConfig(xml);
        assertEquals(11, config.getTryCount());
    }

    @Override
    @Test
    public void testWithClasspathConfig() {
        ClientFailoverConfig config = new ClientFailoverClasspathXmlConfig("hazelcast-client-failover-sample.xml");
        assertSampleFailoverConfig(config);
    }

    @Override
    @Test
    public void testVariableReplacementFromSystemPropertiesWithClasspathConfig() {
        System.setProperty("try-count", "13");
        ClientFailoverConfig config = new ClientFailoverClasspathXmlConfig("hazelcast-client-failover-sample-with-variable.xml");
        assertEquals(13, config.getTryCount());
    }

    @Override
    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() {
        System.setProperty("hazelcast.client.failover.config", "classpath:hazelcast-client-failover-sample.xml");

        ClientFailoverConfig config = buildConfig();
        assertSampleFailoverConfig(config);
    }

    @Override
    ClientFailoverConfig buildConfig() {
        return new XmlClientFailoverConfigBuilder().build();
    }

    public static ClientFailoverConfig buildConfig(String yaml) {
        return buildConfig(yaml, null);
    }

    private static ClientFailoverConfig buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientFailoverConfigBuilder configBuilder = new XmlClientFailoverConfigBuilder(bis);
        if (properties != null) {
            configBuilder.setProperties(properties);
        }
        return configBuilder.build();
    }

}
