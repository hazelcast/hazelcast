/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategiesConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveredNode;
import com.hazelcast.spi.discovery.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.spi.discovery.SimpleDiscoveredNode;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientDiscoverySpiTest extends HazelcastTestSupport {

    @Test
    public void testSchema() throws Exception {
        String xmlFileName = "hazelcast-client-discovery-spi-test.xml";

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = ClientDiscoverySpiTest.class.getClassLoader().getResource("hazelcast-client-config-3.6.xsd");
        Schema schema = factory.newSchema(schemaResource);

        InputStream xmlResource = ClientDiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Source source = new StreamSource(xmlResource);
        Validator validator = schema.newValidator();
        validator.validate(source);
    }

    @Test
    public void testParsing() throws Exception {
        String xmlFileName = "hazelcast-client-discovery-spi-test.xml";
        InputStream xmlResource = ClientDiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        ClientConfig clientConfig = new XmlClientConfigBuilder(xmlResource).build();

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        AwsConfig awsConfig = networkConfig.getAwsConfig();
        assertNull(awsConfig);

        DiscoveryStrategiesConfig discoveryStrategiesConfig = networkConfig.getDiscoveryStrategiesConfig();
        assertTrue(discoveryStrategiesConfig.isEnabled());

        assertEquals(1, discoveryStrategiesConfig.getDiscoveryStrategyConfigs().size());

        DiscoveryStrategyConfig providerConfig = discoveryStrategiesConfig.getDiscoveryStrategyConfigs().iterator().next();

        assertEquals(3, providerConfig.getProperties().size());
        assertEquals("foo", providerConfig.getProperties().get("key-string"));
        assertEquals("123", providerConfig.getProperties().get("key-int"));
        assertEquals("true", providerConfig.getProperties().get("key-boolean"));
    }

    @Test
    public void testNodeStartup() {
        TestHazelcastFactory factory = new TestHazelcastFactory();

        Config config = new Config();
        config.getNetworkConfig().setPort(50001);
        InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
        interfaces.clear();
        interfaces.setEnabled(true);
        interfaces.addInterface("127.0.0.1");

        final HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config);
        final HazelcastInstance hazelcastInstance3 = factory.newHazelcastInstance(config);

        try {
            String xmlFileName = "hazelcast-client-discovery-spi-test.xml";
            InputStream xmlResource = ClientDiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);

            ClientConfig clientConfig = new XmlClientConfigBuilder(xmlResource).build();
            final HazelcastInstance client = factory.newHazelcastClient(clientConfig);

            assertNotNull(hazelcastInstance1);
            assertNotNull(hazelcastInstance2);
            assertNotNull(hazelcastInstance3);
            assertNotNull(client);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    assertEquals(3, hazelcastInstance1.getCluster().getMembers().size());
                    assertEquals(3, hazelcastInstance2.getCluster().getMembers().size());
                    assertEquals(3, hazelcastInstance3.getCluster().getMembers().size());
                    assertEquals(3, client.getCluster().getMembers().size());
                }
            });
        } finally {
            factory.shutdownAll();
        }
    }

    @Test
    public void testNodeFilter_from_xml() throws Exception {
        String xmlFileName = "hazelcast-client-discovery-spi-test.xml";
        InputStream xmlResource = ClientDiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        ClientConfig clientConfig = new XmlClientConfigBuilder(xmlResource).build();

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        DiscoveryStrategiesConfig discoveryStrategiesConfig = networkConfig.getDiscoveryStrategiesConfig();
        assertNotNull(discoveryStrategiesConfig);
        assertNotNull(discoveryStrategiesConfig.getNodeFilterClass());

        DiscoveryServiceProvider provider = new DefaultDiscoveryServiceProvider();
        DiscoveryService discoveryService = provider.newDiscoveryService(DiscoveryMode.Client,
                discoveryStrategiesConfig, ClientDiscoverySpiTest.class.getClassLoader());

        discoveryService.start();
        discoveryService.discoverNodes();
        discoveryService.destroy();

        Field nodeFilterField = DefaultDiscoveryService.class.getDeclaredField("nodeFilter");
        nodeFilterField.setAccessible(true);

        TestNodeFilter nodeFilter = (TestNodeFilter) nodeFilterField.get(discoveryService);

        assertEquals(4, nodeFilter.getNodes().size());
    }

    private static class TestDiscoveryStrategy implements DiscoveryStrategy {

        @Override
        public void start(DiscoveryMode discoveryMode) {
        }

        @Override
        public Collection<DiscoveredNode> discoverNodes() {
            try {
                List<DiscoveredNode> discoveredNodes = new ArrayList<DiscoveredNode>(4);
                Address privateAddress = new Address("127.0.0.1", 1);
                Address publicAddress = new Address("127.0.0.1", 50001);
                discoveredNodes.add(new SimpleDiscoveredNode(privateAddress, publicAddress));
                publicAddress = new Address("127.0.0.1", 50002);
                discoveredNodes.add(new SimpleDiscoveredNode(privateAddress, publicAddress));
                publicAddress = new Address("127.0.0.1", 50003);
                discoveredNodes.add(new SimpleDiscoveredNode(privateAddress, publicAddress));
                publicAddress = new Address("127.0.0.1", 50004);
                discoveredNodes.add(new SimpleDiscoveredNode(privateAddress, publicAddress));

                return discoveredNodes;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void destroy() {
        }
    }

    public static class TestDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        private final Collection<PropertyDefinition> propertyDefinitions;

        public TestDiscoveryStrategyFactory() {
            List<PropertyDefinition> propertyDefinitions = new ArrayList<PropertyDefinition>();
            propertyDefinitions.add(new SimplePropertyDefinition("key-string", PropertyTypeConverter.STRING));
            propertyDefinitions.add(new SimplePropertyDefinition("key-int", PropertyTypeConverter.INTEGER));
            propertyDefinitions.add(new SimplePropertyDefinition("key-boolean", PropertyTypeConverter.BOOLEAN));
            propertyDefinitions.add(new SimplePropertyDefinition("key-something", true, PropertyTypeConverter.STRING));
            this.propertyDefinitions = Collections.unmodifiableCollection(propertyDefinitions);
        }

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return TestDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(Map<String, Comparable> properties) {
            return new TestDiscoveryStrategy();
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return propertyDefinitions;
        }
    }

    public static class TestNodeFilter implements NodeFilter {

        private final List<DiscoveredNode> nodes = new ArrayList<DiscoveredNode>();

        @Override
        public boolean test(DiscoveredNode candidate) {
            nodes.add(candidate);
            return true;
        }

        private List<DiscoveredNode> getNodes() {
            return nodes;
        }
    }
}
