/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.discovery;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class DiscoverySpiTest
        extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(DiscoverySpiTest.class);

    @Test
    public void testSchema() throws Exception {
        String xmlFileName = "test-hazelcast-discovery-spi.xml";

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = DiscoverySpiTest.class.getClassLoader().getResource("hazelcast-config-3.7.xsd");
        Schema schema = factory.newSchema(schemaResource);

        InputStream xmlResource = DiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Source source = new StreamSource(xmlResource);
        Validator validator = schema.newValidator();
        validator.validate(source);
    }

    @Test
    public void testParsing() throws Exception {
        String xmlFileName = "test-hazelcast-discovery-spi.xml";
        InputStream xmlResource = DiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();

        JoinConfig joinConfig = config.getNetworkConfig().getJoin();

        AwsConfig awsConfig = joinConfig.getAwsConfig();
        assertFalse(awsConfig.isEnabled());

        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        assertFalse(tcpIpConfig.isEnabled());

        MulticastConfig multicastConfig = joinConfig.getMulticastConfig();
        assertFalse(multicastConfig.isEnabled());

        DiscoveryConfig discoveryConfig = joinConfig.getDiscoveryConfig();
        assertTrue(discoveryConfig.isEnabled());

        assertEquals(1, discoveryConfig.getDiscoveryStrategyConfigs().size());

        DiscoveryStrategyConfig providerConfig = discoveryConfig.getDiscoveryStrategyConfigs().iterator().next();

        assertEquals(3, providerConfig.getProperties().size());
        assertEquals("foo", providerConfig.getProperties().get("key-string"));
        assertEquals("123", providerConfig.getProperties().get("key-int"));
        assertEquals("true", providerConfig.getProperties().get("key-boolean"));
    }

    @Test
    public void testNodeStartup() {
        String xmlFileName = "test-hazelcast-discovery-spi.xml";
        InputStream xmlResource = DiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();
        config.getNetworkConfig().setPort(50001);
        InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
        interfaces.clear();
        interfaces.setEnabled(true);
        interfaces.addInterface("127.0.0.1");

        List<DiscoveryNode> discoveryNodes = new CopyOnWriteArrayList<DiscoveryNode>();
        DiscoveryStrategyFactory factory = new CollectingDiscoveryStrategyFactory(discoveryNodes);

        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        discoveryConfig.getDiscoveryStrategyConfigs().clear();

        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(factory, Collections.<String, Comparable>emptyMap());
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);

        try {
            final HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(config);
            final HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(config);
            final HazelcastInstance hazelcastInstance3 = Hazelcast.newHazelcastInstance(config);

            assertNotNull(hazelcastInstance1);
            assertNotNull(hazelcastInstance2);
            assertNotNull(hazelcastInstance3);

            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {

                    assertEquals(3, hazelcastInstance1.getCluster().getMembers().size());
                    assertEquals(3, hazelcastInstance2.getCluster().getMembers().size());
                    assertEquals(3, hazelcastInstance3.getCluster().getMembers().size());
                }
            });
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testNodeFilter_from_xml() throws Exception {
        String xmlFileName = "test-hazelcast-discovery-spi.xml";
        InputStream xmlResource = DiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();

        JoinConfig joinConfig = config.getNetworkConfig().getJoin();

        DiscoveryConfig discoveryConfig = joinConfig.getDiscoveryConfig();

        Address address = new Address("localhost", 5701);
        DiscoveryServiceSettings settings = buildDiscoveryServiceSettings(address, discoveryConfig, DiscoveryMode.Client);

        DiscoveryServiceProvider provider = new DefaultDiscoveryServiceProvider();
        DiscoveryService discoveryService = provider.newDiscoveryService(settings);

        discoveryService.start();
        discoveryService.discoverNodes();
        discoveryService.destroy();

        Field nodeFilterField = DefaultDiscoveryService.class.getDeclaredField("nodeFilter");
        nodeFilterField.setAccessible(true);

        TestNodeFilter nodeFilter = (TestNodeFilter) nodeFilterField.get(discoveryService);

        assertEquals(4, nodeFilter.getNodes().size());
    }

    @Test
    public void test_AbstractDiscoveryStrategy_getOrNull() throws Exception {
        PropertyDefinition first = new SimplePropertyDefinition("first", PropertyTypeConverter.STRING);
        PropertyDefinition second = new SimplePropertyDefinition("second", PropertyTypeConverter.BOOLEAN);
        PropertyDefinition third = new SimplePropertyDefinition("third", PropertyTypeConverter.INTEGER);
        PropertyDefinition fourth = new SimplePropertyDefinition("fourth", true, PropertyTypeConverter.STRING);

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("first", "value-first");
        properties.put("second", Boolean.FALSE);
        properties.put("third", 100);

        // System Property > System Environment > Configuration
        // Property 'first' => "value-first"
        // Property 'second' => true
        setEnvironment("test.second", "true");
        // Property 'third' => 300
        setEnvironment("test.third", "200");
        System.setProperty("test.third", "300");
        // Property 'fourth' => null

        PropertyDiscoveryStrategy strategy = new PropertyDiscoveryStrategy(LOGGER, properties);

        // Without lookup of environment
        assertEquals("value-first", strategy.getOrNull(first));
        assertEquals(Boolean.FALSE, strategy.getOrNull(second));
        assertEquals(100, strategy.getOrNull(third));
        assertNull(strategy.getOrNull(fourth));

        // With lookup of environment
        assertEquals("value-first", strategy.getOrNull("test", first));
        assertEquals(Boolean.TRUE, strategy.getOrNull("test", second));
        assertEquals(300, strategy.getOrNull("test", third));
        assertNull(strategy.getOrNull("test", fourth));
    }

    @Test
    public void test_AbstractDiscoveryStrategy_getOrDefault() throws Exception {
        PropertyDefinition value = new SimplePropertyDefinition("value", PropertyTypeConverter.INTEGER);

        Map<String, Comparable> properties = Collections.emptyMap();
        PropertyDiscoveryStrategy strategy = new PropertyDiscoveryStrategy(LOGGER, properties);

        assertEquals(1111, (long) strategy.getOrDefault(value, 1111));
        assertEquals(1111, (long) strategy.getOrDefault("test", value, 1111));
    }

    private static void setEnvironment(String key, String value) throws Exception {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
            if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                Field field = cl.getDeclaredField("m");
                field.setAccessible(true);
                Object obj = field.get(env);
                Map<String, String> map = (Map<String, String>) obj;
                map.put(key, value);
            }
        }
    }

    private DiscoveryServiceSettings buildDiscoveryServiceSettings(Address address, DiscoveryConfig config, DiscoveryMode mode) {
        return new DiscoveryServiceSettings().setConfigClassLoader(DiscoverySpiTest.class.getClassLoader())
                .setDiscoveryConfig(config).setDiscoveryMode(mode).setLogger(LOGGER)
                .setDiscoveryNode(new SimpleDiscoveryNode(address));
    }

    private static class PropertyDiscoveryStrategy extends AbstractDiscoveryStrategy {

        public PropertyDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }

        @Override
        public <T extends Comparable> T getOrNull(PropertyDefinition property) {
            return super.getOrNull(property);
        }

        @Override
        public <T extends Comparable> T getOrNull(String prefix, PropertyDefinition property) {
            return super.getOrNull(prefix, property);
        }

        @Override
        public <T extends Comparable> T getOrDefault(PropertyDefinition property, T defaultValue) {
            return super.getOrDefault(property, defaultValue);
        }

        @Override
        public <T extends Comparable> T getOrDefault(String prefix, PropertyDefinition property, T defaultValue) {
            return super.getOrDefault(prefix, property, defaultValue);
        }
    }

    private static class TestDiscoveryStrategy implements DiscoveryStrategy {

        @Override
        public void start() {
        }

        @Override
        public Collection<DiscoveryNode> discoverNodes() {
            try {
                List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>(4);
                Address address = new Address("127.0.0.1", 50001);
                discoveryNodes.add(new SimpleDiscoveryNode(address));
                address = new Address("127.0.0.1", 50002);
                discoveryNodes.add(new SimpleDiscoveryNode(address));
                address = new Address("127.0.0.1", 50003);
                discoveryNodes.add(new SimpleDiscoveryNode(address));
                address = new Address("127.0.0.1", 50004);
                discoveryNodes.add(new SimpleDiscoveryNode(address));

                return discoveryNodes;
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
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return new TestDiscoveryStrategy();
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return propertyDefinitions;
        }
    }

    public static class CollectingDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        private final List<DiscoveryNode> discoveryNodes;

        private CollectingDiscoveryStrategyFactory(List<DiscoveryNode> discoveryNodes) {
            this.discoveryNodes = discoveryNodes;
        }

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return CollectingDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode node, ILogger logger, Map<String, Comparable> properties) {
            return new CollectingDiscoveryStrategy(node, discoveryNodes, logger, properties);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }

    private static class CollectingDiscoveryStrategy extends AbstractDiscoveryStrategy {

        private final List<DiscoveryNode> discoveryNodes;
        private final DiscoveryNode discoveryNode;

        public CollectingDiscoveryStrategy(DiscoveryNode discoveryNode, List<DiscoveryNode> discoveryNodes, ILogger logger,
                                           Map<String, Comparable> properties) {
            super(logger, properties);
            this.discoveryNodes = discoveryNodes;
            this.discoveryNode = discoveryNode;
        }

        @Override
        public void start() {
            super.start();
            discoveryNodes.add(discoveryNode);
            getLogger();
            getProperties();
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return new ArrayList<DiscoveryNode>(discoveryNodes);
        }

        @Override
        public void destroy() {
            super.destroy();
            discoveryNodes.remove(discoveryNode);
        }
    }

    public static class TestNodeFilter implements NodeFilter {

        private final List<DiscoveryNode> nodes = new ArrayList<DiscoveryNode>();

        @Override
        public boolean test(DiscoveryNode candidate) {
            nodes.add(candidate);
            return true;
        }

        private List<DiscoveryNode> getNodes() {
            return nodes;
        }
    }
}
