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

package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientClasspathXmlConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.partitiongroup.PartitionGroupStrategy;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.internal.verification.AtLeast;

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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientDiscoverySpiTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(ClientDiscoverySpiTest.class);

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSchema() throws Exception {
        String xmlFileName = "hazelcast-client-discovery-spi-test.xml";

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = ClientDiscoverySpiTest.class.getClassLoader().getResource("hazelcast-client-config-4.0.xsd");
        Schema schema = factory.newSchema(schemaResource);

        InputStream xmlResource = ClientDiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Source source = new StreamSource(xmlResource);
        Validator validator = schema.newValidator();
        validator.validate(source);
    }

    @Test
    @Category(QuickTest.class)
    public void testParsing() {
        String xmlFileName = "hazelcast-client-discovery-spi-test.xml";
        InputStream xmlResource = ClientDiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        ClientConfig clientConfig = new XmlClientConfigBuilder(xmlResource).build();

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        DiscoveryConfig discoveryConfig = networkConfig.getDiscoveryConfig();
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
        Config config = new Config();
        config.setProperty("hazelcast.discovery.enabled", "true");

        config.getNetworkConfig().setPort(50001);
        InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
        interfaces.clear();
        interfaces.setEnabled(true);
        interfaces.addInterface("127.0.0.1");

        List<DiscoveryNode> discoveryNodes = new CopyOnWriteArrayList<DiscoveryNode>();
        DiscoveryStrategyFactory factory = new CollectingDiscoveryStrategyFactory(discoveryNodes);

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        DiscoveryConfig discoveryConfig = join.getDiscoveryConfig();
        discoveryConfig.getDiscoveryStrategyConfigs().clear();

        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(factory, Collections.emptyMap());
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);

        final HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance hazelcastInstance3 = Hazelcast.newHazelcastInstance(config);

        try {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setProperty("hazelcast.discovery.enabled", "true");
            discoveryConfig = clientConfig.getNetworkConfig().getDiscoveryConfig();
            discoveryConfig.getDiscoveryStrategyConfigs().clear();

            strategyConfig = new DiscoveryStrategyConfig(factory, Collections.emptyMap());
            discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);

            final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

            assertNotNull(hazelcastInstance1);
            assertNotNull(hazelcastInstance2);
            assertNotNull(hazelcastInstance3);
            assertNotNull(client);

            assertClusterSizeEventually(3, hazelcastInstance1, hazelcastInstance2, hazelcastInstance3, client);
        } finally {
            HazelcastClient.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testDiscoveryServiceLifecycleMethodsCalledWhenClientAndServerStartAndShutdown() {

        //Given
        Config config = new Config();
        config.setProperty("hazelcast.discovery.enabled", "true");

        config.getNetworkConfig().setPort(50001);
        InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
        interfaces.clear();
        interfaces.setEnabled(true);
        interfaces.addInterface("127.0.0.1");

        //Both server and client are using the same LifecycleDiscoveryStrategyFactory so latch count is set to 2.
        CountDownLatch startLatch = new CountDownLatch(2);
        CountDownLatch stopLatch = new CountDownLatch(2);

        List<DiscoveryNode> discoveryNodes = new CopyOnWriteArrayList<DiscoveryNode>();
        DiscoveryStrategyFactory factory = new LifecycleDiscoveryStrategyFactory(startLatch, stopLatch, discoveryNodes);

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        DiscoveryConfig discoveryConfig = join.getDiscoveryConfig();
        discoveryConfig.getDiscoveryStrategyConfigs().clear();

        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(factory, Collections.emptyMap());
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);

        final HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hazelcast.discovery.enabled", "true");
        discoveryConfig = clientConfig.getNetworkConfig().getDiscoveryConfig();
        discoveryConfig.getDiscoveryStrategyConfigs().clear();

        strategyConfig = new DiscoveryStrategyConfig(factory, Collections.emptyMap());
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        assertNotNull(hazelcastInstance);
        assertNotNull(client);

        //When
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();

        //Then
        assertOpenEventually(startLatch);
        assertOpenEventually(stopLatch);
    }

    @Test
    public void testClientCanConnect_afterDiscoveryStrategyThrowsException() {
        Config config = new Config();
        config.getNetworkConfig().setPort(50001);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        Address address = instance.getCluster().getLocalMember().getAddress();
        ClientConfig clientConfig = new ClientConfig();

        clientConfig.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        DiscoveryConfig discoveryConfig = clientConfig.getNetworkConfig().getDiscoveryConfig();
        DiscoveryStrategyFactory factory = new ExceptionThrowingDiscoveryStrategyFactory(address);
        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(factory, Collections.emptyMap());
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);

        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test
    public void testNodeFilter_from_xml() throws Exception {
        String xmlFileName = "hazelcast-client-discovery-spi-test.xml";
        InputStream xmlResource = ClientDiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        ClientConfig clientConfig = new XmlClientConfigBuilder(xmlResource).build();

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        DiscoveryConfig discoveryConfig = networkConfig.getDiscoveryConfig();

        DiscoveryServiceProvider provider = new DefaultDiscoveryServiceProvider();
        DiscoveryService discoveryService = provider.newDiscoveryService(buildDiscoveryServiceSettings(discoveryConfig));

        discoveryService.start();
        discoveryService.discoverNodes();
        discoveryService.destroy();

        Field nodeFilterField = DefaultDiscoveryService.class.getDeclaredField("nodeFilter");
        nodeFilterField.setAccessible(true);

        TestNodeFilter nodeFilter = (TestNodeFilter) nodeFilterField.get(discoveryService);

        assertEquals(4, nodeFilter.getNodes().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_enabled_whenDiscoveryConfigIsNull() {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setDiscoveryConfig(null);
    }

    @Test
    public void test_enabled_whenDiscoveryConfigIsEmpty() {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);

        try {
            HazelcastClient.newHazelcastClient(config);
        } catch (IllegalStateException expected) {
            // no server available
        }
    }

    @Test
    public void test_CustomDiscoveryService_whenDiscoveredNodes_isNull() {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        final DiscoveryService discoveryService = mock(DiscoveryService.class);
        when(discoveryService.discoverNodes()).thenReturn(null);
        DiscoveryServiceProvider discoveryServiceProvider = arg0 -> discoveryService;
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        networkConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(new DiscoveryStrategyConfig());
        networkConfig.getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);

        try {
            HazelcastClient.newHazelcastClient(config);
            fail("Client cannot start, discovery nodes is null!");
        } catch (NullPointerException expected) {
            // discovered nodes is null
        }
        verify(discoveryService).discoverNodes();
    }

    @Test
    public void test_CustomDiscoveryService_whenDiscoveredNodes_isEmpty() {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        final DiscoveryService discoveryService = mock(DiscoveryService.class);
        when(discoveryService.discoverNodes()).thenReturn(Collections.emptyList());
        DiscoveryServiceProvider discoveryServiceProvider = arg0 -> discoveryService;
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(0);
        networkConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(new DiscoveryStrategyConfig());
        networkConfig.getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);

        try {
            HazelcastClient.newHazelcastClient(config);
        } catch (IllegalStateException expected) {
            // no server available
        }
        verify(discoveryService, new AtLeast(1)).discoverNodes();
    }

    @Test(expected = IllegalStateException.class)
    public void testDiscoveryEnabledNoLocalhost() {
        Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        networkConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(
                new DiscoveryStrategyConfig(new NoMemberDiscoveryStrategyFactory(), Collections.emptyMap()));

        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test
    public void testDiscoveryDisabledLocalhost() {
        Hazelcast.newHazelcastInstance();

        // should not throw any exception, localhost is added into the list of addresses
        HazelcastClient.newHazelcastClient();
    }

    @Test(expected = IllegalStateException.class)
    public void testMulticastDiscoveryEnabledNoLocalhost() {
        Hazelcast.newHazelcastInstance();

        ClientClasspathXmlConfig clientConfig = new ClientClasspathXmlConfig(
                "hazelcast-client-dummy-multicast-discovery-test.xml");

        HazelcastClient.newHazelcastClient(clientConfig);
    }

    private DiscoveryServiceSettings buildDiscoveryServiceSettings(DiscoveryConfig config) {
        return new DiscoveryServiceSettings().setConfigClassLoader(ClientDiscoverySpiTest.class.getClassLoader())
                .setDiscoveryConfig(config).setDiscoveryMode(DiscoveryMode.Client).setLogger(LOGGER);
    }

    private static class TestDiscoveryStrategy implements DiscoveryStrategy {

        @Override
        public void start() {
        }

        @Override
        public Collection<DiscoveryNode> discoverNodes() {
            try {
                List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>(4);
                Address privateAddress = new Address("127.0.0.1", 1);
                Address publicAddress = new Address("127.0.0.1", 50001);
                discoveryNodes.add(new SimpleDiscoveryNode(privateAddress, publicAddress));
                privateAddress = new Address("127.0.0.1", 2);
                publicAddress = new Address("127.0.0.1", 50002);
                discoveryNodes.add(new SimpleDiscoveryNode(privateAddress, publicAddress));
                privateAddress = new Address("127.0.0.1", 3);
                publicAddress = new Address("127.0.0.1", 50003);
                discoveryNodes.add(new SimpleDiscoveryNode(privateAddress, publicAddress));
                privateAddress = new Address("127.0.0.1", 4);
                publicAddress = new Address("127.0.0.1", 50004);
                discoveryNodes.add(new SimpleDiscoveryNode(privateAddress, publicAddress));

                return discoveryNodes;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public PartitionGroupStrategy getPartitionGroupStrategy() {
            return null;
        }

        @Override
        public Map<String, String> discoverLocalMetadata() {
            return Collections.emptyMap();
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

    private static class FirstCallExceptionThrowingDiscoveryStrategy implements DiscoveryStrategy {

        AtomicBoolean firstCall = new AtomicBoolean(true);
        private final Address address;

        private FirstCallExceptionThrowingDiscoveryStrategy(Address address) {
            this.address = address;
        }

        @Override
        public void start() {
        }

        @Override
        public Collection<DiscoveryNode> discoverNodes() {
            if (firstCall.compareAndSet(true, false)) {
                throw new RuntimeException();
            }
            List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>(1);
            discoveryNodes.add(new SimpleDiscoveryNode(address));
            return discoveryNodes;
        }

        @Override
        public void destroy() {
        }

        @Override
        public PartitionGroupStrategy getPartitionGroupStrategy() {
            return null;
        }

        @Override
        public Map<String, String> discoverLocalMetadata() {
            return Collections.emptyMap();
        }
    }

    public static class ExceptionThrowingDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        private final Address address;

        public ExceptionThrowingDiscoveryStrategyFactory(Address address) {

            this.address = address;
        }

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return FirstCallExceptionThrowingDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return new FirstCallExceptionThrowingDiscoveryStrategy(address);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return Collections.EMPTY_LIST;
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
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return new CollectingDiscoveryStrategy(discoveryNode, discoveryNodes, logger, properties);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }


    private static class CollectingDiscoveryStrategy extends AbstractDiscoveryStrategy {

        private final List<DiscoveryNode> discoveryNodes;
        private final DiscoveryNode discoveryNode;

        CollectingDiscoveryStrategy(DiscoveryNode discoveryNode, List<DiscoveryNode> discoveryNodes, ILogger logger,
                                    Map<String, Comparable> properties) {
            super(logger, properties);
            this.discoveryNodes = discoveryNodes;
            this.discoveryNode = discoveryNode;
        }

        @Override
        public void start() {
            super.start();
            if (discoveryNode != null) {
                discoveryNodes.add(discoveryNode);
            }
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

    public static class LifecycleDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        private final CountDownLatch startLatch;
        private final CountDownLatch stopLatch;
        private final List<DiscoveryNode> discoveryNodes;

        private LifecycleDiscoveryStrategyFactory(CountDownLatch startLatch, CountDownLatch stopLatch,
                                                  List<DiscoveryNode> discoveryNodes) {
            this.startLatch = startLatch;
            this.stopLatch = stopLatch;
            this.discoveryNodes = discoveryNodes;
        }

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return LifecycleDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return new LifecycleDiscoveryStrategy(startLatch, stopLatch, discoveryNode, discoveryNodes, logger, properties);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }

    private static class LifecycleDiscoveryStrategy extends AbstractDiscoveryStrategy {

        private final CountDownLatch startLatch;
        private final CountDownLatch stopLatch;
        private final List<DiscoveryNode> discoveryNodes;
        private final DiscoveryNode discoveryNode;

        LifecycleDiscoveryStrategy(CountDownLatch startLatch, CountDownLatch stopLatch,
                                   DiscoveryNode discoveryNode, List<DiscoveryNode> discoveryNodes,
                                   ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
            this.startLatch = startLatch;
            this.stopLatch = stopLatch;
            this.discoveryNodes = discoveryNodes;
            this.discoveryNode = discoveryNode;
        }

        @Override
        public void start() {
            super.start();
            startLatch.countDown();
            if (discoveryNode != null) {
                discoveryNodes.add(discoveryNode);
            }
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return new ArrayList<DiscoveryNode>(discoveryNodes);
        }

        @Override
        public void destroy() {
            super.destroy();
            stopLatch.countDown();
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

    private static class NoMemberDiscoveryStrategy extends AbstractDiscoveryStrategy {
        NoMemberDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }
    }

    public static class NoMemberDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return NoMemberDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return new NoMemberDiscoveryStrategy(logger, properties);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }
}
