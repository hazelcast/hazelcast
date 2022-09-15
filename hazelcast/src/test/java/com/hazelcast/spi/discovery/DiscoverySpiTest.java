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

package com.hazelcast.spi.discovery;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.membergroup.DefaultMemberGroup;
import com.hazelcast.internal.partition.membergroup.MemberGroupFactory;
import com.hazelcast.internal.partition.membergroup.SPIAwareMemberGroupFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.spi.partitiongroup.PartitionGroupStrategy;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
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
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.PartitionGroupConfig.MemberGroupType.SPI;
import static com.hazelcast.config.properties.PropertyTypeConverter.BOOLEAN;
import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;
import static com.hazelcast.spi.discovery.DiscoverySpiTest.ParametrizedDiscoveryStrategyFactory.BOOL_PROPERTY;
import static com.hazelcast.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class DiscoverySpiTest extends HazelcastTestSupport {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
    private static final ILogger LOGGER = Logger.getLogger(DiscoverySpiTest.class);


    @Test(expected = InvalidConfigurationException.class)
    public void whenStrategyClassNameNotExist_thenFailFast() {
        Config config = new Config();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig("non.existing.ClassName"));
        config.getNetworkConfig().getJoin().setDiscoveryConfig(discoveryConfig);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        createHazelcastInstance(config);
    }

    @Test
    public void givenDiscoveryStrategyFactoryExistOnClassPath_whenTheSameFactoryIsConfiguredExplicitly_thenOnlyOneInstanceOfStrategyIsCreated() {
        // ParametrizedDiscoveryStrategy has a static counter and throws an exception when its instantiated  more than
        // than once.

        Config config = new Config();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        DiscoveryConfig discoveryConfig = join.getDiscoveryConfig();

        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(new ParametrizedDiscoveryStrategyFactory());
        strategyConfig.addProperty("bool-property", true);
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);

        //this will fail when the discovery strategy throws an exception
        createHazelcastInstance(config);
    }

    public static final class ParametrizedDiscoveryStrategy extends AbstractDiscoveryStrategy {
        private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();

        public ParametrizedDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
            boolean parameterPassed = getOrDefault(BOOL_PROPERTY, false);
            if (!parameterPassed) {
                throw new AssertionError("configured parameter was not passed!");
            }
            if (INSTANCE_COUNTER.getAndIncrement() != 0) {
                throw new AssertionError("only 1 instance of a discovery strategy should be created");
            }
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }
    }


    public static final class ParametrizedDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        public static final PropertyDefinition BOOL_PROPERTY = new SimplePropertyDefinition("bool-property", true, BOOLEAN);

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return ParametrizedDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
            return new ParametrizedDiscoveryStrategy(logger, properties);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return Collections.singleton(BOOL_PROPERTY);
        }
    }

    @Test
    public void test_metadata_discovery_on_node_startup() throws Exception {
        String xmlFileName = "test-hazelcast-discovery-spi-metadata.xml";
        InputStream xmlResource = DiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();

        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(1);
        try {
            HazelcastInstance hazelcastInstance1 = instanceFactory.newHazelcastInstance(config);
            assertNotNull(hazelcastInstance1);

            Member localMember = hazelcastInstance1.getCluster().getLocalMember();
            assertEquals("TEST", localMember.getAttribute("test-string"));
        } finally {
            instanceFactory.shutdownAll();
        }
    }

    @Test
    public void test_metadata_discovery_on_node_startup_overrides_what_is_configured_on_member() throws Exception {
        final String overridenAttribute = "test-string";

        String xmlFileName = "test-hazelcast-discovery-spi-metadata.xml";
        InputStream xmlResource = DiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();
        config.getMemberAttributeConfig().setAttribute(overridenAttribute, "config-property");

        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(1);
        try {
            HazelcastInstance hazelcastInstance1 = instanceFactory.newHazelcastInstance(config);
            assertNotNull(hazelcastInstance1);

            Member localMember = hazelcastInstance1.getCluster().getLocalMember();
            assertEquals("TEST", localMember.getAttribute(overridenAttribute));
        } finally {
            instanceFactory.shutdownAll();
        }
    }

    @Test
    public void testSchema() throws Exception {
        String xmlFileName = "test-hazelcast-discovery-spi.xml";

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = DiscoverySpiTest.class.getClassLoader().getResource("hazelcast-config-5.2.xsd");
        assertNotNull(schemaResource);

        InputStream xmlResource = DiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Source source = new StreamSource(xmlResource);

        Schema schema = factory.newSchema(schemaResource);
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
        Config config = getDiscoverySPIConfig(xmlFileName, false);

        try {
            final HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance(config);
            final HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance(config);
            final HazelcastInstance hazelcastInstance3 = Hazelcast.newHazelcastInstance(config);

            assertNotNull(hazelcastInstance1);
            assertNotNull(hazelcastInstance2);
            assertNotNull(hazelcastInstance3);

            assertClusterSizeEventually(3, hazelcastInstance1, hazelcastInstance2, hazelcastInstance3);
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
        PropertyDefinition second = new SimplePropertyDefinition("second", BOOLEAN);
        PropertyDefinition third = new SimplePropertyDefinition("third", PropertyTypeConverter.INTEGER);
        PropertyDefinition fourth = new SimplePropertyDefinition("fourth", true, PropertyTypeConverter.STRING);
        PropertyDefinition onlyInEnv = new SimplePropertyDefinition("only_in_env", true, PropertyTypeConverter.INTEGER);

        Map<String, Comparable> properties = new HashMap<>();
        properties.put("first", "value-first");
        properties.put("second", Boolean.FALSE);
        properties.put("third", 100);

        Map<String, String> environmentVariables = new HashMap<>();

        // system property > system environment > configuration
        // property 'first' => "value-first"
        // property 'second' => true
        environmentVariables.put("test.second", "true");
        // property 'third' => 300
        environmentVariables.put("test.third", "200");
        System.setProperty("test.third", "300");
        // property 'fourth' => null

        environmentVariables.put("test.only_in_env", "500");

        PropertyDiscoveryStrategy strategy = new PropertyDiscoveryStrategy(LOGGER, properties, environmentVariables::get);

        // without lookup of environment
        assertEquals("value-first", strategy.getOrNull(first));
        assertEquals(Boolean.FALSE, strategy.getOrNull(second));
        assertEquals(100, ((Integer) strategy.getOrNull(third)).intValue());
        assertNull(strategy.getOrNull(fourth));

        assertEquals("value-first", strategy.getOrNull("test", first));
        assertEquals(Boolean.TRUE, strategy.getOrNull("test", second));
        assertEquals(300, ((Integer) strategy.getOrNull("test", third)).intValue());
        assertNull(strategy.getOrNull("test", fourth));

        assertEquals(500, ((Integer) strategy.getOrNull("test", onlyInEnv)).intValue());

    }

    @Test
    public void test_AbstractDiscoveryStrategy_getOrDefault() throws Exception {
        PropertyDefinition value = new SimplePropertyDefinition("value", PropertyTypeConverter.INTEGER);

        Map<String, Comparable> properties = Collections.emptyMap();
        PropertyDiscoveryStrategy strategy = new PropertyDiscoveryStrategy(LOGGER, properties);

        assertEquals(1111, (long) strategy.getOrDefault(value, 1111));
        assertEquals(1111, (long) strategy.getOrDefault("test", value, 1111));
    }

    @Test(expected = RuntimeException.class)
    public void testSPIAwareMemberGroupFactoryInvalidConfig() throws Exception {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        try {
            MemberGroupFactory groupFactory = new SPIAwareMemberGroupFactory(getNode(hazelcastInstance).getDiscoveryService());
            Collection<Member> members = createMembers();
            groupFactory.createMemberGroups(members);
        } finally {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    public void testSPIAwareMemberGroupFactoryCreateMemberGroups() throws Exception {
        String xmlFileName = "test-hazelcast-discovery-spi-metadata.xml";
        Config config = getDiscoverySPIConfig(xmlFileName, false);
        // we create this instance in order to fully create Node
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);
        assertNotNull(node);

        MemberGroupFactory groupFactory = new SPIAwareMemberGroupFactory(node.getDiscoveryService());
        Collection<Member> members = createMembers();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 2, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 2, memberGroup.size());
        }
        hazelcastInstance.shutdown();
    }

    @Test
    public void testSPIAwareMemberGroupFactoryCreateMemberGroups_withDeprecated() throws Exception {
        String xmlFileName = "test-hazelcast-discovery-spi-metadata.xml";
        Config config = getDiscoverySPIConfig(xmlFileName, true);
        // we create this instance in order to fully create Node
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        Node node = getNode(hazelcastInstance);
        assertNotNull(node);

        MemberGroupFactory groupFactory = new SPIAwareMemberGroupFactory(node.getDiscoveryService());
        Collection<Member> members = createMembers();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 2, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 2, memberGroup.size());
        }
        hazelcastInstance.shutdown();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_enabled_whenDiscoveryConfigIsNull() {
        Config config = new Config();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        config.getNetworkConfig().getJoin().setDiscoveryConfig(null);
    }

    @Test
    public void testCustomDiscoveryService_whenDiscoveredNodes_isNull() {
        Config config = new Config();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        DiscoveryServiceProvider discoveryServiceProvider = new DiscoveryServiceProvider() {
            public DiscoveryService newDiscoveryService(DiscoveryServiceSettings arg0) {
                DiscoveryService mocked = mock(DiscoveryService.class);
                when(mocked.discoverNodes()).thenReturn(null);
                return mocked;
            }
        };
        config.getNetworkConfig().getJoin().getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);

        try {
            Hazelcast.newHazelcastInstance(config);
            fail("Instance should not be started!");
        } catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testCustomDiscoveryService_whenDiscoveredNodes_isEmpty() {
        Config config = new Config();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");

        final DiscoveryService discoveryService = mock(DiscoveryService.class);
        DiscoveryServiceProvider discoveryServiceProvider = new DiscoveryServiceProvider() {
            public DiscoveryService newDiscoveryService(DiscoveryServiceSettings arg0) {
                when(discoveryService.discoverNodes()).thenReturn(Collections.<DiscoveryNode>emptyList());
                return discoveryService;
            }
        };
        config.getNetworkConfig().getJoin().getDiscoveryConfig().setDiscoveryServiceProvider(discoveryServiceProvider);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        try {
            verify(discoveryService, atLeastOnce()).discoverNodes();
        } finally {
            instance.getLifecycleService().terminate();
        }
    }

    @Test
    public void testMemberGroup_givenSPIMemberGroupIsActived_whenInstanceStarting_wontThrowNPE() {
        // this test has no assert. it's a regression test checking an instance can start when a SPI-driven member group
        // strategy is configured. see #11681
        Config config = new Config();
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);

        DiscoveryStrategyConfig discoveryStrategyConfig
                = new DiscoveryStrategyConfig(MetadataProvidingDiscoveryStrategy.class.getName());
        joinConfig.getDiscoveryConfig()
                .addDiscoveryStrategyConfig(discoveryStrategyConfig);
        config.getPartitionGroupConfig().setGroupType(SPI).setEnabled(true);

        HazelcastInstance hazelcastInstance = null;
        try {
            // check the instance can actually be started
            hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        } finally {
            if (hazelcastInstance != null) {
                hazelcastInstance.shutdown();
            }
        }
    }

    @Test
    public void noBreakingChangeInPartitionGroupStrategyAbstractClass() {
        TestBreakingChangesDiscoveryStrategy strategy = new TestBreakingChangesDiscoveryStrategy();
        assertNull(strategy.getPartitionGroupStrategy());
    }

    static class TestBreakingChangesDiscoveryStrategy extends AbstractDiscoveryStrategy {
        TestBreakingChangesDiscoveryStrategy() {
            super(null, Collections.<String, Comparable>emptyMap());
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }
    }

    private DiscoveryServiceSettings buildDiscoveryServiceSettings(Address address, DiscoveryConfig config, DiscoveryMode mode) {
        return new DiscoveryServiceSettings().setConfigClassLoader(DiscoverySpiTest.class.getClassLoader())
                .setDiscoveryConfig(config).setDiscoveryMode(mode).setLogger(LOGGER)
                .setDiscoveryNode(new SimpleDiscoveryNode(address));
    }

    private static class PropertyDiscoveryStrategy extends AbstractDiscoveryStrategy {

        PropertyDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
        }

        PropertyDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties, EnvVariableProvider envVariableProvider) {
            super(logger, properties, envVariableProvider);
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

        @Override
        public PartitionGroupStrategy getPartitionGroupStrategy(Collection<? extends Member> allMembers) {
            return null;
        }

        @Override
        public Map<String, String> discoverLocalMetadata() {
            return Collections.emptyMap();
        }
    }

    private static class DeprecatedTestDiscoveryStrategy extends TestDiscoveryStrategy {
        @Override
        public PartitionGroupStrategy getPartitionGroupStrategy() {
            return null;
        }
    }

    public static class TestDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        private final Collection<PropertyDefinition> propertyDefinitions;

        public TestDiscoveryStrategyFactory() {
            List<PropertyDefinition> propertyDefinitions = new ArrayList<PropertyDefinition>();
            propertyDefinitions.add(new SimplePropertyDefinition("key-string", PropertyTypeConverter.STRING));
            propertyDefinitions.add(new SimplePropertyDefinition("key-int", PropertyTypeConverter.INTEGER));
            propertyDefinitions.add(new SimplePropertyDefinition("key-boolean", BOOLEAN));
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

        protected final List<DiscoveryNode> discoveryNodes;

        CollectingDiscoveryStrategyFactory(List<DiscoveryNode> discoveryNodes) {
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

    private static class DeprecatedCollectingDiscoveryStrategyFactory extends CollectingDiscoveryStrategyFactory {
        DeprecatedCollectingDiscoveryStrategyFactory(List<DiscoveryNode> discoveryNodes) {
            super(discoveryNodes);
        }

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return DeprecatedCollectingDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode node, ILogger logger, Map<String, Comparable> properties) {
            return new DeprecatedCollectingDiscoveryStrategy(node, discoveryNodes, logger, properties);
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
            discoveryNodes.add(discoveryNode);
            getLogger();
            getProperties();
        }

        // need to provide a custom impl
        @Override
        public PartitionGroupStrategy getPartitionGroupStrategy(Collection<? extends Member> allMembers) {
            return new SPIPartitionGroupStrategy();
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

    private static class DeprecatedCollectingDiscoveryStrategy extends CollectingDiscoveryStrategy {
        DeprecatedCollectingDiscoveryStrategy(DiscoveryNode discoveryNode, List<DiscoveryNode> discoveryNodes, ILogger logger,
                                              Map<String, Comparable> properties) {
            super(discoveryNode, discoveryNodes, logger, properties);
        }

        @Override
        public PartitionGroupStrategy getPartitionGroupStrategy() {
            return new SPIPartitionGroupStrategy();
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

    public static class MetadataProvidingDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return MetadataProvidingDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {

            return new MetadataProvidingDiscoveryStrategy(discoveryNode, logger, properties);
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return asList((PropertyDefinition) new SimplePropertyDefinition("key-string", true, STRING),
                    new SimplePropertyDefinition("key-int", true, INTEGER),
                    new SimplePropertyDefinition("key-boolean", true, BOOLEAN));
        }
    }

    private static class MetadataProvidingDiscoveryStrategy extends AbstractDiscoveryStrategy {

        private final DiscoveryNode discoveryNode;

        MetadataProvidingDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
            super(logger, properties);
            this.discoveryNode = discoveryNode;
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return Collections.singleton(discoveryNode);
        }

        @Override
        public PartitionGroupStrategy getPartitionGroupStrategy(Collection<? extends Member> allMembers) {
            return new SPIPartitionGroupStrategy();
        }

        @Override
        public Map<String, String> discoverLocalMetadata() {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("test-string", "TEST");
            return metadata;
        }
    }

    private static class DeprecatedMetadataProvidingDiscoveryStrategy extends MetadataProvidingDiscoveryStrategy {
        DeprecatedMetadataProvidingDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
            super(discoveryNode, logger, properties);
        }

        @Override
        public PartitionGroupStrategy getPartitionGroupStrategy() {
            return new SPIPartitionGroupStrategy();
        }
    }

    private static class SPIPartitionGroupStrategy implements PartitionGroupStrategy {

        @Override
        public Iterable<MemberGroup> getMemberGroups() {
            List<MemberGroup> groups = new ArrayList<MemberGroup>();
            try {
                groups.add(new DefaultMemberGroup(createMembers()));
                groups.add(new DefaultMemberGroup(createMembers()));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            return groups;
        }
    }

    private static Collection<Member> createMembers() throws UnknownHostException {
        Collection<Member> members = new HashSet<Member>();
        InetAddress fakeAddress = InetAddress.getLocalHost();
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, true));
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5702), VERSION, false));
        members.add(new MemberImpl(new Address("download.hazelcast.org", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("download.hazelcast.org", fakeAddress, 5702), VERSION, false));
        return members;
    }

    private static Config getDiscoverySPIConfig(String xmlFileName, boolean isDeprecated) {
        InputStream xmlResource = DiscoverySpiTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();
        config.getNetworkConfig().setPort(50001);
        InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
        interfaces.clear();
        interfaces.setEnabled(true);
        interfaces.addInterface("127.0.0.1");

        List<DiscoveryNode> discoveryNodes = new CopyOnWriteArrayList<DiscoveryNode>();
        DiscoveryStrategyFactory factory = isDeprecated ? new DeprecatedCollectingDiscoveryStrategyFactory(discoveryNodes)
                : new CollectingDiscoveryStrategyFactory(discoveryNodes);

        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        discoveryConfig.getDiscoveryStrategyConfigs().clear();

        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(factory, Collections.<String, Comparable>emptyMap());
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);
        return config;
    }
}
