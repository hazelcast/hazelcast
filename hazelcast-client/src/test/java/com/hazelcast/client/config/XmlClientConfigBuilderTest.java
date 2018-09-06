/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.XMLConfigBuilderTest;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.After;
import org.junit.Before;
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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.nio.IOUtil.delete;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class tests the usage of {@link XmlClientConfigBuilder}
 */
// tests need to be executed sequentially because of system properties being set/unset
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientConfigBuilderTest extends HazelcastTestSupport {

    static final String HAZELCAST_CLIENT_START_TAG =
            "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n";

    static final String HAZELCAST_CLIENT_END_TAG = "</hazelcast-client>";

    private ClientConfig fullClientConfig;
    private ClientConfig defaultClientConfig;

    @Before
    public void init() throws Exception {
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-client-full.xml");
        fullClientConfig = new XmlClientConfigBuilder(schemaResource).build();

        URL schemaResourceDefault = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-client-default.xml");
        defaultClientConfig = new XmlClientConfigBuilder(schemaResourceDefault).build();
    }

    @After
    @Before
    public void beforeAndAfter() {
        System.clearProperty("hazelcast.client.config");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String xml = "<hazelcast>"
                + "<group>"
                + "<name>dev</name>"
                + "<password>clusterpass</password>"
                + "</group>"
                + "</hazelcast>";
        buildConfig(xml);
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() throws IOException {
        File file = File.createTempFile("foo", "bar");
        delete(file);
        System.setProperty("hazelcast.client.config", file.getAbsolutePath());

        new XmlClientConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingFile() throws IOException {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <group>\n"
                + "        <name>foobar</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n"
                + "</hazelcast-client>";

        File file = File.createTempFile("foo", "bar");
        file.deleteOnExit();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(xml);
        writer.close();

        System.setProperty("hazelcast.client.config", file.getAbsolutePath());

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder();
        ClientConfig config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() throws IOException {
        System.setProperty("hazelcast.client.config", "classpath:idontexist");
        new XmlClientConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() throws IOException {
        System.setProperty("hazelcast.client.config", "classpath:test-hazelcast-client.xml");

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder();
        ClientConfig config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", config.getNetworkConfig().getSSLConfig().getFactoryClassName());
        assertEquals(128, config.getNetworkConfig().getSocketOptions().getBufferSize());
        assertFalse(config.getNetworkConfig().getSocketOptions().isKeepAlive());
        assertFalse(config.getNetworkConfig().getSocketOptions().isTcpNoDelay());
        assertEquals(3, config.getNetworkConfig().getSocketOptions().getLingerSeconds());
    }

    @Test
    public void testGroupConfig() {
        final GroupConfig groupConfig = fullClientConfig.getGroupConfig();
        assertEquals("dev", groupConfig.getName());
        assertEquals("dev-pass", groupConfig.getPassword());
    }

    @Test
    public void testProperties() {
        assertEquals(6, fullClientConfig.getProperties().size());
        assertEquals("60000", fullClientConfig.getProperty("hazelcast.client.heartbeat.timeout"));
    }

    @Test
    public void testNetworkConfig() {
        final ClientNetworkConfig networkConfig = fullClientConfig.getNetworkConfig();
        assertEquals(2, networkConfig.getConnectionAttemptLimit());
        assertEquals(2, networkConfig.getAddresses().size());
        assertContains(networkConfig.getAddresses(), "127.0.0.1");
        assertContains(networkConfig.getAddresses(), "127.0.0.2");

        Collection<String> allowedPorts = networkConfig.getOutboundPortDefinitions();
        assertEquals(2, allowedPorts.size());
        assertTrue(allowedPorts.contains("34600"));
        assertTrue(allowedPorts.contains("34700-34710"));

        assertTrue(networkConfig.isSmartRouting());
        assertTrue(networkConfig.isRedoOperation());

        final SocketInterceptorConfig socketInterceptorConfig = networkConfig.getSocketInterceptorConfig();
        assertTrue(socketInterceptorConfig.isEnabled());
        assertEquals("com.hazelcast.examples.MySocketInterceptor", socketInterceptorConfig.getClassName());
        assertEquals("bar", socketInterceptorConfig.getProperty("foo"));

        AliasedDiscoveryConfig awsAliasConfig = networkConfig.getAliasedDiscoveryConfigs().get(0);
        assertEquals("aws", awsAliasConfig.getEnvironment());
        assertTrue(awsAliasConfig.isEnabled());
        assertEquals("TEST_ACCESS_KEY", awsAliasConfig.getProperties().get("access-key"));
        assertEquals("TEST_SECRET_KEY", awsAliasConfig.getProperties().get("secret-key"));
        assertEquals("us-east-1", awsAliasConfig.getProperties().get("region"));
        assertEquals("ec2.amazonaws.com", awsAliasConfig.getProperties().get("host-header"));
        assertEquals("type", awsAliasConfig.getProperties().get("tag-key"));
        assertEquals("hz-nodes", awsAliasConfig.getProperties().get("tag-value"));
        assertEquals("11", awsAliasConfig.getProperties().get("connection-timeout-seconds"));
    }

    @Test
    public void testSerializationConfig() {
        final SerializationConfig serializationConfig = fullClientConfig.getSerializationConfig();
        assertEquals(3, serializationConfig.getPortableVersion());

        final Map<Integer, String> dsClasses = serializationConfig.getDataSerializableFactoryClasses();
        assertEquals(1, dsClasses.size());
        assertEquals("com.hazelcast.examples.DataSerializableFactory", dsClasses.get(1));

        final Map<Integer, String> pfClasses = serializationConfig.getPortableFactoryClasses();
        assertEquals(1, pfClasses.size());
        assertEquals("com.hazelcast.examples.PortableFactory", pfClasses.get(2));

        final Collection<SerializerConfig> serializerConfigs = serializationConfig.getSerializerConfigs();
        assertEquals(1, serializerConfigs.size());
        final SerializerConfig serializerConfig = serializerConfigs.iterator().next();

        assertEquals("com.hazelcast.examples.DummyType", serializerConfig.getTypeClassName());
        assertEquals("com.hazelcast.examples.SerializerFactory", serializerConfig.getClassName());

        final GlobalSerializerConfig globalSerializerConfig = serializationConfig.getGlobalSerializerConfig();
        assertEquals("com.hazelcast.examples.GlobalSerializerFactory", globalSerializerConfig.getClassName());

        assertEquals(ByteOrder.BIG_ENDIAN, serializationConfig.getByteOrder());
        assertTrue(serializationConfig.isCheckClassDefErrors());
        assertFalse(serializationConfig.isAllowUnsafe());
        assertFalse(serializationConfig.isEnableCompression());
        assertTrue(serializationConfig.isEnableSharedObject());
        assertTrue(serializationConfig.isUseNativeByteOrder());
    }

    @Test
    public void testProxyFactories() {
        final List<ProxyFactoryConfig> pfc = fullClientConfig.getProxyFactoryConfigs();
        assertEquals(3, pfc.size());
        assertContains(pfc, new ProxyFactoryConfig("com.hazelcast.examples.ProxyXYZ1", "sampleService1"));
        assertContains(pfc, new ProxyFactoryConfig("com.hazelcast.examples.ProxyXYZ2", "sampleService1"));
        assertContains(pfc, new ProxyFactoryConfig("com.hazelcast.examples.ProxyXYZ3", "sampleService3"));
    }

    @Test
    public void testNearCacheConfigs() {
        assertEquals(1, fullClientConfig.getNearCacheConfigMap().size());
        final NearCacheConfig nearCacheConfig = fullClientConfig.getNearCacheConfig("asd");

        assertEquals(2000, nearCacheConfig.getMaxSize());
        assertEquals(2000, nearCacheConfig.getEvictionConfig().getSize());
        assertEquals(90, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(100, nearCacheConfig.getMaxIdleSeconds());
        assertEquals("LFU", nearCacheConfig.getEvictionPolicy());
        assertEquals(EvictionPolicy.LFU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertTrue(nearCacheConfig.isInvalidateOnChange());
        assertTrue(nearCacheConfig.isSerializeKeys());
        assertEquals(InMemoryFormat.OBJECT, nearCacheConfig.getInMemoryFormat());
    }

    @Test
    public void testSSLConfigs() {
        SSLConfig sslConfig = fullClientConfig.getNetworkConfig().getSSLConfig();
        assertNotNull(sslConfig);
        assertFalse(sslConfig.isEnabled());

        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", sslConfig.getFactoryClassName());
        assertEquals(7, sslConfig.getProperties().size());
        assertEquals("TLS", sslConfig.getProperties().get("protocol"));
        assertEquals("/opt/hazelcast-client.truststore", sslConfig.getProperties().get("trustStore"));
        assertEquals("secret.123456", sslConfig.getProperties().get("trustStorePassword"));
        assertEquals("JKS", sslConfig.getProperties().get("trustStoreType"));
        assertEquals("/opt/hazelcast-client.keystore", sslConfig.getProperties().get("keyStore"));
        assertEquals("keystorePassword123", sslConfig.getProperties().get("keyStorePassword"));
        assertEquals("JKS", sslConfig.getProperties().get("keyStoreType"));
    }

    @Test
    public void testNearCacheConfig_withEvictionConfig_withPreloaderConfig() throws IOException {
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-client-test.xml");
        ClientConfig clientConfig = new XmlClientConfigBuilder(schemaResource).build();

        assertEquals("MyInstanceName", clientConfig.getInstanceName());

        NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig("nearCacheWithEvictionAndPreloader");

        assertEquals(10000, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(5000, nearCacheConfig.getMaxIdleSeconds());
        assertFalse(nearCacheConfig.isInvalidateOnChange());
        assertEquals(InMemoryFormat.OBJECT, nearCacheConfig.getInMemoryFormat());
        assertTrue(nearCacheConfig.isCacheLocalEntries());

        assertNotNull(nearCacheConfig.getEvictionConfig());
        assertEquals(100, nearCacheConfig.getEvictionConfig().getSize());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, nearCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(EvictionPolicy.LFU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());

        assertNotNull(nearCacheConfig.getPreloaderConfig());
        assertTrue(nearCacheConfig.getPreloaderConfig().isEnabled());
        assertEquals("/tmp/myNearCache", nearCacheConfig.getPreloaderConfig().getDirectory());
        assertEquals(2342, nearCacheConfig.getPreloaderConfig().getStoreInitialDelaySeconds());
        assertEquals(4223, nearCacheConfig.getPreloaderConfig().getStoreIntervalSeconds());
    }

    @Test
    public void testQueryCacheFullConfig() throws Exception {
        QueryCacheConfig queryCacheConfig = fullClientConfig.getQueryCacheConfigs().get("map-name").get("query-cache-name");
        EntryListenerConfig entryListenerConfig = queryCacheConfig.getEntryListenerConfigs().get(0);

        assertEquals("query-cache-name", queryCacheConfig.getName());
        assertTrue(entryListenerConfig.isIncludeValue());
        assertFalse(entryListenerConfig.isLocal());
        assertEquals("com.hazelcast.examples.EntryListener", entryListenerConfig.getClassName());
        assertTrue(queryCacheConfig.isIncludeValue());
        assertEquals(1, queryCacheConfig.getBatchSize());
        assertEquals(16, queryCacheConfig.getBufferSize());
        assertEquals(0, queryCacheConfig.getDelaySeconds());
        assertEquals(EvictionPolicy.LRU, queryCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, queryCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(10000, queryCacheConfig.getEvictionConfig().getSize());
        assertEquals(InMemoryFormat.BINARY, queryCacheConfig.getInMemoryFormat());
        assertFalse(queryCacheConfig.isCoalesce());
        assertTrue(queryCacheConfig.isPopulate());
        for (MapIndexConfig mapIndexConfig : queryCacheConfig.getIndexConfigs()) {
            assertEquals("name", mapIndexConfig.getAttribute());
            assertFalse(mapIndexConfig.isOrdered());
        }

        assertEquals("com.hazelcast.examples.ExamplePredicate", queryCacheConfig.getPredicateConfig().getClassName());
    }

    @Test
    public void testFlakeIdGeneratorConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<flake-id-generator name='gen'>"
                + "  <prefetch-count>3</prefetch-count>"
                + "  <prefetch-validity-millis>10</prefetch-validity-millis>"
                + "</flake-id-generator>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        ClientFlakeIdGeneratorConfig fConfig = config.findFlakeIdGeneratorConfig("gen");
        assertEquals("gen", fConfig.getName());
        assertEquals(3, fConfig.getPrefetchCount());
        assertEquals(10L, fConfig.getPrefetchValidityMillis());
    }

    @Test
    public void testConnectionStrategyConfig() {
        ClientConnectionStrategyConfig connectionStrategyConfig = fullClientConfig.getConnectionStrategyConfig();
        assertTrue(connectionStrategyConfig.isAsyncStart());
        assertEquals(ClientConnectionStrategyConfig.ReconnectMode.ASYNC, connectionStrategyConfig.getReconnectMode());
    }

    @Test
    public void testConnectionStrategyConfig_defaults() {
        ClientConnectionStrategyConfig connectionStrategyConfig = defaultClientConfig.getConnectionStrategyConfig();
        assertFalse(connectionStrategyConfig.isAsyncStart());
        assertEquals(ClientConnectionStrategyConfig.ReconnectMode.ON, connectionStrategyConfig.getReconnectMode());
    }

    @Test
    public void testExponentialConnectionRetryConfig() {
        ClientConnectionStrategyConfig connectionStrategyConfig = fullClientConfig.getConnectionStrategyConfig();
        ConnectionRetryConfig exponentialRetryConfig = connectionStrategyConfig.getConnectionRetryConfig();
        assertTrue(exponentialRetryConfig.isEnabled());
        assertTrue(exponentialRetryConfig.isFailOnMaxBackoff());
        assertEquals(0.5, exponentialRetryConfig.getJitter(), 0);
        assertEquals(2000, exponentialRetryConfig.getInitialBackoffMillis());
        assertEquals(60000, exponentialRetryConfig.getMaxBackoffMillis());
        assertEquals(3, exponentialRetryConfig.getMultiplier(), 0);
    }

    @Test
    public void testExponentialConnectionRetryConfig_defaults() {
        ClientConnectionStrategyConfig connectionStrategyConfig = defaultClientConfig.getConnectionStrategyConfig();
        ConnectionRetryConfig exponentialRetryConfig = connectionStrategyConfig.getConnectionRetryConfig();
        assertFalse(exponentialRetryConfig.isEnabled());
        assertFalse(exponentialRetryConfig.isFailOnMaxBackoff());
        assertEquals(0.2, exponentialRetryConfig.getJitter(), 0);
        assertEquals(1000, exponentialRetryConfig.getInitialBackoffMillis());
        assertEquals(30000, exponentialRetryConfig.getMaxBackoffMillis());
        assertEquals(2, exponentialRetryConfig.getMultiplier(), 0);
    }

    @Test
    public void testSecurityConfig() {
        ClientSecurityConfig securityConfig = fullClientConfig.getSecurityConfig();
        assertEquals("com.hazelcast.security.UsernamePasswordCredentials", securityConfig.getCredentialsClassname());
        CredentialsFactoryConfig credentialsFactoryConfig = securityConfig.getCredentialsFactoryConfig();
        assertEquals("com.hazelcast.examples.MyCredentialsFactory", credentialsFactoryConfig.getClassName());
        Properties properties = credentialsFactoryConfig.getProperties();
        assertEquals("value", properties.getProperty("property"));
    }

    @Test
    public void testLeftovers() {
        assertEquals(40, fullClientConfig.getExecutorPoolSize());
        assertEquals("com.hazelcast.client.util.RandomLB", fullClientConfig.getLoadBalancer().getClass().getName());

        final List<ListenerConfig> listenerConfigs = fullClientConfig.getListenerConfigs();
        assertEquals(3, listenerConfigs.size());
        assertContains(listenerConfigs, new ListenerConfig("com.hazelcast.examples.MembershipListener"));
        assertContains(listenerConfigs, new ListenerConfig("com.hazelcast.examples.InstanceListener"));
        assertContains(listenerConfigs, new ListenerConfig("com.hazelcast.examples.MigrationListener"));
    }

    @Test
    public void testXSDDefaultXML() throws SAXException, IOException {
        testXSDConfigXML("hazelcast-client-default.xml");
    }

    @Test
    public void testFullConfigXML() throws SAXException, IOException {
        testXSDConfigXML("hazelcast-client-full.xml");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMissingNamespace() {
        String xml = "<hazelcast-client/>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidNamespace() {
        String xml = "<hazelcast-client xmlns=\"http://foo.bar\"/>";
        buildConfig(xml);
    }

    @Test
    public void testValidNamespace() {
        String xml = HAZELCAST_CLIENT_START_TAG + "</hazelcast-client>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastClientTagAppearsTwice() {
        String xml = HAZELCAST_CLIENT_START_TAG + "<hazelcast-client/><hazelcast-client/>";
        buildConfig(xml);
    }

    @Test
    public void testNearCacheInMemoryFormatNative_withKeysByReference() {
        String mapName = "testMapNearCacheInMemoryFormatNative";
        String xml = HAZELCAST_CLIENT_START_TAG
                + "  <near-cache name=\"" + mapName + "\">\n"
                + "    <in-memory-format>NATIVE</in-memory-format>\n"
                + "    <serialize-keys>false</serialize-keys>\n"
                + "  </near-cache>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig clientConfig = buildConfig(xml);
        NearCacheConfig ncConfig = clientConfig.getNearCacheConfig(mapName);

        assertEquals(InMemoryFormat.NATIVE, ncConfig.getInMemoryFormat());
        assertTrue(ncConfig.isSerializeKeys());
    }

    @Test
    public void testNearCacheEvictionPolicy() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<near-cache name=\"lfu\">"
                + "  <eviction eviction-policy=\"LFU\"/>"
                + "</near-cache>"
                + "<near-cache name=\"lru\">"
                + "  <eviction eviction-policy=\"LRU\"/>"
                + "</near-cache>"
                + "<near-cache name=\"none\">"
                + "  <eviction eviction-policy=\"NONE\"/>"
                + "</near-cache>"
                + "<near-cache name=\"random\">"
                + "  <eviction eviction-policy=\"RANDOM\"/>"
                + "</near-cache>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig clientConfig = buildConfig(xml);
        assertEquals(EvictionPolicy.LFU, getNearCacheEvictionPolicy("lfu", clientConfig));
        assertEquals(EvictionPolicy.LRU, getNearCacheEvictionPolicy("lru", clientConfig));
        assertEquals(EvictionPolicy.NONE, getNearCacheEvictionPolicy("none", clientConfig));
        assertEquals(EvictionPolicy.RANDOM, getNearCacheEvictionPolicy("random", clientConfig));
    }

    @Test
    public void testClientUserCodeDeploymentConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<user-code-deployment enabled=\"true\">\n"
                + "        <jarPaths>\n"
                + "            <jarPath>/User/test/test.jar</jarPath>\n"
                + "        </jarPaths>\n"
                + "        <classNames>\n"
                + "            <className>test.testClassName</className>\n"
                + "            <className>test.testClassName2</className>\n"
                + "        </classNames>\n"
                + "    </user-code-deployment>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig clientConfig = buildConfig(xml);
        ClientUserCodeDeploymentConfig userCodeDeploymentConfig = clientConfig.getUserCodeDeploymentConfig();
        assertEquals(true, userCodeDeploymentConfig.isEnabled());
        List<String> classNames = userCodeDeploymentConfig.getClassNames();
        assertEquals(2, classNames.size());
        assertEquals(true, classNames.contains("test.testClassName"));
        assertEquals(true, classNames.contains("test.testClassName2"));
        List<String> jarPaths = userCodeDeploymentConfig.getJarPaths();
        assertEquals(1, jarPaths.size());
        assertEquals(true, jarPaths.contains("/User/test/test.jar"));
    }

    @Test
    public void testClientIcmpPingConfig() {
        ClientIcmpPingConfig icmpPingConfig = fullClientConfig.getNetworkConfig().getClientIcmpPingConfig();
        assertEquals(false, icmpPingConfig.isEnabled());
        assertEquals(2000, icmpPingConfig.getTimeoutMilliseconds());
        assertEquals(3000, icmpPingConfig.getIntervalMilliseconds());
        assertEquals(100, icmpPingConfig.getTtl());
        assertEquals(5, icmpPingConfig.getMaxAttempts());
        assertEquals(false, icmpPingConfig.isEchoFailFastOnStartup());
    }

    @Test
    public void testClientIcmpPingConfig_defaults() {
        ClientIcmpPingConfig icmpPingConfig = defaultClientConfig.getNetworkConfig().getClientIcmpPingConfig();
        assertEquals(false, icmpPingConfig.isEnabled());
        assertEquals(1000, icmpPingConfig.getTimeoutMilliseconds());
        assertEquals(1000, icmpPingConfig.getIntervalMilliseconds());
        assertEquals(255, icmpPingConfig.getTtl());
        assertEquals(2, icmpPingConfig.getMaxAttempts());
        assertEquals(true, icmpPingConfig.isEchoFailFastOnStartup());
    }

    @Test
    public void testReliableTopic() {
        ClientReliableTopicConfig reliableTopicConfig = fullClientConfig.getReliableTopicConfig("rel-topic");
        assertEquals(100, reliableTopicConfig.getReadBatchSize());
        assertEquals(TopicOverloadPolicy.DISCARD_NEWEST, reliableTopicConfig.getTopicOverloadPolicy());
    }

    @Test
    public void testReliableTopic_defaults() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<reliable-topic name=\"rel-topic\">"
                + "</reliable-topic>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        ClientReliableTopicConfig reliableTopicConfig = config.getReliableTopicConfig("rel-topic");
        assertEquals("rel-topic", reliableTopicConfig.getName());
        assertEquals(10, reliableTopicConfig.getReadBatchSize());
        assertEquals(TopicOverloadPolicy.BLOCK, reliableTopicConfig.getTopicOverloadPolicy());
    }

    @Test
    public void testCloudConfig() {
        ClientCloudConfig cloudConfig = fullClientConfig.getNetworkConfig().getCloudConfig();
        assertEquals(false, cloudConfig.isEnabled());
        assertEquals("EXAMPLE_TOKEN", cloudConfig.getDiscoveryToken());
    }

    @Test
    public void testCloudConfig_defaults() {
        ClientCloudConfig cloudConfig = defaultClientConfig.getNetworkConfig().getCloudConfig();
        assertEquals(false, cloudConfig.isEnabled());
        assertEquals(null, cloudConfig.getDiscoveryToken());
    }

    private EvictionPolicy getNearCacheEvictionPolicy(String mapName, ClientConfig clientConfig) {
        return clientConfig.getNearCacheConfig(mapName).getEvictionConfig().getEvictionPolicy();
    }

    static ClientConfig buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    static ClientConfig buildConfig(String xml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return buildConfig(xml, properties);
    }

    public static ClientConfig buildConfig(String xml) {
        return buildConfig(xml, null);
    }

    private void testXSDConfigXML(String xmlFileName) throws SAXException, IOException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-client-config-"
                + Versions.CURRENT_CLUSTER_VERSION + ".xsd");
        InputStream xmlResource = XMLConfigBuilderTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Schema schema = factory.newSchema(schemaResource);
        Source source = new StreamSource(xmlResource);
        Validator validator = schema.newValidator();
        try {
            validator.validate(source);
        } catch (SAXException ex) {
            fail(xmlFileName + " is not valid because: " + ex.toString());
        }
    }
}
