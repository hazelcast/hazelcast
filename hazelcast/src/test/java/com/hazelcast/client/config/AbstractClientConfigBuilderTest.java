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

package com.hazelcast.client.config;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JavaSerializationFilterConfig;
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
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractClientConfigBuilderTest extends HazelcastTestSupport {
    protected ClientConfig fullClientConfig;
    protected ClientConfig defaultClientConfig;

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

        AwsConfig awsConfig = networkConfig.getAwsConfig();
        assertTrue(awsConfig.isEnabled());
        assertEquals("TEST_ACCESS_KEY", awsConfig.getProperty("access-key"));
        assertEquals("TEST_SECRET_KEY", awsConfig.getProperty("secret-key"));
        assertEquals("us-east-1", awsConfig.getProperty("region"));
        assertEquals("ec2.amazonaws.com", awsConfig.getProperty("host-header"));
        assertEquals("type", awsConfig.getProperty("tag-key"));
        assertEquals("hz-nodes", awsConfig.getProperty("tag-value"));
        assertEquals("11", awsConfig.getProperty("connection-timeout-seconds"));
        assertFalse(networkConfig.getGcpConfig().isEnabled());
        assertFalse(networkConfig.getAzureConfig().isEnabled());
        assertFalse(networkConfig.getKubernetesConfig().isEnabled());
        assertFalse(networkConfig.getEurekaConfig().isEnabled());
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

        JavaSerializationFilterConfig javaSerializationFilterConfig = serializationConfig.getJavaSerializationFilterConfig();
        ClassFilter blacklist = javaSerializationFilterConfig.getBlacklist();
        assertEquals(1, blacklist.getClasses().size());
        assertTrue(blacklist.getClasses().contains("com.acme.app.BeanComparator"));

        ClassFilter whitelist = javaSerializationFilterConfig.getWhitelist();
        assertEquals(2, whitelist.getClasses().size());
        assertTrue(whitelist.getClasses().contains("java.lang.String"));
        assertTrue(whitelist.getClasses().contains("example.Foo"));
        assertEquals(2, whitelist.getPackages().size());
        assertTrue(whitelist.getPackages().contains("com.acme.app"));
        assertTrue(whitelist.getPackages().contains("com.acme.app.subpkg"));
        assertEquals(3, whitelist.getPrefixes().size());
        assertTrue(whitelist.getPrefixes().contains("java"));
        assertTrue(whitelist.getPrefixes().contains("["));
        assertTrue(whitelist.getPrefixes().contains("com."));
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
        assertEquals(2, fullClientConfig.getNearCacheConfigMap().size());
        final NearCacheConfig nearCacheConfig = fullClientConfig.getNearCacheConfig("asd");

        assertEquals(2000, nearCacheConfig.getEvictionConfig().getSize());
        assertEquals(90, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(100, nearCacheConfig.getMaxIdleSeconds());
        assertEquals(EvictionPolicy.LFU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertTrue(nearCacheConfig.isInvalidateOnChange());
        assertTrue(nearCacheConfig.isSerializeKeys());
        assertEquals(InMemoryFormat.OBJECT, nearCacheConfig.getInMemoryFormat());

        final NearCacheConfig evictableNearCacheConfig = fullClientConfig.getNearCacheConfig("NearCacheEvictionConfigExample");
        EvictionConfig nearCacheEvictionConfig = evictableNearCacheConfig.getEvictionConfig();
        assertEquals(EvictionPolicy.LRU, nearCacheEvictionConfig.getEvictionPolicy());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, nearCacheEvictionConfig.getMaximumSizePolicy());
        assertEquals(10000, nearCacheEvictionConfig.getSize());
        assertEquals("com.hazelcast.examples.MyEvictionComparator", nearCacheEvictionConfig.getComparatorClassName());
    }

    @Test
    public void testSSLConfigs() {
        SSLConfig sslConfig = fullClientConfig.getNetworkConfig().getSSLConfig();
        assertNotNull(sslConfig);
        assertFalse(sslConfig.isEnabled());

        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", sslConfig.getFactoryClassName());
        assertEquals(7, sslConfig.getProperties().size());
        assertEquals("TLS", sslConfig.getProperty("protocol"));
        assertEquals("/opt/hazelcast-client.truststore", sslConfig.getProperty("trustStore"));
        assertEquals("secret.123456", sslConfig.getProperty("trustStorePassword"));
        assertEquals("JKS", sslConfig.getProperty("trustStoreType"));
        assertEquals("/opt/hazelcast-client.keystore", sslConfig.getProperty("keyStore"));
        assertEquals("keystorePassword123", sslConfig.getProperty("keyStorePassword"));
        assertEquals("JKS", sslConfig.getProperty("keyStoreType"));
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
        assertFalse(nearCacheConfig.isCacheLocalEntries());

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
        QueryCacheConfig queryCacheClassPredicateConfig = fullClientConfig.getQueryCacheConfigs().get("map-name")
                .get("query-cache-class-name-predicate");
        QueryCacheConfig queryCacheSqlPredicateConfig = fullClientConfig.getQueryCacheConfigs().get("map-name")
                .get("query-cache-sql-predicate");
        EntryListenerConfig entryListenerConfig = queryCacheClassPredicateConfig.getEntryListenerConfigs().get(0);

        assertEquals("query-cache-class-name-predicate", queryCacheClassPredicateConfig.getName());
        assertTrue(entryListenerConfig.isIncludeValue());
        assertFalse(entryListenerConfig.isLocal());
        assertEquals("com.hazelcast.examples.EntryListener", entryListenerConfig.getClassName());
        assertTrue(queryCacheClassPredicateConfig.isIncludeValue());
        assertEquals(1, queryCacheClassPredicateConfig.getBatchSize());
        assertEquals(16, queryCacheClassPredicateConfig.getBufferSize());
        assertEquals(0, queryCacheClassPredicateConfig.getDelaySeconds());
        EvictionConfig evictionConfig = queryCacheClassPredicateConfig.getEvictionConfig();
        assertEquals(EvictionPolicy.LRU, evictionConfig.getEvictionPolicy());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, evictionConfig.getMaximumSizePolicy());
        assertEquals(10000, evictionConfig.getSize());
        assertEquals("com.hazelcast.examples.MyEvictionComparator", evictionConfig.getComparatorClassName());
        assertEquals(InMemoryFormat.BINARY, queryCacheClassPredicateConfig.getInMemoryFormat());
        assertFalse(queryCacheClassPredicateConfig.isCoalesce());
        assertTrue(queryCacheClassPredicateConfig.isPopulate());
        for (MapIndexConfig mapIndexConfig : queryCacheClassPredicateConfig.getIndexConfigs()) {
            assertEquals("name", mapIndexConfig.getAttribute());
            assertFalse(mapIndexConfig.isOrdered());
        }

        assertEquals("com.hazelcast.examples.ExamplePredicate",
                queryCacheClassPredicateConfig.getPredicateConfig().getClassName());

        assertEquals("query-cache-sql-predicate", queryCacheSqlPredicateConfig.getName());
        assertEquals("%age=40", queryCacheSqlPredicateConfig.getPredicateConfig().getSql());
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

    @Test
    public void testDiscoveryStrategyConfig() {
        DiscoveryConfig discoveryConfig = fullClientConfig.getNetworkConfig().getDiscoveryConfig();
        assertEquals("DummyFilterClass", discoveryConfig.getNodeFilterClass());
        Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs = discoveryConfig.getDiscoveryStrategyConfigs();
        assertEquals(1, discoveryStrategyConfigs.size());
        DiscoveryStrategyConfig discoveryStrategyConfig = discoveryStrategyConfigs.iterator().next();
        assertEquals("DummyDiscoveryStrategy1", discoveryStrategyConfig.getClassName());
        Map<String, Comparable> properties = discoveryStrategyConfig.getProperties();
        assertEquals(3, properties.size());
        assertEquals("foo", properties.get("key-string"));
        assertEquals("123", properties.get("key-int"));
        assertEquals("true", properties.get("key-boolean"));
    }

    protected EvictionPolicy getNearCacheEvictionPolicy(String mapName, ClientConfig clientConfig) {
        return clientConfig.getNearCacheConfig(mapName).getEvictionConfig().getEvictionPolicy();
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
    public void testLabels() {
        Set<String> labels = fullClientConfig.getLabels();
        assertEquals(2, labels.size());
        assertContains(labels, "admin");
        assertContains(labels, "foo");
    }

    @Test
    public void testInstanceName() {
        assertEquals("CLIENT_NAME", fullClientConfig.getInstanceName());
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

    @Test(expected = HazelcastException.class)
    public abstract void loadingThroughSystemProperty_nonExistingFile() throws IOException;

    @Test
    public abstract void loadingThroughSystemProperty_existingFile() throws IOException;

    @Test(expected = HazelcastException.class)
    public abstract void loadingThroughSystemProperty_nonExistingClasspathResource() throws IOException;

    @Test
    public abstract void loadingThroughSystemProperty_existingClasspathResource() throws IOException;

    @Test
    public abstract void testFlakeIdGeneratorConfig();

    @Test
    public abstract void testSecurityConfig_onlyFactory();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testHazelcastClientTagAppearsTwice();

    @Test
    public abstract void testNearCacheInMemoryFormatNative_withKeysByReference();

    @Test
    public abstract void testNearCacheEvictionPolicy();

    @Test
    public abstract void testClientUserCodeDeploymentConfig();

    @Test
    public abstract void testReliableTopic_defaults();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testQueryCacheBothPredicateDefinedThrows();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testQueryCacheNoPredicateDefinedThrows();

    @Test
    public abstract void testLoadBalancerRandom();

    @Test
    public abstract void testLoadBalancerRoundRobin();

    @Test
    public abstract void testWhitespaceInNonSpaceStrings();
}
