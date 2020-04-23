/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.YamlConfigBuilderTest;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.internal.nio.IOUtil.delete;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the usage of {@link YamlClientConfigBuilder}
 */
// tests need to be executed sequentially because of system properties being set/unset
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlClientConfigBuilderTest extends AbstractClientConfigBuilderTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void init() throws Exception {
        URL schemaResource = YamlConfigBuilderTest.class.getClassLoader().getResource("hazelcast-client-full.yaml");
        fullClientConfig = new YamlClientConfigBuilder(schemaResource).build();

        URL schemaResourceDefault = YamlConfigBuilderTest.class.getClassLoader().getResource("hazelcast-client-default.yaml");
        defaultClientConfig = new YamlClientConfigBuilder(schemaResourceDefault).build();
    }

    @After
    @Before
    public void beforeAndAfter() {
        System.clearProperty("hazelcast.client.config");
    }

    @Test
    public void testNoHazelcastClientRootElement() {
        String yaml = ""
                + "instance-name: my-instance";
        ClientConfig clientConfig = buildConfig(yaml);
        assertEquals("my-instance", clientConfig.getInstanceName());
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() throws IOException {
        File file = File.createTempFile("foo", ".yaml");
        delete(file);
        System.setProperty("hazelcast.client.config", file.getAbsolutePath());

        new YamlClientConfigBuilder();
    }

    @Override
    @Test
    public void loadingThroughSystemProperty_existingFile() throws IOException {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: foobar";

        File file = File.createTempFile("foo", ".yaml");
        file.deleteOnExit();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(yaml);
        writer.close();

        System.setProperty("hazelcast.client.config", file.getAbsolutePath());

        YamlClientConfigBuilder configBuilder = new YamlClientConfigBuilder();
        ClientConfig config = configBuilder.build();
        assertEquals("foobar", config.getClusterName());
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() throws IOException {
        System.setProperty("hazelcast.client.config", "classpath:idontexist.yaml");
        new YamlClientConfigBuilder();
    }

    @Override
    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() throws IOException {
        System.setProperty("hazelcast.client.config", "classpath:test-hazelcast-client.yaml");

        YamlClientConfigBuilder configBuilder = new YamlClientConfigBuilder();
        ClientConfig config = configBuilder.build();
        assertEquals("foobar-yaml", config.getClusterName());
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory",
                config.getNetworkConfig().getSSLConfig().getFactoryClassName());
        assertEquals(128, config.getNetworkConfig().getSocketOptions().getBufferSize());
        assertFalse(config.getNetworkConfig().getSocketOptions().isKeepAlive());
        assertFalse(config.getNetworkConfig().getSocketOptions().isTcpNoDelay());
        assertEquals(3, config.getNetworkConfig().getSocketOptions().getLingerSeconds());
    }

    @Override
    @Test
    public void testFlakeIdGeneratorConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  flake-id-generator:\n"
                + "    gen:\n"
                + "      prefetch-count: 3\n"
                + "      prefetch-validity-millis: 10";
        ClientConfig config = buildConfig(yaml);
        ClientFlakeIdGeneratorConfig fConfig = config.findFlakeIdGeneratorConfig("gen");
        assertEquals("gen", fConfig.getName());
        assertEquals(3, fConfig.getPrefetchCount());
        assertEquals(10L, fConfig.getPrefetchValidityMillis());
    }

    @Override
    @Test
    public void testSecurityConfig_onlyFactory() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  security:\n"
                + "    credentials-factory:\n"
                + "      class-name: com.hazelcast.examples.MyCredentialsFactory\n"
                + "      properties:\n"
                + "        property: value";
        ClientConfig config = buildConfig(yaml);
        ClientSecurityConfig securityConfig = config.getSecurityConfig();
        CredentialsFactoryConfig credentialsFactoryConfig = securityConfig.getCredentialsFactoryConfig();
        assertEquals("com.hazelcast.examples.MyCredentialsFactory", credentialsFactoryConfig.getClassName());
        Properties properties = credentialsFactoryConfig.getProperties();
        assertEquals("value", properties.getProperty("property"));
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastClientTagAppearsTwice() {
        String yaml = ""
                + "hazelcast-client: {}\n"
                + "hazelcast-client: {}";
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testNearCacheInMemoryFormatNative_withKeysByReference() {
        String mapName = "testMapNearCacheInMemoryFormatNative";
        String yaml = ""
                + "hazelcast-client:\n"
                + "  near-cache:\n"
                + "    " + mapName + ":\n"
                + "      in-memory-format: NATIVE\n"
                + "      serialize-keys: false";

        ClientConfig clientConfig = buildConfig(yaml);
        NearCacheConfig ncConfig = clientConfig.getNearCacheConfig(mapName);

        assertEquals(InMemoryFormat.NATIVE, ncConfig.getInMemoryFormat());
        assertTrue(ncConfig.isSerializeKeys());
    }

    @Override
    @Test
    public void testNearCacheEvictionPolicy() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  near-cache:\n"
                + "    lfu:\n"
                + "      eviction:\n"
                + "        eviction-policy: LFU\n"
                + "    lru:\n"
                + "      eviction:\n"
                + "        eviction-policy: LRU\n"
                + "    none:\n"
                + "      eviction:\n"
                + "        eviction-policy: NONE\n"
                + "    random:\n"
                + "      eviction:\n"
                + "        eviction-policy: RANDOM";

        ClientConfig clientConfig = buildConfig(yaml);
        assertEquals(EvictionPolicy.LFU, getNearCacheEvictionPolicy("lfu", clientConfig));
        assertEquals(EvictionPolicy.LRU, getNearCacheEvictionPolicy("lru", clientConfig));
        assertEquals(EvictionPolicy.NONE, getNearCacheEvictionPolicy("none", clientConfig));
        assertEquals(EvictionPolicy.RANDOM, getNearCacheEvictionPolicy("random", clientConfig));
    }

    @Override
    @Test
    public void testClientUserCodeDeploymentConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  user-code-deployment:\n"
                + "    enabled: true\n"
                + "    jarPaths:\n"
                + "      - /User/test/test.jar\n"
                + "    classNames:\n"
                + "      - test.testClassName\n"
                + "      - test.testClassName2";

        ClientConfig clientConfig = buildConfig(yaml);
        ClientUserCodeDeploymentConfig userCodeDeploymentConfig = clientConfig.getUserCodeDeploymentConfig();
        assertTrue(userCodeDeploymentConfig.isEnabled());
        List<String> classNames = userCodeDeploymentConfig.getClassNames();
        assertEquals(2, classNames.size());
        assertTrue(classNames.contains("test.testClassName"));
        assertTrue(classNames.contains("test.testClassName2"));
        List<String> jarPaths = userCodeDeploymentConfig.getJarPaths();
        assertEquals(1, jarPaths.size());
        assertTrue(jarPaths.contains("/User/test/test.jar"));
    }

    @Override
    @Test
    public void testReliableTopic_defaults() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  reliable-topic:\n"
                + "    rel-topic: {}";

        ClientConfig config = buildConfig(yaml);
        ClientReliableTopicConfig reliableTopicConfig = config.getReliableTopicConfig("rel-topic");
        assertEquals("rel-topic", reliableTopicConfig.getName());
        assertEquals(10, reliableTopicConfig.getReadBatchSize());
        assertEquals(TopicOverloadPolicy.BLOCK, reliableTopicConfig.getTopicOverloadPolicy());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testQueryCacheBothPredicateDefinedThrows() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  query-caches:\n"
                + "    query-cache-name:\n"
                + "      map-name: map-name\n"
                + "      predicate:\n"
                + "        class-name: com.hazelcast.example.Predicate\n"
                + "        sql: \"%age=40\"";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testQueryCacheNoPredicateDefinedThrows() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  query-caches:\n"
                + "    query-cache-name:\n"
                + "      predicate: {}";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testLoadBalancerRandom() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  load-balancer:\n"
                + "    type: random";

        ClientConfig config = buildConfig(yaml);

        assertInstanceOf(RandomLB.class, config.getLoadBalancer());
    }

    @Override
    @Test
    public void testLoadBalancerRoundRobin() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  load-balancer:\n"
                + "    type: round-robin";

        ClientConfig config = buildConfig(yaml);

        assertInstanceOf(RoundRobinLB.class, config.getLoadBalancer());
    }

    @Test
    public void testNullInMapThrows() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  group:\n"
                + "  name: instanceName";

        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/group"));
        buildConfig(yaml);
    }

    @Test
    public void testNullInSequenceThrows() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  client-labels:\n"
                + "    - admin\n"
                + "    -\n";

        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/client-labels"));
        buildConfig(yaml);
    }

    @Test
    public void testExplicitNullScalarThrows() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  group:\n"
                + "   name: !!null";

        expected.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/group/name"));
        buildConfig(yaml);
    }

    @Override
    @Test
    public void testWhitespaceInNonSpaceStrings() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  load-balancer:\n"
                + "    type:   random   \n";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testTokenIdentityConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  security:\n"
                + "    token:\n"
                + "      encoding: base64\n"
                + "      value: SGF6ZWxjYXN0\n";

        ClientConfig config = buildConfig(yaml);
        TokenIdentityConfig tokenIdentityConfig = config.getSecurityConfig().getTokenIdentityConfig();
        assertNotNull(tokenIdentityConfig);
        assertArrayEquals("Hazelcast".getBytes(StandardCharsets.US_ASCII), tokenIdentityConfig.getToken());
        assertEquals("SGF6ZWxjYXN0", tokenIdentityConfig.getTokenEncoded());
    }

    @Override
    @Test
    public void testKerberosIdentityConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  security:\n"
                + "    kerberos:\n"
                + "      realm: HAZELCAST.COM\n"
                + "      security-realm: krb5Initiator\n"
                + "      service-name-prefix: hz/\n"
                + "      spn: hz/127.0.0.1@HAZELCAST.COM\n";

        ClientConfig config = buildConfig(yaml);
        KerberosIdentityConfig identityConfig = config.getSecurityConfig().getKerberosIdentityConfig();
        assertNotNull(identityConfig);
        assertEquals("HAZELCAST.COM", identityConfig.getRealm());
        assertEquals("krb5Initiator", identityConfig.getSecurityRealm());
        assertEquals("hz/", identityConfig.getServiceNamePrefix());
        assertEquals("hz/127.0.0.1@HAZELCAST.COM", identityConfig.getSpn());
    }

    @Override
    @Test
    public void testMetricsConfig() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  metrics:\n"
                + "    enabled: false\n"
                + "    jmx:\n"
                + "      enabled: false\n"
                + "    collection-frequency-seconds: 10\n";
        ClientConfig config = buildConfig(yaml);
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(10, metricsConfig.getCollectionFrequencySeconds());
    }

    @Override
    @Test
    public void testMetricsConfigMasterSwitchDisabled() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  metrics:\n"
                + "    enabled: false";
        ClientConfig config = buildConfig(yaml);
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertTrue(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testMetricsConfigJmxDisabled() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  metrics:\n"
                + "    jmx:\n"
                + "      enabled: false";
        ClientConfig config = buildConfig(yaml);
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
    }

    public static ClientConfig buildConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlClientConfigBuilder configBuilder = new YamlClientConfigBuilder(bis);
        return configBuilder.build();
    }

    static ClientConfig buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        YamlClientConfigBuilder configBuilder = new YamlClientConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    static ClientConfig buildConfig(String yaml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return buildConfig(yaml, properties);
    }


}
