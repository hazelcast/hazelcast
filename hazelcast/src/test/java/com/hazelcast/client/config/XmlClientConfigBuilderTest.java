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

import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.config.XMLConfigBuilderTest;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.config.PersistentMemoryMode.MOUNTED;
import static com.hazelcast.config.PersistentMemoryMode.SYSTEM_MEMORY;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class tests the usage of {@link XmlClientConfigBuilder}
 */
// tests need to be executed sequentially because of system properties being set/unset
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientConfigBuilderTest extends AbstractClientConfigBuilderTest {

    static final String HAZELCAST_CLIENT_START_TAG =
            "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n";

    static final String HAZELCAST_CLIENT_END_TAG = "</hazelcast-client>";

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
                + "<cluster-name>dev</cluster-name>"
                + "</hazelcast>";
        buildConfig(xml);
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() throws IOException {
        File file = File.createTempFile("foo", ".xml");
        delete(file);
        System.setProperty("hazelcast.client.config", file.getAbsolutePath());

        new XmlClientConfigBuilder();
    }

    @Override
    @Test
    public void loadingThroughSystemProperty_existingFile() throws IOException {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <cluster-name>foobar</cluster-name>\n"
                + "</hazelcast-client>";

        File file = File.createTempFile("foo", ".xml");
        file.deleteOnExit();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(xml);
        writer.close();

        System.setProperty("hazelcast.client.config", file.getAbsolutePath());

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder();
        ClientConfig config = configBuilder.build();
        assertEquals("foobar", config.getClusterName());
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() throws IOException {
        System.setProperty("hazelcast.client.config", "classpath:idontexist.xml");
        new XmlClientConfigBuilder();
    }

    @Override
    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() throws IOException {
        System.setProperty("hazelcast.client.config", "classpath:test-hazelcast-client.xml");

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder();
        ClientConfig config = configBuilder.build();
        assertEquals("foobar-xml", config.getClusterName());
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", config.getNetworkConfig().getSSLConfig().getFactoryClassName());
        assertEquals(128, config.getNetworkConfig().getSocketOptions().getBufferSize());
        assertFalse(config.getNetworkConfig().getSocketOptions().isKeepAlive());
        assertFalse(config.getNetworkConfig().getSocketOptions().isTcpNoDelay());
        assertEquals(3, config.getNetworkConfig().getSocketOptions().getLingerSeconds());
    }

    @Override
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

    @Override
    @Test
    public void testSecurityConfig_onlyFactory() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "  <security>\n"
                + "        <credentials-factory class-name=\"com.hazelcast.examples.MyCredentialsFactory\">\n"
                + "            <properties>\n"
                + "                <property name=\"property\">value</property>\n"
                + "            </properties>\n"
                + "        </credentials-factory>\n"
                + "    </security>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        ClientSecurityConfig securityConfig = config.getSecurityConfig();
        CredentialsFactoryConfig credentialsFactoryConfig = securityConfig.getCredentialsFactoryConfig();
        assertEquals("com.hazelcast.examples.MyCredentialsFactory", credentialsFactoryConfig.getClassName());
        Properties properties = credentialsFactoryConfig.getProperties();
        assertEquals("value", properties.getProperty("property"));
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

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastClientTagAppearsTwice() {
        String xml = HAZELCAST_CLIENT_START_TAG + "<hazelcast-client/><hazelcast-client/>";
        buildConfig(xml);
    }

    @Test
    public void testNearCacheDefaultName() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "  <near-cache>\n"
                + "    <in-memory-format>NATIVE</in-memory-format>\n"
                + "    <serialize-keys>false</serialize-keys>\n"
                + "  </near-cache>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig clientConfig = buildConfig(xml);
        NearCacheConfig ncConfig = clientConfig.getNearCacheConfig("default");

        assertEquals(InMemoryFormat.NATIVE, ncConfig.getInMemoryFormat());
        assertTrue(ncConfig.isSerializeKeys());
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testQueryCacheBothPredicateDefinedThrows() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<query-caches>"
                + "  <query-cache name=\"cache-name\" mapName=\"map-name\">"
                + "    <predicate type=\"class-name\">com.hazelcast.example.Predicate</predicate>"
                + "    <predicate type=\"sql\">%age=40</predicate>"
                + "  </query-cache>"
                + "</query-caches>"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testQueryCacheNoPredicateDefinedThrows() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<query-caches>"
                + "  <query-cache name=\"cache-name\" mapName=\"map-name\">"
                + "  </query-cache>"
                + "</query-caches>"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test
    public void testLoadBalancerRandom() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<load-balancer type=\"random\" />"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);

        assertInstanceOf(RandomLB.class, config.getLoadBalancer());
        assertNull(config.getLoadBalancerClassName());
    }

    @Override
    @Test
    public void testLoadBalancerRoundRobin() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<load-balancer type=\"round-robin\" />"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);

        assertInstanceOf(RoundRobinLB.class, config.getLoadBalancer());
        assertNull(config.getLoadBalancerClassName());
    }

    @Override
    @Test
    public void testLoadBalancerCustom() {
        String xml = HAZELCAST_CLIENT_START_TAG
                     + "<load-balancer type=\"custom\">com.hazelcast.client.test.CustomLoadBalancer</load-balancer>"
                     + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);

        assertNull(config.getLoadBalancer());
        assertEquals("com.hazelcast.client.test.CustomLoadBalancer", config.getLoadBalancerClassName());
    }

    @Override
    @Test
    public void testWhitespaceInNonSpaceStrings() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<load-balancer type=' \n random \n'/>"
                + HAZELCAST_CLIENT_END_TAG;
        buildConfig(xml);
    }

    @Override
    @Test
    public void testTokenIdentityConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<security>"
                + "  <token encoding='base64'>SGF6ZWxjYXN0</token>"
                + "</security>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        TokenIdentityConfig tokenIdentityConfig = config.getSecurityConfig().getTokenIdentityConfig();
        assertNotNull(tokenIdentityConfig);
        assertArrayEquals("Hazelcast".getBytes(StandardCharsets.US_ASCII), tokenIdentityConfig.getToken());
        assertEquals("SGF6ZWxjYXN0", tokenIdentityConfig.getTokenEncoded());
    }

    @Override
    @Test
    public void testKerberosIdentityConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<security>"
                + "    <kerberos>\n"
                + "        <realm>HAZELCAST.COM</realm>"
                + "        <principal>jduke</principal>"
                + "        <keytab-file>/opt/jduke.keytab</keytab-file>"
                + "        <security-realm>krb5Initiator</security-realm>"
                + "        <service-name-prefix>hz/</service-name-prefix>"
                + "        <use-canonical-hostname>true</use-canonical-hostname>"
                + "        <spn>hz/127.0.0.1@HAZELCAST.COM</spn>"
                + "    </kerberos>"
                + "</security>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        KerberosIdentityConfig identityConfig = config.getSecurityConfig().getKerberosIdentityConfig();
        assertNotNull(identityConfig);
        assertEquals("HAZELCAST.COM", identityConfig.getRealm());
        assertEquals("jduke", identityConfig.getPrincipal());
        assertEquals("/opt/jduke.keytab", identityConfig.getKeytabFile());
        assertEquals("krb5Initiator", identityConfig.getSecurityRealm());
        assertEquals("hz/", identityConfig.getServiceNamePrefix());
        assertTrue(identityConfig.getUseCanonicalHostname());
        assertEquals("hz/127.0.0.1@HAZELCAST.COM", identityConfig.getSpn());
    }

    @Override
    @Test
    public void testMetricsConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<metrics enabled=\"false\">"
                + "  <jmx enabled=\"false\" />"
                + "  <collection-frequency-seconds>10</collection-frequency-seconds>\n"
                + "</metrics>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
        assertEquals(10, metricsConfig.getCollectionFrequencySeconds());
    }

    @Override
    public void testInstanceTrackingConfig() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<instance-tracking enabled=\"true\">"
                + "  <file-name>/dummy/file</file-name>"
                + "  <format-pattern>dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}</format-pattern>"
                + "</instance-tracking>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        InstanceTrackingConfig trackingConfig = config.getInstanceTrackingConfig();
        assertTrue(trackingConfig.isEnabled());
        assertEquals("/dummy/file", trackingConfig.getFileName());
        assertEquals("dummy-pattern with $HZ_INSTANCE_TRACKING{placeholder} and $RND{placeholder}",
                trackingConfig.getFormatPattern());
    }

    @Override
    @Test
    public void testMetricsConfigMasterSwitchDisabled() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<metrics enabled=\"false\"/>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertTrue(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testMetricsConfigJmxDisabled() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<metrics>"
                + "  <jmx enabled=\"false\" />"
                + "</metrics>"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml);
        ClientMetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertFalse(metricsConfig.getJmxConfig().isEnabled());
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfiguration() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory>\n"
                + "    <directories>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem0</directory>\n"
                + "      <directory numa-node=\"1\">/mnt/pmem1</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig xmlConfig = buildConfig(xml);

        List<PersistentMemoryDirectoryConfig> directoryConfigs = xmlConfig.getNativeMemoryConfig()
                                                                          .getPersistentMemoryConfig()
                                                                          .getDirectoryConfigs();
        assertEquals(2, directoryConfigs.size());
        PersistentMemoryDirectoryConfig dir0Config = directoryConfigs.get(0);
        PersistentMemoryDirectoryConfig dir1Config = directoryConfigs.get(1);
        assertEquals("/mnt/pmem0", dir0Config.getDirectory());
        assertEquals(0, dir0Config.getNumaNode());
        assertEquals("/mnt/pmem1", dir1Config.getDirectory());
        assertEquals(1, dir1Config.getNumaNode());
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfigurationSimple() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory-directory>/mnt/pmem0</persistent-memory-directory>\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig xmlConfig = buildConfig(xml);

        PersistentMemoryConfig pmemConfig = xmlConfig.getNativeMemoryConfig().getPersistentMemoryConfig();
        assertTrue(pmemConfig.isEnabled());

        List<PersistentMemoryDirectoryConfig> directoryConfigs = pmemConfig.getDirectoryConfigs();
        assertEquals(1, directoryConfigs.size());
        PersistentMemoryDirectoryConfig dir0Config = directoryConfigs.get(0);
        assertEquals("/mnt/pmem0", dir0Config.getDirectory());
        assertFalse(dir0Config.isNumaNodeSet());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_uniqueDirViolationThrows() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory>\n"
                + "    <directories>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem0</directory>\n"
                + "      <directory numa-node=\"1\">/mnt/pmem0</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_uniqueNumaNodeViolationThrows() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory>\n"
                + "    <directories>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem0</directory>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem1</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_numaNodeConsistencyViolationThrows() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory>\n"
                + "    <directories>\n"
                + "      <directory numa-node=\"0\">/mnt/pmem0</directory>\n"
                + "      <directory>/mnt/pmem1</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test
    public void testPersistentMemoryDirectoryConfiguration_simpleAndAdvancedPasses() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory-directory>/mnt/optane</persistent-memory-directory>\n"
                + "  <persistent-memory enabled=\"false\">\n"
                + "    <directories>\n"
                + "      <directory>/mnt/pmem0</directory>\n"
                + "      <directory>/mnt/pmem1</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);
        PersistentMemoryConfig pmemConfig = config.getNativeMemoryConfig().getPersistentMemoryConfig();
        assertFalse(pmemConfig.isEnabled());
        assertEquals(MOUNTED, pmemConfig.getMode());

        List<PersistentMemoryDirectoryConfig> directoryConfigs = pmemConfig.getDirectoryConfigs();
        assertEquals(3, directoryConfigs.size());
        PersistentMemoryDirectoryConfig dir0Config = directoryConfigs.get(0);
        PersistentMemoryDirectoryConfig dir1Config = directoryConfigs.get(1);
        PersistentMemoryDirectoryConfig dir2Config = directoryConfigs.get(2);
        assertEquals("/mnt/optane", dir0Config.getDirectory());
        assertFalse(dir0Config.isNumaNodeSet());
        assertEquals("/mnt/pmem0", dir1Config.getDirectory());
        assertFalse(dir1Config.isNumaNodeSet());
        assertEquals("/mnt/pmem1", dir2Config.getDirectory());
        assertFalse(dir2Config.isNumaNodeSet());
    }

    @Override
    @Test
    public void testPersistentMemoryConfiguration_SystemMemoryMode() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory enabled=\"true\" mode=\"SYSTEM_MEMORY\" />\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);
        PersistentMemoryConfig pmemConfig = config.getNativeMemoryConfig().getPersistentMemoryConfig();
        assertTrue(pmemConfig.isEnabled());
        assertEquals(SYSTEM_MEMORY, pmemConfig.getMode());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryConfiguration_NotExistingModeThrows() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory mode=\"NOT_EXISTING_MODE\" />\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testPersistentMemoryDirectoryConfiguration_SystemMemoryModeThrows() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<native-memory>\n"
                + "  <persistent-memory mode=\"SYSTEM_MEMORY\">\n"
                + "    <directories>\n"
                + "      <directory>/mnt/pmem0</directory>\n"
                + "    </directories>\n"
                + "  </persistent-memory>\n"
                + "</native-memory>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    public void testCompactSerialization() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\" />\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);
        assertTrue(config.getSerializationConfig().getCompactSerializationConfig().isEnabled());
    }

    @Override
    public void testCompactSerialization_explicitSerializationRegistration() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class type-name=\"obj\" serializer=\"example.serialization.EmployeeDTOSerializer\">"
                + "                    example.serialization.EmployeeDTO\n"
                + "                </class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);
        CompactTestUtil.verifyExplicitSerializerIsUsed(config.getSerializationConfig());
    }

    @Override
    public void testCompactSerialization_reflectiveSerializerRegistration() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class>example.serialization.ExternalizableEmployeeDTO</class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);
        CompactTestUtil.verifyReflectiveSerializerIsUsed(config.getSerializationConfig());
    }

    @Override
    public void testCompactSerialization_registrationWithJustTypeName() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class type-name=\"employee\">example.serialization.EmployeeDTO</class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    public void testCompactSerialization_registrationWithJustSerializer() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <serialization>\n"
                + "        <compact-serialization enabled=\"true\">\n"
                + "            <registered-classes>\n"
                + "                <class serializer=\"example.serialization.EmployeeDTOSerializer\">\n"
                + "                    example.serialization.EmployeeDTO\n"
                + "                </class>\n"
                + "            </registered-classes>\n"
                + "        </compact-serialization>\n"
                + "    </serialization>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    static ClientConfig buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        if (properties != null) {
            configBuilder.setProperties(properties);
        }
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
