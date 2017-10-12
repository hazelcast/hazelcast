/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.config.helpers.DummyMapStore;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
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
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.PermissionConfig.PermissionType.CACHE;
import static com.hazelcast.config.PermissionConfig.PermissionType.CONFIG;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings({"WeakerAccess", "deprecation"})
public class XMLConfigBuilderTest extends HazelcastTestSupport {

    static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    static final String HAZELCAST_END_TAG = "</hazelcast>\n";

    static final String SECURITY_START_TAG = "<security enabled=\"true\">\n";
    static final String SECURITY_END_TAG = "</security>\n";
    static final String ACTIONS_FRAGMENT = "<actions>"
            + "<action>create</action>"
            + "<action>destroy</action>"
            + "<action>add</action>"
            + "<action>remove</action>"
            + "</actions>";

    @Test
    public void testConfigurationURL() throws Exception {
        URL configURL = getClass().getClassLoader().getResource("hazelcast-default.xml");
        Config config = new XmlConfigBuilder(configURL).build();
        assertEquals(configURL, config.getConfigurationUrl());
    }

    @Test
    public void testConfigurationWithFileName() throws Exception {
        File file = createTempFile("foo", "bar");
        file.deleteOnExit();

        String xml = HAZELCAST_START_TAG
                + "    <group>\n"
                + "        <name>foobar</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n"
                + HAZELCAST_END_TAG;
        Writer writer = new PrintWriter(file, "UTF-8");
        writer.write(xml);
        writer.close();

        String path = file.getAbsolutePath();
        Config config = new XmlConfigBuilder(path).build();
        assertEquals(path, config.getConfigurationFile().getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfiguration_withNullInputStream() {
        new XmlConfigBuilder((InputStream) null);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String xml = "<hazelcast-client>"
                + "<group>"
                + "<name>dev</name>"
                + "<password>clusterpass</password>"
                + "</group>"
                + "</hazelcast-client>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testJoinValidation() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"true\"/>\n"
                + "            <tcp-ip enabled=\"true\"/>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test
    public void testSecurityInterceptorConfig() {
        String xml = HAZELCAST_START_TAG
                + "<security enabled=\"true\">"
                + "  <security-interceptors>"
                + "    <interceptor class-name=\"foo\"/>"
                + "    <interceptor class-name=\"bar\"/>"
                + "  </security-interceptors>"
                + "</security>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SecurityConfig securityConfig = config.getSecurityConfig();
        List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();

        assertEquals(2, interceptorConfigs.size());
        assertEquals("foo", interceptorConfigs.get(0).className);
        assertEquals("bar", interceptorConfigs.get(1).className);
    }

    @Test
    public void readAwsConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <group>\n" +
                "        <name>dev</name>\n" +
                "        <password>dev-pass</password>\n" +
                "    </group>\n" +
                "    <network>\n" +
                "        <port auto-increment=\"true\">5701</port>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\">\n" +
                "                <multicast-group>224.2.2.3</multicast-group>\n" +
                "                <multicast-port>54327</multicast-port>\n" +
                "            </multicast>\n" +
                "            <tcp-ip enabled=\"false\">\n" +
                "                <interface>127.0.0.1</interface>\n" +
                "            </tcp-ip>\n" +
                "            <aws enabled=\"true\" connection-timeout-seconds=\"10\" >\n" +
                "                <access-key>sample-access-key</access-key>\n" +
                "                <secret-key>sample-secret-key</secret-key>\n" +
                "                <iam-role>sample-role</iam-role>\n" +
                "                <region>sample-region</region>\n" +
                "                <host-header>sample-header</host-header>\n" +
                "                <security-group-name>sample-group</security-group-name>\n" +
                "                <tag-key>sample-tag-key</tag-key>\n" +
                "                <tag-value>sample-tag-value</tag-value>\n" +
                "            </aws>\n" +
                "        </join>\n" +
                "        <interfaces enabled=\"false\">\n" +
                "            <interface>10.10.1.*</interface>\n" +
                "        </interfaces>\n" +
                "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        final AwsConfig aws = config.getNetworkConfig().getJoin().getAwsConfig();
        assertTrue(aws.isEnabled());
        assertAwsConfig(aws);
    }

    @Test
    public void readDiscoveryConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <group>\n" +
                "        <name>dev</name>\n" +
                "        <password>dev-pass</password>\n" +
                "    </group>\n" +
                "    <network>\n" +
                "        <port auto-increment=\"true\">5701</port>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\">\n" +
                "                <multicast-group>224.2.2.3</multicast-group>\n" +
                "                <multicast-port>54327</multicast-port>\n" +
                "            </multicast>\n" +
                "            <tcp-ip enabled=\"false\">\n" +
                "                <interface>127.0.0.1</interface>\n" +
                "            </tcp-ip>\n" +
                "            <discovery-strategies>\n" +
                "                <node-filter class=\"DummyFilterClass\" />\n" +
                "                <discovery-strategy class=\"DummyDiscoveryStrategy1\" enabled=\"true\">\n" +
                "                    <properties>\n" +
                "                        <property name=\"key-string\">foo</property>\n" +
                "                        <property name=\"key-int\">123</property>\n" +
                "                        <property name=\"key-boolean\">true</property>\n" +
                "                    </properties>\n" +
                "                </discovery-strategy>\n" +
                "            </discovery-strategies>\n" +
                "        </join>\n" +
                "        <interfaces enabled=\"false\">\n" +
                "            <interface>10.10.1.*</interface>\n" +
                "        </interfaces>\n" +
                "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        final DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        assertTrue(discoveryConfig.isEnabled());
        assertDiscoveryConfig(discoveryConfig);
    }

    @Test
    public void readPortCount() {
        // check when it is explicitly set
        Config config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <port port-count=\"200\">5701</port>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertEquals(200, config.getNetworkConfig().getPortCount());

        // check if the default is passed in correctly
        config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <port>5701</port>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertEquals(100, config.getNetworkConfig().getPortCount());
    }

    @Test
    public void readPortAutoIncrement() {
        // explicitly set
        Config config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <port auto-increment=\"false\">5701</port>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertFalse(config.getNetworkConfig().isPortAutoIncrement());

        // check if the default is picked up correctly
        config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <port>5701</port>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
    }

    @Test
    public void networkReuseAddress() {
        Config config = buildConfig(HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <reuse-address>true</reuse-address>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG);
        assertTrue(config.getNetworkConfig().isReuseAddress());
    }

    @Test
    public void readSemaphoreConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <semaphore name=\"default\">\n"
                + "        <initial-permits>1</initial-permits>\n"
                + "    </semaphore>"
                + "    <semaphore name=\"custom\">\n"
                + "        <initial-permits>10</initial-permits>\n"
                + "    </semaphore>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        SemaphoreConfig defaultConfig = config.getSemaphoreConfig("default");
        SemaphoreConfig customConfig = config.getSemaphoreConfig("custom");
        assertEquals(1, defaultConfig.getInitialPermits());
        assertEquals(10, customConfig.getInitialPermits());
    }

    @Test
    public void readQueueConfig() {
        final String xml = HAZELCAST_START_TAG
                + "      <queue name=\"custom\">" +
                "        <statistics-enabled>true</statistics-enabled>" +
                "        <max-size>100</max-size>" +
                "        <backup-count>1</backup-count>" +
                "        <async-backup-count>0</async-backup-count>" +
                "        <empty-queue-ttl>-1</empty-queue-ttl>" +
                "        <item-listeners>" +
                "            <item-listener>com.hazelcast.examples.ItemListener</item-listener>" +
                "        </item-listeners>" +
                "        <queue-store>" +
                "            <class-name>com.hazelcast.QueueStoreImpl</class-name>" +
                "            <properties>" +
                "                <property name=\"binary\">false</property>" +
                "                <property name=\"memory-limit\">1000</property>" +
                "                <property name=\"bulk-load\">500</property>" +
                "            </properties>" +
                "        </queue-store>" +
                "        <quorum-ref>customQuorumRule</quorum-ref>" +
                "    </queue>"
                + HAZELCAST_END_TAG;
        final Config config = buildConfig(xml);
        final QueueConfig qConfig = config.getQueueConfig("custom");
        assertTrue(qConfig.isStatisticsEnabled());
        assertEquals(100, qConfig.getMaxSize());
        assertEquals(1, qConfig.getBackupCount());
        assertEquals(0, qConfig.getAsyncBackupCount());
        assertEquals(-1, qConfig.getEmptyQueueTtl());

        assertTrue(qConfig.getItemListenerConfigs().size() == 1);
        final ItemListenerConfig listenerConfig = qConfig.getItemListenerConfigs().iterator().next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig.getClassName());

        final QueueStoreConfig storeConfig = qConfig.getQueueStoreConfig();
        assertNotNull(storeConfig);
        assertTrue(storeConfig.isEnabled());
        assertEquals("com.hazelcast.QueueStoreImpl", storeConfig.getClassName());
        final Properties storeConfigProperties = storeConfig.getProperties();
        assertEquals(3, storeConfigProperties.size());
        assertEquals("500", storeConfigProperties.getProperty("bulk-load"));
        assertEquals("1000", storeConfigProperties.getProperty("memory-limit"));
        assertEquals("false", storeConfigProperties.getProperty("binary"));

        assertEquals("customQuorumRule", qConfig.getQuorumName());
    }

    @Test
    public void readLockConfig() {
        final String xml = HAZELCAST_START_TAG
                + "  <lock name=\"default\">"
                + "        <quorum-ref>quorumRuleWithThreeNodes</quorum-ref>"
                + "    </lock>"
                + "  <lock name=\"custom\">"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "    </lock>"
                + HAZELCAST_END_TAG;
        final Config config = buildConfig(xml);
        final LockConfig defaultConfig = config.getLockConfig("default");
        final LockConfig customConfig = config.getLockConfig("custom");
        assertEquals("quorumRuleWithThreeNodes", defaultConfig.getQuorumName());
        assertEquals("customQuorumRule", customConfig.getQuorumName());
    }

    @Test
    public void readReliableTopic() {
        String xml = HAZELCAST_START_TAG
                + "    <reliable-topic name=\"custom\">"
                + "           <read-batch-size>35</read-batch-size>"
                + "           <statistics-enabled>false</statistics-enabled>"
                + "           <topic-overload-policy>DISCARD_OLDEST</topic-overload-policy>"
                + "           <message-listeners>"
                + "               <message-listener>MessageListenerImpl</message-listener>"
                + "           </message-listeners>"
                + "    </reliable-topic>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        ReliableTopicConfig topicConfig = config.getReliableTopicConfig("custom");

        assertEquals(35, topicConfig.getReadBatchSize());
        assertFalse(topicConfig.isStatisticsEnabled());
        assertEquals(TopicOverloadPolicy.DISCARD_OLDEST, topicConfig.getTopicOverloadPolicy());

        // checking listener configuration
        assertEquals(1, topicConfig.getMessageListenerConfigs().size());
        ListenerConfig listenerConfig = topicConfig.getMessageListenerConfigs().get(0);
        assertEquals("MessageListenerImpl", listenerConfig.getClassName());
        assertNull(listenerConfig.getImplementation());
    }

    @Test
    public void readRingbuffer() {
        String xml = HAZELCAST_START_TAG
                + "    <ringbuffer name=\"custom\">"
                + "        <capacity>10</capacity>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <time-to-live-seconds>9</time-to-live-seconds>"
                + "        <in-memory-format>OBJECT</in-memory-format>"
                + "        <ringbuffer-store>"
                + "            <class-name>com.hazelcast.RingbufferStoreImpl</class-name>"
                + "            <properties>"
                + "                <property name=\"store-path\">.//tmp//bufferstore</property>"
                + "            </properties>"
                + "        </ringbuffer-store>"
                + "    </ringbuffer>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        RingbufferConfig ringbufferConfig = config.getRingbufferConfig("custom");
        assertEquals(10, ringbufferConfig.getCapacity());
        assertEquals(2, ringbufferConfig.getBackupCount());
        assertEquals(1, ringbufferConfig.getAsyncBackupCount());
        assertEquals(9, ringbufferConfig.getTimeToLiveSeconds());
        assertEquals(InMemoryFormat.OBJECT, ringbufferConfig.getInMemoryFormat());
        RingbufferStoreConfig ringbufferStoreConfig = ringbufferConfig.getRingbufferStoreConfig();
        assertEquals("com.hazelcast.RingbufferStoreImpl", ringbufferStoreConfig.getClassName());
        Properties ringbufferStoreProperties = ringbufferStoreConfig.getProperties();
        assertEquals(".//tmp//bufferstore", ringbufferStoreProperties.get("store-path"));
    }

    @Test
    public void testConfig2Xml2DefaultConfig() {
        testConfig2Xml2Config("hazelcast-default.xml");
    }

    @Test
    public void testConfig2Xml2FullConfig() {
        testConfig2Xml2Config("hazelcast-fullconfig.xml");
    }

    private static void testConfig2Xml2Config(String fileName) {
        String pass = "password";
        Config config = new ClasspathXmlConfig(fileName);
        config.getGroupConfig().setPassword(pass);

        String xml = new ConfigXmlGenerator(true).generate(config);
        Config config2 = new InMemoryXmlConfig(xml);
        config2.getGroupConfig().setPassword(pass);

        assertTrue(ConfigCompatibilityChecker.isCompatible(config, config2));
    }

    @Test
    public void testXSDDefaultXML() throws Exception {
        testXSDConfigXML("hazelcast-default.xml");
    }

    @Test
    public void testFullConfigXML() throws Exception {
        testXSDConfigXML("hazelcast-fullconfig.xml");
    }

    @Test
    public void testCaseInsensitivityOfSettings() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"testCaseInsensitivity\">"
                + "    <in-memory-format>BINARY</in-memory-format>"
                + "    <backup-count>1</backup-count>"
                + "    <async-backup-count>0</async-backup-count>"
                + "    <time-to-live-seconds>0</time-to-live-seconds>"
                + "    <max-idle-seconds>0</max-idle-seconds>    "
                + "    <eviction-policy>NONE</eviction-policy>  "
                + "    <max-size policy=\"per_partition\">0</max-size>"
                + "    <eviction-percentage>25</eviction-percentage>"
                + "    <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("testCaseInsensitivity");

        assertTrue(mapConfig.getInMemoryFormat().equals(InMemoryFormat.BINARY));
        assertTrue(mapConfig.getEvictionPolicy().equals(EvictionPolicy.NONE));
        assertTrue(mapConfig.getMaxSizeConfig().getMaxSizePolicy().equals(MaxSizeConfig.MaxSizePolicy.PER_PARTITION));
    }

    @Test
    public void testManagementCenterConfig() {
        String xml = HAZELCAST_START_TAG
                + "<management-center enabled=\"true\">"
                + "someUrl"
                + "</management-center>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertTrue(manCenterCfg.isEnabled());
        assertEquals("someUrl", manCenterCfg.getUrl());
    }

    @Test
    public void testManagementCenterConfigComplex() {
        String xml = HAZELCAST_START_TAG
                + "<management-center enabled=\"true\">"
                + "<url>wowUrl</url>"
                + "<mutual-auth enabled=\"true\">"
                + "<properties>"
                + "<property name=\"keyStore\">/tmp/foo_keystore</property>"
                + "<property name=\"trustStore\">/tmp/foo_truststore</property>"
                + "</properties>"
                + "</mutual-auth>"
                + "</management-center>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertTrue(manCenterCfg.isEnabled());
        assertEquals("wowUrl", manCenterCfg.getUrl());
        assertTrue(manCenterCfg.getMutualAuthConfig().isEnabled());
        assertEquals("/tmp/foo_keystore", manCenterCfg.getMutualAuthConfig().getProperty("keyStore"));
        assertEquals("/tmp/foo_truststore", manCenterCfg.getMutualAuthConfig().getProperty("trustStore"));
    }

    @Test
    public void testNullManagementCenterConfig() {
        String xml = HAZELCAST_START_TAG
                + "<management-center>"
                + "</management-center>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Test
    public void testEmptyManagementCenterConfig() {
        String xml = HAZELCAST_START_TAG + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Test
    public void testNotEnabledManagementCenterConfig() {
        String xml = HAZELCAST_START_TAG
                + "<management-center enabled=\"false\">"
                + "</management-center>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Test
    public void testNotEnabledWithURLManagementCenterConfig() {
        String xml = HAZELCAST_START_TAG
                + "<management-center enabled=\"false\">"
                + "http://localhost:8080/mancenter"
                + "</management-center>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertFalse(manCenterCfg.isEnabled());
        assertEquals("http://localhost:8080/mancenter", manCenterCfg.getUrl());
    }

    @Test
    public void testManagementCenterConfigComplexDisabledMutualAuth() {
        String xml = HAZELCAST_START_TAG
                + "<management-center enabled=\"true\">"
                + "<url>wowUrl</url>"
                + "<mutual-auth enabled=\"false\">"
                + "</mutual-auth>"
                + "</management-center>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();

        assertTrue(manCenterCfg.isEnabled());
        assertEquals("wowUrl", manCenterCfg.getUrl());
        assertFalse(manCenterCfg.getMutualAuthConfig().isEnabled());
    }

    @Test
    public void testMapStoreInitialModeLazy() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store enabled=\"true\" initial-mode=\"LAZY\"></map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, mapStoreConfig.getInitialLoadMode());
    }

    @Test
    public void testMapConfig_minEvictionCheckMillis() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<min-eviction-check-millis>123456789</min-eviction-check-millis>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(123456789L, mapConfig.getMinEvictionCheckMillis());
    }

    @Test
    public void testMapConfig_minEvictionCheckMillis_defaultValue() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS, mapConfig.getMinEvictionCheckMillis());
    }

    @Test
    public void testMapConfig_evictions() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"lruMap\">" +
                "        <eviction-policy>LRU</eviction-policy>\n" +
                "    </map>\n"
                + "<map name=\"lfuMap\">" +
                "        <eviction-policy>LFU</eviction-policy>\n" +
                "    </map>\n"
                + "<map name=\"noneMap\">" +
                "        <eviction-policy>NONE</eviction-policy>\n" +
                "    </map>\n"
                + "<map name=\"randomMap\">" +
                "        <eviction-policy>RANDOM</eviction-policy>\n" +
                "    </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        assertEquals(EvictionPolicy.LRU, config.getMapConfig("lruMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.LFU, config.getMapConfig("lfuMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.NONE, config.getMapConfig("noneMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.RANDOM, config.getMapConfig("randomMap").getEvictionPolicy());
    }

    @Test
    public void testMapConfig_optimizeQueries() {
        String xml1 = HAZELCAST_START_TAG
                + "<map name=\"mymap1\">"
                + "<optimize-queries>true</optimize-queries>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config1 = buildConfig(xml1);
        MapConfig mapConfig1 = config1.getMapConfig("mymap1");
        assertEquals(CacheDeserializedValues.ALWAYS, mapConfig1.getCacheDeserializedValues());

        String xml2 = HAZELCAST_START_TAG
                + "<map name=\"mymap2\">"
                + "<optimize-queries>false</optimize-queries>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config2 = buildConfig(xml2);
        MapConfig mapConfig2 = config2.getMapConfig("mymap2");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig2.getCacheDeserializedValues());
    }

    @Test
    public void testMapConfig_cacheValueConfig_defaultValue() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Test
    public void testMapConfig_cacheValueConfig_never() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<cache-deserialized-values>NEVER</cache-deserialized-values>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.NEVER, mapConfig.getCacheDeserializedValues());
    }

    @Test
    public void testMapConfig_cacheValueConfig_always() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<cache-deserialized-values>ALWAYS</cache-deserialized-values>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.ALWAYS, mapConfig.getCacheDeserializedValues());
    }

    @Test
    public void testMapConfig_cacheValueConfig_indexOnly() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<cache-deserialized-values>INDEX-ONLY</cache-deserialized-values>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("mymap");

        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Test
    public void testMapStoreInitialModeEager() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store enabled=\"true\" initial-mode=\"EAGER\"></map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.EAGER, mapStoreConfig.getInitialLoadMode());
    }

    @Test
    public void testMapStoreWriteBatchSize() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store >"
                + "<write-batch-size>23</write-batch-size>"
                + "</map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();

        assertEquals(23, mapStoreConfig.getWriteBatchSize());
    }

    @Test
    public void testMapStoreConfig_writeCoalescing_whenDefault() {
        MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(MapStoreConfig.DEFAULT_WRITE_COALESCING, true);

        assertTrue(mapStoreConfig.isWriteCoalescing());
    }

    @Test
    public void testMapStoreConfig_writeCoalescing_whenSetFalse() {
        MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(false, false);

        assertFalse(mapStoreConfig.isWriteCoalescing());
    }

    @Test
    public void testMapStoreConfig_writeCoalescing_whenSetTrue() {
        MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(true, false);

        assertTrue(mapStoreConfig.isWriteCoalescing());
    }

    private MapStoreConfig getWriteCoalescingMapStoreConfig(boolean writeCoalescing, boolean useDefault) {
        String xml = getWriteCoalescingConfigXml(writeCoalescing, useDefault);
        Config config = buildConfig(xml);
        return config.getMapConfig("mymap").getMapStoreConfig();
    }

    private String getWriteCoalescingConfigXml(boolean value, boolean useDefault) {
        return HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<map-store >"
                + (useDefault ? "" : "<write-coalescing>" + String.valueOf(value) + "</write-coalescing>")
                + "</map-store>"
                + "</map>"
                + HAZELCAST_END_TAG;
    }

    @Test
    public void testNearCacheInMemoryFormat() {
        String mapName = "testMapNearCacheInMemoryFormat";
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"" + mapName + "\">\n"
                + "    <near-cache>\n"
                + "      <in-memory-format>OBJECT</in-memory-format>\n"
                + "    </near-cache>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig ncConfig = mapConfig.getNearCacheConfig();

        assertEquals(InMemoryFormat.OBJECT, ncConfig.getInMemoryFormat());
    }

    @Test
    public void testNearCacheInMemoryFormatNative_withKeysByReference() {
        String mapName = "testMapNearCacheInMemoryFormatNative";
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"" + mapName + "\">\n"
                + "    <near-cache>\n"
                + "      <in-memory-format>NATIVE</in-memory-format>\n"
                + "      <serialize-keys>false</serialize-keys>\n"
                + "    </near-cache>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig ncConfig = mapConfig.getNearCacheConfig();

        assertEquals(InMemoryFormat.NATIVE, ncConfig.getInMemoryFormat());
        assertTrue(ncConfig.isSerializeKeys());
    }

    @Test
    public void testNearCacheEvictionPolicy() {
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"lfuNearCache\">"
                + "    <near-cache>"
                + "      <eviction eviction-policy=\"LFU\"/>"
                + "    </near-cache>"
                + "  </map>"
                + "  <map name=\"lruNearCache\">"
                + "    <near-cache>"
                + "      <eviction eviction-policy=\"LRU\"/>"
                + "    </near-cache>"
                + "  </map>"
                + "  <map name=\"noneNearCache\">"
                + "    <near-cache>"
                + "      <eviction eviction-policy=\"NONE\"/>"
                + "    </near-cache>"
                + "  </map>"
                + "  <map name=\"randomNearCache\">"
                + "    <near-cache>"
                + "      <eviction eviction-policy=\"RANDOM\"/>"
                + "    </near-cache>"
                + "  </map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertEquals(EvictionPolicy.LFU, getNearCacheEvictionPolicy("lfuNearCache", config));
        assertEquals(EvictionPolicy.LRU, getNearCacheEvictionPolicy("lruNearCache", config));
        assertEquals(EvictionPolicy.NONE, getNearCacheEvictionPolicy("noneNearCache", config));
        assertEquals(EvictionPolicy.RANDOM, getNearCacheEvictionPolicy("randomNearCache", config));
    }

    private EvictionPolicy getNearCacheEvictionPolicy(String mapName, Config config) {
        return config.getMapConfig(mapName).getNearCacheConfig().getEvictionConfig().getEvictionPolicy();
    }

    @Test
    public void testPartitionGroupZoneAware() {
        String xml = HAZELCAST_START_TAG +
                "<partition-group enabled=\"true\" group-type=\"ZONE_AWARE\" />"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertEquals(config.getPartitionGroupConfig().getGroupType(), PartitionGroupConfig.MemberGroupType.ZONE_AWARE);
    }

    @Test
    public void testPartitionGroupSPI() {
        String xml = HAZELCAST_START_TAG +
                "<partition-group enabled=\"true\" group-type=\"SPI\" />"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertEquals(config.getPartitionGroupConfig().getGroupType(), PartitionGroupConfig.MemberGroupType.SPI);
    }

    @Test
    public void testNearCacheFullConfig() {
        String mapName = "testNearCacheFullConfig";
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"" + mapName + "\">\n"
                + "    <near-cache name=\"test\">\n"
                + "      <in-memory-format>OBJECT</in-memory-format>\n"
                + "      <serialize-keys>false</serialize-keys>\n"
                + "      <max-size>1234</max-size>\n"
                + "      <time-to-live-seconds>77</time-to-live-seconds>\n"
                + "      <max-idle-seconds>92</max-idle-seconds>\n"
                + "      <eviction-policy>LFU</eviction-policy>\n"
                + "      <invalidate-on-change>false</invalidate-on-change>\n"
                + "      <cache-local-entries>false</cache-local-entries>\n"
                + "      <eviction eviction-policy=\"LRU\" max-size-policy=\"ENTRY_COUNT\" size=\"3333\"/>\n"
                + "    </near-cache>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();

        assertEquals(InMemoryFormat.OBJECT, nearCacheConfig.getInMemoryFormat());
        assertEquals(1234, nearCacheConfig.getMaxSize());
        assertEquals(77, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(92, nearCacheConfig.getMaxIdleSeconds());
        assertEquals("LFU", nearCacheConfig.getEvictionPolicy());
        assertFalse(nearCacheConfig.isInvalidateOnChange());
        assertFalse(nearCacheConfig.isCacheLocalEntries());
        assertEquals(LRU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(ENTRY_COUNT, nearCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(3333, nearCacheConfig.getEvictionConfig().getSize());
        assertEquals("test", nearCacheConfig.getName());
    }

    @Test
    public void testMapWanReplicationRef() {
        String mapName = "testMapWanReplicationRef";
        String refName = "test";
        String mergePolicy = "TestMergePolicy";
        String xml = HAZELCAST_START_TAG
                + "  <map name=\"" + mapName + "\">\n"
                + "    <wan-replication-ref name=\"test\">\n"
                + "      <merge-policy>TestMergePolicy</merge-policy>\n"
                + "      <filters>\n"
                + "        <filter-impl>com.example.SampleFilter</filter-impl>\n"
                + "      </filters>\n"
                + "    </wan-replication-ref>\n"
                + "  </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        WanReplicationRef wanRef = config.getMapConfig(mapName).getWanReplicationRef();

        assertEquals(refName, wanRef.getName());
        assertEquals(mergePolicy, wanRef.getMergePolicy());
        assertTrue(wanRef.isRepublishingEnabled());
        assertEquals(1, wanRef.getFilters().size());
        assertEquals("com.example.SampleFilter", wanRef.getFilters().get(0));
    }

    @Test
    public void testMapEventJournalConfig() {
        final String journalName = "mapName";
        final String xml = HAZELCAST_START_TAG
                + "  <event-journal enabled=\"true\">\n" +
                "        <mapName>" + journalName + "</mapName>\n" +
                "        <capacity>120</capacity>\n" +
                "        <time-to-live-seconds>20</time-to-live-seconds>\n" +
                "    </event-journal>"
                + HAZELCAST_END_TAG;

        final Config config = buildConfig(xml);
        final EventJournalConfig journalConfig = config.getMapEventJournalConfig(journalName);

        assertTrue(journalConfig.isEnabled());
        assertEquals(120, journalConfig.getCapacity());
        assertEquals(20, journalConfig.getTimeToLiveSeconds());
    }

    @Test
    public void testCacheEventJournalConfig() {
        final String journalName = "cacheName";
        final String xml = HAZELCAST_START_TAG
                + "  <event-journal enabled=\"true\">\n" +
                "        <cacheName>" + journalName + "</cacheName>\n" +
                "        <capacity>120</capacity>\n" +
                "        <time-to-live-seconds>20</time-to-live-seconds>\n" +
                "    </event-journal>"
                + HAZELCAST_END_TAG;

        final Config config = buildConfig(xml);
        final EventJournalConfig journalConfig = config.getCacheEventJournalConfig(journalName);

        assertTrue(journalConfig.isEnabled());
        assertEquals(120, journalConfig.getCapacity());
        assertEquals(20, journalConfig.getTimeToLiveSeconds());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testParseExceptionIsNotSwallowed() {
        String invalidXml = HAZELCAST_START_TAG + "</hazelcast";
        buildConfig(invalidXml);

        // if we (for any reason) get through the parsing, then fail
        fail();
    }

    @Test
    public void setMapStoreConfigImplementationTest() {
        String mapName = "mapStoreImpObjTest";
        String xml = HAZELCAST_START_TAG
                + "<map name=\"" + mapName + "\">\n"
                + "<map-store enabled=\"true\">\n"
                + "<class-name>com.hazelcast.config.helpers.DummyMapStore</class-name>\n"
                + "<write-delay-seconds>5</write-delay-seconds>\n"
                + "</map-store>\n"
                + "</map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<String, String> map = hz.getMap(mapName);
        // MapStore is not instantiated until the MapContainer is created lazily
        map.put("sample", "data");

        MapConfig mapConfig = hz.getConfig().getMapConfig(mapName);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        Object o = mapStoreConfig.getImplementation();

        assertNotNull(o);
        assertTrue(o instanceof DummyMapStore);
    }

    @Test
    public void testMapPartitionLostListenerConfig() {
        String mapName = "map1";
        String listenerName = "DummyMapPartitionLostListenerImpl";
        String xml = createMapPartitionLostListenerConfiguredXml(mapName, listenerName);

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("map1");
        assertMapPartitionLostListener(listenerName, mapConfig);
    }

    @Test
    public void testMapPartitionLostListenerConfigReadOnly() {
        String mapName = "map1";
        String listenerName = "DummyMapPartitionLostListenerImpl";
        String xml = createMapPartitionLostListenerConfiguredXml(mapName, listenerName);

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.findMapConfig("map1");
        assertMapPartitionLostListener(listenerName, mapConfig);
    }

    private void assertMapPartitionLostListener(String listenerName, MapConfig mapConfig) {
        assertFalse(mapConfig.getPartitionLostListenerConfigs().isEmpty());
        assertEquals(listenerName, mapConfig.getPartitionLostListenerConfigs().get(0).getClassName());
    }

    private String createMapPartitionLostListenerConfiguredXml(String mapName, String listenerName) {
        return HAZELCAST_START_TAG
                + "<map name=\"" + mapName + "\">\n"
                + "<partition-lost-listeners>\n"
                + "<partition-lost-listener>" + listenerName + "</partition-lost-listener>\n"
                + "</partition-lost-listeners>\n"
                + "</map>\n"
                + HAZELCAST_END_TAG;
    }

    @Test
    public void testCachePartitionLostListenerConfig() {
        String cacheName = "cache1";
        String listenerName = "DummyCachePartitionLostListenerImpl";
        String xml = createCachePartitionLostListenerConfiguredXml(cacheName, listenerName);

        Config config = buildConfig(xml);
        CacheSimpleConfig cacheConfig = config.getCacheConfig("cache1");
        assertCachePartitionLostListener(listenerName, cacheConfig);
    }

    @Test
    public void testCachePartitionLostListenerConfigReadOnly() {
        String cacheName = "cache1";
        String listenerName = "DummyCachePartitionLostListenerImpl";
        String xml = createCachePartitionLostListenerConfiguredXml(cacheName, listenerName);

        Config config = buildConfig(xml);
        CacheSimpleConfig cacheConfig = config.findCacheConfig("cache1");
        assertCachePartitionLostListener(listenerName, cacheConfig);
    }

    private void assertCachePartitionLostListener(String listenerName, CacheSimpleConfig cacheConfig) {
        assertFalse(cacheConfig.getPartitionLostListenerConfigs().isEmpty());
        assertEquals(listenerName, cacheConfig.getPartitionLostListenerConfigs().get(0).getClassName());
    }

    private String createCachePartitionLostListenerConfiguredXml(String cacheName, String listenerName) {
        return HAZELCAST_START_TAG
                + "<cache name=\"" + cacheName + "\">\n"
                + "<partition-lost-listeners>\n"
                + "<partition-lost-listener>" + listenerName + "</partition-lost-listener>\n"
                + "</partition-lost-listeners>\n"
                + "</cache>\n"
                + HAZELCAST_END_TAG;
    }

    private void testXSDConfigXML(String xmlFileName) throws Exception {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-config-3.9.xsd");
        assertNotNull(schemaResource);

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

    private Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    @Test
    public void readMulticastConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <group>\n"
                + "        <name>dev</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n"
                + "    <network>\n"
                + "        <port auto-increment=\"true\">5701</port>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"true\" loopbackModeEnabled=\"true\">\n"
                + "                <multicast-group>224.2.2.3</multicast-group>\n"
                + "                <multicast-port>54327</multicast-port>\n"
                + "            </multicast>\n"
                + "            <tcp-ip enabled=\"false\">\n"
                + "                <interface>127.0.0.1</interface>\n"
                + "            </tcp-ip>\n"
                + "            <aws enabled=\"false\" connection-timeout-seconds=\"10\" >\n"
                + "                <access-key>access</access-key>\n"
                + "                <secret-key>secret</secret-key>\n"
                + "            </aws>\n"
                + "        </join>\n"
                + "        <interfaces enabled=\"false\">\n"
                + "            <interface>10.10.1.*</interface>\n"
                + "        </interfaces>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();

        assertTrue(multicastConfig.isEnabled());
        assertTrue(multicastConfig.isLoopbackModeEnabled());
        assertEquals("224.2.2.3", multicastConfig.getMulticastGroup());
        assertEquals(54327, multicastConfig.getMulticastPort());
    }

    @Test
    public void testWanConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <wan-replication name=\"my-wan-cluster\">\n" +
                "      <wan-publisher group-name=\"istanbul\">\n" +
                "         <class-name>com.hazelcast.wan.custom.WanPublisher</class-name>\n" +
                "         <queue-full-behavior>THROW_EXCEPTION</queue-full-behavior>\n" +
                "         <queue-capacity>21</queue-capacity>\n" +
                "         <aws enabled=\"false\" connection-timeout-seconds=\"10\" >\n" +
                "            <access-key>sample-access-key</access-key>\n" +
                "            <secret-key>sample-secret-key</secret-key>\n" +
                "            <iam-role>sample-role</iam-role>\n" +
                "            <region>sample-region</region>\n" +
                "            <host-header>sample-header</host-header>\n" +
                "            <security-group-name>sample-group</security-group-name>\n" +
                "            <tag-key>sample-tag-key</tag-key>\n" +
                "            <tag-value>sample-tag-value</tag-value>\n" +
                "         </aws>\n" +
                "         <discovery-strategies>\n" +
                "            <node-filter class=\"DummyFilterClass\" />\n" +
                "            <discovery-strategy class=\"DummyDiscoveryStrategy1\" enabled=\"true\">\n" +
                "               <properties>\n" +
                "                  <property name=\"key-string\">foo</property>\n" +
                "                  <property name=\"key-int\">123</property>\n" +
                "                  <property name=\"key-boolean\">true</property>\n" +
                "               </properties>\n" +
                "            </discovery-strategy>\n" +
                "         </discovery-strategies>\n" +
                "         <properties>\n" +
                "            <property name=\"custom.prop.publisher\">prop.publisher</property>\n" +
                "            <property name=\"discovery.period\">5</property>\n" +
                "            <property name=\"maxEndpoints\">2</property>\n" +
                "         </properties>\n" +
                "      </wan-publisher>\n" +
                "      <wan-publisher group-name=\"ankara\">\n" +
                "         <class-name>com.hazelcast.wan.custom.WanPublisher</class-name>\n" +
                "         <queue-full-behavior>THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE</queue-full-behavior>\n" +
                "      </wan-publisher>\n" +
                "      <wan-consumer>\n" +
                "         <class-name>com.hazelcast.wan.custom.WanConsumer</class-name>\n" +
                "         <properties>\n" +
                "            <property name=\"custom.prop.consumer\">prop.consumer</property>\n" +
                "         </properties>\n" +
                "      </wan-consumer>\n" +
                "   </wan-replication>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        WanReplicationConfig wanConfig = config.getWanReplicationConfig("my-wan-cluster");
        assertNotNull(wanConfig);

        List<WanPublisherConfig> publisherConfigs = wanConfig.getWanPublisherConfigs();
        assertEquals(2, publisherConfigs.size());
        WanPublisherConfig publisherConfig1 = publisherConfigs.get(0);
        assertEquals("istanbul", publisherConfig1.getGroupName());
        assertEquals("com.hazelcast.wan.custom.WanPublisher", publisherConfig1.getClassName());
        assertEquals(WANQueueFullBehavior.THROW_EXCEPTION, publisherConfig1.getQueueFullBehavior());
        assertEquals(21, publisherConfig1.getQueueCapacity());
        Map<String, Comparable> pubProperties = publisherConfig1.getProperties();
        assertEquals("prop.publisher", pubProperties.get("custom.prop.publisher"));
        assertEquals("5", pubProperties.get("discovery.period"));
        assertEquals("2", pubProperties.get("maxEndpoints"));
        assertFalse(publisherConfig1.getAwsConfig().isEnabled());
        assertAwsConfig(publisherConfig1.getAwsConfig());
        assertDiscoveryConfig(publisherConfig1.getDiscoveryConfig());

        WanPublisherConfig publisherConfig2 = publisherConfigs.get(1);
        assertEquals("ankara", publisherConfig2.getGroupName());
        assertEquals(WANQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE, publisherConfig2.getQueueFullBehavior());

        WanConsumerConfig consumerConfig = wanConfig.getWanConsumerConfig();
        assertEquals("com.hazelcast.wan.custom.WanConsumer", consumerConfig.getClassName());
        Map<String, Comparable> consProperties = consumerConfig.getProperties();
        assertEquals("prop.consumer", consProperties.get("custom.prop.consumer"));
    }

    private void assertDiscoveryConfig(DiscoveryConfig c) {
        assertEquals("DummyFilterClass", c.getNodeFilterClass());
        assertEquals(1, c.getDiscoveryStrategyConfigs().size());
        final DiscoveryStrategyConfig config = c.getDiscoveryStrategyConfigs().iterator().next();
        assertEquals("DummyDiscoveryStrategy1", config.getClassName());
        final Map<String, Comparable> props = config.getProperties();
        assertEquals("foo", props.get("key-string"));
        assertEquals("123", props.get("key-int"));
        assertEquals("true", props.get("key-boolean"));
    }

    private void assertAwsConfig(AwsConfig aws) {
        assertEquals(10, aws.getConnectionTimeoutSeconds());
        assertEquals("sample-access-key", aws.getAccessKey());
        assertEquals("sample-secret-key", aws.getSecretKey());
        assertEquals("sample-region", aws.getRegion());
        assertEquals("sample-header", aws.getHostHeader());
        assertEquals("sample-group", aws.getSecurityGroupName());
        assertEquals("sample-tag-key", aws.getTagKey());
        assertEquals("sample-tag-value", aws.getTagValue());
        assertEquals("sample-role", aws.getIamRole());
    }

    @Test
    public void testQuorumConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <quorum-function-class-name>com.my.quorum.function</quorum-function-class-name>\n"
                + "        <quorum-type>READ</quorum-type>\n"
                + "      </quorum>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");

        assertTrue("quorum should be enabled", quorumConfig.isEnabled());
        assertEquals(3, quorumConfig.getSize());
        assertEquals(QuorumType.READ, quorumConfig.getType());
        assertEquals("com.my.quorum.function", quorumConfig.getQuorumFunctionClassName());
        assertTrue(quorumConfig.getListenerConfigs().isEmpty());
    }

    @Test
    public void testQuorumListenerConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <quorum-listeners>"
                + "           <quorum-listener>com.abc.my.quorum.listener</quorum-listener>"
                + "           <quorum-listener>com.abc.my.second.listener</quorum-listener>"
                + "       </quorum-listeners> "
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");

        assertFalse(quorumConfig.getListenerConfigs().isEmpty());
        assertEquals("com.abc.my.quorum.listener", quorumConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("com.abc.my.second.listener", quorumConfig.getListenerConfigs().get(1).getClassName());
    }


    @Test
    public void testDurableExecutorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <durable-executor-service name=\"foobar\">\n" +
                "        <pool-size>2</pool-size>\n" +
                "        <durability>3</durability>\n" +
                "        <capacity>4</capacity>\n" +
                "    </durable-executor-service>"
                + HAZELCAST_END_TAG;

        final Config config = buildConfig(xml);
        final DurableExecutorConfig durableExecutorConfig = config.getDurableExecutorConfig("foobar");

        assertFalse(config.getDurableExecutorConfigs().isEmpty());
        assertEquals(2, durableExecutorConfig.getPoolSize());
        assertEquals(3, durableExecutorConfig.getDurability());
        assertEquals(4, durableExecutorConfig.getCapacity());
    }

    @Test
    public void testScheduledExecutorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <scheduled-executor-service name=\"foobar\">\n"
                + "        <durability>4</durability>\n"
                + "        <pool-size>5</pool-size>\n"
                + "        <capacity>2</capacity>\n"
                + "    </scheduled-executor-service>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ScheduledExecutorConfig scheduledExecutorConfig = config.getScheduledExecutorConfig("foobar");

        assertFalse(config.getScheduledExecutorConfigs().isEmpty());
        assertEquals(4, scheduledExecutorConfig.getDurability());
        assertEquals(5, scheduledExecutorConfig.getPoolSize());
        assertEquals(2, scheduledExecutorConfig.getCapacity());
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <cardinality-estimator name=\"foobar\">\n"
                + "        <backup-count>2</backup-count>\n"
                + "        <async-backup-count>3</async-backup-count>\n"
                + "    </cardinality-estimator>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        CardinalityEstimatorConfig cardinalityEstimatorConfig = config.getCardinalityEstimatorConfig("foobar");

        assertFalse(config.getCardinalityEstimatorConfigs().isEmpty());
        assertEquals(2, cardinalityEstimatorConfig.getBackupCount());
        assertEquals(3, cardinalityEstimatorConfig.getAsyncBackupCount());
    }

    @Test
    public void testIndexesConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <indexes>\n"
                + "           <index ordered=\"false\">name</index>\n"
                + "           <index ordered=\"true\">age</index>\n"
                + "       </indexes>"
                + "   </map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("people");

        assertFalse(mapConfig.getMapIndexConfigs().isEmpty());
        assertIndexEqual("name", false, mapConfig.getMapIndexConfigs().get(0));
        assertIndexEqual("age", true, mapConfig.getMapIndexConfigs().get(1));
    }

    private static void assertIndexEqual(String expectedAttribute, boolean expectedOrdered, MapIndexConfig indexConfig) {
        assertEquals(expectedAttribute, indexConfig.getAttribute());
        assertEquals(expectedOrdered, indexConfig.isOrdered());
    }

    @Test
    public void testAttributeConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute extractor=\"com.car.PowerExtractor\">power</attribute>\n"
                + "           <attribute extractor=\"com.car.WeightExtractor\">weight</attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("people");

        assertFalse(mapConfig.getMapAttributeConfigs().isEmpty());
        assertAttributeEqual("power", "com.car.PowerExtractor", mapConfig.getMapAttributeConfigs().get(0));
        assertAttributeEqual("weight", "com.car.WeightExtractor", mapConfig.getMapAttributeConfigs().get(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_emptyTag() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute extractor=\"com.car.WeightExtractor\"></attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    private static void assertAttributeEqual(String expectedName, String expectedExtractor, MapAttributeConfig attributeConfig) {
        assertEquals(expectedName, attributeConfig.getName());
        assertEquals(expectedExtractor, attributeConfig.getExtractor());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_singleTag() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute extractor=\"com.car.WeightExtractor\"/>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_noExtractor() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute></attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_noExtractor_singleTag() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute/>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noExtractor() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute>weight</attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_emptyExtractor() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute extractor=\"\">weight</attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test
    public void testQueryCacheFullConfig() {
        String xml = HAZELCAST_START_TAG
                + "<map name=\"test\">"
                + "<query-caches>"
                + "<query-cache name=\"cache-name\">"
                + "<entry-listeners>"
                + "<entry-listener include-value=\"true\" local=\"false\">com.hazelcast.examples.EntryListener</entry-listener>"
                + "</entry-listeners>"
                + "<include-value>true</include-value>"
                + "<batch-size>1</batch-size>"
                + "<buffer-size>16</buffer-size>"
                + "<delay-seconds>0</delay-seconds>"
                + "<in-memory-format>BINARY</in-memory-format>"
                + "<coalesce>false</coalesce>"
                + "<populate>true</populate>"
                + "<indexes>"
                + "<index ordered=\"false\">name</index>"
                + "</indexes>"
                + "<predicate type=\"class-name\"> "
                + "com.hazelcast.examples.SimplePredicate"
                + "</predicate>"
                + "<eviction eviction-policy=\"LRU\" max-size-policy=\"ENTRY_COUNT\" size=\"133\"/>"
                + "</query-cache>"
                + "</query-caches>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QueryCacheConfig queryCacheConfig = config.getMapConfig("test").getQueryCacheConfigs().get(0);
        EntryListenerConfig entryListenerConfig = queryCacheConfig.getEntryListenerConfigs().get(0);

        assertEquals("cache-name", queryCacheConfig.getName());
        assertTrue(entryListenerConfig.isIncludeValue());
        assertFalse(entryListenerConfig.isLocal());
        assertEquals("com.hazelcast.examples.EntryListener", entryListenerConfig.getClassName());
        assertTrue(queryCacheConfig.isIncludeValue());
        assertEquals(1, queryCacheConfig.getBatchSize());
        assertEquals(16, queryCacheConfig.getBufferSize());
        assertEquals(0, queryCacheConfig.getDelaySeconds());
        assertEquals(InMemoryFormat.BINARY, queryCacheConfig.getInMemoryFormat());
        assertFalse(queryCacheConfig.isCoalesce());
        assertTrue(queryCacheConfig.isPopulate());
        assertIndexesEqual(queryCacheConfig);
        assertEquals("com.hazelcast.examples.SimplePredicate", queryCacheConfig.getPredicateConfig().getClassName());
        assertEquals(LRU, queryCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(ENTRY_COUNT, queryCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(133, queryCacheConfig.getEvictionConfig().getSize());
    }

    @Test
    public void testLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member enabled=\"true\"/>\n"
                + HAZELCAST_END_TAG;

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();

        assertTrue(config.isLiteMember());
    }

    @Test
    public void testNonLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member enabled=\"false\"/>\n"
                + HAZELCAST_END_TAG;

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();

        assertFalse(config.isLiteMember());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNonLiteMemberConfigWithoutEnabledField() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member/>\n"
                + HAZELCAST_END_TAG;

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member enabled=\"dummytext\"/>\n"
                + HAZELCAST_END_TAG;

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testDuplicateLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <lite-member enabled=\"true\"/>\n"
                + "    <lite-member enabled=\"true\"/>\n"
                + HAZELCAST_END_TAG;

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
        fail();
    }

    private void assertIndexesEqual(QueryCacheConfig queryCacheConfig) {
        for (MapIndexConfig mapIndexConfig : queryCacheConfig.getIndexConfigs()) {
            assertEquals("name", mapIndexConfig.getAttribute());
            assertFalse(mapIndexConfig.isOrdered());
        }
    }

    @Test
    public void testMapNativeMaxSizePolicy() {
        String xmlFormat = HAZELCAST_START_TAG
                + "<map name=\"mymap\">"
                + "<in-memory-format>NATIVE</in-memory-format>"
                + "<max-size policy=\"{0}\">9991</max-size>"
                + "</map>"
                + HAZELCAST_END_TAG;
        MessageFormat messageFormat = new MessageFormat(xmlFormat);

        MaxSizeConfig.MaxSizePolicy[] maxSizePolicies = MaxSizeConfig.MaxSizePolicy.values();
        for (MaxSizeConfig.MaxSizePolicy maxSizePolicy : maxSizePolicies) {
            Object[] objects = {maxSizePolicy.toString()};
            String xml = messageFormat.format(objects);
            Config config = buildConfig(xml);
            MapConfig mapConfig = config.getMapConfig("mymap");
            MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();

            assertEquals(9991, maxSizeConfig.getSize());
            assertEquals(maxSizePolicy, maxSizeConfig.getMaxSizePolicy());
        }
    }

    @Test
    public void testInstanceName() {
        String name = randomName();
        String xml = HAZELCAST_START_TAG
                + "<instance-name>" + name + "</instance-name>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        assertEquals(name, config.getInstanceName());
    }

    @Test
    public void testUserCodeDeployment() {
        String xml = HAZELCAST_START_TAG
                + "<user-code-deployment enabled=\"true\">"
                + "<class-cache-mode>OFF</class-cache-mode>"
                + "<provider-mode>LOCAL_CLASSES_ONLY</provider-mode>"
                + "<blacklist-prefixes>com.blacklisted,com.other.blacklisted</blacklist-prefixes>"
                + "<whitelist-prefixes>com.whitelisted,com.other.whitelisted</whitelist-prefixes>"
                + "<provider-filter>HAS_ATTRIBUTE:foo</provider-filter>"
                + "</user-code-deployment>"
                + HAZELCAST_END_TAG;
        Config config = new InMemoryXmlConfig(xml);
        UserCodeDeploymentConfig dcConfig = config.getUserCodeDeploymentConfig();
        assertTrue(dcConfig.isEnabled());
        assertEquals(UserCodeDeploymentConfig.ClassCacheMode.OFF, dcConfig.getClassCacheMode());
        assertEquals(UserCodeDeploymentConfig.ProviderMode.LOCAL_CLASSES_ONLY, dcConfig.getProviderMode());
        assertEquals("com.blacklisted,com.other.blacklisted", dcConfig.getBlacklistedPrefixes());
        assertEquals("com.whitelisted,com.other.whitelisted", dcConfig.getWhitelistedPrefixes());
        assertEquals("HAS_ATTRIBUTE:foo", dcConfig.getProviderFilter());
    }

    @Test
    public void testGlobalSerializer() {
        String name = randomName();
        String val = "true";
        String xml = HAZELCAST_START_TAG
                + "<serialization><serializers><global-serializer override-java-serialization=\"" + val + "\">" + name
                + "</global-serializer></serializers></serialization>"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        GlobalSerializerConfig globalSerializerConfig = config.getSerializationConfig().getGlobalSerializerConfig();
        assertEquals(name, globalSerializerConfig.getClassName());
        assertTrue(globalSerializerConfig.isOverrideJavaSerialization());
    }

    @Test
    public void testHotRestart() {
        final String dir = "/mnt/hot-restart-root/";
        final String backupDir = "/mnt/hot-restart-backup/";
        final int parallelism = 3;
        final int validationTimeout = 13131;
        final int dataLoadTimeout = 45454;
        final HotRestartClusterDataRecoveryPolicy policy = HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
        final String xml = HAZELCAST_START_TAG
                + "<hot-restart-persistence enabled=\"true\">"
                + "<base-dir>" + dir + "</base-dir>"
                + "<backup-dir>" + backupDir + "</backup-dir>"
                + "<parallelism>" + parallelism + "</parallelism>"
                + "<validation-timeout-seconds>" + validationTimeout + "</validation-timeout-seconds>"
                + "<data-load-timeout-seconds>" + dataLoadTimeout + "</data-load-timeout-seconds>"
                + "<cluster-data-recovery-policy>" + policy + "</cluster-data-recovery-policy>"
                + "</hot-restart-persistence>\n" +
                HAZELCAST_END_TAG;

        final Config config = new InMemoryXmlConfig(xml);
        final HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();

        assertTrue(hotRestartPersistenceConfig.isEnabled());
        assertEquals(new File(dir).getAbsolutePath(), hotRestartPersistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(new File(backupDir).getAbsolutePath(), hotRestartPersistenceConfig.getBackupDir().getAbsolutePath());
        assertEquals(parallelism, hotRestartPersistenceConfig.getParallelism());
        assertEquals(validationTimeout, hotRestartPersistenceConfig.getValidationTimeoutSeconds());
        assertEquals(dataLoadTimeout, hotRestartPersistenceConfig.getDataLoadTimeoutSeconds());
        assertEquals(policy, hotRestartPersistenceConfig.getClusterDataRecoveryPolicy());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMissingNamespace() {
        String xml = "<hazelcast/>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidNamespace() {
        String xml = "<hazelcast xmlns=\"http://foo.bar\"/>";
        buildConfig(xml);
    }

    @Test
    public void testValidNamespace() {
        String xml = HAZELCAST_START_TAG + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastTagAppearsTwice() {
        String xml = HAZELCAST_START_TAG + "<hazelcast/>" + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test
    public void testMapEvictionPolicyClassName() {
        String mapEvictionPolicyClassName = "com.hazelcast.map.eviction.LRUEvictionPolicy";
        String xml = HAZELCAST_START_TAG
                + "<map name=\"test\">"
                + "<map-eviction-policy-class-name>" + mapEvictionPolicyClassName + "</map-eviction-policy-class-name> "
                + "</map>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("test");

        assertEquals(mapEvictionPolicyClassName, mapConfig.getMapEvictionPolicy().getClass().getName());
    }

    @Test
    public void testMapEvictionPolicyIsSelected_whenEvictionPolicySet() {
        String mapEvictionPolicyClassName = "com.hazelcast.map.eviction.LRUEvictionPolicy";
        String xml = HAZELCAST_START_TAG
                + "<map name=\"test\">"
                + "<map-eviction-policy-class-name>" + mapEvictionPolicyClassName + "</map-eviction-policy-class-name> "
                + "<eviction-policy>LFU</eviction-policy>"
                + "</map>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("test");

        assertEquals(mapEvictionPolicyClassName, mapConfig.getMapEvictionPolicy().getClass().getName());
    }

    @Test
    public void testCachePermission() {
        String xml = HAZELCAST_START_TAG + SECURITY_START_TAG
                + "  <client-permissions>"
                + "    <cache-permission name=\"/hz/cachemanager1/cache1\" principal=\"dev\">"
                + ACTIONS_FRAGMENT
                + "    </cache-permission>\n"
                + "  </client-permissions>"
                + SECURITY_END_TAG + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PermissionConfig expected = new PermissionConfig(CACHE, "/hz/cachemanager1/cache1", "dev");
        expected.addAction("create").addAction("destroy").addAction("add").addAction("remove");
        assertPermissionConfig(expected, config);
    }

    @Test
    public void testConfigPermission() {
        String xml = HAZELCAST_START_TAG + SECURITY_START_TAG
                + "  <client-permissions>"
                + "    <config-permission principal=\"dev\">"
                + "       <endpoints><endpoint>127.0.0.1</endpoint></endpoints>"
                + "    </config-permission>\n"
                + "  </client-permissions>"
                + SECURITY_END_TAG + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PermissionConfig expected = new PermissionConfig(CONFIG, "*", "dev");
        expected.getEndpoints().add("127.0.0.1");
        assertPermissionConfig(expected, config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testCacheConfig_withInvalidEvictionConfig_failsFast() {
        String xml = HAZELCAST_START_TAG
                + "    <cache name=\"cache\">"
                + "        <eviction size=\"10000000\" max-size-policy=\"ENTRY_COUNT\" eviction-policy=\"LFU\"/>"
                + "        <in-memory-format>NATIVE</in-memory-format>\n"
                + "    </cache>"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testMemberAddressProvider_classnameIsMandatory() {
        String xml = HAZELCAST_START_TAG
                + "<network> "
                + "  <member-address-provider enabled=\"true\">"
                + "  </member-address-provider>"
                + "</network> "
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Test
    public void testMemberAddressProviderEnabled() {
        String xml = HAZELCAST_START_TAG
                + "<network> "
                + "  <member-address-provider enabled=\"true\">"
                + "    <class-name>foo.bar.Clazz</class-name>"
                + "  </member-address-provider>"
                + "</network> "
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        assertTrue(memberAddressProviderConfig.isEnabled());
        assertEquals("foo.bar.Clazz", memberAddressProviderConfig.getClassName());
    }

    @Test
    public void testMemberAddressProviderEnabled_withProperties() {
        String xml = HAZELCAST_START_TAG
                + "<network> "
                + "  <member-address-provider enabled=\"true\">"
                + "    <class-name>foo.bar.Clazz</class-name>"
                + "    <properties>"
                + "       <property name=\"propName1\">propValue1</property>"
                + "    </properties>"
                + "  </member-address-provider>"
                + "</network> "
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        Properties properties = memberAddressProviderConfig.getProperties();
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));
    }

    private static void assertPermissionConfig(PermissionConfig expected, Config config) {
        Iterator<PermissionConfig> permConfigs = config.getSecurityConfig().getClientPermissionConfigs().iterator();
        PermissionConfig configured = permConfigs.next();
        assertEquals(expected.getType(), configured.getType());
        assertEquals(expected.getPrincipal(), configured.getPrincipal());
        assertEquals(expected.getName(), configured.getName());
        assertEquals(expected.getActions(), configured.getActions());
    }
}
