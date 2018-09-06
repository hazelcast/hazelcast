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

package com.hazelcast.config;

import com.hazelcast.config.helpers.DummyMapStore;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.quorum.impl.ProbabilisticQuorumFunction;
import com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
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
import static com.hazelcast.config.WANQueueFullBehavior.DISCARD_AFTER_MUTATION;
import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        assumeThatNotZingJDK6(); // https://github.com/hazelcast/hazelcast/issues/9044

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
                + "  <client-block-unmapped-actions>false</client-block-unmapped-actions>"
                + "</security>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SecurityConfig securityConfig = config.getSecurityConfig();
        List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();

        assertEquals(2, interceptorConfigs.size());
        assertEquals("foo", interceptorConfigs.get(0).className);
        assertEquals("bar", interceptorConfigs.get(1).className);
        assertFalse(securityConfig.getClientBlockUnmappedActions());
    }

    @Test
    public void readAliasedDiscoveryStrategyConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <group>\n"
                + "        <name>dev</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n"
                + "    <network>\n"
                + "        <port auto-increment=\"true\">5701</port>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\">\n"
                + "                <multicast-group>224.2.2.3</multicast-group>\n"
                + "                <multicast-port>54327</multicast-port>\n"
                + "            </multicast>\n"
                + "            <tcp-ip enabled=\"false\">\n"
                + "                <interface>127.0.0.1</interface>\n"
                + "            </tcp-ip>\n"
                + "            <aws enabled=\"true\" connection-timeout-seconds=\"10\" >\n"
                + "                <access-key>sample-access-key</access-key>\n"
                + "                <secret-key>sample-secret-key</secret-key>\n"
                + "                <iam-role>sample-role</iam-role>\n"
                + "                <region>sample-region</region>\n"
                + "                <host-header>sample-header</host-header>\n"
                + "                <security-group-name>sample-group</security-group-name>\n"
                + "                <tag-key>sample-tag-key</tag-key>\n"
                + "                <tag-value>sample-tag-value</tag-value>\n"
                + "            </aws>\n"
                + "        </join>\n"
                + "        <interfaces enabled=\"false\">\n"
                + "            <interface>10.10.1.*</interface>\n"
                + "        </interfaces>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);

        AliasedDiscoveryConfig aliasedConfig = config.getNetworkConfig().getJoin().getAliasedDiscoveryConfigs().get(0);
        assertTrue(aliasedConfig.isEnabled());
        assertAwsConfig(aliasedConfig);
    }

    @Test
    public void readDiscoveryConfig() {
        String xml = HAZELCAST_START_TAG
                + "   <group>\n"
                + "        <name>dev</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n"
                + "    <network>\n"
                + "        <port auto-increment=\"true\">5701</port>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\">\n"
                + "                <multicast-group>224.2.2.3</multicast-group>\n"
                + "                <multicast-port>54327</multicast-port>\n"
                + "            </multicast>\n"
                + "            <tcp-ip enabled=\"false\">\n"
                + "                <interface>127.0.0.1</interface>\n"
                + "            </tcp-ip>\n"
                + "            <discovery-strategies>\n"
                + "                <node-filter class=\"DummyFilterClass\" />\n"
                + "                <discovery-strategy class=\"DummyDiscoveryStrategy1\" enabled=\"true\">\n"
                + "                    <properties>\n"
                + "                        <property name=\"key-string\">foo</property>\n"
                + "                        <property name=\"key-int\">123</property>\n"
                + "                        <property name=\"key-boolean\">true</property>\n"
                + "                    </properties>\n"
                + "                </discovery-strategy>\n"
                + "            </discovery-strategies>\n"
                + "        </join>\n"
                + "        <interfaces enabled=\"false\">\n"
                + "            <interface>10.10.1.*</interface>\n"
                + "        </interfaces>\n"
                + "    </network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        DiscoveryConfig discoveryConfig = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        assertTrue(discoveryConfig.isEnabled());
        assertDiscoveryConfig(discoveryConfig);
    }

    @Test
    public void testSSLConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <ssl enabled=\"true\">\r\n"
                + "          <factory-class-name>\r\n"
                + "              com.hazelcast.nio.ssl.BasicSSLContextFactory\r\n"
                + "          </factory-class-name>\r\n"
                + "          <properties>\r\n"
                + "            <property name=\"protocol\">TLS</property>\r\n"
                + "          </properties>\r\n"
                + "        </ssl>\r\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        assertTrue(sslConfig.isEnabled());
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory", sslConfig.getFactoryClassName());
        assertEquals(1, sslConfig.getProperties().size());
        assertEquals("TLS", sslConfig.getProperties().get("protocol"));
    }

    @Test
    public void testSymmetricEncryptionConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <network>\n"
                + "      <symmetric-encryption enabled=\"true\">\n"
                + "        <algorithm>AES</algorithm>\n"
                + "        <salt>thesalt</salt>\n"
                + "        <password>thepass</password>\n"
                + "        <iteration-count>7531</iteration-count>\n"
                + "      </symmetric-encryption>"
                + "    </network>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SymmetricEncryptionConfig symmetricEncryptionConfig = config.getNetworkConfig().getSymmetricEncryptionConfig();
        assertTrue(symmetricEncryptionConfig.isEnabled());
        assertEquals("AES", symmetricEncryptionConfig.getAlgorithm());
        assertEquals("thesalt", symmetricEncryptionConfig.getSalt());
        assertEquals("thepass", symmetricEncryptionConfig.getPassword());
        assertEquals(7531, symmetricEncryptionConfig.getIterationCount());
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
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "    </semaphore>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        SemaphoreConfig defaultConfig = config.getSemaphoreConfig("default");
        SemaphoreConfig customConfig = config.getSemaphoreConfig("custom");
        assertEquals(1, defaultConfig.getInitialPermits());
        assertEquals(10, customConfig.getInitialPermits());
        assertEquals("customQuorumRule", customConfig.getQuorumName());
    }

    @Test
    public void readQueueConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <queue name=\"custom\">"
                + "        <statistics-enabled>true</statistics-enabled>"
                + "        <max-size>100</max-size>"
                + "        <backup-count>1</backup-count>"
                + "        <async-backup-count>0</async-backup-count>"
                + "        <empty-queue-ttl>-1</empty-queue-ttl>"
                + "        <item-listeners>"
                + "            <item-listener>com.hazelcast.examples.ItemListener</item-listener>"
                + "        </item-listeners>"
                + "        <queue-store>"
                + "            <class-name>com.hazelcast.QueueStoreImpl</class-name>"
                + "            <properties>"
                + "                <property name=\"binary\">false</property>"
                + "                <property name=\"memory-limit\">1000</property>"
                + "                <property name=\"bulk-load\">500</property>"
                + "            </properties>"
                + "        </queue-store>"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <merge-policy batch-size=\"23\">CustomMergePolicy</merge-policy>"
                + "    </queue>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QueueConfig queueConfig = config.getQueueConfig("custom");
        assertTrue(queueConfig.isStatisticsEnabled());
        assertEquals(100, queueConfig.getMaxSize());
        assertEquals(1, queueConfig.getBackupCount());
        assertEquals(0, queueConfig.getAsyncBackupCount());
        assertEquals(-1, queueConfig.getEmptyQueueTtl());

        MergePolicyConfig mergePolicyConfig = queueConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(23, mergePolicyConfig.getBatchSize());

        assertTrue(queueConfig.getItemListenerConfigs().size() == 1);
        ItemListenerConfig listenerConfig = queueConfig.getItemListenerConfigs().iterator().next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig.getClassName());

        QueueStoreConfig storeConfig = queueConfig.getQueueStoreConfig();
        assertNotNull(storeConfig);
        assertTrue(storeConfig.isEnabled());
        assertEquals("com.hazelcast.QueueStoreImpl", storeConfig.getClassName());

        Properties storeConfigProperties = storeConfig.getProperties();
        assertEquals(3, storeConfigProperties.size());
        assertEquals("500", storeConfigProperties.getProperty("bulk-load"));
        assertEquals("1000", storeConfigProperties.getProperty("memory-limit"));
        assertEquals("false", storeConfigProperties.getProperty("binary"));

        assertEquals("customQuorumRule", queueConfig.getQuorumName());
    }

    @Test
    public void readListConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <list name=\"myList\">"
                + "        <statistics-enabled>true</statistics-enabled>"
                + "        <max-size>100</max-size>"
                + "        <backup-count>1</backup-count>"
                + "        <async-backup-count>0</async-backup-count>"
                + "        <item-listeners>"
                + "            <item-listener>com.hazelcast.examples.ItemListener</item-listener>"
                + "        </item-listeners>"
                + "        <merge-policy batch-size=\"4223\">PassThroughMergePolicy</merge-policy>"
                + "    </list>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        ListConfig listConfig = config.getListConfig("myList");

        assertEquals("myList", listConfig.getName());
        assertTrue(listConfig.isStatisticsEnabled());
        assertEquals(100, listConfig.getMaxSize());
        assertEquals(1, listConfig.getBackupCount());
        assertEquals(0, listConfig.getAsyncBackupCount());
        assertTrue(listConfig.getItemListenerConfigs().size() == 1);

        ItemListenerConfig listenerConfig = listConfig.getItemListenerConfigs().iterator().next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig.getClassName());

        MergePolicyConfig mergePolicyConfig = listConfig.getMergePolicyConfig();
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(4223, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void readSetConfig() {
        String xml = HAZELCAST_START_TAG
                + "      <set name=\"mySet\">"
                + "        <statistics-enabled>true</statistics-enabled>"
                + "        <max-size>100</max-size>"
                + "        <backup-count>1</backup-count>"
                + "        <async-backup-count>0</async-backup-count>"
                + "        <item-listeners>"
                + "            <item-listener>com.hazelcast.examples.ItemListener</item-listener>"
                + "        </item-listeners>"
                + "        <merge-policy batch-size=\"4223\">PassThroughMergePolicy</merge-policy>"
                + "    </set>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        SetConfig setConfig = config.getSetConfig("mySet");

        assertEquals("mySet", setConfig.getName());
        assertTrue(setConfig.isStatisticsEnabled());
        assertEquals(100, setConfig.getMaxSize());
        assertEquals(1, setConfig.getBackupCount());
        assertEquals(0, setConfig.getAsyncBackupCount());
        assertTrue(setConfig.getItemListenerConfigs().size() == 1);

        ItemListenerConfig listenerConfig = setConfig.getItemListenerConfigs().iterator().next();
        assertEquals("com.hazelcast.examples.ItemListener", listenerConfig.getClassName());

        MergePolicyConfig mergePolicyConfig = setConfig.getMergePolicyConfig();
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(4223, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void readLockConfig() {
        String xml = HAZELCAST_START_TAG
                + "  <lock name=\"default\">"
                + "        <quorum-ref>quorumRuleWithThreeNodes</quorum-ref>"
                + "  </lock>"
                + "  <lock name=\"custom\">"
                + "       <quorum-ref>customQuorumRule</quorum-ref>"
                + "  </lock>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        LockConfig defaultConfig = config.getLockConfig("default");
        LockConfig customConfig = config.getLockConfig("custom");
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
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <merge-policy batch-size=\"2342\">CustomMergePolicy</merge-policy>"
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
        assertEquals("customQuorumRule", ringbufferConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = ringbufferConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void readAtomicLong() {
        String xml = HAZELCAST_START_TAG
                + "    <atomic-long name=\"custom\">"
                + "        <merge-policy batch-size=\"23\">CustomMergePolicy</merge-policy>"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "    </atomic-long>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        AtomicLongConfig atomicLongConfig = config.getAtomicLongConfig("custom");
        assertEquals("custom", atomicLongConfig.getName());
        assertEquals("customQuorumRule", atomicLongConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = atomicLongConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(23, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void readAtomicReference() {
        String xml = HAZELCAST_START_TAG
                + "    <atomic-reference name=\"custom\">"
                + "        <merge-policy batch-size=\"23\">CustomMergePolicy</merge-policy>"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "    </atomic-reference>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        AtomicReferenceConfig atomicReferenceConfig = config.getAtomicReferenceConfig("custom");
        assertEquals("custom", atomicReferenceConfig.getName());
        assertEquals("customQuorumRule", atomicReferenceConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = atomicReferenceConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(23, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void readCountDownLatch() {
        String xml = HAZELCAST_START_TAG
                + "    <count-down-latch name=\"custom\">"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "    </count-down-latch>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        CountDownLatchConfig countDownLatchConfig = config.getCountDownLatchConfig("custom");
        assertEquals("custom", countDownLatchConfig.getName());
        assertEquals("customQuorumRule", countDownLatchConfig.getQuorumName());
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

        String xml = new ConfigXmlGenerator(true, false).generate(config);
        Config config2 = new InMemoryXmlConfig(xml);

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
                + "    <merge-policy batch-size=\"2342\">CustomMergePolicy</merge-policy>"
                + "</map>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("testCaseInsensitivity");

        assertEquals(InMemoryFormat.BINARY, mapConfig.getInMemoryFormat());
        assertEquals(EvictionPolicy.NONE, mapConfig.getEvictionPolicy());
        assertEquals(MaxSizeConfig.MaxSizePolicy.PER_PARTITION, mapConfig.getMaxSizeConfig().getMaxSizePolicy());

        MergePolicyConfig mergePolicyConfig = mapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
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
                + "<map name=\"lruMap\">"
                + "        <eviction-policy>LRU</eviction-policy>\n"
                + "    </map>\n"
                + "<map name=\"lfuMap\">"
                +
                "        <eviction-policy>LFU</eviction-policy>\n"
                + "    </map>\n"
                + "<map name=\"noneMap\">"
                + "        <eviction-policy>NONE</eviction-policy>\n"
                + "    </map>\n"
                + "<map name=\"randomMap\">"
                + "        <eviction-policy>RANDOM</eviction-policy>\n"
                + "    </map>\n"
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
        String xml = HAZELCAST_START_TAG
                + "<partition-group enabled=\"true\" group-type=\"ZONE_AWARE\" />"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertEquals(config.getPartitionGroupConfig().getGroupType(), PartitionGroupConfig.MemberGroupType.ZONE_AWARE);
    }

    @Test
    public void testPartitionGroupSPI() {
        String xml = HAZELCAST_START_TAG
                + "<partition-group enabled=\"true\" group-type=\"SPI\" />"
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
        MapConfig mapConfig = config.getMapConfig(mapName);
        WanReplicationRef wanRef = mapConfig.getWanReplicationRef();

        assertEquals(refName, wanRef.getName());
        assertEquals(mergePolicy, wanRef.getMergePolicy());
        assertTrue(wanRef.isRepublishingEnabled());
        assertEquals(1, wanRef.getFilters().size());
        assertEquals("com.example.SampleFilter", wanRef.getFilters().get(0));
    }

    @Test
    public void testWanReplicationConfig() {
        String configName  = "test";
        String xml = HAZELCAST_START_TAG
                + "  <wan-replication name=\"" + configName + "\">\n"
                + "        <wan-publisher group-name=\"nyc\">\n"
                + "            <class-name>PublisherClassName</class-name>\n"
                + "            <queue-capacity>15000</queue-capacity>\n"
                + "            <queue-full-behavior>DISCARD_AFTER_MUTATION</queue-full-behavior>\n"
                + "            <initial-publisher-state>STOPPED</initial-publisher-state>\n"
                + "            <properties>\n"
                + "                <property name=\"propName1\">propValue1</property>\n"
                + "            </properties>\n"
                + "        </wan-publisher>\n"
                + "        <wan-consumer>\n"
                + "            <class-name>ConsumerClassName</class-name>\n"
                + "            <properties>\n"
                + "                <property name=\"propName1\">propValue1</property>\n"
                + "            </properties>\n"
                + "        </wan-consumer>\n"
                + "    </wan-replication>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);

        assertEquals(configName, wanReplicationConfig.getName());

        WanConsumerConfig consumerConfig = wanReplicationConfig.getWanConsumerConfig();
        assertNotNull(consumerConfig);
        assertEquals("ConsumerClassName", consumerConfig.getClassName());

        Map<String, Comparable> properties = consumerConfig.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));

        List<WanPublisherConfig> publishers = wanReplicationConfig.getWanPublisherConfigs();
        assertNotNull(publishers);
        assertEquals(1, publishers.size());
        WanPublisherConfig publisherConfig = publishers.get(0);
        assertEquals("PublisherClassName", publisherConfig.getClassName());
        assertEquals("nyc", publisherConfig.getGroupName());
        assertEquals(15000, publisherConfig.getQueueCapacity());
        assertEquals(DISCARD_AFTER_MUTATION, publisherConfig.getQueueFullBehavior());
        assertEquals(WanPublisherState.STOPPED, publisherConfig.getInitialPublisherState());

        properties = publisherConfig.getProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertEquals("propValue1", properties.get("propName1"));
    }

    @Test
    public void default_value_of_persist_wan_replicated_data_is_false() {
        String configName  = "test";
        String xml = HAZELCAST_START_TAG
                + "  <wan-replication name=\"" + configName + "\">\n"
                + "        <wan-consumer>\n"
                + "        </wan-consumer>\n"
                + "    </wan-replication>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);
        WanConsumerConfig consumerConfig = wanReplicationConfig.getWanConsumerConfig();
        assertFalse(consumerConfig.isPersistWanReplicatedData());
    }


    @Test
    public void testWanReplicationSyncConfig() {
        String configName  = "test";
        String xml = HAZELCAST_START_TAG
                + "  <wan-replication name=\"" + configName + "\">\n"
                + "        <wan-publisher group-name=\"nyc\">\n"
                + "            <class-name>PublisherClassName</class-name>\n"
                + "            <wan-sync>\n"
                + "                <consistency-check-strategy>MERKLE_TREES</consistency-check-strategy>\n"
                + "            </wan-sync>\n"
                + "        </wan-publisher>\n"
                + "    </wan-replication>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        WanReplicationConfig wanReplicationConfig = config.getWanReplicationConfig(configName);

        assertEquals(configName, wanReplicationConfig.getName());

        List<WanPublisherConfig> publishers = wanReplicationConfig.getWanPublisherConfigs();
        assertNotNull(publishers);
        assertEquals(1, publishers.size());
        WanPublisherConfig publisherConfig = publishers.get(0);
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, publisherConfig.getWanSyncConfig()
                                                                           .getConsistencyCheckStrategy());
    }

    @Test
    public void testMapEventJournalConfig() {
        String journalName = "mapName";
        String xml = HAZELCAST_START_TAG
                + "<event-journal enabled=\"true\">\n"
                + "    <mapName>" + journalName + "</mapName>\n"
                + "    <capacity>120</capacity>\n"
                + "    <time-to-live-seconds>20</time-to-live-seconds>\n"
                + "</event-journal>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        EventJournalConfig journalConfig = config.getMapEventJournalConfig(journalName);

        assertTrue(journalConfig.isEnabled());
        assertEquals(120, journalConfig.getCapacity());
        assertEquals(20, journalConfig.getTimeToLiveSeconds());
    }

    @Test
    public void testMapMerkleTreeConfig() {
        String mapName = "mapName";
        String xml = HAZELCAST_START_TAG
                + "<merkle-tree enabled=\"true\">\n"
                + "    <mapName>" + mapName + "</mapName>\n"
                + "    <depth>20</depth>\n"
                + "</merkle-tree>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MerkleTreeConfig treeConfig = config.getMapMerkleTreeConfig(mapName);

        assertTrue(treeConfig.isEnabled());
        assertEquals(20, treeConfig.getDepth());
    }

    @Test
    public void testCacheEventJournalConfig() {
        String journalName = "cacheName";
        String xml = HAZELCAST_START_TAG
                + "<event-journal enabled=\"true\">\n"
                + "    <cacheName>" + journalName + "</cacheName>\n"
                + "    <capacity>120</capacity>\n"
                + "    <time-to-live-seconds>20</time-to-live-seconds>\n"
                + "</event-journal>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        EventJournalConfig journalConfig = config.getCacheEventJournalConfig(journalName);

        assertTrue(journalConfig.isEnabled());
        assertEquals(120, journalConfig.getCapacity());
        assertEquals(20, journalConfig.getTimeToLiveSeconds());
    }

    @Test
    public void testFlakeIdGeneratorConfig() {
        String xml = HAZELCAST_START_TAG
                + "<flake-id-generator name='gen'>"
                + "  <prefetch-count>3</prefetch-count>"
                + "  <prefetch-validity-millis>10</prefetch-validity-millis>"
                + "  <id-offset>20</id-offset>"
                + "  <node-id-offset>30</node-id-offset>"
                + "  <statistics-enabled>false</statistics-enabled>"
                + "</flake-id-generator>"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml);
        FlakeIdGeneratorConfig fConfig = config.findFlakeIdGeneratorConfig("gen");
        assertEquals("gen", fConfig.getName());
        assertEquals(3, fConfig.getPrefetchCount());
        assertEquals(10L, fConfig.getPrefetchValidityMillis());
        assertEquals(20L, fConfig.getIdOffset());
        assertEquals(30L, fConfig.getNodeIdOffset());
        assertFalse(fConfig.isStatisticsEnabled());
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
                + "    <map-store enabled=\"true\">\n"
                + "        <class-name>com.hazelcast.config.helpers.DummyMapStore</class-name>\n"
                + "        <write-delay-seconds>5</write-delay-seconds>\n"
                + "    </map-store>\n"
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
                + "    <partition-lost-listeners>\n"
                + "        <partition-lost-listener>" + listenerName + "</partition-lost-listener>\n"
                + "    </partition-lost-listeners>\n"
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
                + "    <partition-lost-listeners>\n"
                + "        <partition-lost-listener>" + listenerName + "</partition-lost-listener>\n"
                + "    </partition-lost-listeners>\n"
                + "</cache>\n"
                + HAZELCAST_END_TAG;
    }

    private void testXSDConfigXML(String xmlFileName) throws Exception {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-config-"
                + Versions.CURRENT_CLUSTER_VERSION + ".xsd");
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
                + "   <wan-replication name=\"my-wan-cluster\">\n"
                + "      <wan-publisher group-name=\"istanbul\">\n"
                + "         <class-name>com.hazelcast.wan.custom.WanPublisher</class-name>\n"
                + "         <queue-full-behavior>THROW_EXCEPTION</queue-full-behavior>\n"
                + "         <queue-capacity>21</queue-capacity>\n"
                + "         <aws enabled=\"false\" connection-timeout-seconds=\"10\" >\n"
                + "            <access-key>sample-access-key</access-key>\n"
                + "            <secret-key>sample-secret-key</secret-key>\n"
                + "            <iam-role>sample-role</iam-role>\n"
                + "            <region>sample-region</region>\n"
                + "            <host-header>sample-header</host-header>\n"
                + "            <security-group-name>sample-group</security-group-name>\n"
                + "            <tag-key>sample-tag-key</tag-key>\n"
                + "            <tag-value>sample-tag-value</tag-value>\n"
                + "         </aws>\n"
                + "         <discovery-strategies>\n"
                + "            <node-filter class=\"DummyFilterClass\" />\n"
                + "            <discovery-strategy class=\"DummyDiscoveryStrategy1\" enabled=\"true\">\n"
                + "               <properties>\n"
                + "                  <property name=\"key-string\">foo</property>\n"
                + "                  <property name=\"key-int\">123</property>\n"
                + "                  <property name=\"key-boolean\">true</property>\n"
                + "               </properties>\n"
                + "            </discovery-strategy>\n"
                + "         </discovery-strategies>\n"
                + "         <properties>\n"
                + "            <property name=\"custom.prop.publisher\">prop.publisher</property>\n"
                + "            <property name=\"discovery.period\">5</property>\n"
                + "            <property name=\"maxEndpoints\">2</property>\n"
                + "         </properties>\n"
                + "      </wan-publisher>\n"
                + "      <wan-publisher group-name=\"ankara\">\n"
                + "         <class-name>com.hazelcast.wan.custom.WanPublisher</class-name>\n"
                + "         <queue-full-behavior>THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE</queue-full-behavior>\n"
                + "         <initial-publisher-state>STOPPED</initial-publisher-state>\n"
                + "      </wan-publisher>\n"
                + "      <wan-consumer>\n"
                + "         <class-name>com.hazelcast.wan.custom.WanConsumer</class-name>\n"
                + "         <properties>\n"
                + "            <property name=\"custom.prop.consumer\">prop.consumer</property>\n"
                + "         </properties>\n"
                + "      <persist-wan-replicated-data>false</persist-wan-replicated-data>\n"
                + "      </wan-consumer>\n"
                + "   </wan-replication>"
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
        assertEquals(WanPublisherState.REPLICATING, publisherConfig1.getInitialPublisherState());
        assertEquals(21, publisherConfig1.getQueueCapacity());
        Map<String, Comparable> pubProperties = publisherConfig1.getProperties();
        assertEquals("prop.publisher", pubProperties.get("custom.prop.publisher"));
        assertEquals("5", pubProperties.get("discovery.period"));
        assertEquals("2", pubProperties.get("maxEndpoints"));
        assertFalse(publisherConfig1.getAliasedDiscoveryConfigs().get(0).isEnabled());
        assertAwsConfig(publisherConfig1.getAliasedDiscoveryConfigs().get(0));
        assertDiscoveryConfig(publisherConfig1.getDiscoveryConfig());

        WanPublisherConfig publisherConfig2 = publisherConfigs.get(1);
        assertEquals("ankara", publisherConfig2.getGroupName());
        assertEquals(WANQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE, publisherConfig2.getQueueFullBehavior());
        assertEquals(WanPublisherState.STOPPED, publisherConfig2.getInitialPublisherState());

        WanConsumerConfig consumerConfig = wanConfig.getWanConsumerConfig();
        assertEquals("com.hazelcast.wan.custom.WanConsumer", consumerConfig.getClassName());
        Map<String, Comparable> consProperties = consumerConfig.getProperties();
        assertEquals("prop.consumer", consProperties.get("custom.prop.consumer"));
        assertFalse(consumerConfig.isPersistWanReplicatedData());
    }

    private void assertDiscoveryConfig(DiscoveryConfig c) {
        assertEquals("DummyFilterClass", c.getNodeFilterClass());
        assertEquals(1, c.getDiscoveryStrategyConfigs().size());

        DiscoveryStrategyConfig config = c.getDiscoveryStrategyConfigs().iterator().next();
        assertEquals("DummyDiscoveryStrategy1", config.getClassName());

        Map<String, Comparable> props = config.getProperties();
        assertEquals("foo", props.get("key-string"));
        assertEquals("123", props.get("key-int"));
        assertEquals("true", props.get("key-boolean"));
    }

    private void assertAwsConfig(AliasedDiscoveryConfig aws) {
        assertEquals("aws", aws.getEnvironment());
        assertEquals("sample-access-key", aws.getProperties().get("access-key"));
        assertEquals("sample-secret-key", aws.getProperties().get("secret-key"));
        assertEquals("sample-role", aws.getProperties().get("iam-role"));
        assertEquals("sample-region", aws.getProperties().get("region"));
        assertEquals("sample-header", aws.getProperties().get("host-header"));
        assertEquals("sample-group", aws.getProperties().get("security-group-name"));
        assertEquals("sample-tag-key", aws.getProperties().get("tag-key"));
        assertEquals("sample-tag-value", aws.getProperties().get("tag-value"));
        assertEquals("10", aws.getProperties().get("connection-timeout-seconds"));
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
                + "        <quorum-function-class-name>com.hazelcast.SomeQuorumFunction</quorum-function-class-name>"
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");

        assertFalse(quorumConfig.getListenerConfigs().isEmpty());
        assertEquals("com.abc.my.quorum.listener", quorumConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("com.abc.my.second.listener", quorumConfig.getListenerConfigs().get(1).getClassName());
        assertEquals("com.hazelcast.SomeQuorumFunction", quorumConfig.getQuorumFunctionClassName());
    }

    @Test(expected = ConfigurationException.class)
    public void testQuorumConfig_whenClassNameAndRecentlyActiveQuorumDefined_exceptionIsThrown() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <quorum-function-class-name>com.hazelcast.SomeQuorumFunction</quorum-function-class-name>"
                + "        <recently-active-quorum />"
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Test(expected = ConfigurationException.class)
    public void testQuorumConfig_whenClassNameAndProbabilisticQuorumDefined_exceptionIsThrown() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <quorum-function-class-name>com.hazelcast.SomeQuorumFunction</quorum-function-class-name>"
                + "        <probabilistic-quorum />"
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testQuorumConfig_whenBothBuiltinQuorumsDefined_exceptionIsThrown() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <probabilistic-quorum />"
                + "        <recently-active-quorum />"
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Test
    public void testQuorumConfig_whenRecentlyActiveQuorum_withDefaultValues() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <recently-active-quorum />"
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertInstanceOf(RecentlyActiveQuorumFunction.class, quorumConfig.getQuorumFunctionImplementation());
        RecentlyActiveQuorumFunction quorumFunction = (RecentlyActiveQuorumFunction) quorumConfig
                .getQuorumFunctionImplementation();
        assertEquals(RecentlyActiveQuorumConfigBuilder.DEFAULT_HEARTBEAT_TOLERANCE_MILLIS,
                quorumFunction.getHeartbeatToleranceMillis());
    }

    @Test
    public void testQuorumConfig_whenRecentlyActiveQuorum_withCustomValues() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <recently-active-quorum heartbeat-tolerance-millis=\"13000\" />"
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertEquals(3, quorumConfig.getSize());
        assertInstanceOf(RecentlyActiveQuorumFunction.class, quorumConfig.getQuorumFunctionImplementation());
        RecentlyActiveQuorumFunction quorumFunction = (RecentlyActiveQuorumFunction) quorumConfig
                .getQuorumFunctionImplementation();
        assertEquals(13000, quorumFunction.getHeartbeatToleranceMillis());
    }

    @Test
    public void testQuorumConfig_whenProbabilisticQuorum_withDefaultValues() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <probabilistic-quorum />"
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertInstanceOf(ProbabilisticQuorumFunction.class, quorumConfig.getQuorumFunctionImplementation());
        ProbabilisticQuorumFunction quorumFunction = (ProbabilisticQuorumFunction) quorumConfig.getQuorumFunctionImplementation();
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_HEARTBEAT_INTERVAL_MILLIS,
                quorumFunction.getHeartbeatIntervalMillis());
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_HEARTBEAT_PAUSE_MILLIS,
                quorumFunction.getAcceptableHeartbeatPauseMillis());
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_MIN_STD_DEVIATION,
                quorumFunction.getMinStdDeviationMillis());
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_PHI_THRESHOLD, quorumFunction.getSuspicionThreshold(), 0.01);
        assertEquals(ProbabilisticQuorumConfigBuilder.DEFAULT_SAMPLE_SIZE, quorumFunction.getMaxSampleSize());
    }

    @Test
    public void testQuorumConfig_whenProbabilisticQuorum_withCustomValues() {
        String xml = HAZELCAST_START_TAG
                + "      <quorum enabled=\"true\" name=\"myQuorum\">\n"
                + "        <quorum-size>3</quorum-size>\n"
                + "        <probabilistic-quorum acceptable-heartbeat-pause-millis=\"37400\" suspicion-threshold=\"3.14592\" "
                + "                 max-sample-size=\"42\" min-std-deviation-millis=\"1234\""
                + "                 heartbeat-interval-millis=\"4321\" />"
                + "    </quorum>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertInstanceOf(ProbabilisticQuorumFunction.class, quorumConfig.getQuorumFunctionImplementation());
        ProbabilisticQuorumFunction quorumFunction = (ProbabilisticQuorumFunction) quorumConfig.getQuorumFunctionImplementation();
        assertEquals(4321, quorumFunction.getHeartbeatIntervalMillis());
        assertEquals(37400, quorumFunction.getAcceptableHeartbeatPauseMillis());
        assertEquals(1234, quorumFunction.getMinStdDeviationMillis());
        assertEquals(3.14592d, quorumFunction.getSuspicionThreshold(), 0.001d);
        assertEquals(42, quorumFunction.getMaxSampleSize());
    }

    @Test
    public void testCacheConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <cache name=\"foobar\">\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <key-type class-name=\"java.lang.Object\"/>"
                + "        <value-type class-name=\"java.lang.Object\"/>"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <management-enabled>false</management-enabled>"
                + "        <read-through>true</read-through>"
                + "        <write-through>true</write-through>"
                + "        <cache-loader-factory class-name=\"com.example.cache.MyCacheLoaderFactory\"/>"
                + "        <cache-writer-factory class-name=\"com.example.cache.MyCacheWriterFactory\"/>"
                + "        <expiry-policy-factory class-name=\"com.example.cache.MyExpirePolicyFactory\"/>"
                + "        <in-memory-format>BINARY</in-memory-format>"
                + "        <backup-count>1</backup-count>"
                + "        <async-backup-count>0</async-backup-count>"
                + "        <eviction size=\"1000\" max-size-policy=\"ENTRY_COUNT\" eviction-policy=\"LFU\"/>"
                + "        <merge-policy>com.hazelcast.cache.merge.LatestAccessCacheMergePolicy</merge-policy>"
                + "        <disable-per-entry-invalidation-events>true</disable-per-entry-invalidation-events>"
                + "        <hot-restart enabled=\"false\">\n"
                + "            <fsync>false</fsync>\n"
                + "          </hot-restart>"
                + "        <partition-lost-listeners>\n"
                + "            <partition-lost-listener>com.your-package.YourPartitionLostListener</partition-lost-listener>\n"
                + "          </partition-lost-listeners>"
                + "        <cache-entry-listeners>\n"
                + "            <cache-entry-listener old-value-required=\"false\" synchronous=\"false\">\n"
                + "                <cache-entry-listener-factory\n"
                + "                        class-name=\"com.example.cache.MyEntryListenerFactory\"/>\n"
                + "                <cache-entry-event-filter-factory\n"
                + "                        class-name=\"com.example.cache.MyEntryEventFilterFactory\"/>\n"
                + "            </cache-entry-listener>\n"
                + "        </cache-entry-listeners>"
                + "    </cache>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        CacheSimpleConfig cacheConfig = config.getCacheConfig("foobar");

        assertFalse(config.getCacheConfigs().isEmpty());
        assertEquals("customQuorumRule", cacheConfig.getQuorumName());
        assertEquals("java.lang.Object", cacheConfig.getKeyType());
        assertEquals("java.lang.Object", cacheConfig.getValueType());
        assertFalse(cacheConfig.isStatisticsEnabled());
        assertFalse(cacheConfig.isManagementEnabled());
        assertTrue(cacheConfig.isReadThrough());
        assertTrue(cacheConfig.isWriteThrough());
        assertEquals("com.example.cache.MyCacheLoaderFactory", cacheConfig.getCacheLoaderFactory());
        assertEquals("com.example.cache.MyCacheWriterFactory", cacheConfig.getCacheWriterFactory());
        assertEquals("com.example.cache.MyExpirePolicyFactory", cacheConfig.getExpiryPolicyFactoryConfig().getClassName());
        assertEquals(InMemoryFormat.BINARY, cacheConfig.getInMemoryFormat());
        assertEquals(1, cacheConfig.getBackupCount());
        assertEquals(0, cacheConfig.getAsyncBackupCount());
        assertEquals(1000, cacheConfig.getEvictionConfig().getSize());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, cacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(EvictionPolicy.LFU, cacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals("com.hazelcast.cache.merge.LatestAccessCacheMergePolicy", cacheConfig.getMergePolicy());
        assertTrue(cacheConfig.isDisablePerEntryInvalidationEvents());
        assertFalse(cacheConfig.getHotRestartConfig().isEnabled());
        assertFalse(cacheConfig.getHotRestartConfig().isFsync());
        assertEquals(1, cacheConfig.getPartitionLostListenerConfigs().size());
        assertEquals("com.your-package.YourPartitionLostListener", cacheConfig.getPartitionLostListenerConfigs().get(0).getClassName());
        assertEquals(1, cacheConfig.getCacheEntryListeners().size());
        assertEquals("com.example.cache.MyEntryListenerFactory", cacheConfig.getCacheEntryListeners().get(0).getCacheEntryListenerFactory());
        assertEquals("com.example.cache.MyEntryEventFilterFactory", cacheConfig.getCacheEntryListeners().get(0).getCacheEntryEventFilterFactory());
    }

    @Test
    public void testExecutorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <executor-service name=\"foobar\">\n"
                + "        <pool-size>2</pool-size>\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <statistics-enabled>true</statistics-enabled>"
                + "        <queue-capacity>0</queue-capacity>"
                + "    </executor-service>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ExecutorConfig executorConfig = config.getExecutorConfig("foobar");

        assertFalse(config.getExecutorConfigs().isEmpty());
        assertEquals(2, executorConfig.getPoolSize());
        assertEquals("customQuorumRule", executorConfig.getQuorumName());
        assertTrue(executorConfig.isStatisticsEnabled());
        assertEquals(0, executorConfig.getQueueCapacity());
    }

    @Test
    public void testDurableExecutorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <durable-executor-service name=\"foobar\">\n"
                + "        <pool-size>2</pool-size>\n"
                + "        <durability>3</durability>\n"
                + "        <capacity>4</capacity>\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "    </durable-executor-service>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        DurableExecutorConfig durableExecutorConfig = config.getDurableExecutorConfig("foobar");

        assertFalse(config.getDurableExecutorConfigs().isEmpty());
        assertEquals(2, durableExecutorConfig.getPoolSize());
        assertEquals(3, durableExecutorConfig.getDurability());
        assertEquals(4, durableExecutorConfig.getCapacity());
        assertEquals("customQuorumRule", durableExecutorConfig.getQuorumName());
    }

    @Test
    public void testScheduledExecutorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <scheduled-executor-service name=\"foobar\">\n"
                + "        <durability>4</durability>\n"
                + "        <pool-size>5</pool-size>\n"
                + "        <capacity>2</capacity>\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <merge-policy batch-size='99'>PutIfAbsent</merge-policy>"
                + "    </scheduled-executor-service>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ScheduledExecutorConfig scheduledExecutorConfig = config.getScheduledExecutorConfig("foobar");

        assertFalse(config.getScheduledExecutorConfigs().isEmpty());
        assertEquals(4, scheduledExecutorConfig.getDurability());
        assertEquals(5, scheduledExecutorConfig.getPoolSize());
        assertEquals(2, scheduledExecutorConfig.getCapacity());
        assertEquals("customQuorumRule", scheduledExecutorConfig.getQuorumName());
        assertEquals(99, scheduledExecutorConfig.getMergePolicyConfig().getBatchSize());
        assertEquals("PutIfAbsent", scheduledExecutorConfig.getMergePolicyConfig().getPolicy());
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <cardinality-estimator name=\"foobar\">\n"
                + "        <backup-count>2</backup-count>\n"
                + "        <async-backup-count>3</async-backup-count>\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <merge-policy>com.hazelcast.spi.merge.HyperLogLogMergePolicy</merge-policy>"
                + "    </cardinality-estimator>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        CardinalityEstimatorConfig cardinalityEstimatorConfig = config.getCardinalityEstimatorConfig("foobar");

        assertFalse(config.getCardinalityEstimatorConfigs().isEmpty());
        assertEquals(2, cardinalityEstimatorConfig.getBackupCount());
        assertEquals(3, cardinalityEstimatorConfig.getAsyncBackupCount());
        assertEquals("com.hazelcast.spi.merge.HyperLogLogMergePolicy",
                cardinalityEstimatorConfig.getMergePolicyConfig().getPolicy());
        assertEquals("customQuorumRule", cardinalityEstimatorConfig.getQuorumName());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testCardinalityEstimatorConfigWithInvalidMergePolicy() {
        String xml = HAZELCAST_START_TAG
                + "    <cardinality-estimator name=\"foobar\">\n"
                + "        <backup-count>2</backup-count>\n"
                + "        <async-backup-count>3</async-backup-count>\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <merge-policy>CustomMergePolicy</merge-policy>"
                + "    </cardinality-estimator>\n"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
        fail();
    }

    @Test
    public void testPNCounterConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <pn-counter name=\"pn-counter-1\">\n"
                + "        <replica-count>100</replica-count>\n"
                + "        <quorum-ref>quorumRuleWithThreeMembers</quorum-ref>\n"
                + "        <statistics-enabled>false</statistics-enabled>\n"
                + "    </pn-counter>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        PNCounterConfig pnCounterConfig = config.getPNCounterConfig("pn-counter-1");

        assertFalse(config.getPNCounterConfigs().isEmpty());
        assertEquals(100, pnCounterConfig.getReplicaCount());
        assertEquals("quorumRuleWithThreeMembers", pnCounterConfig.getQuorumName());
        assertFalse(pnCounterConfig.isStatisticsEnabled());
    }

    @Test
    public void testMultiMapConfig() {
        String xml = HAZELCAST_START_TAG
                + "  <multimap name=\"myMultiMap\">"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>3</async-backup-count>"
                + "        <binary>false</binary>"
                + "        <value-collection-type>SET</value-collection-type>"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <entry-listeners>\n"
                +
                "            <entry-listener include-value=\"true\" local=\"true\">com.hazelcast.examples.EntryListener</entry-listener>\n"
                + "          </entry-listeners>"
                + "        <merge-policy batch-size=\"23\">CustomMergePolicy</merge-policy>"
                + "  </multimap>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        assertFalse(config.getMultiMapConfigs().isEmpty());

        MultiMapConfig multiMapConfig = config.getMultiMapConfig("myMultiMap");
        assertEquals(2, multiMapConfig.getBackupCount());
        assertEquals(3, multiMapConfig.getAsyncBackupCount());
        assertFalse(multiMapConfig.isBinary());
        assertEquals(MultiMapConfig.ValueCollectionType.SET, multiMapConfig.getValueCollectionType());
        assertEquals(1, multiMapConfig.getEntryListenerConfigs().size());
        assertEquals("com.hazelcast.examples.EntryListener", multiMapConfig.getEntryListenerConfigs().get(0).getClassName());
        assertTrue(multiMapConfig.getEntryListenerConfigs().get(0).isIncludeValue());
        assertTrue(multiMapConfig.getEntryListenerConfigs().get(0).isLocal());

        MergePolicyConfig mergePolicyConfig = multiMapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals("customQuorumRule", multiMapConfig.getQuorumName());
        assertEquals(23, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void testReplicatedMapConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <replicatedmap name=\"foobar\">\n"
                + "        <in-memory-format>BINARY</in-memory-format>\n"
                + "        <async-fillup>false</async-fillup>\n"
                + "        <statistics-enabled>false</statistics-enabled>\n"
                + "        <quorum-ref>CustomQuorumRule</quorum-ref>\n"
                + "        <merge-policy batch-size=\"2342\">CustomMergePolicy</merge-policy>\n"
                + "    </replicatedmap>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("foobar");

        assertFalse(config.getReplicatedMapConfigs().isEmpty());
        assertEquals(InMemoryFormat.BINARY, replicatedMapConfig.getInMemoryFormat());
        assertFalse(replicatedMapConfig.isAsyncFillup());
        assertFalse(replicatedMapConfig.isStatisticsEnabled());
        assertEquals("CustomQuorumRule", replicatedMapConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = replicatedMapConfig.getMergePolicyConfig();
        assertEquals("CustomMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void testListConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <list name=\"foobar\">\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <statistics-enabled>false</statistics-enabled>"
                + "        <max-size>42</max-size>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <merge-policy batch-size=\"100\">SplitBrainMergePolicy</merge-policy>"
                + "        <item-listeners>\n"
                + "            <item-listener include-value=\"true\">com.hazelcast.examples.ItemListener</item-listener>\n"
                + "        </item-listeners>"
                + "    </list>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        ListConfig listConfig = config.getListConfig("foobar");

        assertFalse(config.getListConfigs().isEmpty());
        assertEquals("customQuorumRule", listConfig.getQuorumName());
        assertEquals(42, listConfig.getMaxSize());
        assertEquals(2, listConfig.getBackupCount());
        assertEquals(1, listConfig.getAsyncBackupCount());
        assertEquals(1, listConfig.getItemListenerConfigs().size());
        assertEquals("com.hazelcast.examples.ItemListener", listConfig.getItemListenerConfigs().get(0).getClassName());

        MergePolicyConfig mergePolicyConfig = listConfig.getMergePolicyConfig();
        assertEquals(100, mergePolicyConfig.getBatchSize());
        assertEquals("SplitBrainMergePolicy", mergePolicyConfig.getPolicy());
    }

    @Test
    public void testSetConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <set name=\"foobar\">\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <max-size>42</max-size>"
                + "        <merge-policy batch-size=\"42\">SplitBrainMergePolicy</merge-policy>"
                + "        <item-listeners>\n"
                + "            <item-listener include-value=\"true\">com.hazelcast.examples.ItemListener</item-listener>\n"
                + "          </item-listeners>"
                + "    </set>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        SetConfig setConfig = config.getSetConfig("foobar");

        assertFalse(config.getSetConfigs().isEmpty());
        assertEquals("customQuorumRule", setConfig.getQuorumName());
        assertEquals(2, setConfig.getBackupCount());
        assertEquals(1, setConfig.getAsyncBackupCount());
        assertEquals(42, setConfig.getMaxSize());
        assertEquals(1, setConfig.getItemListenerConfigs().size());
        assertTrue(setConfig.getItemListenerConfigs().get(0).isIncludeValue());
        assertEquals("com.hazelcast.examples.ItemListener", setConfig.getItemListenerConfigs().get(0).getClassName());

        MergePolicyConfig mergePolicyConfig = setConfig.getMergePolicyConfig();
        assertEquals(42, mergePolicyConfig.getBatchSize());
        assertEquals("SplitBrainMergePolicy", mergePolicyConfig.getPolicy());
    }

    @Test
    public void testMapConfig() {
        String xml = HAZELCAST_START_TAG
                + "    <map name=\"foobar\">\n"
                + "        <quorum-ref>customQuorumRule</quorum-ref>"
                + "        <in-memory-format>BINARY</in-memory-format>"
                + "        <statistics-enabled>true</statistics-enabled>"
                + "        <optimize-queries>false</optimize-queries>"
                + "        <cache-deserialized-values>INDEX-ONLY</cache-deserialized-values>"
                + "        <backup-count>2</backup-count>"
                + "        <async-backup-count>1</async-backup-count>"
                + "        <time-to-live-seconds>42</time-to-live-seconds>"
                + "        <max-idle-seconds>42</max-idle-seconds>"
                + "        <eviction-policy>RANDOM</eviction-policy>"
                + "        <max-size policy=\"PER_NODE\">42</max-size>"
                + "        <eviction-percentage>25</eviction-percentage>"
                + "        <min-eviction-check-millis>256</min-eviction-check-millis>"
                + "        <read-backup-data>true</read-backup-data>"
                + "        <hot-restart enabled=\"false\">\n"
                + "            <fsync>false</fsync>\n"
                + "          </hot-restart>"
                + "        <map-store enabled=\"true\" initial-mode=\"LAZY\">\n"
                + "            <class-name>com.hazelcast.examples.DummyStore</class-name>\n"
                + "            <write-delay-seconds>42</write-delay-seconds>\n"
                + "            <write-batch-size>42</write-batch-size>\n"
                + "            <write-coalescing>true</write-coalescing>\n"
                + "            <properties>\n"
                + "                <property name=\"jdbc_url\">my.jdbc.com</property>\n"
                + "            </properties>\n"
                + "          </map-store>"
                + "        <near-cache>\n"
                + "            <max-size>5000</max-size>\n"
                + "            <time-to-live-seconds>42</time-to-live-seconds>\n"
                + "            <max-idle-seconds>42</max-idle-seconds>\n"
                + "            <eviction-policy>LRU</eviction-policy>\n"
                + "            <invalidate-on-change>true</invalidate-on-change>\n"
                + "            <in-memory-format>BINARY</in-memory-format>\n"
                + "            <cache-local-entries>false</cache-local-entries>\n"
                + "            <eviction size=\"1000\" max-size-policy=\"ENTRY_COUNT\" eviction-policy=\"LFU\"/>\n"
                + "          </near-cache>"
                + "        <wan-replication-ref name=\"my-wan-cluster-batch\">\n"
                + "            <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>\n"
                + "            <filters>\n"
                + "                <filter-impl>com.example.SampleFilter</filter-impl>\n"
                + "            </filters>\n"
                + "            <republishing-enabled>false</republishing-enabled>\n"
                + "          </wan-replication-ref>"
                + "        <indexes>\n"
                + "            <index ordered=\"true\">age</index>\n"
                + "          </indexes>"
                + "        <attributes>\n"
                + "            <attribute extractor=\"com.bank.CurrencyExtractor\">currency</attribute>\n"
                + "           </attributes>"
                + "        <partition-lost-listeners>\n"
                + "            <partition-lost-listener>com.your-package.YourPartitionLostListener</partition-lost-listener>\n"
                + "          </partition-lost-listeners>"
                + "        <entry-listeners>\n"
                +
                "            <entry-listener include-value=\"false\" local=\"false\">com.your-package.MyEntryListener</entry-listener>\n"
                + "          </entry-listeners>"
                + "    </map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("foobar");

        assertFalse(config.getMapConfigs().isEmpty());
        assertEquals("customQuorumRule", mapConfig.getQuorumName());
        assertEquals(InMemoryFormat.BINARY, mapConfig.getInMemoryFormat());
        assertTrue(mapConfig.isStatisticsEnabled());
        assertFalse(mapConfig.isOptimizeQueries());
        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
        assertEquals(2, mapConfig.getBackupCount());
        assertEquals(1, mapConfig.getAsyncBackupCount());
        assertEquals(1, mapConfig.getAsyncBackupCount());
        assertEquals(42, mapConfig.getTimeToLiveSeconds());
        assertEquals(42, mapConfig.getMaxIdleSeconds());
        assertEquals(EvictionPolicy.RANDOM, mapConfig.getEvictionPolicy());
        assertEquals(MaxSizeConfig.MaxSizePolicy.PER_NODE, mapConfig.getMaxSizeConfig().getMaxSizePolicy());
        assertEquals(42, mapConfig.getMaxSizeConfig().getSize());
        assertEquals(25, mapConfig.getEvictionPercentage());
        assertEquals(256, mapConfig.getMinEvictionCheckMillis());
        assertTrue(mapConfig.isReadBackupData());
        assertEquals(1, mapConfig.getMapIndexConfigs().size());
        assertEquals("age", mapConfig.getMapIndexConfigs().get(0).getAttribute());
        assertTrue(mapConfig.getMapIndexConfigs().get(0).isOrdered());
        assertEquals(1, mapConfig.getMapAttributeConfigs().size());
        assertEquals("com.bank.CurrencyExtractor", mapConfig.getMapAttributeConfigs().get(0).getExtractor());
        assertEquals("currency", mapConfig.getMapAttributeConfigs().get(0).getName());
        assertEquals(1, mapConfig.getPartitionLostListenerConfigs().size());
        assertEquals("com.your-package.YourPartitionLostListener", mapConfig.getPartitionLostListenerConfigs().get(0).getClassName());
        assertEquals(1, mapConfig.getEntryListenerConfigs().size());
        assertFalse(mapConfig.getEntryListenerConfigs().get(0).isIncludeValue());
        assertFalse(mapConfig.getEntryListenerConfigs().get(0).isLocal());
        assertEquals("com.your-package.MyEntryListener", mapConfig.getEntryListenerConfigs().get(0).getClassName());
        assertFalse(mapConfig.getHotRestartConfig().isEnabled());
        assertFalse(mapConfig.getHotRestartConfig().isFsync());

        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        assertNotNull(mapStoreConfig);
        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, mapStoreConfig.getInitialLoadMode());
        assertEquals(42, mapStoreConfig.getWriteDelaySeconds());
        assertEquals(42, mapStoreConfig.getWriteBatchSize());
        assertTrue(mapStoreConfig.isWriteCoalescing());
        assertEquals("com.hazelcast.examples.DummyStore", mapStoreConfig.getClassName());
        assertEquals(1, mapStoreConfig.getProperties().size());
        assertEquals("my.jdbc.com", mapStoreConfig.getProperties().getProperty("jdbc_url"));

        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        assertNotNull(nearCacheConfig);
        assertEquals(5000, nearCacheConfig.getMaxSize());
        assertEquals(42, nearCacheConfig.getMaxIdleSeconds());
        assertEquals(42, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(InMemoryFormat.BINARY, nearCacheConfig.getInMemoryFormat());
        assertFalse(nearCacheConfig.isCacheLocalEntries());
        assertTrue(nearCacheConfig.isInvalidateOnChange());
        assertEquals(1000, nearCacheConfig.getEvictionConfig().getSize());
        assertEquals(EvictionPolicy.LFU, nearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, nearCacheConfig.getEvictionConfig().getMaximumSizePolicy());

        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        assertNotNull(wanReplicationRef);
        assertFalse(wanReplicationRef.isRepublishingEnabled());
        assertEquals("com.hazelcast.map.merge.PassThroughMergePolicy", wanReplicationRef.getMergePolicy());
        assertEquals(1, wanReplicationRef.getFilters().size());
        assertEquals("com.example.SampleFilter".toLowerCase(), wanReplicationRef.getFilters().get(0).toLowerCase());
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
    public void testCRDTReplicationConfig() {
        final String xml = HAZELCAST_START_TAG
                + "<crdt-replication>\n"
                + "        <max-concurrent-replication-targets>10</max-concurrent-replication-targets>\n"
                + "        <replication-period-millis>2000</replication-period-millis>\n"
                + "</crdt-replication>"
                + HAZELCAST_END_TAG;
        final Config config = new InMemoryXmlConfig(xml);
        final CRDTReplicationConfig replicationConfig = config.getCRDTReplicationConfig();
        assertEquals(10, replicationConfig.getMaxConcurrentReplicationTargets());
        assertEquals(2000, replicationConfig.getReplicationPeriodMillis());
    }

    @Test
    public void testGlobalSerializer() {
        String name = randomName();
        String val = "true";
        String xml = HAZELCAST_START_TAG
                + "  <serialization>\n"
                + "      <serializers>\n"
                + "          <global-serializer override-java-serialization=\"" + val + "\">" + name + "</global-serializer>\n"
                + "      </serializers>\n"
                + "  </serialization>"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        GlobalSerializerConfig globalSerializerConfig = config.getSerializationConfig().getGlobalSerializerConfig();
        assertEquals(name, globalSerializerConfig.getClassName());
        assertTrue(globalSerializerConfig.isOverrideJavaSerialization());
    }

    @Test
    public void testJavaSerializationFilter() {
        String xml = HAZELCAST_START_TAG
                + "  <serialization>\n"
                + "      <java-serialization-filter defaults-disabled='true'>\n"
                + "          <whitelist>\n"
                + "              <class>java.lang.String</class>\n"
                + "              <class>example.Foo</class>\n"
                + "              <package>com.acme.app</package>\n"
                + "              <package>com.acme.app.subpkg</package>\n"
                + "              <prefix>java</prefix>\n"
                + "              <prefix>com.hazelcast.</prefix>\n"
                + "              <prefix>[</prefix>\n"
                + "          </whitelist>\n"
                + "          <blacklist>\n"
                + "              <class>com.acme.app.BeanComparator</class>\n"
                + "          </blacklist>\n"
                + "      </java-serialization-filter>\n"
                + "  </serialization>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        JavaSerializationFilterConfig javaSerializationFilterConfig
                = config.getSerializationConfig().getJavaSerializationFilterConfig();
        assertNotNull(javaSerializationFilterConfig);
        ClassFilter blackList = javaSerializationFilterConfig.getBlacklist();
        assertNotNull(blackList);
        ClassFilter whiteList = javaSerializationFilterConfig.getWhitelist();
        assertNotNull(whiteList);
        assertTrue(whiteList.getClasses().contains("java.lang.String"));
        assertTrue(whiteList.getClasses().contains("example.Foo"));
        assertTrue(whiteList.getPackages().contains("com.acme.app"));
        assertTrue(whiteList.getPackages().contains("com.acme.app.subpkg"));
        assertTrue(whiteList.getPrefixes().contains("java"));
        assertTrue(whiteList.getPrefixes().contains("["));
        assertTrue(blackList.getClasses().contains("com.acme.app.BeanComparator"));
    }

    @Test
    public void testHotRestart() {
        String dir = "/mnt/hot-restart-root/";
        String backupDir = "/mnt/hot-restart-backup/";
        int parallelism = 3;
        int validationTimeout = 13131;
        int dataLoadTimeout = 45454;
        HotRestartClusterDataRecoveryPolicy policy = HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_RECENT;
        String xml = HAZELCAST_START_TAG
                + "<hot-restart-persistence enabled=\"true\">"
                + "    <base-dir>" + dir + "</base-dir>"
                + "    <backup-dir>" + backupDir + "</backup-dir>"
                + "    <parallelism>" + parallelism + "</parallelism>"
                + "    <validation-timeout-seconds>" + validationTimeout + "</validation-timeout-seconds>"
                + "    <data-load-timeout-seconds>" + dataLoadTimeout + "</data-load-timeout-seconds>"
                + "    <cluster-data-recovery-policy>" + policy + "</cluster-data-recovery-policy>"
                + "</hot-restart-persistence>\n"
                + HAZELCAST_END_TAG;

        Config config = new InMemoryXmlConfig(xml);
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();

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

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastInstanceNameEmpty() {
        String xml = HAZELCAST_START_TAG + "<instance-name></instance-name>" + HAZELCAST_END_TAG;
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
    public void testMemberAddressProvider_classNameIsMandatory() {
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

    @Test
    public void testFailureDetector_withProperties() {
        String xml = HAZELCAST_START_TAG
                + "<network>"
                + "  <failure-detector>\n"
                + "            <icmp enabled=\"true\">\n"
                + "                <timeout-milliseconds>42</timeout-milliseconds>\n"
                + "                <fail-fast-on-startup>true</fail-fast-on-startup>\n"
                + "                <interval-milliseconds>4200</interval-milliseconds>\n"
                + "                <max-attempts>42</max-attempts>\n"
                + "                <parallel-mode>true</parallel-mode>\n"
                + "                <ttl>255</ttl>\n"
                + "            </icmp>\n"
                + "  </failure-detector>"
                + "</network>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        NetworkConfig networkConfig = config.getNetworkConfig();
        IcmpFailureDetectorConfig icmpFailureDetectorConfig = networkConfig.getIcmpFailureDetectorConfig();
        assertNotNull(icmpFailureDetectorConfig);

        assertTrue(icmpFailureDetectorConfig.isEnabled());
        assertTrue(icmpFailureDetectorConfig.isParallelMode());
        assertTrue(icmpFailureDetectorConfig.isFailFastOnStartup());
        assertEquals(42, icmpFailureDetectorConfig.getTimeoutMilliseconds());
        assertEquals(42, icmpFailureDetectorConfig.getMaxAttempts());
        assertEquals(4200, icmpFailureDetectorConfig.getIntervalMilliseconds());
    }

    @Test
    public void testHandleMemberAttributes() {
        String xml = HAZELCAST_START_TAG
                + "<member-attributes>\n"
                + "     <attribute name=\"IDENTIFIER\" type=\"string\">ID</attribute>\n"
                + "</member-attributes>"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml);
        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        assertNotNull(memberAttributeConfig);
        assertEquals("ID", memberAttributeConfig.getStringAttribute("IDENTIFIER"));
    }

    @Test
    public void testXsdVersion() {
        String origVersionOverride = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        assertXsdVersion("0.0", "0.0");
        assertXsdVersion("3.9", "3.9");
        assertXsdVersion("3.9-SNAPSHOT", "3.9");
        assertXsdVersion("3.9.1-SNAPSHOT", "3.9");
        assertXsdVersion("3.10", "3.10");
        assertXsdVersion("3.10-SNAPSHOT", "3.10");
        assertXsdVersion("3.10.1-SNAPSHOT", "3.10");
        assertXsdVersion("99.99.99", "99.99");
        assertXsdVersion("99.99.99-SNAPSHOT", "99.99");
        assertXsdVersion("99.99.99-Beta", "99.99");
        assertXsdVersion("99.99.99-Beta-SNAPSHOT", "99.99");
        if (origVersionOverride != null) {
            System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, origVersionOverride);
        } else {
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        }
    }

    private void assertXsdVersion(String buildVersion, String expectedXsdVersion) {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, buildVersion);
        assertEquals("Unexpected release version retrieved for build version " + buildVersion, expectedXsdVersion,
                new XmlConfigBuilder().getReleaseVersion());
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
