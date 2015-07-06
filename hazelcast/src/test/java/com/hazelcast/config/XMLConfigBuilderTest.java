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

package com.hazelcast.config;

import com.hazelcast.config.helpers.DummyMapStore;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.quorum.QuorumType;
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
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

//it needs to run serial because some tests are relying on System properties they are setting themselves.
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XMLConfigBuilderTest extends HazelcastTestSupport {

    @After
    @Before
    public void after() {
        System.clearProperty("hazelcast.config");
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() throws IOException {
        File file = File.createTempFile("foo", "bar");
        file.delete();
        System.setProperty("hazelcast.config", file.getAbsolutePath());
        new XmlConfigBuilder();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testJoinValidation() {
        String xml = "<hazelcast>\n" +
                "    <network>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"true\"/>\n" +
                "            <tcp-ip enabled=\"true\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test
    public void testSecurityInterceptorConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">" +
                        "<security enabled=\"true\">" +
                        "<security-interceptors>" +
                        "<interceptor class-name=\"foo\"/>" +
                        "<interceptor class-name=\"bar\"/>" +
                        "</security-interceptors>" +
                        "</security>" +
                        "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);

        final Config config = configBuilder.build();
        final SecurityConfig securityConfig = config.getSecurityConfig();
        final List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();
        assertEquals(2, interceptorConfigs.size());
        assertEquals("foo", interceptorConfigs.get(0).className);
        assertEquals("bar", interceptorConfigs.get(1).className);
    }

    @Test
    public void loadingThroughSystemProperty_existingFile() throws IOException {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "    <group>\n" +
                        "        <name>foobar</name>\n" +
                        "        <password>dev-pass</password>\n" +
                        "    </group>" +
                        "</hazelcast>";

        File file = File.createTempFile("foo", "bar");
        file.deleteOnExit();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(xml);
        writer.close();

        System.setProperty("hazelcast.config", file.getAbsolutePath());

        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() throws IOException {
        System.setProperty("hazelcast.config", "classpath:idontexist");
        new XmlConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() throws IOException {
        System.setProperty("hazelcast.config", "classpath:test-hazelcast.xml");

        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
    }

    @Test
    public void testCleanNodeName() {
        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        assertEquals("nocolon", configBuilder.cleanNodeName("noColon"));
        assertEquals("after", configBuilder.cleanNodeName("Before:After"));
        assertNull(configBuilder.cleanNodeName((String) null));
    }

    @Test
    public void readAwsConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <group>\n" +
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
                        "                <access-key>access</access-key>\n" +
                        "                <secret-key>secret</secret-key>\n" +
                        "            </aws>\n" +
                        "        </join>\n" +
                        "        <interfaces enabled=\"false\">\n" +
                        "            <interface>10.10.1.*</interface>\n" +
                        "        </interfaces>\n" +
                        "    </network>\n" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        AwsConfig awsConfig = config.getNetworkConfig().getJoin().getAwsConfig();
        assertTrue(awsConfig.isEnabled());
        assertEquals(10, config.getNetworkConfig().getJoin().getAwsConfig().getConnectionTimeoutSeconds());
        assertEquals("access", awsConfig.getAccessKey());
        assertEquals("secret", awsConfig.getSecretKey());
    }

    @Test
    public void readPortCount() {
        //check when it is explicitly set.
        Config config = buildConfig("<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "    <network>\n" +
                "        <port port-count=\"200\">5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertEquals(200, config.getNetworkConfig().getPortCount());

        //check if the default is passed in correctly
        config = buildConfig("<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "    <network>\n" +
                "        <port>5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertEquals(100, config.getNetworkConfig().getPortCount());
    }

    @Test
    public void readPortAutoIncrement() {
        //explicitly set.
        Config config = buildConfig("<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "    <network>\n" +
                "        <port auto-increment=\"false\">5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertFalse(config.getNetworkConfig().isPortAutoIncrement());

        //check if the default is picked up correctly
        config = buildConfig("<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "    <network>\n" +
                "        <port>5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
    }

    @Test
    public void networkReuseAddress() {
        Config config = buildConfig("<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "    <network>\n" +
                "        <reuse-address>true</reuse-address>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertTrue(config.getNetworkConfig().isReuseAddress());
    }

    @Test
    public void readSemaphoreConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "    <semaphore name=\"default\">\n" +
                        "        <initial-permits>1</initial-permits>\n" +
                        "    </semaphore>" +
                        "    <semaphore name=\"custom\">\n" +
                        "        <initial-permits>10</initial-permits>\n" +
                        "    </semaphore>" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        SemaphoreConfig defaultConfig = config.getSemaphoreConfig("default");
        SemaphoreConfig customConfig = config.getSemaphoreConfig("custom");
        assertEquals(1, defaultConfig.getInitialPermits());
        assertEquals(10, customConfig.getInitialPermits());
    }

    @Test
    public void readReliableTopic() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "    <reliable-topic name=\"custom\">\n" +
                        "           <read-batch-size>35</read-batch-size>\n" +
                        "           <statistics-enabled>false</statistics-enabled>\n" +
                        "           <topic-overload-policy>DISCARD_OLDEST</topic-overload-policy>\n" +
                        "           <message-listeners>" +
                        "               <message-listener>MessageListenerImpl</message-listener>" +
                        "           </message-listeners>" +
                        "    </reliable-topic>\n" +
                        "</hazelcast>";

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
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "    <ringbuffer name=\"custom\">\n" +
                        "        <capacity>10</capacity>\n" +
                        "        <backup-count>2</backup-count>\n" +
                        "        <async-backup-count>1</async-backup-count>\n" +
                        "        <time-to-live-seconds>9</time-to-live-seconds>\n" +
                        "        <in-memory-format>OBJECT</in-memory-format>\n" +
                        "    </ringbuffer>" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        RingbufferConfig ringbufferConfig = config.getRingbufferConfig("custom");
        assertEquals(10, ringbufferConfig.getCapacity());
        assertEquals(2, ringbufferConfig.getBackupCount());
        assertEquals(1, ringbufferConfig.getAsyncBackupCount());
        assertEquals(9, ringbufferConfig.getTimeToLiveSeconds());
        assertEquals(InMemoryFormat.OBJECT, ringbufferConfig.getInMemoryFormat());
    }

    @Test
    public void testConfig2Xml2DefaultConfig() {
        testConfig2Xml2Config("hazelcast-default.xml");
    }

    @Test
    public void testConfig2Xml2FullConfig() {
        testConfig2Xml2Config("hazelcast-fullconfig.xml");
    }

    private void testConfig2Xml2Config(String fileName) {
        String pass = "password";
        final Config config = new ClasspathXmlConfig(fileName);
        config.getGroupConfig().setPassword(pass);

        final String xml = new ConfigXmlGenerator(true).generate(config);
        final Config config2 = new InMemoryXmlConfig(xml);
        config2.getGroupConfig().setPassword(pass);

        assertTrue(config.isCompatible(config2));
        assertTrue(config2.isCompatible(config));
    }

    @Test
    public void testXSDDefaultXML() throws SAXException, IOException {
        testXSDConfigXML("hazelcast-default.xml");
    }

    @Test
    public void testFullConfigXML() throws SAXException, IOException {
        testXSDConfigXML("hazelcast-fullconfig.xml");
    }

    @Test
    public void testCaseInsensitivityOfSettings() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<map name=\"testCaseInsensitivity\">" +
                        "<in-memory-format>BINARY</in-memory-format>     " +
                        "<backup-count>1</backup-count>                 " +
                        "<async-backup-count>0</async-backup-count>    " +
                        "<time-to-live-seconds>0</time-to-live-seconds>" +
                        "<max-idle-seconds>0</max-idle-seconds>    " +
                        "<eviction-policy>NONE</eviction-policy>  " +
                        "<max-size policy=\"per_partition\">0</max-size>" +
                        "<eviction-percentage>25</eviction-percentage>" +
                        "<merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("testCaseInsensitivity");
        assertTrue(mapConfig.getInMemoryFormat().equals(InMemoryFormat.BINARY));
        assertTrue(mapConfig.getEvictionPolicy().equals(EvictionPolicy.NONE));
        assertTrue(mapConfig.getMaxSizeConfig().getMaxSizePolicy().equals(MaxSizeConfig.MaxSizePolicy.PER_PARTITION));
    }


    @Test
    public void testManagementCenterConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<management-center enabled=\"true\">" +
                        "someUrl" +
                        "</management-center>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertTrue(manCenterCfg.isEnabled());
        assertEquals("someUrl", manCenterCfg.getUrl());
    }

    @Test
    public void testNullManagementCenterConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<management-center>" +
                        "</management-center>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Test
    public void testEmptyManagementCenterConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Test
    public void testNotEnabledManagementCenterConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<management-center enabled=\"false\">" +
                        "</management-center>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Test
    public void testNotEnabledWithURLManagementCenterConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<management-center enabled=\"false\">" +
                        "http://localhost:8080/mancenter" +
                        "</management-center>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertEquals("http://localhost:8080/mancenter", manCenterCfg.getUrl());
    }

    @Test
    public void testMapStoreInitialModeLazy() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<map name=\"mymap\">" +
                        "<map-store enabled=\"true\" initial-mode=\"LAZY\"></map-store>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        final MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();
        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, mapStoreConfig.getInitialLoadMode());
    }

    @Test
    public void testMapConfig_minEvictionCheckMillis() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<map name=\"mymap\">" +
                        "<min-eviction-check-millis>123456789</min-eviction-check-millis>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("mymap");
        assertEquals(123456789L, mapConfig.getMinEvictionCheckMillis());
    }

    @Test
    public void testMapConfig_minEvictionCheckMillis_defaultValue() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<map name=\"mymap\">" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("mymap");
        assertEquals(MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS, mapConfig.getMinEvictionCheckMillis());
    }

    @Test
    public void testMapConfig_optimizeQueries() {
        String xml1 =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">" +
                        "<map name=\"mymap1\">" +
                        "<optimize-queries>true</optimize-queries>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config1 = buildConfig(xml1);
        final MapConfig mapConfig1 = config1.getMapConfig("mymap1");
        assertTrue(mapConfig1.isOptimizeQueries());

        String xml2 =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">" +
                        "<map name=\"mymap2\">" +
                        "<optimize-queries>false</optimize-queries>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config2 = buildConfig(xml2);
        final MapConfig mapConfig2 = config2.getMapConfig("mymap2");
        assertFalse(mapConfig2.isOptimizeQueries());
    }

    @Test
    public void testMapConfig_optimizeQueries_defaultValue() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">" +
                        "<map name=\"mymap\">" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("mymap");
        assertFalse(mapConfig.isOptimizeQueries());
    }

    @Test
    public void testMapStoreInitialModeEager() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<map name=\"mymap\">" +
                        "<map-store enabled=\"true\" initial-mode=\"EAGER\"></map-store>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        final MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();
        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.EAGER, mapStoreConfig.getInitialLoadMode());
    }

    @Test
    public void testMapStoreWriteBatchSize() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<map name=\"mymap\">" +
                        "<map-store >" +
                        "<write-batch-size>23</write-batch-size>" +
                        "</map-store>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        final MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();
        assertEquals(23, mapStoreConfig.getWriteBatchSize());
    }

    @Test
    public void testMapStoreConfig_writeCoalescing_whenDefault() {
        boolean writeCoalescingEnabled = MapStoreConfig.DEFAULT_WRITE_COALESCING;
        final MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(writeCoalescingEnabled, true);

        assertTrue(mapStoreConfig.isWriteCoalescing());
    }

    @Test
    public void testMapStoreConfig_writeCoalescing_whenSetFalse() {
        boolean writeCoalescingEnabled = false;
        final MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(writeCoalescingEnabled, false);

        assertFalse(mapStoreConfig.isWriteCoalescing());
    }

    @Test
    public void testMapStoreConfig_writeCoalescing_whenSetTrue() {
        boolean writeCoalescingEnabled = true;
        final MapStoreConfig mapStoreConfig = getWriteCoalescingMapStoreConfig(writeCoalescingEnabled, false);

        assertTrue(mapStoreConfig.isWriteCoalescing());
    }

    private MapStoreConfig getWriteCoalescingMapStoreConfig(boolean writeCoalescing, boolean useDefault) {
        String xml = getWriteCoalescingConfigXml(writeCoalescing, useDefault);
        final Config config = buildConfig(xml);
        return config.getMapConfig("mymap").getMapStoreConfig();
    }


    private String getWriteCoalescingConfigXml(boolean value, boolean useDefault) {
        String writeCoalescingConfigPart = useDefault ? ""
                : "<write-coalescing>" + String.valueOf(value) + "</write-coalescing>";
        return "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "<map name=\"mymap\">" +
                "<map-store >" +
                writeCoalescingConfigPart +
                "</map-store>" +
                "</map>" +
                "</hazelcast>";
    }

    @Test
    public void testNearCacheInMemoryFormat() {
        String mapName = "testMapNearCacheInMemoryFormat";
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "  <map name=\"" + mapName + "\">\n" +
                        "    <near-cache>\n" +
                        "      <in-memory-format>OBJECT</in-memory-format>\n" +
                        "    </near-cache>\n" +
                        "  </map>\n" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        MapConfig mapConfig = config.getMapConfig(mapName);
        NearCacheConfig ncConfig = mapConfig.getNearCacheConfig();
        assertEquals(InMemoryFormat.OBJECT, ncConfig.getInMemoryFormat());
    }

    @Test
    public void testMapWanReplicationRef() {
        String mapName = "testMapWanReplicationRef";
        String refName = "test";
        String mergePolicy = "TestMergePolicy";
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "  <map name=\"" + mapName + "\">\n" +
                        "    <wan-replication-ref name=\"test\">\n" +
                        "      <merge-policy>TestMergePolicy</merge-policy>\n" +
                        "    </wan-replication-ref>\n" +
                        "  </map>\n" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        WanReplicationRef wanRef = config.getMapConfig(mapName).getWanReplicationRef();
        assertEquals(refName, wanRef.getName());
        assertEquals(mergePolicy, wanRef.getMergePolicy());
        assertTrue(wanRef.isRepublishingEnabled());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testParseExceptionIsNotSwallowed() {
        String invalidXml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "</hazelcast";
        buildConfig(invalidXml);
        fail(); //if we, for any reason, we get through the parsing, fail.
    }

    @Test
    public void setMapStoreConfigImplementationTest() {
        String mapName = "mapStoreImpObjTest";
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<map name=\"" + mapName + "\">\n" +
                        "<map-store enabled=\"true\">\n" +
                        "<class-name>com.hazelcast.config.helpers.DummyMapStore</class-name>\n" +
                        "<write-delay-seconds>5</write-delay-seconds>\n" +
                        "</map-store>\n" +
                        "</map>\n" +
                        "</hazelcast>\n";

        Config config = buildConfig(xml);
        HazelcastInstance hz = createHazelcastInstance(config);
        hz.getMap(mapName);

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
        return "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "<map name=\"" + mapName + "\">\n" +
                "<partition-lost-listeners>\n" +
                "<partition-lost-listener>" + listenerName + "</partition-lost-listener>\n" +
                "</partition-lost-listeners>\n" +
                "</map>\n" +
                "</hazelcast>\n";
    }

    private void testXSDConfigXML(String xmlFileName) throws SAXException, IOException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-config-3.5.xsd");
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
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <group>\n" +
                        "        <name>dev</name>\n" +
                        "        <password>dev-pass</password>\n" +
                        "    </group>\n" +
                        "    <network>\n" +
                        "        <port auto-increment=\"true\">5701</port>\n" +
                        "        <join>\n" +
                        "            <multicast enabled=\"true\" loopbackModeEnabled=\"true\">\n" +
                        "                <multicast-group>224.2.2.3</multicast-group>\n" +
                        "                <multicast-port>54327</multicast-port>\n" +
                        "            </multicast>\n" +
                        "            <tcp-ip enabled=\"false\">\n" +
                        "                <interface>127.0.0.1</interface>\n" +
                        "            </tcp-ip>\n" +
                        "            <aws enabled=\"false\" connection-timeout-seconds=\"10\" >\n" +
                        "                <access-key>access</access-key>\n" +
                        "                <secret-key>secret</secret-key>\n" +
                        "            </aws>\n" +
                        "        </join>\n" +
                        "        <interfaces enabled=\"false\">\n" +
                        "            <interface>10.10.1.*</interface>\n" +
                        "        </interfaces>\n" +
                        "    </network>\n" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        MulticastConfig mcastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        assertTrue(mcastConfig.isEnabled());
        assertTrue(mcastConfig.isLoopbackModeEnabled());
        assertEquals("224.2.2.3", mcastConfig.getMulticastGroup());
        assertEquals(54327, mcastConfig.getMulticastPort());
    }

    @Test
    public void testWanConfig() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "<wan-replication name=\"my-wan-cluster\" snapshot-enabled=\"true\">\n" +
                        "    <target-cluster group-name=\"test-cluster-1\" group-password=\"test-pass\">\n" +
                        "       <replication-impl>com.hazelcast.enterprise.wan.replication.WanBatchReplication</replication-impl>\n" +
                        "       <end-points>\n" +
                        "          <address>20.30.40.50:5701</address>\n" +
                        "          <address>20.30.40.50:5702</address>\n" +
                        "       </end-points>\n" +
                        "    </target-cluster>\n" +
                        "</wan-replication>\n" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        WanReplicationConfig wanConfig = config.getWanReplicationConfig("my-wan-cluster");
        assertNotNull(wanConfig);
        assertTrue(wanConfig.isSnapshotEnabled());
        List<WanTargetClusterConfig> targetClusterConfigs = wanConfig.getTargetClusterConfigs();
        assertEquals(1, targetClusterConfigs.size());
        WanTargetClusterConfig targetClusterConfig = targetClusterConfigs.get(0);
        assertEquals("test-cluster-1", targetClusterConfig.getGroupName());
        assertEquals("test-pass", targetClusterConfig.getGroupPassword());
        List<String> targetEndpoints = targetClusterConfig.getEndpoints();
        assertEquals(2, targetEndpoints.size());
        assertTrue(targetEndpoints.contains("20.30.40.50:5701"));
        assertTrue(targetEndpoints.contains("20.30.40.50:5702"));
    }

    @Test
    public void testQuorumConfig() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "      <quorum enabled=\"true\" name=\"myQuorum\">\n" +
                        "        <quorum-size>3</quorum-size>\n" +
                        "        <quorum-function-class-name>com.my.quorum.function</quorum-function-class-name>\n" +
                        "        <quorum-type>READ</quorum-type>\n" +
                        "      </quorum>\n" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertTrue("quorum should be enabled", quorumConfig.isEnabled());
        assertEquals(3, quorumConfig.getSize());
        assertEquals(QuorumType.READ, quorumConfig.getType());
        assertEquals("com.my.quorum.function", quorumConfig.getQuorumFunctionClassName());
        assertTrue(quorumConfig.getListenerConfigs().isEmpty());
    }

    @Test
    public void testQuorumListenerConfig() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "      <quorum enabled=\"true\" name=\"myQuorum\">\n" +
                        "        <quorum-size>3</quorum-size>\n" +
                        "        <quorum-listeners>" +
                        "           <quorum-listener>com.abc.my.quorum.listener</quorum-listener>" +
                        "           <quorum-listener>com.abc.my.second.listener</quorum-listener>" +
                        "       </quorum-listeners> " +
                        "    </quorum>\n" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        QuorumConfig quorumConfig = config.getQuorumConfig("myQuorum");
        assertFalse(quorumConfig.getListenerConfigs().isEmpty());
        assertEquals("com.abc.my.quorum.listener", quorumConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("com.abc.my.second.listener", quorumConfig.getListenerConfigs().get(1).getClassName());

    }

    @Test
    public void testQueryCacheFullConfig() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">"
                        + "<map name=\"test\">"
                        + "<query-caches>"
                        + "<query-cache name=\"cache-name\">"
                        + "<entry-listeners>"
                        + "<entry-listener include-value=\"true\" " +
                        "local=\"false\">com.hazelcast.examples.EntryListener</entry-listener>"
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
                        + "</hazelcast>";
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
        assertEquals(EvictionPolicy.LRU, queryCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, queryCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(133, queryCacheConfig.getEvictionConfig().getSize());
    }

    private void assertIndexesEqual(QueryCacheConfig queryCacheConfig) {
        Iterator<MapIndexConfig> iterator = queryCacheConfig.getIndexConfigs().iterator();
        while (iterator.hasNext()) {
            MapIndexConfig mapIndexConfig = iterator.next();
            assertEquals("name", mapIndexConfig.getAttribute());
            assertFalse(mapIndexConfig.isOrdered());
        }
    }
}
