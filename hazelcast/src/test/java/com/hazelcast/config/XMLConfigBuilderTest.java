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
import com.hazelcast.core.HazelcastInstance;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class XMLConfigBuilderTest extends HazelcastTestSupport {

    public static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";

    @Test
    public void testConfigurationURL() throws IOException {
        URL configURL = getClass().getClassLoader().getResource("hazelcast-default.xml");
        Config config = new XmlConfigBuilder(configURL).build();
        assertEquals(configURL, config.getConfigurationUrl());
    }

    @Test
    public void testConfigurationWithFileName() throws Exception {
        File file = File.createTempFile("foo", "bar");
        file.deleteOnExit();
        String xml =
                HAZELCAST_START_TAG +
                        "    <group>\n" +
                        "        <name>foobar</name>\n" +
                        "        <password>dev-pass</password>\n" +
                        "    </group>\n" +
                        "</hazelcast>\n";
        final Writer writer = new PrintWriter(file, "UTF-8");
        writer.write(xml);
        writer.close();

        Config config = new XmlConfigBuilder(file.getAbsolutePath()).build();
        assertEquals(file, config.getConfigurationFile());
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
        buildConfig(xml);
    }

    @Test
    public void testSecurityInterceptorConfig() {
        String xml =
                HAZELCAST_START_TAG +
                        "<security enabled=\"true\">" +
                        "<security-interceptors>" +
                        "<interceptor class-name=\"foo\"/>" +
                        "<interceptor class-name=\"bar\"/>" +
                        "</security-interceptors>" +
                        "</security>" +
                        "</hazelcast>";

        final Config config = buildConfig(xml);
        final SecurityConfig securityConfig = config.getSecurityConfig();
        final List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();
        assertEquals(2, interceptorConfigs.size());
        assertEquals("foo", interceptorConfigs.get(0).className);
        assertEquals("bar", interceptorConfigs.get(1).className);
    }

    @Test
    public void readAwsConfig() {
        String xml =
                HAZELCAST_START_TAG +
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
        Config config = buildConfig(HAZELCAST_START_TAG +
                "    <network>\n" +
                "        <port port-count=\"200\">5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertEquals(200, config.getNetworkConfig().getPortCount());

        //check if the default is passed in correctly
        config = buildConfig(HAZELCAST_START_TAG +
                "    <network>\n" +
                "        <port>5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertEquals(100, config.getNetworkConfig().getPortCount());
    }

    @Test
    public void readPortAutoIncrement() {
        //explicitly set.
        Config config = buildConfig(HAZELCAST_START_TAG +
                "    <network>\n" +
                "        <port auto-increment=\"false\">5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertFalse(config.getNetworkConfig().isPortAutoIncrement());

        //check if the default is picked up correctly
        config = buildConfig(HAZELCAST_START_TAG +
                "    <network>\n" +
                "        <port>5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
    }

    @Test
    public void networkReuseAddress() {
        Config config = buildConfig(HAZELCAST_START_TAG +
                "    <network>\n" +
                "        <reuse-address>true</reuse-address>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertTrue(config.getNetworkConfig().isReuseAddress());
    }

    @Test
    public void readSemaphoreConfig() {
        String xml =
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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

    private static void testConfig2Xml2Config(String fileName) {
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Test
    public void testNotEnabledManagementCenterConfig() {
        String xml =
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
                        "<map name=\"mymap1\">" +
                        "<optimize-queries>true</optimize-queries>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config1 = buildConfig(xml1);
        final MapConfig mapConfig1 = config1.getMapConfig("mymap1");
        assertEquals(CacheDeserializedValues.ALWAYS, mapConfig1.getCacheDeserializedValues());

        String xml2 =
                HAZELCAST_START_TAG +
                        "<map name=\"mymap2\">" +
                        "<optimize-queries>false</optimize-queries>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config2 = buildConfig(xml2);
        final MapConfig mapConfig2 = config2.getMapConfig("mymap2");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig2.getCacheDeserializedValues());
    }

    @Test
    public void testMapConfig_cacheValueConfig_defaultValue() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">" +
                        "<map name=\"mymap\">" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("mymap");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Test
    public void testMapConfig_cacheValueConfig_never() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">" +
                        "<map name=\"mymap\">" +
                        "<cache-deserialized-values>NEVER</cache-deserialized-values>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("mymap");
        assertEquals(CacheDeserializedValues.NEVER, mapConfig.getCacheDeserializedValues());
    }

    @Test
    public void testMapConfig_cacheValueConfig_always() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">" +
                        "<map name=\"mymap\">" +
                        "<cache-deserialized-values>ALWAYS</cache-deserialized-values>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("mymap");
        assertEquals(CacheDeserializedValues.ALWAYS, mapConfig.getCacheDeserializedValues());
    }

    @Test
    public void testMapConfig_cacheValueConfig_indexOnly() {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">" +
                        "<map name=\"mymap\">" +
                        "<cache-deserialized-values>INDEX-ONLY</cache-deserialized-values>" +
                        "</map>" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("mymap");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapConfig.getCacheDeserializedValues());
    }

    @Test
    public void testMapStoreInitialModeEager() {
        String xml =
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
        return HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
    public void testNearCacheFullConfig() {
        String mapName = "testNearCacheFullConfig";
        String xml =
                HAZELCAST_START_TAG +
                        "  <map name=\"" + mapName + "\">\n" +
                        "    <near-cache name=\"test\">\n" +
                        "      <in-memory-format>OBJECT</in-memory-format>\n" +
                        "      <max-size>1234</max-size>\n" +
                        "      <time-to-live-seconds>77</time-to-live-seconds>\n" +
                        "      <max-idle-seconds>92</max-idle-seconds>\n" +
                        "      <eviction-policy>LFU</eviction-policy>\n" +
                        "      <invalidate-on-change>false</invalidate-on-change>\n" +
                        "      <cache-local-entries>false</cache-local-entries>\n" +
                        "      <eviction eviction-policy=\"LRU\" max-size-policy=\"ENTRY_COUNT\" size=\"3333\"/>\n" +
                        "    </near-cache>\n" +
                        "  </map>\n" +
                        "</hazelcast>";
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
        String xml =
                HAZELCAST_START_TAG +
                        "  <map name=\"" + mapName + "\">\n" +
                        "    <wan-replication-ref name=\"test\">\n" +
                        "      <merge-policy>TestMergePolicy</merge-policy>\n" +
                        "      <filters>\n" +
                        "        <filter-impl>com.example.SampleFilter</filter-impl>\n" +
                        "      </filters>\n" +
                        "    </wan-replication-ref>\n" +
                        "  </map>\n" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        WanReplicationRef wanRef = config.getMapConfig(mapName).getWanReplicationRef();
        assertEquals(refName, wanRef.getName());
        assertEquals(mergePolicy, wanRef.getMergePolicy());
        assertTrue(wanRef.isRepublishingEnabled());
        assertEquals(1, wanRef.getFilters().size());
        assertEquals("com.example.SampleFilter", wanRef.getFilters().get(0));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testParseExceptionIsNotSwallowed() {
        String invalidXml =
                HAZELCAST_START_TAG +
                        "</hazelcast";
        buildConfig(invalidXml);
        fail(); //if we, for any reason, we get through the parsing, fail.
    }

    @Test
    public void setMapStoreConfigImplementationTest() {
        String mapName = "mapStoreImpObjTest";
        String xml =
                HAZELCAST_START_TAG +
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
        return HAZELCAST_START_TAG +
                "<map name=\"" + mapName + "\">\n" +
                "<partition-lost-listeners>\n" +
                "<partition-lost-listener>" + listenerName + "</partition-lost-listener>\n" +
                "</partition-lost-listeners>\n" +
                "</map>\n" +
                "</hazelcast>\n";
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
        return HAZELCAST_START_TAG +
                "<cache name=\"" + cacheName + "\">\n" +
                "<partition-lost-listeners>\n" +
                "<partition-lost-listener>" + listenerName + "</partition-lost-listener>\n" +
                "</partition-lost-listeners>\n" +
                "</cache>\n" +
                "</hazelcast>\n";
    }

    private void testXSDConfigXML(String xmlFileName) throws SAXException, IOException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-config-3.6.xsd");
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
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
                        "<wan-replication name=\"my-wan-cluster\" snapshot-enabled=\"true\">\n" +
                        "    <target-cluster group-name=\"test-cluster-1\" group-password=\"test-pass\">\n" +
                        "       <replication-impl>com.hazelcast.enterprise.wan.replication.WanBatchReplication</replication-impl>\n" +
                        "       <end-points>\n" +
                        "          <address>20.30.40.50:5701</address>\n" +
                        "          <address>20.30.40.50:5702</address>\n" +
                        "       </end-points>\n" +
                        "       <acknowledge-type>ACK_ON_RECEIPT</acknowledge-type>\n" +
                        "       <queue-full-behavior>THROW_EXCEPTION</queue-full-behavior>\n" +
                        "       <batch-size>7</batch-size>" +
                        "       <batch-max-delay-millis>14</batch-max-delay-millis>\n" +
                        "       <queue-capacity>21</queue-capacity>\n" +
                        "       <response-timeout-millis>28</response-timeout-millis>\n" +
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
        assertEquals(WanAcknowledgeType.ACK_ON_RECEIPT, targetClusterConfig.getAcknowledgeType());
        assertEquals(WANQueueFullBehavior.THROW_EXCEPTION, targetClusterConfig.getQueueFullBehavior());
        assertEquals(7, targetClusterConfig.getBatchSize());
        assertEquals(14, targetClusterConfig.getBatchMaxDelayMillis());
        assertEquals(21, targetClusterConfig.getQueueCapacity());
        assertEquals(28, targetClusterConfig.getResponseTimeoutMillis());
    }

    @Test
    public void testQuorumConfig() throws Exception {
        String xml =
                HAZELCAST_START_TAG +
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
                HAZELCAST_START_TAG +
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
    public void testIndexesConfig() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <map name=\"people\">\n" +
                        "       <indexes>\n" +
                        "           <index ordered=\"false\">name</index>\n" +
                        "           <index ordered=\"true\">age</index>\n" +
                        "       </indexes>" +
                        "   </map>" +
                        "</hazelcast>";
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
    public void testAttributeConfig() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <map name=\"people\">\n" +
                        "       <attributes>\n" +
                        "           <attribute extractor=\"com.car.PowerExtractor\">power</attribute>\n" +
                        "           <attribute extractor=\"com.car.WeightExtractor\">weight</attribute>\n" +
                        "       </attributes>" +
                        "   </map>" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        MapConfig mapConfig = config.getMapConfig("people");
        assertFalse(mapConfig.getMapAttributeConfigs().isEmpty());
        assertAttributeEqual("power", "com.car.PowerExtractor", mapConfig.getMapAttributeConfigs().get(0));
        assertAttributeEqual("weight", "com.car.WeightExtractor", mapConfig.getMapAttributeConfigs().get(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_emptyTag() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <map name=\"people\">\n" +
                        "       <attributes>\n" +
                        "           <attribute extractor=\"com.car.WeightExtractor\"></attribute>\n" +
                        "       </attributes>" +
                        "   </map>" +
                        "</hazelcast>";
        buildConfig(xml);
    }

    private static void assertAttributeEqual(String expectedName, String expectedExtractor, MapAttributeConfig attributeConfig) {
        assertEquals(expectedName, attributeConfig.getName());
        assertEquals(expectedExtractor, attributeConfig.getExtractor());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_singleTag() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <map name=\"people\">\n" +
                        "       <attributes>\n" +
                        "           <attribute extractor=\"com.car.WeightExtractor\"/>\n" +
                        "       </attributes>" +
                        "   </map>" +
                        "</hazelcast>";
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_noExtractor() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <map name=\"people\">\n" +
                        "       <attributes>\n" +
                        "           <attribute></attribute>\n" +
                        "       </attributes>" +
                        "   </map>" +
                        "</hazelcast>";
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_noExtractor_singleTag() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <map name=\"people\">\n" +
                        "       <attributes>\n" +
                        "           <attribute/>\n" +
                        "       </attributes>" +
                        "   </map>" +
                        "</hazelcast>";
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noExtractor() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <map name=\"people\">\n" +
                        "       <attributes>\n" +
                        "           <attribute>weight</attribute>\n" +
                        "       </attributes>" +
                        "   </map>" +
                        "</hazelcast>";
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_emptyExtractor() throws Exception {
        String xml =
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "   <map name=\"people\">\n" +
                        "       <attributes>\n" +
                        "           <attribute extractor=\"\">weight</attribute>\n" +
                        "       </attributes>" +
                        "   </map>" +
                        "</hazelcast>";
        buildConfig(xml);
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
        assertEquals(LRU, queryCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(ENTRY_COUNT, queryCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(133, queryCacheConfig.getEvictionConfig().getSize());
    }

    @Test
    public void testLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG +
                "    <lite-member enabled=\"true\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        final Config config = configBuilder.build();
        assertTrue(config.isLiteMember());
    }

    @Test
    public void testNonLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG +
                "    <lite-member enabled=\"false\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        final Config config = configBuilder.build();
        assertFalse(config.isLiteMember());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNonLiteMemberConfigWithoutEnabledField() {
        String xml = HAZELCAST_START_TAG +
                "    <lite-member/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG +
                "    <lite-member enabled=\"dummytext\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testDuplicateLiteMemberConfig() {
        String xml = HAZELCAST_START_TAG +
                "    <lite-member enabled=\"true\"/>\n" +
                "    <lite-member enabled=\"true\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
        fail();
    }

    private void assertIndexesEqual(QueryCacheConfig queryCacheConfig) {
        Iterator<MapIndexConfig> iterator = queryCacheConfig.getIndexConfigs().iterator();
        while (iterator.hasNext()) {
            MapIndexConfig mapIndexConfig = iterator.next();
            assertEquals("name", mapIndexConfig.getAttribute());
            assertFalse(mapIndexConfig.isOrdered());
        }
    }

    @Test
    public void testMapNativeMaxSizePolicy() {
        String xmlFormat = HAZELCAST_START_TAG +
                "<map name=\"mymap\">" +
                "<in-memory-format>NATIVE</in-memory-format>" +
                "<max-size policy=\"{0}\">9991</max-size>" +
                "</map>" +
                "</hazelcast>";
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
        String xml = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "<instance-name>" + name + "</instance-name>\n" +
                "</hazelcast>";

        Config config = new InMemoryXmlConfig(xml);
        assertEquals(name, config.getInstanceName());
    }

    @Test
    public void testGlobalSerializer() {
        String name = randomName();
        String val = "true";
        String xml = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "<serialization><serializers><global-serializer override-java-serialization=\"" + val + "\">" + name
                + "</global-serializer></serializers></serialization>" + "</hazelcast>";

        Config config = new InMemoryXmlConfig(xml);
        GlobalSerializerConfig globalSerializerConfig = config.getSerializationConfig().getGlobalSerializerConfig();
        globalSerializerConfig.getClassName();
        assertEquals(name, globalSerializerConfig.getClassName());
        assertTrue(globalSerializerConfig.isOverrideJavaSerialization());
    }

    @Test
    public void testHotRestart() {
        String dir = "/mnt/hot-restart-root/";
        int validationTimeout = 13131;
        int dataLoadTimeout = 45454;
        String xml = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                "<hot-restart-persistence enabled=\"true\">"
                + "<base-dir>" + dir + "</base-dir>"
                + "<validation-timeout-seconds>" + validationTimeout + "</validation-timeout-seconds>"
                + "<data-load-timeout-seconds>" + dataLoadTimeout + "</data-load-timeout-seconds>"
                + "</hot-restart-persistence>\n" +
                "</hazelcast>";

        Config config = new InMemoryXmlConfig(xml);
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        assertTrue(hotRestartPersistenceConfig.isEnabled());
        assertEquals(new File(dir).getAbsolutePath(), hotRestartPersistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(validationTimeout, hotRestartPersistenceConfig.getValidationTimeoutSeconds());
        assertEquals(dataLoadTimeout, hotRestartPersistenceConfig.getDataLoadTimeoutSeconds());
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
        String xml = HAZELCAST_START_TAG + "</hazelcast>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastTagAppearsTwice() {
        String xml = HAZELCAST_START_TAG + "<hazelcast/></hazelcast>";
        buildConfig(xml);
    }
}
