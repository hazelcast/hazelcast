/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.io.FileOutputStream;
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
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

//it needs to run serial because some tests are relying on System properties they are setting themselves.
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XMLConfigBuilderTest {

    @After
    @Before
    public void after() {
        System.clearProperty("hazelcast.config");
    }


    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig =  "<hazelcast>" +
                "    <network>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\"/>\n" +
                "            <tcp-ip enabled=\"true\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = "<hazelcast>\n" +
                "    <import resource=\"${config.location}\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);

        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        configBuilder.setProperties(properties);
        Config config = configBuilder.build();
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Test
    public void testImportedConfigVariableReplacement() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig =  "<hazelcast>" +
                "    <network>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\"/>\n" +
                "            <tcp-ip enabled=\"${tcp.ip.enabled}\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = "<hazelcast>\n" +
                "    <import resource=\"${config.location}\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);

        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        properties.setProperty("tcp.ip.enabled", "true");
        configBuilder.setProperties(properties);
        Config config = configBuilder.build();
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }


    @Test(expected = HazelcastException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        File config2 = createConfigFile("hz2", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        String config1Xml =  "<hazelcast>" +
                "    <import resource=\"file://"+config2.getAbsolutePath()+"\"/>\n" +
                "</hazelcast>";
        String config2Xml =  "<hazelcast>" +
                "    <import resource=\"file://"+config1.getAbsolutePath()+"\"/>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);

        ByteArrayInputStream bis = new ByteArrayInputStream(config1Xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        File config2 = createConfigFile("hz2", "xml");
        File config3 = createConfigFile("hz3", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        FileOutputStream os3 = new FileOutputStream(config2);
        String config1Xml =  "<hazelcast>" +
                "    <import resource=\"file://"+config2.getAbsolutePath()+"\"/>\n" +
                "</hazelcast>";
        String config2Xml =  "<hazelcast>" +
                "    <import resource=\"file://"+config3.getAbsolutePath()+"\"/>\n" +
                "</hazelcast>";
        String config3Xml =  "<hazelcast>" +
                "    <import resource=\"file://"+config1.getAbsolutePath()+"\"/>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);
        writeStringToStreamAndClose(os3, config3Xml);
        ByteArrayInputStream bis = new ByteArrayInputStream(config1Xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void testImportEmptyResourceContent() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Xml =  "<hazelcast>" +
                "    <import resource=\"file://"+config1.getAbsolutePath()+"\"/>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os1, "");
        ByteArrayInputStream bis = new ByteArrayInputStream(config1Xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void testImportEmptyResourceThrowsException() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <import resource=\"\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void testImportNotExistingResourceThrowsException() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <import resource=\"notexisting.xml\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }


    @Test
    public void testImportNetworkConfigFromFile() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig =  "<hazelcast>" +
                "    <network>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\"/>\n" +
                "            <tcp-ip enabled=\"true\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = "<hazelcast>\n" +
                "    <import resource=\"file://"+file.getAbsolutePath()+"\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Test
    public void testImportMapConfigFromFile() throws Exception {
        File file = createConfigFile("mymap", "config");
        FileOutputStream os = new FileOutputStream(file);
        String mapConfig =  "<hazelcast>" +
                "    <map name=\"mymap\">\n" +
                "       <backup-count>6</backup-count>" +
                "       <time-to-live-seconds>10</time-to-live-seconds>" +
                "       <map-store enabled=\"true\" initial-mode=\"LAZY\">\n" +
                "            <class-name>com.hazelcast.examples.MyMapStore</class-name>\n" +
                "            <write-delay-seconds>10</write-delay-seconds>\n" +
                "            <write-batch-size>100</write-batch-size>\n" +
                "        </map-store>"+
                "</map>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, mapConfig);

        String xml = "<hazelcast>\n" +
                "    <import resource=\"file://"+file.getAbsolutePath()+"\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
        MapConfig myMapConfig = config.getMapConfig("mymap");
        assertEquals("mymap", myMapConfig.getName());
        assertEquals(6, myMapConfig.getBackupCount());
        assertEquals(10, myMapConfig.getTimeToLiveSeconds());
        MapStoreConfig myMapStoreConfig = myMapConfig.getMapStoreConfig();
        assertEquals(10, myMapStoreConfig.getWriteDelaySeconds());
        assertEquals(100, myMapStoreConfig.getWriteBatchSize());
        assertEquals("com.hazelcast.examples.MyMapStore", myMapStoreConfig.getClassName());
    }


    @Test
    public void testImportGroupConfigFromClassPath() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <import resource=\"classpath:test-hazelcast.xml\"/>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
        GroupConfig groupConfig = config.getGroupConfig();
        assertEquals("foobar", groupConfig.getName());
        assertEquals("dev-pass", groupConfig.getPassword());
    }


    @Test(expected = HazelcastException.class)
    public void testImportElementOnlyAppersInTopLevel() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <network>" +
                "        <import resource=\"\"/>\n" +
                "   </network>" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void testHazelcastElementOnlyAppersOnce() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <hazelcast>" +
                "   </hazelcast>" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() throws IOException {
        File file = File.createTempFile("foo", "bar");
        file.delete();
        System.setProperty("hazelcast.config", file.getAbsolutePath());

        new XmlConfigBuilder();
    }

    @Test(expected = HazelcastException.class)
    public void testJoinValidation(){
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
                "<hazelcast>" +
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
                "<hazelcast>\n" +
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
    public void readVariables() {
        String xml =
                "<hazelcast>\n" +
                        "    <semaphore name=\"${name}\">\n" +
                        "        <initial-permits>${initial.permits}</initial-permits>\n" +
                        "        <backup-count>${backupcount.part1}${backupcount.part2}</backup-count>\n" +
                        "        <async-backup-count>${notreplaced}</async-backup-count>\n" +
                        "    </semaphore>" +
                        "</hazelcast>";
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);

        Properties properties = new Properties();
        properties.setProperty("name", "s");
        properties.setProperty("initial.permits", "25");
        properties.setProperty("backupcount.part1", "1");
        properties.setProperty("backupcount.part2", "0");
        configBuilder.setProperties(properties);

        Config config = configBuilder.build();
        SemaphoreConfig semaphoreConfig = config.getSemaphoreConfig("s");
        assertEquals(25, semaphoreConfig.getInitialPermits());
        assertEquals(10, semaphoreConfig.getBackupCount());
        assertEquals(0, semaphoreConfig.getAsyncBackupCount());
    }

    @Test
    public void readAwsConfig() {
        String xml =
                "<hazelcast>\n" +
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
        Config config = buildConfig("<hazelcast>\n" +
                "    <network>\n" +
                "        <port port-count=\"200\">5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertEquals(200, config.getNetworkConfig().getPortCount());

        //check if the default is passed in correctly
        config = buildConfig("<hazelcast>\n" +
                "    <network>\n" +
                "        <port>5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertEquals(100, config.getNetworkConfig().getPortCount());
    }

    @Test
    public void readPortAutoIncrement() {
        //explicitly set.
        Config config = buildConfig("<hazelcast>\n" +
                "    <network>\n" +
                "        <port auto-increment=\"false\">5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertFalse(config.getNetworkConfig().isPortAutoIncrement());

        //check if the default is picked up correctly
        config = buildConfig("<hazelcast>\n" +
                "    <network>\n" +
                "        <port>5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
    }

    @Test
    public void networkReuseAddress() {
       Config config = buildConfig("<hazelcast>\n" +
                "    <network>\n" +
                "        <reuse-address>true</reuse-address>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertTrue(config.getNetworkConfig().isReuseAddress());
    }

    @Test
    public void readSemaphoreConfig() {
        String xml =
                "<hazelcast>\n" +
                        "    <semaphore name=\"default\">\n" +
                        "        <initial-permits>1</initial-permits>\n" +
                        "    </semaphore>" +
                        "    <semaphore name=\"custom\">\n" +
                        "        <initial-permits>10</initial-permits>\n" +
                        "        <semaphore-factory enabled=\"true\">" +
                        "             <class-name>com.acme.MySemaphore</class-name>\n" +
                        "        </semaphore-factory>" +
                        "    </semaphore>" +
                        "</hazelcast>";
        Config config = buildConfig(xml);
        SemaphoreConfig defaultConfig = config.getSemaphoreConfig("default");
        SemaphoreConfig customConfig = config.getSemaphoreConfig("custom");
        assertEquals(1, defaultConfig.getInitialPermits());
        assertEquals(10, customConfig.getInitialPermits());
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
                "<hazelcast>\n" +
                        "<map name=\"testCaseInsensitivity\">" +
                        "<in-memory-format>binary</in-memory-format>     " +
                        "<backup-count>1</backup-count>                 " +
                        "<async-backup-count>0</async-backup-count>    " +
                        "<time-to-live-seconds>0</time-to-live-seconds>" +
                        "<max-idle-seconds>0</max-idle-seconds>    " +
                        "<eviction-policy>none</eviction-policy>  " +
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
                "<hazelcast>\n" +
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
                "<hazelcast>\n" +
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
                "<hazelcast>\n" +
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getUrl());
    }

    @Test
    public void testNotEnabledManagementCenterConfig() {
        String xml =
                "<hazelcast>\n" +
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
                "<hazelcast>\n" +
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
                "<hazelcast>\n" +
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
                "<hazelcast>\n" +
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
                "<hazelcast>\n" +
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
                "<hazelcast>" +
                    "<map name=\"mymap1\">" +
                        "<optimize-queries>true</optimize-queries>" +
                    "</map>" +
                "</hazelcast>";
        final Config config1 = buildConfig(xml1);
        final MapConfig mapConfig1 = config1.getMapConfig("mymap1");
        assertTrue(mapConfig1.isOptimizeQueries());

        String xml2 =
                "<hazelcast>" +
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
                "<hazelcast>" +
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
                "<hazelcast>\n" +
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
                "<hazelcast>\n" +
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
    public void testNearCacheInMemoryFormat() {
        String mapName = "testMapNearCacheInMemoryFormat";
        String xml =
                "<hazelcast>\n" +
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

    @Test(expected = HazelcastException.class)
    public void testParseExceptionIsNotSwallowed() {
        String invalidXml =
                "<hazelcast>\n" +
                        "</hazelcast";
        buildConfig(invalidXml);
        fail(); //if we, for any reason, we get through the parsing, fail.
    }


    @Test
    public void setMapStoreConfigImplementationTest() {
        String mapName = "mapStoreImpObjTest";
        String xml =
                "<hazelcast>\n" +
                        "<map name=\"" + mapName + "\">\n" +
                        "<map-store enabled=\"true\">\n" +
                        "<class-name>com.hazelcast.config.helpers.DummyMapStore</class-name>\n" +
                        "<write-delay-seconds>5</write-delay-seconds>\n" +
                        "</map-store>\n" +
                        "</map>\n" +
                        "</hazelcast>\n";

        Config config = buildConfig(xml);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        hz.getMap(mapName);

        MapConfig mapConfig = hz.getConfig().getMapConfig(mapName);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        Object o = mapStoreConfig.getImplementation();

        assertNotNull(o);
        assertTrue(o instanceof DummyMapStore);
    }

    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateNetworkConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <network>\n" +
                "        <join>\n" +
                "            <tcp-ip enabled=\"true\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "    <network>\n" +
                "        <join>\n" +
                "            <tcp-ip enabled=\"false\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateGroupConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <group>\n" +
                "        <name>foobar</name>\n" +
                "        <password>dev-pass</password>\n" +
                "    </group>" +
                "    <group>\n" +
                "        <name>foobar</name>\n" +
                "        <password>dev-pass</password>\n" +
                "    </group>" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }
    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateLicenseKeyConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <license-key>foo</license-key>" +
                "    <license-key>bar</license-key>" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }
    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicatePropertiesConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <properties>\n" +
                "        <property>foo</property>\n" +
                "    </properties>" +
                "    <properties>\n" +
                "        <property>bar</property>\n" +
                "    </properties>" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }
    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicatePartitionGroupConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <partition-group>\n" +
                "      <member-group>\n" +
                "          <interface>foo</interface>\n" +
                "      </member-group>\n" +
                "   </partition-group>\n" +
                "   <partition-group>\n" +
                "      <member-group>\n" +
                "          <interface>bar</interface>\n" +
                "      </member-group>\n" +
                "   </partition-group>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }
    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateListenersConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <listeners>" +
                "        <listener>foo</listener>\n\n" +
                "   </listeners>\n" +
                "   <listeners>" +
                "        <listener>bar</listener>\n\n" +
                "   </listeners>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateSerializationConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "       <serialization>\n" +
                "        <portable-version>0</portable-version>\n" +
                "        <data-serializable-factories>\n" +
                "            <data-serializable-factory factory-id=\"1\">com.hazelcast.examples.DataSerializableFactory\n" +
                "            </data-serializable-factory>\n" +
                "        </data-serializable-factories>\n" +
                "        <portable-factories>\n" +
                "            <portable-factory factory-id=\"1\">com.hazelcast.examples.PortableFactory</portable-factory>\n" +
                "        </portable-factories>\n" +
                "        <serializers>\n" +
                "            <global-serializer>com.hazelcast.examples.GlobalSerializerFactory</global-serializer>\n" +
                "            <serializer type-class=\"com.hazelcast.examples.DummyType\" class-name=\"com.hazelcast.examples.SerializerFactory\"/>\n" +
                "        </serializers>\n" +
                "        <check-class-def-errors>true</check-class-def-errors>\n" +
                "    </serialization>\n <serialization>\n" +
                "        <portable-version>0</portable-version>\n" +
                "        <data-serializable-factories>\n" +
                "            <data-serializable-factory factory-id=\"1\">com.hazelcast.examples.DataSerializableFactory\n" +
                "            </data-serializable-factory>\n" +
                "        </data-serializable-factories>\n" +
                "        <portable-factories>\n" +
                "            <portable-factory factory-id=\"1\">com.hazelcast.examples.PortableFactory</portable-factory>\n" +
                "        </portable-factories>\n" +
                "        <serializers>\n" +
                "            <global-serializer>com.hazelcast.examples.GlobalSerializerFactory</global-serializer>\n" +
                "            <serializer type-class=\"com.hazelcast.examples.DummyType\" class-name=\"com.hazelcast.examples.SerializerFactory\"/>\n" +
                "        </serializers>\n" +
                "        <check-class-def-errors>true</check-class-def-errors>\n" +
                "    </serialization>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateServicesConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <services>       " +
                "       <service enabled=\"true\">\n" +
                "            <name>custom-service</name>\n" +
                "            <class-name>com.hazelcast.examples.MyService</class-name>\n" +
                "        </service>\n" +
                "   </services>" +
                "   <services>   " +
                "        <service enabled=\"true\">\n" +
                "            <name>custom-service</name>\n" +
                "            <class-name>com.hazelcast.examples.MyService</class-name>\n" +
                "        </service>\n" +
                "   </services>" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }
    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateSecurityConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <security>       " +
                "   </security>" +
                "   <security>   " +
                "   </security>" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }
    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateMemberAttributesConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <member-attributes>\n" +
                "        <attribute name=\"attribute.float\" type=\"float\">1234.5678</attribute>\n" +
                "    </member-attributes>\n" +
                "    <member-attributes>\n" +
                "        <attribute name=\"attribute.float\" type=\"float\">1234.5678</attribute>\n" +
                "    </member-attributes>\n" +
                "</hazelcast>";

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.build();
    }

    private void testXSDConfigXML(String xmlFileName) throws SAXException, IOException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-config-3.4.xsd");
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


    private File createConfigFile(String filename, String suffix) throws IOException {
        File file = File.createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    private void writeStringToStreamAndClose(FileOutputStream os, String string) throws IOException {
        os.write(string.getBytes());
        os.flush();
        os.close();
    }

}
