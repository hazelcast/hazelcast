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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
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
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class XMLConfigBuilderTest {

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
        properties.setProperty("name","s");
        properties.setProperty("initial.permits","25");
        properties.setProperty("backupcount.part1","1");
        properties.setProperty("backupcount.part2","0");
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
        config = buildConfig( "<hazelcast>\n" +
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
        config = buildConfig( "<hazelcast>\n" +
                "    <network>\n" +
                "        <port>5701</port>\n" +
                "    </network>\n" +
                "</hazelcast>");
        assertTrue(config.getNetworkConfig().isPortAutoIncrement());
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
        final Config config = new ClasspathXmlConfig(fileName);
        final String xml = new ConfigXmlGenerator(true).generate(config);
        final Config config2 = new InMemoryXmlConfig(xml);
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
                        "<map name=\"testCaseInsensitivity\">"+
                        "<in-memory-format>binary</in-memory-format>     "+
                        "<backup-count>1</backup-count>                 "  +
                        "<async-backup-count>0</async-backup-count>    "    +
                        "<time-to-live-seconds>0</time-to-live-seconds>"     +
                        "<max-idle-seconds>0</max-idle-seconds>    "          +
                        "<eviction-policy>none</eviction-policy>  "            +
                        "<max-size policy=\"per_partition\">0</max-size>"              +
                        "<eviction-percentage>25</eviction-percentage>"          +
                        "<merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>"+
                        "</map>"+
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final MapConfig mapConfig = config.getMapConfig("testCaseInsensitivity");
        assertTrue(mapConfig.getInMemoryFormat().equals(InMemoryFormat.BINARY));
        assertTrue(mapConfig.getEvictionPolicy().equals(MapConfig.EvictionPolicy.NONE));
        assertTrue(mapConfig.getMaxSizeConfig().getMaxSizePolicy().equals(MaxSizeConfig.MaxSizePolicy.PER_PARTITION));
    }


    @Test
    public void testManagementCenterConfig() {
        String xml =
                "<hazelcast>\n" +
                        "<management-center enabled=\"true\" security-token=\"someToken\" cluster-id=\"someClusterId\">"+
                        "someUrl"+
                        "</management-center>"+
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertTrue(manCenterCfg.isEnabled());
        assertEquals("someClusterId",manCenterCfg.getClusterId());
        assertEquals("someToken",manCenterCfg.getSecurityToken());
        assertEquals("someUrl",manCenterCfg.getUrl());
    }
    @Test
    public void testNullManagementCenterConfig() {
        String xml =
                "<hazelcast>\n" +
                        "<management-center>"+
                        "</management-center>"+
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getClusterId());
        assertNull(manCenterCfg.getSecurityToken());
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
        assertNull(manCenterCfg.getClusterId());
        assertNull(manCenterCfg.getSecurityToken());
        assertNull(manCenterCfg.getUrl());
    }
    @Test
    public void testNotEnabledManagementCenterConfig() {
        String xml =
                "<hazelcast>\n" +
                        "<management-center enabled=\"false\">"+
                        "</management-center>"+
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getClusterId());
        assertNull(manCenterCfg.getSecurityToken());
        assertNull(manCenterCfg.getUrl());
    }
    @Test
    public void testNotEnabledWithURLManagementCenterConfig() {
        String xml =
                "<hazelcast>\n" +
                        "<management-center enabled=\"false\">"+
                        "http://localhost:8080/mancenter"+
                        "</management-center>"+
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertFalse(manCenterCfg.isEnabled());
        assertNull(manCenterCfg.getClusterId());
        assertNull(manCenterCfg.getSecurityToken());
        assertEquals("http://localhost:8080/mancenter", manCenterCfg.getUrl());
    }

    @Test
    public void testManagementCenterConfig_onlySecurityTokenSet() {
        String xml =
                "<hazelcast>\n" +
                        "<management-center security-token=\"someToken\">"+
                        "</management-center>"+
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        final ManagementCenterConfig manCenterCfg = config.getManagementCenterConfig();
        assertTrue(manCenterCfg.isEnabled());
        assertEquals("someToken",manCenterCfg.getSecurityToken());
        assertNull(manCenterCfg.getClusterId());
        assertNull(manCenterCfg.getUrl());
    }
    @Test
    public void testMapStoreInitialModeLazy() {
        String xml =
                "<hazelcast>\n" +
                        "<map name=\"mymap\">"+
                        "<map-store enabled=\"true\" initial-mode=\"LAZY\"></map-store>"+
                        "</map>"+
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        final MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();
        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, mapStoreConfig.getInitialLoadMode());
    }
    @Test
    public void testMapStoreInitialModeEager() {
        String xml =
                "<hazelcast>\n" +
                        "<map name=\"mymap\">"+
                        "<map-store enabled=\"true\" initial-mode=\"EAGER\"></map-store>"+
                        "</map>"+
                        "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        final MapStoreConfig mapStoreConfig = config.getMapConfig("mymap").getMapStoreConfig();
        assertTrue(mapStoreConfig.isEnabled());
        assertEquals(MapStoreConfig.InitialLoadMode.EAGER, mapStoreConfig.getInitialLoadMode());
    }

    @Test
    public void testCustomJoinerFactoryConfigWithProperties(){
        String xml = "<hazelcast>\n" +
                "   <network>\n" +
                "      <port auto-increment=\"true\" port-count=\"100\">5701</port>\n" +
                "      <join>\n" +
                "         <custom enabled=\"true\" joiner-factory-class=\"org.myorg.myapp.MyCustomJoinerFactory\">\n" +
                "            <property key=\"my-key-1\" value=\"my-value-1\" />\n" +
                "            <property key=\"my-key-2\" value=\"my-value-2\" />\n" +
                "         </custom>\n" +
                "      </join>\n" +
                "   </network>\n" +
                "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        assertTrue(config.getNetworkConfig().getJoin().getCustomConfig().isEnabled());
        assertEquals("org.myorg.myapp.MyCustomJoinerFactory", config.getNetworkConfig().getJoin().getCustomConfig().getJoinerFactoryClassName());
        assertEquals(2, config.getNetworkConfig().getJoin().getCustomConfig().getProperties().size());
        assertEquals("my-value-1", config.getNetworkConfig().getJoin().getCustomConfig().getProperties().get("my-key-1"));
        assertEquals("my-value-2", config.getNetworkConfig().getJoin().getCustomConfig().getProperties().get("my-key-2"));
    }

    @Test
    public void testCustomJoinerFactoryConfigNoProperties(){
        String xml = "<hazelcast>\n" +
                "   <network>\n" +
                "      <port auto-increment=\"true\" port-count=\"100\">5701</port>\n" +
                "      <join>\n" +
                "         <custom enabled=\"true\" joiner-factory-class=\"org.myorg.myapp.MyCustomJoinerFactory\" />\n" +
                "      </join>\n" +
                "   </network>\n" +
                "</hazelcast>";
        final Config config = buildConfig(xml);
        System.out.println("config = " + config);
        assertTrue(config.getNetworkConfig().getJoin().getCustomConfig().isEnabled());
        assertEquals("org.myorg.myapp.MyCustomJoinerFactory", config.getNetworkConfig().getJoin().getCustomConfig().getJoinerFactoryClassName());
        assertEquals(0, config.getNetworkConfig().getJoin().getCustomConfig().getProperties().size());
    }

    @Test(expected = HazelcastException.class)
    public void testParseExceptionIsNotSwallowed() {
        String invalidXml =
                "<hazelcast>\n" +
                "</hazelcast";
        buildConfig(invalidXml);
        fail(); //if we, for any reason, we get through the parsing, fail.
    }

    private void testXSDConfigXML(String xmlFileName) throws SAXException, IOException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-config-3.3.xsd");
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

}
