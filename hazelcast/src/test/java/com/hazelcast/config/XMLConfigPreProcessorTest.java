/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.config;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XMLConfigPreProcessorTest {


    @Test(expected = HazelcastException.class)
    public void testImportElementOnlyAppersInTopLevel() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <network>" +
                "        <import resource=\"\"/>\n" +
                "   </network>" +
                "</hazelcast>";
        buildConfig(xml, null);
    }

    @Test(expected = HazelcastException.class)
    public void testHazelcastElementOnlyAppersOnce() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <hazelcast>" +
                "   </hazelcast>" +
                "</hazelcast>";
        buildConfig(xml, null);
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

        Properties properties = new Properties();
        properties.setProperty("name", "s");
        properties.setProperty("initial.permits", "25");
        properties.setProperty("backupcount.part1", "1");
        properties.setProperty("backupcount.part2", "0");
        Config config = buildConfig(xml, properties);
        SemaphoreConfig semaphoreConfig = config.getSemaphoreConfig("s");
        assertEquals(25, semaphoreConfig.getInitialPermits());
        assertEquals(10, semaphoreConfig.getBackupCount());
        assertEquals(0, semaphoreConfig.getAsyncBackupCount());
    }

    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = "<hazelcast>" +
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
        Config config = buildConfig(xml, "config.location", file.getAbsolutePath());
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Test
    public void testImportedConfigVariableReplacement() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = "<hazelcast>" +
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

        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        properties.setProperty("tcp.ip.enabled", "true");
        Config config = buildConfig(xml, properties);
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
        String config1Xml = "<hazelcast>" +
                "    <import resource=\"file://" + config2.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";
        String config2Xml = "<hazelcast>" +
                "    <import resource=\"file://" + config1.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);
        buildConfig(config1Xml, null);
    }

    @Test(expected = HazelcastException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        File config2 = createConfigFile("hz2", "xml");
        File config3 = createConfigFile("hz3", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        FileOutputStream os3 = new FileOutputStream(config2);
        String config1Xml = "<hazelcast>" +
                "    <import resource=\"file://" + config2.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";
        String config2Xml = "<hazelcast>" +
                "    <import resource=\"file://" + config3.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";
        String config3Xml = "<hazelcast>" +
                "    <import resource=\"file://" + config1.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);
        writeStringToStreamAndClose(os3, config3Xml);
        buildConfig(config1Xml, null);
    }

    @Test(expected = HazelcastException.class)
    public void testImportEmptyResourceContent() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Xml = "<hazelcast>" +
                "    <import resource=\"file://" + config1.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os1, "");
        buildConfig(config1Xml, null);
    }

    @Test(expected = HazelcastException.class)
    public void testImportEmptyResourceThrowsException() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <import resource=\"\"/>\n" +
                "</hazelcast>";
        buildConfig(xml, null);
    }

    @Test(expected = HazelcastException.class)
    public void testImportNotExistingResourceThrowsException() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <import resource=\"notexisting.xml\"/>\n" +
                "</hazelcast>";
        buildConfig(xml, null);
    }


    @Test
    public void testImportNetworkConfigFromFile() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = "<hazelcast>" +
                "    <network>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\"/>\n" +
                "            <tcp-ip enabled=\"true\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = "<hazelcast>\n" +
                "    <import resource=\"file://" + file.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";

        Config config = buildConfig(xml, null);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Test
    public void testImportMapConfigFromFile() throws Exception {
        File file = createConfigFile("mymap", "config");
        FileOutputStream os = new FileOutputStream(file);
        String mapConfig = "<hazelcast>" +
                "    <map name=\"mymap\">\n" +
                "       <backup-count>6</backup-count>" +
                "       <time-to-live-seconds>10</time-to-live-seconds>" +
                "       <map-store enabled=\"true\" initial-mode=\"LAZY\">\n" +
                "            <class-name>com.hazelcast.examples.MyMapStore</class-name>\n" +
                "            <write-delay-seconds>10</write-delay-seconds>\n" +
                "            <write-batch-size>100</write-batch-size>\n" +
                "        </map-store>" +
                "</map>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, mapConfig);

        String xml = "<hazelcast>\n" +
                "    <import resource=\"file://" + file.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";

        Config config = buildConfig(xml, null);
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

        Config config = buildConfig(xml, null);
        GroupConfig groupConfig = config.getGroupConfig();
        assertEquals("foobar", groupConfig.getName());
        assertEquals("dev-pass", groupConfig.getPassword());
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
        buildConfig(xml, null);
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
        buildConfig(xml, null);
    }

    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateLicenseKeyConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <license-key>foo</license-key>" +
                "    <license-key>bar</license-key>" +
                "</hazelcast>";
        buildConfig(xml, null);
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
        buildConfig(xml, null);
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

        buildConfig(xml, null);
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

        buildConfig(xml, null);
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

        buildConfig(xml, null);
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

        buildConfig(xml, null);
    }

    @Test(expected = HazelcastException.class)
    public void testXmlDeniesDuplicateSecurityConfig() throws Exception {
        String xml = "<hazelcast>\n" +
                "   <security>       " +
                "   </security>" +
                "   <security>   " +
                "   </security>" +
                "</hazelcast>";
        buildConfig(xml, null);
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
        buildConfig(xml, null);
    }

    @Test
    public void testXmlVariableReplacementAsSubstring() throws Exception {
        String xml = "<hazelcast>\n" +
                "    <properties>\n" +
                "        <property name=\"${env}-with-suffix\">local-with-suffix</property>\n" +
                "        <property name=\"with-prefix-${env}\">with-prefix-local</property>\n" +
                "    </properties>\n" +
                "</hazelcast>";

        Config config = buildConfig(xml, "env", "local");
        assertEquals(config.getProperty("local-with-suffix"), "local-with-suffix");
        assertEquals(config.getProperty("with-prefix-local"), "with-prefix-local");
    }

    @Test
    public void testXmlImportWithVariableReplacementAsSubstring() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = "<hazelcast>" +
                "    <properties>\n" +
                "        <property name=\"prop1\">value1</property>\n" +
                "        <property name=\"prop2\">value2</property>\n" +
                "    </properties>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = "<hazelcast>\n" +
                "    <import resource=\"file://" + "${file}" + "\"/>\n" +
                "</hazelcast>";
        Config config = buildConfig(xml, "file", file.getAbsolutePath());
        assertEquals(config.getProperty("prop1"), "value1");
        assertEquals(config.getProperty("prop2"), "value2");
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

    Config buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.setProperties(properties);
        Config config = configBuilder.build();
        return config;
    }

    Config buildConfig(String xml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return buildConfig(xml, properties);
    }
}
