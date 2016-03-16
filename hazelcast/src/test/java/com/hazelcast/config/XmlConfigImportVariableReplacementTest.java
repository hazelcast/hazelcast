/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_START_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlConfigImportVariableReplacementTest {
    @Rule
    public ExpectedException rule = ExpectedException.none();

    @Test
    public void testImportElementOnlyAppersInTopLevel() throws Exception {
        String xml = HAZELCAST_START_TAG +
                "   <network>" +
                "        <import resource=\"\"/>\n" +
                "   </network>" +
                "</hazelcast>";
        expectInvalid("<import> element can appear only in the top level of the XML");
        buildConfig(xml, null);
    }

    @Test
    public void testHazelcastElementOnlyAppersOnce() throws Exception {
        String xml = HAZELCAST_START_TAG +
                "   <hazelcast>" +
                "   </hazelcast>" +
                "</hazelcast>";
        expectInvalid("Invalid content was found starting with element");
        buildConfig(xml, null);
    }

    @Test
    public void readVariables() {
        String xml =
                "<hazelcast  xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
                        "    <semaphore name=\"${name}\">\n" +
                        "        <initial-permits>${initial.permits}</initial-permits>\n" +
                        "        <backup-count>${backupcount.part1}${backupcount.part2}</backup-count>\n" +
                        "    </semaphore>" +
                        "</hazelcast>";

        Properties properties = new Properties();
        properties.setProperty("name", "s");
        properties.setProperty("initial.permits", "25");

        properties.setProperty("backupcount.part1", "0");
        properties.setProperty("backupcount.part2", "6");
        Config config = buildConfig(xml, properties);
        SemaphoreConfig semaphoreConfig = config.getSemaphoreConfig("s");
        assertEquals(25, semaphoreConfig.getInitialPermits());
        assertEquals(6, semaphoreConfig.getBackupCount());
        assertEquals(0, semaphoreConfig.getAsyncBackupCount());
    }

    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig =
                HAZELCAST_START_TAG +
                        "    <network>\n" +
                        "        <join>\n" +
                        "            <multicast enabled=\"false\"/>\n" +
                        "            <tcp-ip enabled=\"true\"/>\n" +
                        "        </join>\n" +
                        "    </network>\n" +
                        "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_START_TAG +
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
        String networkConfig = HAZELCAST_START_TAG +
                "    <network>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\"/>\n" +
                "            <tcp-ip enabled=\"${tcp.ip.enabled}\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_START_TAG +
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

    @Test
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        File config2 = createConfigFile("hz2", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        String config1Xml = HAZELCAST_START_TAG +
                "    <import resource=\"file:///" + config2.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";
        String config2Xml = HAZELCAST_START_TAG +
                "    <import resource=\"file:///" + config1.getAbsolutePath() + "\"/>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);
        expectInvalid("Cyclic loading of resource");
        buildConfig(config1Xml, null);
    }

    @Test
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        final String template = HAZELCAST_START_TAG + "    <import resource=\"file:///%s\"/>\n</hazelcast>";
        final File config1 = createConfigFile("hz1", "xml");
        final File config2 = createConfigFile("hz2", "xml");
        final File config3 = createConfigFile("hz3", "xml");
        final String config1Xml = String.format(template, config2.getAbsolutePath());
        final String config2Xml = String.format(template, config3.getAbsolutePath());
        final String config3Xml = String.format(template, config1.getAbsolutePath());
        writeStringToStreamAndClose(new FileOutputStream(config1), config1Xml);
        writeStringToStreamAndClose(new FileOutputStream(config2), config2Xml);
        writeStringToStreamAndClose(new FileOutputStream(config3), config3Xml);
        expectInvalid("Cyclic loading of resource");
        buildConfig(config1Xml, null);
    }

    @Test
    public void testImportEmptyResourceContent() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Xml = HAZELCAST_START_TAG +
                "    <import resource='file:///" + config1.getAbsolutePath() + "'/>\n</hazelcast>";
        writeStringToStreamAndClose(os1, "");
        expectInvalid("Premature end of file.");
        buildConfig(config1Xml, null);
    }

    @Test
    public void testImportEmptyResourceThrowsException() throws Exception {
        String xml = HAZELCAST_START_TAG +
                "    <import resource=\"\"/>\n" +
                "</hazelcast>";
        expectInvalid("Failed to load resource:");
        buildConfig(xml, null);
    }

    @Test
    public void testImportNotExistingResourceThrowsException() throws Exception {
        expectInvalid("Failed to load resource: notexisting.xml");
        String xml = HAZELCAST_START_TAG + "    <import resource=\"notexisting.xml\"/>\n</hazelcast>";
        buildConfig(xml, null);
    }

    @Test
    public void testImportNetworkConfigFromFile() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = HAZELCAST_START_TAG +
                "    <network>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\"/>\n" +
                "            <tcp-ip enabled=\"true\"/>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_START_TAG +
                "    <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n" +
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
        String mapConfig = HAZELCAST_START_TAG +
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

        String xml = HAZELCAST_START_TAG +
                "    <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n" +
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
        String xml = HAZELCAST_START_TAG +
                "    <import resource=\"classpath:test-hazelcast.xml\"/>\n" +
                "</hazelcast>";
        Config config = buildConfig(xml, null);
        GroupConfig groupConfig = config.getGroupConfig();
        assertEquals("foobar", groupConfig.getName());
        assertEquals("dev-pass", groupConfig.getPassword());
    }

    @Test
    public void testXmlDeniesDuplicateNetworkConfig() throws Exception {
        expectDuplicateElementError("network");
        final String networkConfig =
                "    <network>\n" +
                        "        <join>\n" +
                        "            <tcp-ip enabled=\"true\"/>\n" +
                        "        </join>\n" +
                        "    </network>\n";
        buildConfig(HAZELCAST_START_TAG + networkConfig + networkConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicateGroupConfig() throws Exception {
        expectDuplicateElementError("group");
        final String groupConfig =
                "    <group>\n" +
                        "        <name>foobar</name>\n" +
                        "        <password>dev-pass</password>\n" +
                        "    </group>\n";
        buildConfig(HAZELCAST_START_TAG + groupConfig + groupConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicateLicenseKeyConfig() throws Exception {
        expectDuplicateElementError("license-key");
        final String licenseConfig = "    <license-key>foo</license-key>";
        buildConfig(HAZELCAST_START_TAG + licenseConfig + licenseConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicatePropertiesConfig() throws Exception {
        expectDuplicateElementError("properties");
        final String propertiesConfig =
                "    <properties>\n" +
                        "        <property name='foo'>fooval</property>\n" +
                        "    </properties>\n";
        buildConfig(HAZELCAST_START_TAG + propertiesConfig + propertiesConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicatePartitionGroupConfig() throws Exception {
        expectDuplicateElementError("partition-group");
        final String partitionConfig =
                "   <partition-group>\n" +
                        "      <member-group>\n" +
                        "          <interface>foo</interface>\n" +
                        "      </member-group>\n" +
                        "   </partition-group>\n";
        buildConfig(HAZELCAST_START_TAG + partitionConfig + partitionConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicateListenersConfig() throws Exception {
        expectDuplicateElementError("listeners");
        final String listenersConfig =
                "   <listeners>" +
                        "        <listener>foo</listener>\n\n" +
                        "   </listeners>\n";
        buildConfig(HAZELCAST_START_TAG + listenersConfig + listenersConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicateSerializationConfig() throws Exception {
        expectDuplicateElementError("serialization");
        final String serializationConfig =
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
                        "    </serialization>\n";
        buildConfig(HAZELCAST_START_TAG + serializationConfig + serializationConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicateServicesConfig() throws Exception {
        expectDuplicateElementError("services");
        final String servicesConfig =
                "   <services>       " +
                        "       <service enabled=\"true\">\n" +
                        "            <name>custom-service</name>\n" +
                        "            <class-name>com.hazelcast.examples.MyService</class-name>\n" +
                        "        </service>\n" +
                        "   </services>";
        buildConfig(HAZELCAST_START_TAG + servicesConfig + servicesConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicateSecurityConfig() throws Exception {
        expectDuplicateElementError("security");
        final String securityConfig = "   <security/>\n";
        buildConfig(HAZELCAST_START_TAG + securityConfig + securityConfig + "</hazelcast>", null);
    }

    @Test
    public void testXmlDeniesDuplicateMemberAttributesConfig() throws Exception {
        expectDuplicateElementError("member-attributes");
        final String memberAttConfig =
                "    <member-attributes>\n" +
                        "        <attribute name=\"attribute.float\" type=\"float\">1234.5678</attribute>\n" +
                        "    </member-attributes>\n";
        buildConfig(HAZELCAST_START_TAG + memberAttConfig + memberAttConfig + "</hazelcast>", null);
    }

    private void expectDuplicateElementError(String elName) {
        expectInvalid("Duplicate '" + elName + "' definition found in XML configuration.");
    }

    @Test
    public void testXmlVariableReplacementAsSubstring() throws Exception {
        String xml = HAZELCAST_START_TAG +
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
        String networkConfig = HAZELCAST_START_TAG +
                "    <properties>\n" +
                "        <property name=\"prop1\">value1</property>\n" +
                "        <property name=\"prop2\">value2</property>\n" +
                "    </properties>\n" +
                "</hazelcast>";
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_START_TAG +
                "    <import resource=\"file:///" + "${file}" + "\"/>\n" +
                "</hazelcast>";
        Config config = buildConfig(xml, "file", file.getAbsolutePath());
        assertEquals(config.getProperty("prop1"), "value1");
        assertEquals(config.getProperty("prop2"), "value2");
    }

    private void expectInvalid(String message) {
        InvalidConfigurationTest.expectInvalid(rule, message);
    }

    private static File createConfigFile(String filename, String suffix) throws IOException {
        File file = File.createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    private static void writeStringToStreamAndClose(FileOutputStream os, String string) throws IOException {
        os.write(string.getBytes());
        os.flush();
        os.close();
    }

    private static Config buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    private static Config buildConfig(String xml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return buildConfig(xml, properties);
    }
}
