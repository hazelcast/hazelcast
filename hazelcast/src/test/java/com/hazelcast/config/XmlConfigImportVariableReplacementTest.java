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
import java.util.Properties;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_END_TAG;
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
    public void testImportElementOnlyAppearsInTopLevel() {
        String xml = HAZELCAST_START_TAG
                + "   <network>"
                + "        <import resource=\"\"/>\n"
                + "   </network>"
                + HAZELCAST_END_TAG;
        expectInvalid();
        buildConfig(xml, null);
    }

    @Test
    public void testHazelcastElementOnlyAppearsOnce() {
        String xml = HAZELCAST_START_TAG
                + "   <hazelcast>"
                + "   </hazelcast>"
                + HAZELCAST_END_TAG;
        expectInvalid();
        buildConfig(xml, null);
    }

    @Test
    public void readVariables() {
        String xml = HAZELCAST_START_TAG
                + "    <semaphore name=\"${name}\">\n"
                + "        <initial-permits>${initial.permits}</initial-permits>\n"
                + "        <backup-count>${backupcount.part1}${backupcount.part2}</backup-count>\n"
                + "    </semaphore>"
                + HAZELCAST_END_TAG;

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
    public void testImportConfigFromResourceVariables() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"true\"/>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, "config.location", file.getAbsolutePath());
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Test
    public void testImportedConfigVariableReplacement() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"${tcp.ip.enabled}\"/>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HAZELCAST_END_TAG;

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
        String config1Xml = HAZELCAST_START_TAG
                + "    <import resource=\"file:///" + config2.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_END_TAG;
        String config2Xml = HAZELCAST_START_TAG
                + "    <import resource=\"file:///" + config1.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);
        expectInvalid();
        buildConfig(config1Xml, null);
    }

    @Test
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        String template = HAZELCAST_START_TAG
                + "    <import resource=\"file:///%s\"/>\n"
                + HAZELCAST_END_TAG;
        File config1 = createConfigFile("hz1", "xml");
        File config2 = createConfigFile("hz2", "xml");
        File config3 = createConfigFile("hz3", "xml");
        String config1Xml = String.format(template, config2.getAbsolutePath());
        String config2Xml = String.format(template, config3.getAbsolutePath());
        String config3Xml = String.format(template, config1.getAbsolutePath());
        writeStringToStreamAndClose(new FileOutputStream(config1), config1Xml);
        writeStringToStreamAndClose(new FileOutputStream(config2), config2Xml);
        writeStringToStreamAndClose(new FileOutputStream(config3), config3Xml);
        expectInvalid();
        buildConfig(config1Xml, null);
    }

    @Test
    public void testImportEmptyResourceContent() throws Exception {
        File config1 = createConfigFile("hz1", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Xml = HAZELCAST_START_TAG
                + "    <import resource='file:///" + config1.getAbsolutePath() + "'/>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os1, "");
        expectInvalid();
        buildConfig(config1Xml, null);
    }

    @Test
    public void testImportEmptyResourceThrowsException() {
        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"\"/>\n"
                + HAZELCAST_END_TAG;
        expectInvalid();
        buildConfig(xml, null);
    }

    @Test
    public void testImportNotExistingResourceThrowsException() {
        expectInvalid();
        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"notexisting.xml\"/>\n"
                + HAZELCAST_END_TAG;
        buildConfig(xml, null);
    }

    @Test
    public void testImportNetworkConfigFromFile() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"true\"/>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml, null);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Test
    public void testImportMapConfigFromFile() throws Exception {
        File file = createConfigFile("mymap", "config");
        FileOutputStream os = new FileOutputStream(file);
        String mapConfig = HAZELCAST_START_TAG
                + "    <map name=\"mymap\">\n"
                + "       <backup-count>6</backup-count>"
                + "       <time-to-live-seconds>10</time-to-live-seconds>"
                + "       <map-store enabled=\"true\" initial-mode=\"LAZY\">\n"
                + "            <class-name>com.hazelcast.examples.MyMapStore</class-name>\n"
                + "            <write-delay-seconds>10</write-delay-seconds>\n"
                + "            <write-batch-size>100</write-batch-size>\n"
                + "        </map-store>"
                + "</map>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os, mapConfig);

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_END_TAG;

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
    public void testImportGroupConfigFromClassPath() {
        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"classpath:test-hazelcast.xml\"/>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, null);
        GroupConfig groupConfig = config.getGroupConfig();
        assertEquals("foobar", groupConfig.getName());
        assertEquals("dev-pass", groupConfig.getPassword());
    }

    @Test
    public void testXmlDeniesDuplicateNetworkConfig() {
        expectDuplicateElementError("network");
        String networkConfig = "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"true\"/>\n"
                + "        </join>\n"
                + "    </network>\n";
        buildConfig(HAZELCAST_START_TAG + networkConfig + networkConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicateGroupConfig() {
        expectDuplicateElementError("group");
        String groupConfig = "    <group>\n"
                + "        <name>foobar</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n";
        buildConfig(HAZELCAST_START_TAG + groupConfig + groupConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicateLicenseKeyConfig() {
        expectDuplicateElementError("license-key");
        String licenseConfig = "    <license-key>foo</license-key>";
        buildConfig(HAZELCAST_START_TAG + licenseConfig + licenseConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicatePropertiesConfig() {
        expectDuplicateElementError("properties");
        String propertiesConfig = "    <properties>\n"
                + "        <property name='foo'>fooval</property>\n"
                + "    </properties>\n";
        buildConfig(HAZELCAST_START_TAG + propertiesConfig + propertiesConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicatePartitionGroupConfig() {
        expectDuplicateElementError("partition-group");
        String partitionConfig = "   <partition-group>\n"
                + "      <member-group>\n"
                + "          <interface>foo</interface>\n"
                + "      </member-group>\n"
                + "   </partition-group>\n";
        buildConfig(HAZELCAST_START_TAG + partitionConfig + partitionConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicateListenersConfig() {
        expectDuplicateElementError("listeners");
        String listenersConfig = "   <listeners>"
                + "        <listener>foo</listener>\n\n"
                + "   </listeners>\n";
        buildConfig(HAZELCAST_START_TAG + listenersConfig + listenersConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicateSerializationConfig() {
        expectDuplicateElementError("serialization");
        String serializationConfig = "       <serialization>\n"
                + "        <portable-version>0</portable-version>\n"
                + "        <data-serializable-factories>\n"
                + "            <data-serializable-factory factory-id=\"1\">com.hazelcast.examples.DataSerializableFactory\n"
                + "            </data-serializable-factory>\n"
                + "        </data-serializable-factories>\n"
                + "        <portable-factories>\n"
                + "            <portable-factory factory-id=\"1\">com.hazelcast.examples.PortableFactory</portable-factory>\n"
                + "        </portable-factories>\n"
                + "        <serializers>\n"
                + "            <global-serializer>com.hazelcast.examples.GlobalSerializerFactory</global-serializer>\n"
                + "            <serializer type-class=\"com.hazelcast.examples.DummyType\"\n"
                + "                class-name=\"com.hazelcast.examples.SerializerFactory\"/>\n"
                + "        </serializers>\n"
                + "        <check-class-def-errors>true</check-class-def-errors>\n"
                + "    </serialization>\n";
        buildConfig(HAZELCAST_START_TAG + serializationConfig + serializationConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicateServicesConfig() {
        expectDuplicateElementError("services");
        String servicesConfig = "   <services>       "
                + "       <service enabled=\"true\">\n"
                + "            <name>custom-service</name>\n"
                + "            <class-name>com.hazelcast.examples.MyService</class-name>\n"
                + "        </service>\n"
                + "   </services>";
        buildConfig(HAZELCAST_START_TAG + servicesConfig + servicesConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicateSecurityConfig() {
        expectDuplicateElementError("security");
        String securityConfig = "   <security/>\n";
        buildConfig(HAZELCAST_START_TAG + securityConfig + securityConfig + HAZELCAST_END_TAG, null);
    }

    @Test
    public void testXmlDeniesDuplicateMemberAttributesConfig() {
        expectDuplicateElementError("member-attributes");
        String memberAttConfig = "    <member-attributes>\n"
                + "        <attribute name=\"attribute.float\" type=\"float\">1234.5678</attribute>\n"
                + "    </member-attributes>\n";
        buildConfig(HAZELCAST_START_TAG + memberAttConfig + memberAttConfig + HAZELCAST_END_TAG, null);
    }

    private void expectDuplicateElementError(String elName) {
        expectInvalid();
    }

    @Test
    public void testXmlVariableReplacementAsSubstring() {
        String xml = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"${env}-with-suffix\">local-with-suffix</property>\n"
                + "        <property name=\"with-prefix-${env}\">with-prefix-local</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml, "env", "local");
        assertEquals(config.getProperty("local-with-suffix"), "local-with-suffix");
        assertEquals(config.getProperty("with-prefix-local"), "with-prefix-local");
    }

    @Test
    public void testXmlImportWithVariableReplacementAsSubstring() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop1\">value1</property>\n"
                + "        <property name=\"prop2\">value2</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"file:///" + "${file}" + "\"/>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, "file", file.getAbsolutePath());
        assertEquals(config.getProperty("prop1"), "value1");
        assertEquals(config.getProperty("prop2"), "value2");
    }

    private void expectInvalid() {
        InvalidConfigurationTest.expectInvalid(rule);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static File createConfigFile(String filename, String suffix) throws Exception {
        File file = File.createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    private static void writeStringToStreamAndClose(FileOutputStream os, String string) throws Exception {
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
