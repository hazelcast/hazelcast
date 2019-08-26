/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.replacer.EncryptionReplacer;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Properties;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_END_TAG;
import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_START_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlConfigImportVariableReplacementTest extends AbstractConfigImportVariableReplacementTest {

    private static final String IMPORT_RESOURCE_TEMPLATE = HAZELCAST_START_TAG
            + "    <import resource=\"" + "%s" + "\"/>\n"
            + HAZELCAST_END_TAG;

    @Test(expected = InvalidConfigurationException.class)
    public void testImportElementOnlyAppearsInTopLevel() {
        String xml = HAZELCAST_START_TAG
                + "   <network>"
                + "        <import resource=\"\"/>\n"
                + "   </network>"
                + HAZELCAST_END_TAG;
        buildConfig(xml, null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastElementOnlyAppearsOnce() {
        String xml = HAZELCAST_START_TAG
                + "   <hazelcast>"
                + "   </hazelcast>"
                + HAZELCAST_END_TAG;
        buildConfig(xml, null);
    }

    @Override
    @Test
    public void readVariables() {
        String xml = HAZELCAST_START_TAG
                + "    <map name=\"${name}\">\n"
                + "        <backup-count>${backupcount.part1}${backupcount.part2}</backup-count>\n"
                + "    </map>"
                + HAZELCAST_END_TAG;

        Properties properties = new Properties();
        properties.setProperty("name", "s");

        properties.setProperty("backupcount.part1", "0");
        properties.setProperty("backupcount.part2", "6");
        Config config = buildConfig(xml, properties);
        MapConfig mapConfig = config.getMapConfig("s");
        assertEquals(6, mapConfig.getBackupCount());
        assertEquals(0, mapConfig.getAsyncBackupCount());
    }

    @Test
    @Ignore
    public void testImportResourceWithConfigReplacers() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String configReplacer = HAZELCAST_START_TAG
                + "    <config-replacers>\n"
                + "        <replacer class-name='" + IdentityReplacer.class.getName() + "'/>\n"
                + "    </config-replacers>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os, configReplacer);

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + "    <group>\n"
                + "        <name>${java.version} $ID{dev}</name>\n"
                + "    </group>\n"
                + HAZELCAST_END_TAG;
        GroupConfig groupConfig = buildConfig(
                xml,
                "config.location",
                file.getAbsolutePath()
        ).getGroupConfig();
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getName());
    }

    @Override
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

    @Override
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

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {

        String xmlWithCyclicImport = createFilesWithCycleImports(
                createConfigFile("hz1", "xml"),
                createConfigFile("hz2", "xml")
        );

        buildConfig(xmlWithCyclicImport, null);
    }

    private String createFilesWithCycleImports(File... files) throws Exception {
        for (int i = 1; i < files.length; i++) {
            createFileWithDependencyImport(files[i - 1], files[i].getAbsolutePath());
        }
        return createFileWithDependencyImport(files[0], files[1].getAbsolutePath());
    }

    private String createFileWithDependencyImport(
            File dependent,
            String pathToDependency) throws Exception {
        final String xmlContent = xmlContentWithImportResource(pathToDependency);
        writeStringToStreamAndClose(new FileOutputStream(dependent), xmlContent);
        return xmlContent;
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        String xmlWithCyclicImport = createFilesWithCycleImports(
                createConfigFile("hz1", "xml"),
                createConfigFile("hz2", "xml"),
                createConfigFile("hz3", "xml")
        );

        buildConfig(xmlWithCyclicImport, null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        String pathToEmptyFile = createEmptyFile();
        buildConfig(xmlContentWithImportResource("file://" + pathToEmptyFile), null);
    }

    private String xmlContentWithImportResource(String pathToResource) {
        return String.format(IMPORT_RESOURCE_TEMPLATE, pathToResource);
    }

    private String createEmptyFile() throws Exception {
        File file = createConfigFile("hz1", "xml");
        FileOutputStream outputStream = new FileOutputStream(file);
        writeStringToStreamAndClose(outputStream, "");
        return file.getAbsolutePath();
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceThrowsException() {
        buildConfig(xmlContentWithImportResource(""), null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportNotExistingResourceThrowsException() {
        buildConfig(xmlContentWithImportResource("notexisting.xml"), null);
    }

    @Test(expected = HazelcastException.class)
    public void testImportFromNonHazelcastConfigThrowsException() throws Exception {
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

        String xml = ""
                + "<non-hazelcast>\n"
                + "  <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n"
                + "</non-hazelcast>";

        Config config = buildConfig(xml, null);
        assertNull(config.getMapConfig("mymap"));
    }

    @Override
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

    @Override
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
                + "    </map>\n"
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

    @Override
    @Test
    public void testImportOverlappingMapConfigFromFile() throws Exception {
        File file = createConfigFile("mymap", "config");
        FileOutputStream os = new FileOutputStream(file);
        String mapConfig = HAZELCAST_START_TAG
                + "    <map name=\"mymap\">\n"
                + "       <backup-count>6</backup-count>"
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
                + "    <map name=\"mymap\">\n"
                + "       <time-to-live-seconds>10</time-to-live-seconds>"
                + "</map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml, null);
        MapConfig myMapConfig = config.getMapConfig("mymap");
        assertEquals("mymap", myMapConfig.getName());
        assertEquals(10, myMapConfig.getTimeToLiveSeconds());

        // these are the defaults here not overridden with the content of
        // the imported document
        // this is a difference between importing overlapping XML and YAML
        // configuration
        // YAML recursively merges the two files
        assertEquals(1, myMapConfig.getBackupCount());
        MapStoreConfig myMapStoreConfig = myMapConfig.getMapStoreConfig();
        assertEquals(0, myMapStoreConfig.getWriteDelaySeconds());
        assertEquals(1, myMapStoreConfig.getWriteBatchSize());
        assertNull(myMapStoreConfig.getClassName());
    }

    @Override
    @Test
    public void testMapConfigFromMainAndImportedFile() throws Exception {
        File file = createConfigFile("importmap", "config");
        FileOutputStream os = new FileOutputStream(file);
        String mapConfig = HAZELCAST_START_TAG
                + "    <map name=\"importedMap\">\n"
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
                + "    <map name=\"mapInMain\">\n"
                + "       <backup-count>2</backup-count>"
                + "       <time-to-live-seconds>5</time-to-live-seconds>"
                + "</map>\n"
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml, null);
        MapConfig mapInMainMapConfig = config.getMapConfig("mapInMain");
        assertEquals("mapInMain", mapInMainMapConfig.getName());
        assertEquals(5, mapInMainMapConfig.getTimeToLiveSeconds());
        assertEquals(2, mapInMainMapConfig.getBackupCount());

        MapConfig importedMap = config.getMapConfig("importedMap");
        assertEquals("importedMap", importedMap.getName());
        assertEquals(10, importedMap.getTimeToLiveSeconds());
        assertEquals(6, importedMap.getBackupCount());
        MapStoreConfig myMapStoreConfig = importedMap.getMapStoreConfig();
        assertEquals(10, myMapStoreConfig.getWriteDelaySeconds());
        assertEquals(100, myMapStoreConfig.getWriteBatchSize());
        assertEquals("com.hazelcast.examples.MyMapStore", myMapStoreConfig.getClassName());
    }

    @Override
    @Test
    public void testImportConfigFromClassPath() {
        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"classpath:test-hazelcast.xml\"/>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, null);
        assertEquals("foobar-xml", config.getClusterName());
    }

    @Override
    @Test
    public void testReplacers() throws Exception {
        File passwordFile = tempFolder.newFile(getClass().getSimpleName() + ".pwd");
        PrintWriter out = new PrintWriter(passwordFile);
        try {
            out.print("This is a password");
        } finally {
            IOUtil.closeResource(out);
        }
        String xml = HAZELCAST_START_TAG
                + "    <config-replacers>\n"
                + "        <replacer class-name='" + EncryptionReplacer.class.getName() + "'>\n"
                + "            <properties>\n"
                + "                <property name='passwordFile'>" + passwordFile.getAbsolutePath() + "</property>\n"
                + "                <property name='passwordUserProperties'>false</property>\n"
                + "                <property name='keyLengthBits'>64</property>\n"
                + "                <property name='saltLengthBytes'>8</property>\n"
                + "                <property name='cipherAlgorithm'>DES</property>\n"
                + "                <property name='secretKeyFactoryAlgorithm'>PBKDF2WithHmacSHA1</property>\n"
                + "                <property name='secretKeyAlgorithm'>DES</property>\n"
                + "            </properties>\n"
                + "        </replacer>\n"
                + "        <replacer class-name='" + IdentityReplacer.class.getName() + "'/>\n"
                + "    </config-replacers>\n"
                + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
                + "    <instance-name>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</instance-name>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, System.getProperties());
        assertEquals(System.getProperty("java.version") + " dev", config.getClusterName());
        assertEquals("My very secret secret", config.getInstanceName());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testMissingReplacement() throws Exception {
        String xml = HAZELCAST_START_TAG
                + "    <config-replacers>\n"
                + "        <replacer class-name='" + EncryptionReplacer.class.getName() + "'/>\n"
                + "    </config-replacers>\n"
                + "    <cluster-name>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</cluster-name>\n"
                + HAZELCAST_END_TAG;
        buildConfig(xml, System.getProperties());
    }

    @Override
    @Test
    public void testBadVariableSyntaxIsIgnored() {
        String xml = HAZELCAST_START_TAG
                + "    <cluster-name>${noSuchPropertyAvailable]</cluster-name>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, System.getProperties());
        assertEquals("${noSuchPropertyAvailable]", config.getClusterName());
    }

    @Override
    @Test
    public void testReplacerProperties() throws Exception {
        String xml = HAZELCAST_START_TAG
                + "    <config-replacers fail-if-value-missing='false'>\n"
                + "        <replacer class-name='" + TestReplacer.class.getName() + "'>\n"
                + "            <properties>\n"
                + "                <property name='p1'>a property</property>\n"
                + "                <property name='p2'/>\n"
                + "                <property name='p3'>another property</property>\n"
                + "                <property name='p4'>&lt;test/&gt;</property>\n"
                + "            </properties>\n"
                + "        </replacer>\n"
                + "    </config-replacers>\n"
                + "    <cluster-name>$T{p1} $T{p2} $T{p3} $T{p4} $T{p5}</cluster-name>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, System.getProperties());
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    @Override
    @Test
    public void testNoConfigReplacersMissingProperties() throws Exception {
        String xml = HAZELCAST_START_TAG
                + "    <cluster-name>${noSuchPropertyAvailable}</cluster-name>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, System.getProperties());
        assertEquals("${noSuchPropertyAvailable}", config.getClusterName());
    }

    @Override
    @Test
    public void testVariableReplacementAsSubstring() {
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

    @Override
    @Test
    public void testImportWithVariableReplacementAsSubstring() throws Exception {
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

    @Override
    @Test
    public void testReplaceVariablesWithFileSystemConfig() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String configXml = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop\">${variable}</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os, configXml);

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new FileSystemXmlConfig(file, properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithInMemoryConfig() {
        String configXml = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop\">${variable}</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new InMemoryXmlConfig(configXml, properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithClasspathConfig() {
        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new ClasspathXmlConfig("test-hazelcast-variable.xml", properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithUrlConfig() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String configXml = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop\">${variable}</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;
        writeStringToStreamAndClose(os, configXml);

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new UrlXmlConfig("file:///" + file.getPath(), properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesUseSystemProperties() {
        String configXml = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop\">${variable}</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;

        System.setProperty("variable", "foobar");
        Config config = buildConfig(configXml);

        assertEquals("foobar", config.getProperty("prop"));
    }

    private static Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
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
