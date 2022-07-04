/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.helpers.DeclarativeConfigFileHelper;
import com.hazelcast.config.replacer.EncryptionReplacer;
import com.hazelcast.config.test.builders.ConfigReplacerBuilder;
import com.hazelcast.config.test.builders.MapXmlConfigBuilder;
import com.hazelcast.config.test.builders.MapXmlStoreConfigBuilder;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_END_TAG;
import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_START_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlConfigImportVariableReplacementTest extends AbstractConfigImportVariableReplacementTest {

    private DeclarativeConfigFileHelper helper;

    @Before
    public void setUp() {
        helper = new DeclarativeConfigFileHelper();
    }

    @After
    public void tearDown() {
        helper.ensureTestConfigDeleted();
    }

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
                + "        <backup-count>${async.backup.count}${backup.count}</backup-count>\n"
                + "    </map>"
                + HAZELCAST_END_TAG;

        Properties properties = createMapProperties("map", 6, 0);
        Config config = buildConfig(xml, properties);
        MapConfig mapConfig = config.getMapConfig("map");
        assertEquals(6, mapConfig.getBackupCount());
        assertEquals(0, mapConfig.getAsyncBackupCount());
    }

    private Properties createMapProperties(String name, int backupCount, int asyncBackupCount) {
        Properties properties = new Properties();
        properties.setProperty("name", name);

        properties.setProperty("async.backup.count", String.valueOf(asyncBackupCount));
        properties.setProperty("backup.count", String.valueOf(backupCount));
        return properties;
    }

    @Override
    @Test
    public void testImportResourceWithConfigReplacers() throws IOException {
        String configReplacer = HAZELCAST_START_TAG
                + "    <config-replacers>\n"
                + "        <replacer class-name='" + IdentityReplacer.class.getName() + "'/>\n"
                + "    </config-replacers>\n"
                + HAZELCAST_END_TAG;
        String configLocation = helper.givenConfigFileInWorkDir("config-replacer.xml", configReplacer).getAbsolutePath();

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
                + HAZELCAST_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", configLocation);
        Config groupConfig = buildConfig(xml, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Override
    @Test
    public void testImportResourceWithNestedImports() throws IOException {
        String configReplacer = HAZELCAST_START_TAG
            + "    <config-replacers>\n"
            + "        <replacer class-name='" + IdentityReplacer.class.getName() + "'/>\n"
            + "    </config-replacers>\n"
            + HAZELCAST_END_TAG;
        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.xml", configReplacer).getAbsolutePath();

        String clusterName = HAZELCAST_START_TAG
            + "    <import resource=\"" + "file:///" + configReplacerLocation + "\"/>\n"
            + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
            + HAZELCAST_END_TAG;

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.xml", clusterName).getAbsolutePath();

        String xml = HAZELCAST_START_TAG
            + "    <import resource=\"${config.location}\"/>\n"
            + HAZELCAST_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        Config groupConfig = buildConfig(xml, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Override
    @Test
    public void testImportResourceWithNestedImportsAndProperties() throws IOException {
        ConfigReplacerBuilder testReplacer = new ConfigReplacerBuilder()
            .withClass(TestReplacer.class)
            .addProperty("p1", "${p1}")
            .addProperty("p2", "")
            .addProperty("p3", "another property")
            .addProperty("p4", "&lt;test/&gt;");
        String configReplacer = HAZELCAST_START_TAG
            + "    <config-replacers fail-if-value-missing='false'>\n"
            + testReplacer.build()
            + "    </config-replacers>\n"
            + HAZELCAST_END_TAG;
        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.xml", configReplacer).getAbsolutePath();

        String clusterName = HAZELCAST_START_TAG
            + "    <import resource=\"" + "file:///" + configReplacerLocation + "\"/>\n"
            + "    <cluster-name>$T{p1} $T{p2} $T{p3} $T{p4} $T{p5}</cluster-name>\n"
            + HAZELCAST_END_TAG;

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.xml", clusterName).getAbsolutePath();

        String xml = HAZELCAST_START_TAG
            + "    <import resource=\"${config.location}\"/>\n"
            + HAZELCAST_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        properties.put("p1", "a property");
        Config config = buildConfig(xml, properties);
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    @Override
    @Test
    public void testImportConfigFromResourceVariables() throws Exception {
        String networkConfig = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"true\"/>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;
        String configLocation = helper.givenConfigFileInWorkDir("config-network.xml", networkConfig).getAbsolutePath();

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, "config.location", configLocation);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test
    public void testImportedConfigVariableReplacement() throws Exception {
        String networkConfig = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"${tcp.ip.enabled}\"/>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;
        String configLocation = helper.givenConfigFileInWorkDir("config-network.xml", networkConfig).getAbsolutePath();

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HAZELCAST_END_TAG;

        Properties properties = new Properties();
        properties.setProperty("config.location", configLocation);
        properties.setProperty("tcp.ip.enabled", "true");
        Config config = buildConfig(xml, properties);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {

        String xmlWithCyclicImport = helper.createFilesWithCycleImports(
            this::xmlContentWithImportResource,
            helper.givenConfigFileInWorkDir("hz1.xml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz2.xml", "").getAbsolutePath()
        );

        buildConfig(xmlWithCyclicImport, null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        String xmlWithCyclicImport = helper.createFilesWithCycleImports(
            this::xmlContentWithImportResource,
            helper.givenConfigFileInWorkDir("hz1.xml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz2.xml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz3.xml", "").getAbsolutePath()
        );

        buildConfig(xmlWithCyclicImport, null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        String pathToEmptyFile = createEmptyFile();
        buildConfig(xmlContentWithImportResource(pathToEmptyFile), null);
    }

    @Override
    String contentWithImportResource(String url) {
        return xmlContentWithImportResource(url);
    }

    private String xmlContentWithImportResource(String importPath) {
        return HAZELCAST_START_TAG
                + "    <import resource=\"" + importPath + "\"/>\n"
                + HAZELCAST_END_TAG;
    }

    private String createEmptyFile() throws Exception {
        return helper.givenConfigFileInWorkDir("hz1.xml", "").getAbsolutePath();
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceThrowsException() {
        buildConfig(xmlContentWithImportResource(""), null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportNotExistingResourceThrowsException() {
        buildConfig(xmlContentWithImportResource("not_existing.xml"), null);
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void testImportNotExistingUrlResourceThrowsException() {
        buildConfig(xmlContentWithImportResource("file:///not_existing.xml"), null);
    }

    @Test(expected = HazelcastException.class)
    public void testImportFromNonHazelcastConfigThrowsException() throws Exception {
        String xmlContent = new MapXmlConfigBuilder()
            .withName("mymap")
            .build();
        String pathTo = helper.givenConfigFileInWorkDir("mymap.xml", HAZELCAST_START_TAG + xmlContent + HAZELCAST_END_TAG).getAbsolutePath();

        String nonHazelcastXml = "<non-hazelcast>\n"
                + "    <import resource=\"file:///" + pathTo + "\"/>\n"
                + "</non-hazelcast>";

        buildConfig(nonHazelcastXml, null);
    }

    @Override
    @Test
    public void testImportNetworkConfigFromFile() throws Exception {
        String networkConfig = HAZELCAST_START_TAG
                + "    <network>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"true\"/>\n"
                + "        </join>\n"
                + "    </network>\n"
                + HAZELCAST_END_TAG;
        String path = helper.givenConfigFileInWorkDir("config-network.xml", networkConfig).getAbsolutePath();

        Config config = buildConfig(xmlContentWithImportResource(path), null);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test
    public void testImportMapConfigFromFile() throws Exception {
        final String mapName = "mymap";
        final int mapBackupCount = 6;
        final int mapTimeToLiveSeconds = 10;
        final int mapStoreWriteDelaySeconds = 10;
        final int mapStoreWriteBatchSize = 100;
        MapXmlConfigBuilder mapXmlConfigBuilder = new MapXmlConfigBuilder()
                .withName(mapName)
                .withBackupCount(mapBackupCount)
                .withTimeToLive(mapTimeToLiveSeconds)
                .withStore(new MapXmlStoreConfigBuilder()
                                   .enabled()
                                   .withInitialMode(MapStoreConfig.InitialLoadMode.LAZY)
                                   .withClassName("com.hazelcast.examples.MyMapStore")
                                   .withWriteDelay(mapStoreWriteDelaySeconds)
                                   .withWriteBatchSize(mapStoreWriteBatchSize));

        String mapConfig = HAZELCAST_START_TAG
                + mapXmlConfigBuilder.build()
                + HAZELCAST_END_TAG;
        String path = helper.givenConfigFileInWorkDir("mymap.xml", mapConfig).getAbsolutePath();

        Config config = buildConfig(xmlContentWithImportResource(path), null);

        MapConfig myMapConfig = config.getMapConfig(mapName);
        assertEquals(mapName, myMapConfig.getName());
        assertEquals(mapBackupCount, myMapConfig.getBackupCount());
        assertEquals(mapTimeToLiveSeconds, myMapConfig.getTimeToLiveSeconds());

        MapStoreConfig myMapStoreConfig = myMapConfig.getMapStoreConfig();
        assertEquals(mapStoreWriteDelaySeconds, myMapStoreConfig.getWriteDelaySeconds());
        assertEquals(mapStoreWriteBatchSize, myMapStoreConfig.getWriteBatchSize());
        assertEquals("com.hazelcast.examples.MyMapStore", myMapStoreConfig.getClassName());
    }

    @Override
    @Test
    public void testImportOverlappingMapConfigFromFile() throws Exception {
        final String mapName = "mymap";
        final int mapBackupCount = 6;
        final int mapStoreWriteDelaySeconds = 10;
        final int mapStoreWriteBatchSize = 100;
        final String mapStoreClassName = "com.hazelcast.examples.MyMapStore";
        MapXmlConfigBuilder importedMapConfig = new MapXmlConfigBuilder()
                .withName(mapName)
                .withBackupCount(mapBackupCount)
                .withStore(new MapXmlStoreConfigBuilder()
                                   .enabled()
                                   .withInitialMode(MapStoreConfig.InitialLoadMode.LAZY)
                                   .withClassName(mapStoreClassName)
                                   .withWriteDelay(mapStoreWriteDelaySeconds)
                                   .withWriteBatchSize(mapStoreWriteBatchSize));

        final String mapConfigXml = HAZELCAST_START_TAG + importedMapConfig.build() + HAZELCAST_END_TAG;
        String path = helper.givenConfigFileInWorkDir("mymap.xml", mapConfigXml).getAbsolutePath();

        final int mapTimeToLiveSeconds = 10;
        MapXmlConfigBuilder mainMapConfig = new MapXmlConfigBuilder()
            .withName(mapName)
            .withTimeToLive(mapTimeToLiveSeconds);

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"file:///" + path + "\"/>\n"
                + mainMapConfig.build()
                + HAZELCAST_END_TAG;

        Config config = buildConfig(xml, null);
        MapConfig myMapConfig = config.getMapConfig("mymap");
        assertEquals("mymap", myMapConfig.getName());
        assertEquals(10, myMapConfig.getTimeToLiveSeconds());

        assertEquals(1, myMapConfig.getBackupCount());
        MapStoreConfig myMapStoreConfig = myMapConfig.getMapStoreConfig();
        assertEquals(mapStoreWriteDelaySeconds, myMapStoreConfig.getWriteDelaySeconds());
        assertEquals(mapStoreWriteBatchSize, myMapStoreConfig.getWriteBatchSize());
        assertEquals(mapStoreClassName, myMapStoreConfig.getClassName());
    }

    @Override
    @Test
    public void testMapConfigFromMainAndImportedFile() throws Exception {
        MapXmlConfigBuilder importedMapConfig = new MapXmlConfigBuilder()
                .withName("importedMap")
                .withBackupCount(6)
                .withTimeToLive(10)
                .withStore(new MapXmlStoreConfigBuilder().enabled()
                                   .withInitialMode(MapStoreConfig.InitialLoadMode.LAZY)
                                   .withClassName("com.hazelcast.examples.MyMapStore")
                                   .withWriteDelay(10)
                                   .withWriteBatchSize(100));
        String mapConfig = HAZELCAST_START_TAG
                + importedMapConfig.build()
                + HAZELCAST_END_TAG;
        String path = helper.givenConfigFileInWorkDir("imported_map.xml", mapConfig).getAbsolutePath();

        MapXmlConfigBuilder mapInMain = new MapXmlConfigBuilder()
            .withName("mapInMain")
            .withBackupCount(2)
            .withTimeToLive(5);
        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"file:///" + path + "\"/>\n"
                + mapInMain.build()
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
        String pathToFileWithPassword = helper.givenConfigFileInWorkDir(getClass().getSimpleName() + ".pwd", "This is a password").getAbsolutePath();

        ConfigReplacerBuilder encryptionReplacer = new ConfigReplacerBuilder()
                .withClass(EncryptionReplacer.class)
                .addProperty("passwordFile", pathToFileWithPassword)
                .addProperty("passwordUserProperties", false)
                .addProperty("keyLengthBits", 64)
                .addProperty("saltLengthBytes", 8)
                .addProperty("cipherAlgorithm", "DES")
                .addProperty("secretKeyFactoryAlgorithm", "PBKDF2WithHmacSHA1")
                .addProperty("secretKeyAlgorithm", "DES");
        ConfigReplacerBuilder identityReplacer = new ConfigReplacerBuilder()
                .withClass(IdentityReplacer.class);

        String xml = HAZELCAST_START_TAG
                + "    <config-replacers>\n"
                + encryptionReplacer.build()
                + identityReplacer.build()
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
    public void testMissingReplacement() {
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
    public void testReplacerProperties() {
        ConfigReplacerBuilder testReplacer = new ConfigReplacerBuilder()
            .withClass(TestReplacer.class)
            .addProperty("p1", "a property")
            .addProperty("p2", "")
            .addProperty("p3", "another property")
            .addProperty("p4", "&lt;test/&gt;");
        String xml = HAZELCAST_START_TAG
                + "    <config-replacers fail-if-value-missing='false'>\n"
                + testReplacer.build()
                + "    </config-replacers>\n"
                + "    <cluster-name>$T{p1} $T{p2} $T{p3} $T{p4} $T{p5}</cluster-name>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, System.getProperties());
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    @Override
    @Test
    public void testNoConfigReplacersMissingProperties() {
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
        String networkConfig = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop1\">value1</property>\n"
                + "        <property name=\"prop2\">value2</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;

        String pathToFile = helper.givenConfigFileInWorkDir("config-properties.xml", networkConfig).getAbsolutePath();

        String xml = HAZELCAST_START_TAG
                + "    <import resource=\"file:///" + "${file}" + "\"/>\n"
                + HAZELCAST_END_TAG;
        Config config = buildConfig(xml, "file", pathToFile);
        assertEquals(config.getProperty("prop1"), "value1");
        assertEquals(config.getProperty("prop2"), "value2");
    }

    @Override
    @Test
    public void testReplaceVariablesWithFileSystemConfig() throws Exception {
        String configXml = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop\">${variable}</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;

        String pathToFile = helper.givenConfigFileInWorkDir("config-properties.xml", configXml).getAbsolutePath();

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new FileSystemXmlConfig(new File(pathToFile), properties);

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
        String configXml = HAZELCAST_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop\">${variable}</property>\n"
                + "    </properties>\n"
                + HAZELCAST_END_TAG;
        String path = helper.givenConfigFileInWorkDir("config-properties.xml", configXml).getAbsolutePath();

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new UrlXmlConfig("file:///" + path, properties);

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
