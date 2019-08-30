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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RootCauseMatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlConfigImportVariableReplacementTest extends AbstractConfigImportVariableReplacementTest {

    @Override
    @Test
    public void testHazelcastElementOnlyAppearsOnce() {
        String yaml = ""
                + "hazelcast:\n{}"
                + "hazelcast:";
        expectInvalid();
        buildConfig(yaml, null);
    }

    @Override
    @Test
    public void readVariables() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    ${name}:\n"
                + "      backup-count: ${backupcount.part1}${backupcount.part2}\n";

        Properties properties = new Properties();
        properties.setProperty("name", "s");

        properties.setProperty("backupcount.part1", "0");
        properties.setProperty("backupcount.part2", "6");
        Config config = buildConfig(yaml, properties);
        MapConfig mapConfig = config.getMapConfig("s");
        assertEquals(6, mapConfig.getBackupCount());
        assertEquals(0, mapConfig.getAsyncBackupCount());
    }

    @Override
    @Test
    public void testImportConfigFromResourceVariables() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n";
        writeStringToStreamAndClose(os, networkConfig);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n";
        Config config = buildConfig(yaml, "config.location", file.getAbsolutePath());
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test
    public void testImportedConfigVariableReplacement() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      tcp-ip:\n"
                + "        enabled: ${tcp.ip.enabled}\n";
        writeStringToStreamAndClose(os, networkConfig);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}";

        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        properties.setProperty("tcp.ip.enabled", "true");
        Config config = buildConfig(yaml, properties);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", "yaml");
        File config2 = createConfigFile("hz2", "yaml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        String config1Yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///" + config2.getAbsolutePath();
        String config2Yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///" + config1.getAbsolutePath();
        writeStringToStreamAndClose(os1, config1Yaml);
        writeStringToStreamAndClose(os2, config2Yaml);
        expectInvalid();
        buildConfig(config1Yaml, null);
    }

    @Override
    @Test
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        String template = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///%s";
        File config1 = createConfigFile("hz1", "yaml");
        File config2 = createConfigFile("hz2", "yaml");
        File config3 = createConfigFile("hz3", "yaml");
        String config1Yaml = String.format(template, config2.getAbsolutePath());
        String config2Yaml = String.format(template, config3.getAbsolutePath());
        String config3Yaml = String.format(template, config1.getAbsolutePath());
        writeStringToStreamAndClose(new FileOutputStream(config1), config1Yaml);
        writeStringToStreamAndClose(new FileOutputStream(config2), config2Yaml);
        writeStringToStreamAndClose(new FileOutputStream(config3), config3Yaml);
        expectInvalid();
        buildConfig(config1Yaml, null);
    }

    @Override
    @Test
    public void testImportEmptyResourceContent() throws Exception {
        File config1 = createConfigFile("hz1", "yaml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///" + config1.getAbsolutePath();
        writeStringToStreamAndClose(os1, "%invalid-yaml");
        expectInvalid();
        buildConfig(config1Yaml, null);
    }

    @Override
    @Test
    public void testImportEmptyResourceThrowsException() {
        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - \"\"";
        expectInvalid();
        buildConfig(yaml, null);
    }

    @Override
    @Test
    public void testImportNotExistingResourceThrowsException() {
        expectInvalid();
        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - notexisting.yaml";
        buildConfig(yaml, null);
    }

    @Override
    @Test
    public void testImportNetworkConfigFromFile() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n";
        writeStringToStreamAndClose(os, networkConfig);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///" + file.getAbsolutePath();

        Config config = buildConfig(yaml, null);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test
    public void testImportMapConfigFromFile() throws Exception {
        File file = createConfigFile("mymap", "config");
        FileOutputStream os = new FileOutputStream(file);
        String mapConfig = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      backup-count: 6\n"
                + "      time-to-live-seconds: 10\n"
                + "      map-store:\n"
                + "        enabled: true\n"
                + "        initial-mode: LAZY\n"
                + "        class-name: com.hazelcast.examples.MyMapStore\n"
                + "        write-delay-seconds: 10\n"
                + "        write-batch-size: 100\n";
        writeStringToStreamAndClose(os, mapConfig);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///" + file.getAbsolutePath();

        Config config = buildConfig(yaml, null);
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
        String mapConfig = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      backup-count: 6\n"
                + "      map-store:\n"
                + "        enabled: true\n"
                + "        initial-mode: LAZY\n"
                + "        class-name: com.hazelcast.examples.MyMapStore\n"
                + "        write-delay-seconds: 10\n"
                + "        write-batch-size: 100\n";
        writeStringToStreamAndClose(os, mapConfig);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///" + file.getAbsolutePath() + "\n"
                + "  map:\n"
                + "    mymap:\n"
                + "      time-to-live-seconds: 10\n";

        Config config = buildConfig(yaml, null);
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
    public void testMapConfigFromMainAndImportedFile() throws Exception {
        File file = createConfigFile("importmap", "config");
        FileOutputStream os = new FileOutputStream(file);
        String mapConfig = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    importedMap:\n"
                + "      backup-count: 6\n"
                + "      time-to-live-seconds: 10\n"
                + "      map-store:\n"
                + "        enabled: true\n"
                + "        initial-mode: LAZY\n"
                + "        class-name: com.hazelcast.examples.MyMapStore\n"
                + "        write-delay-seconds: 10\n"
                + "        write-batch-size: 100\n";
        writeStringToStreamAndClose(os, mapConfig);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///" + file.getAbsolutePath() + "\n"
                + "  map:\n"
                + "    mapInMain:\n"
                + "      backup-count: 2\n"
                + "      time-to-live-seconds: 5\n";

        Config config = buildConfig(yaml, null);

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
        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - classpath:test-hazelcast.yaml";
        Config config = buildConfig(yaml, null);
        assertEquals("foobar-yaml", config.getClusterName());
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
        String yaml = ""
                + "hazelcast:\n"
                + "  config-replacers:\n"
                + "    replacers:\n"
                + "      - class-name: " + EncryptionReplacer.class.getName() + "\n"
                + "        properties:\n"
                + "          passwordFile: " + passwordFile.getAbsolutePath() + "\n"
                + "          passwordUserProperties: false\n"
                + "          keyLengthBits: 64\n"
                + "          saltLengthBytes: 8\n"
                + "          cipherAlgorithm: DES\n"
                + "          secretKeyFactoryAlgorithm: PBKDF2WithHmacSHA1\n"
                + "          secretKeyAlgorithm: DES\n"
                + "      - class-name: " + IdentityReplacer.class.getName() + "\n"
                + "  cluster-name: ${java.version} $ID{dev}\n"
                + "  instance-name: $ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}\n";
        Config config = buildConfig(yaml, System.getProperties());
        assertEquals(System.getProperty("java.version") + " dev", config.getClusterName());
        assertEquals("My very secret secret", config.getInstanceName());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testMissingReplacement() {
        String yaml = ""
                + "hazelcast:\n"
                + "  config-replacers:\n"
                + "    replacers:\n"
                + "      - class-name: " + EncryptionReplacer.class.getName() + "\n"
                + "  cluster-name: $ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}";
        buildConfig(yaml, System.getProperties());
    }

    @Override
    @Test
    public void testBadVariableSyntaxIsIgnored() {
        String yaml = ""
                + "hazelcast:\n"
                + "  cluster-name: ${noSuchPropertyAvailable]";
        Config config = buildConfig(yaml, System.getProperties());
        assertEquals("${noSuchPropertyAvailable]", config.getClusterName());
    }

    @Override
    @Test
    public void testReplacerProperties() {
        String yaml = ""
                + "hazelcast:\n"
                + "  config-replacers:\n"
                + "    fail-if-value-missing: false\n"
                + "    replacers:\n"
                + "      - class-name: " + TestReplacer.class.getName() + "\n"
                + "        properties:\n"
                + "          p1: a property\n"
                + "          p2: \"\"\n"
                + "          p3: another property\n"
                + "          p4: <test/>\n"
                + "  cluster-name: $T{p1} $T{p2} $T{p3} $T{p4} $T{p5}\n";
        Config config = buildConfig(yaml, System.getProperties());
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    /**
     * Given: No replacer is used in the configuration file<br>
     * When: A property variable is used within the file<br>
     * Then: The configuration parsing doesn't fail and the variable string remains unchanged (i.e. backward compatible
     * behavior, as if {@code fail-if-value-missing} attribute is {@code false}).
     */
    @Override
    @Test
    public void testNoConfigReplacersMissingProperties() {
        String yaml = ""
                + "hazelcast:\n"
                + "  cluster-name: ${noSuchPropertyAvailable}";
        Config config = buildConfig(yaml, System.getProperties());
        assertEquals("${noSuchPropertyAvailable}", config.getClusterName());
    }

    @Override
    @Test
    public void testVariableReplacementAsSubstring() {
        String yaml = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    ${env}-with-suffix: local-with-suffix\n"
                + "    with-prefix-${env}: with-prefix-local";

        Config config = buildConfig(yaml, "env", "local");
        assertEquals("local-with-suffix", config.getProperty("local-with-suffix"));
        assertEquals("with-prefix-local", config.getProperty("with-prefix-local"));
    }

    @Override
    @Test
    public void testImportWithVariableReplacementAsSubstring() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    prop1: value1\n"
                + "    prop2: value2\n";
        writeStringToStreamAndClose(os, networkConfig);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///" + "${file}";
        Config config = buildConfig(yaml, "file", file.getAbsolutePath());
        assertEquals("value1", config.getProperty("prop1"));
        assertEquals("value2", config.getProperty("prop2"));
    }

    @Test
    public void testImportNoHazelcastRootNode() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "properties:\n"
                + "  prop1: value1\n"
                + "  prop2: value2\n";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "import:\n"
                + "  - file:///" + "${file}\n"
                + "instance-name: my-instance";
        Config config = buildConfig(yaml, "file", file.getAbsolutePath());
        assertEquals("my-instance", config.getInstanceName());
        assertEquals("value1", config.getProperty("prop1"));
        assertEquals("value2", config.getProperty("prop2"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithFileSystemConfig() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String configYaml = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    prop: ${variable}";
        writeStringToStreamAndClose(os, configYaml);

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new FileSystemYamlConfig(file, properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithInMemoryConfig() {
        String configYaml = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    prop: ${variable}";

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new InMemoryYamlConfig(configYaml, properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithClasspathConfig() {
        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new ClasspathYamlConfig("test-hazelcast-variable.yaml", properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithUrlConfig() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String configYaml = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    prop: ${variable}";
        writeStringToStreamAndClose(os, configYaml);

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new UrlYamlConfig("file:///" + file.getPath(), properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesUseSystemProperties() {
        String configYaml = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    prop: ${variable}";

        System.setProperty("variable", "foobar");
        Config config = buildConfig(configYaml);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Test
    public void testImportRedefinesSameConfigScalarThrows() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name: name1";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: name2";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/cluster-name"));

        buildConfig(yaml, "config.location", file.getAbsolutePath());
    }

    @Test
    public void testImportSameScalarConfig() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name: name";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: name";

        Config config = buildConfig(yaml, "config.location", file.getAbsolutePath());
        assertEquals("name", config.getClusterName());
    }

    @Test
    public void testImportNodeScalarVsSequenceThrows() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name: name1";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name:\n"
                + "    - seqName: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/cluster-name"));

        buildConfig(yaml, "config.location", file.getAbsolutePath());
    }

    @Test
    public void testImportNodeScalarVsMappingThrows() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name: name1";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/cluster-name"));

        buildConfig(yaml, "config.location", file.getAbsolutePath());
    }

    @Test
    public void testImportNodeSequenceVsMappingThrows() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name:\n"
                + "    - seqname";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/cluster-name"));

        buildConfig(yaml, "config.location", file.getAbsolutePath());
    }

    @Test
    public void testImportNodeSequenceVsSequenceMerges() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast:\n"
                + "  listeners:\n"
                + "    - com.hazelcast.examples.MembershipListener\n";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  listeners:\n"
                + "    - com.hazelcast.examples.MigrationListener\n";

        Config config = buildConfig(yaml, "config.location", file.getAbsolutePath());
        List<ListenerConfig> listenerConfigs = config.getListenerConfigs();
        assertEquals(2, listenerConfigs.size());
        for (ListenerConfig listenerConfig : listenerConfigs) {
            assertTrue("com.hazelcast.examples.MembershipListener".equals(listenerConfig.getClassName())
                    || "com.hazelcast.examples.MigrationListener".equals(listenerConfig.getClassName()));
        }

    }

    private static Config buildConfig(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlConfigBuilder configBuilder = new YamlConfigBuilder(bis);
        return configBuilder.build();
    }

    private static Config buildConfig(String yaml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlConfigBuilder configBuilder = new YamlConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    private static Config buildConfig(String yaml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return buildConfig(yaml, properties);
    }

}
