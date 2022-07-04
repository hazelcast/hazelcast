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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlConfigImportVariableReplacementTest extends AbstractConfigImportVariableReplacementTest {
    @Rule
    public ExpectedException rule = ExpectedException.none();

    private DeclarativeConfigFileHelper helper;

    @Before
    public void setUp() {
        helper = new DeclarativeConfigFileHelper();
    }

    @After
    public void tearDown() {
        helper.ensureTestConfigDeleted();
    }

    @Override
    String contentWithImportResource(String path) {
        return ""
            + "hazelcast:\n"
            + "  import:\n"
            + "    - " + path;
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastElementOnlyAppearsOnce() {
        String yaml = ""
                + "hazelcast:\n{}"
                + "hazelcast:";
        buildConfig(yaml, null);
    }

    @Override
    @Test
    public void readVariables() {
        String yaml = ""
                + "hazelcast:\n"
                + "  map:\n"
                + "    ${name}:\n"
                + "      backup-count: ${backup.count}\n"
                + "      async-backup-count: ${async.backup.count}\n";

        Properties properties = new Properties();
        properties.setProperty("name", "s");

        properties.setProperty("async.backup.count", "0");
        properties.setProperty("backup.count", "6");
        Config config = buildConfig(yaml, properties);
        MapConfig mapConfig = config.getMapConfig("s");
        assertEquals(6, mapConfig.getBackupCount());
        assertEquals(0, mapConfig.getAsyncBackupCount());
    }

    @Override
    @Test
    public void testImportResourceWithConfigReplacers() throws IOException {
        String configReplacer = ""
            + "hazelcast:\n"
            + "  config-replacers:\n"
            + "    replacers:\n"
            + "      - class-name: " + IdentityReplacer.class.getName() + "\n";

        String configLocation = helper.givenConfigFileInWorkDir("config-replacer.yaml", configReplacer).getAbsolutePath();

        String clusterName = ""
            + "hazelcast:\n"
            + "  import:\n"
            + "    - " + "${config.location}\n"
            + "  cluster-name: ${java.version} $ID{dev}\n";

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", configLocation);
        Config groupConfig = buildConfig(clusterName, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Override
    @Test
    public void testImportResourceWithNestedImports() throws IOException {
        String configReplacer = ""
            + "hazelcast:\n"
            + "  config-replacers:\n"
            + "    replacers:\n"
            + "      - class-name: " + IdentityReplacer.class.getName() + "\n";

        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.yaml", configReplacer).getAbsolutePath();

        String clusterName = ""
            + "hazelcast:\n"
            + "  import:\n"
            + "    - " + configReplacerLocation + "\n"
            + "  cluster-name: ${java.version} $ID{dev}\n";

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.yaml", clusterName).getAbsolutePath();

        String yaml = ""
            + "hazelcast:\n"
            + "  import:\n"
            + "    - " + "${config.location}\n";

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        Config groupConfig = buildConfig(yaml, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Override
    @Test
    public void testImportResourceWithNestedImportsAndProperties() throws IOException {
        String configReplacer = ""
            + "hazelcast:\n"
            + "  config-replacers:\n"
            + "    fail-if-value-missing: false\n"
            + "    replacers:\n"
            + "      - class-name: " + TestReplacer.class.getName() + "\n"
            + "        properties:\n"
            + "          p1: ${p1}\n"
            + "          p2: \"\"\n"
            + "          p3: another property\n"
            + "          p4: <test/>\n";

        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.yaml", configReplacer).getAbsolutePath();

        String clusterName = ""
            + "hazelcast:\n"
            + "  import:\n"
            + "    - " + configReplacerLocation + "\n"
            + "  cluster-name: $T{p1} $T{p2} $T{p3} $T{p4} $T{p5}\n";

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.yaml", clusterName).getAbsolutePath();

        String yaml = ""
            + "hazelcast:\n"
            + "  import:\n"
            + "    - " + "${config.location}\n";

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        properties.put("p1", "a property");
        Config config = buildConfig(yaml, properties);
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    @Override
    @Test
    public void testImportConfigFromResourceVariables() throws Exception {
        String networkConfig = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n";

        String configLocation = helper.givenConfigFileInWorkDir("foo.bar", networkConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n";
        Config config = buildConfig(yaml, "config.location", configLocation);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test
    public void testImportedConfigVariableReplacement() throws Exception {
        String networkConfig = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      tcp-ip:\n"
                + "        enabled: ${tcp.ip.enabled}\n";

        String configLocation = helper.givenConfigFileInWorkDir("foo.bar", networkConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}";

        Properties properties = new Properties();
        properties.setProperty("config.location", configLocation);
        properties.setProperty("tcp.ip.enabled", "true");
        Config config = buildConfig(yaml, properties);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        String yamlWithCyclicImport = helper.createFilesWithCycleImports(
            this::contentWithImportResource,
            helper.givenConfigFileInWorkDir("hz1.yaml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz2.yaml", "").getAbsolutePath()
        );

        buildConfig(yamlWithCyclicImport, null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        String yamlWithCyclicImport = helper.createFilesWithCycleImports(
            this::contentWithImportResource,
            helper.givenConfigFileInWorkDir("hz1.yaml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz2.yaml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz3.yaml", "").getAbsolutePath()
        );

        buildConfig(yamlWithCyclicImport, null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - " + helper.givenConfigFileInWorkDir("hz1.yaml", "%invalid-yaml").getAbsolutePath();

        buildConfig(yaml, null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceThrowsException() {
        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - \"\"";
        buildConfig(yaml, null);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportNotExistingResourceThrowsException() {
        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - notexisting.yaml";
        buildConfig(yaml, null);
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void testImportNotExistingUrlResourceThrowsException() {
        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - file:///notexisting.yaml";
        buildConfig(yaml, null);
    }

    @Override
    @Test
    public void testImportNetworkConfigFromFile() throws Exception {
        String networkConfig = ""
                + "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n";
        String path = helper.givenConfigFileInWorkDir("foo.bar", networkConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - " + path;

        Config config = buildConfig(yaml, null);
        JoinConfig join = config.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
    }

    @Override
    @Test
    public void testImportMapConfigFromFile() throws Exception {
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

        String path = helper.givenConfigFileInWorkDir("mymap.config", mapConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - " + path;

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

        String path = helper.givenConfigFileInWorkDir("mymap.config", mapConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - " + path + "\n"
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

        String path = helper.givenConfigFileInWorkDir("importmap.config", mapConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - " + path + "\n"
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
        String passwordFilePath = helper.givenConfigFileInWorkDir(getClass().getSimpleName() + ".pwd", "This is a password")
            .getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  config-replacers:\n"
                + "    replacers:\n"
                + "      - class-name: " + EncryptionReplacer.class.getName() + "\n"
                + "        properties:\n"
                + "          passwordFile: " + passwordFilePath + "\n"
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
        String networkConfig = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    prop1: value1\n"
                + "    prop2: value2\n";

        String path = helper.givenConfigFileInWorkDir("foo.bar", networkConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - " + "${file}";
        Config config = buildConfig(yaml, "file", path);
        assertEquals("value1", config.getProperty("prop1"));
        assertEquals("value2", config.getProperty("prop2"));
    }

    @Test
    public void testImportNoHazelcastRootNode() throws Exception {
        String importedYaml = ""
                + "properties:\n"
                + "  prop1: value1\n"
                + "  prop2: value2\n";

        String path = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = "hazelcast:\n"
                + "  import:\n"
                + "    - " + "${file}\n"
                + "  instance-name: my-instance";
        Config config = buildConfig(yaml, "file", path);
        assertEquals("my-instance", config.getInstanceName());
        assertEquals("value1", config.getProperty("prop1"));
        assertEquals("value2", config.getProperty("prop2"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithFileSystemConfig() throws Exception {
        String configYaml = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    prop: ${variable}";

        String path = helper.givenConfigFileInWorkDir("foo.bar", configYaml).getAbsolutePath();

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new FileSystemYamlConfig(path, properties);

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
        String configYaml = ""
                + "hazelcast:\n"
                + "  properties:\n"
                + "    prop: ${variable}";

        String path = helper.givenConfigFileInWorkDir("foo.bar", configYaml).getAbsolutePath();

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        Config config = new UrlYamlConfig("file:///" + path, properties);

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
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name: name1";

        String path = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: name2";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/cluster-name"));
        buildConfig(yaml, "config.location", path);
    }

    @Test
    public void testImportSameScalarConfig() throws Exception {
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name: name";

        String path = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: name";

        Config config = buildConfig(yaml, "config.location", path);
        assertEquals("name", config.getClusterName());
    }

    @Test
    public void testImportNodeScalarVsSequenceThrows() throws Exception {
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name: name1";
        String path = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name:\n"
                + "    - seqName: {}";
        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/cluster-name"));

        buildConfig(yaml, "config.location", path);
    }

    @Test
    public void testImportNodeScalarVsMappingThrows() throws Exception {
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name: name1";
        String path = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: {}";
        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/cluster-name"));

        buildConfig(yaml, "config.location", path);
    }

    @Test
    public void testImportNodeSequenceVsMappingThrows() throws Exception {
        String importedYaml = ""
                + "hazelcast:\n"
                + "  cluster-name:\n"
                + "    - seqname";
        String path = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast/cluster-name"));
        buildConfig(yaml, "config.location", path);
    }

    @Test
    public void testImportNodeSequenceVsSequenceMerges() throws Exception {
        String importedYaml = ""
                + "hazelcast:\n"
                + "  listeners:\n"
                + "    - com.hazelcast.examples.MembershipListener\n";
        String path = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  listeners:\n"
                + "    - com.hazelcast.examples.MigrationListener\n";

        Config config = buildConfig(yaml, "config.location", path);
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
