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

package com.hazelcast.client.config;

import com.hazelcast.config.AbstractConfigImportVariableReplacementTest.IdentityReplacer;
import com.hazelcast.config.AbstractConfigImportVariableReplacementTest.TestReplacer;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.helpers.DeclarativeConfigFileHelper;
import com.hazelcast.config.replacer.EncryptionReplacer;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.config.SchemaViolationConfigurationException;
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

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.client.config.YamlClientConfigBuilderTest.buildConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlClientConfigImportVariableReplacementTest extends AbstractClientConfigImportVariableReplacementTest {
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
    @Test(expected = SchemaViolationConfigurationException.class)
    public void testImportElementOnlyAppearsInTopLevel() throws IOException {
        String config1Yaml = ""
                + "hazelcast-client:\n"
                + "  instance-name: my-instance";
        String configPath = helper.givenConfigFileInWorkDir("hz1.yaml", config1Yaml).getAbsolutePath();
        String yaml = ""
                + "hazelcast-client:\n"
                + "  network:\n"
                + "    import:\n"
                + "      - " + configPath + "\"";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastElementOnlyAppearsOnce() {
        String yaml = ""
                + "hazelcast-client: {}\n"
                + "hazelcast-client: {}";

        buildConfig(yaml);
    }

    @Test
    @Override
    public void testImportResourceWithConfigReplacers() throws IOException {
        String configReplacer = ""
            + "hazelcast-client:\n"
            + "  config-replacers:\n"
            + "    replacers:\n"
            + "      - class-name: " + IdentityReplacer.class.getName() + "\n";

        String configLocation = helper.givenConfigFileInWorkDir("config-replacer.yaml", configReplacer).getAbsolutePath();

        String clusterName = ""
            + "hazelcast-client:\n"
            + "  import:\n"
            + "    - " + "${config.location}\n"
            + "  cluster-name: ${java.version} $ID{dev}\n";

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", configLocation);
        ClientConfig groupConfig = buildConfig(clusterName, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Test
    public void testImportResourceWithNestedImports() throws IOException {
        String configReplacer = ""
            + "hazelcast-client:\n"
            + "  config-replacers:\n"
            + "    replacers:\n"
            + "      - class-name: " + IdentityReplacer.class.getName() + "\n";

        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.yaml", configReplacer).getAbsolutePath();

        String clusterName = ""
            + "hazelcast-client:\n"
            + "  import:\n"
            + "    - " + configReplacerLocation + "\n"
            + "  cluster-name: ${java.version} $ID{dev}\n";

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.yaml", clusterName).getAbsolutePath();

        String yaml = ""
            + "hazelcast-client:\n"
            + "  import:\n"
            + "    - " + "${config.location}\n";

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        ClientConfig groupConfig = buildConfig(yaml, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Test
    @Override
    public void testImportResourceWithNestedImportsAndProperties() throws IOException {
        String configReplacer = ""
            + "hazelcast-client:\n"
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
            + "hazelcast-client:\n"
            + "  import:\n"
            + "    - " + configReplacerLocation + "\n"
            + "  cluster-name: $T{p1} $T{p2} $T{p3} $T{p4} $T{p5}\n";

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.yaml", clusterName).getAbsolutePath();

        String yaml = ""
            + "hazelcast-client:\n"
            + "  import:\n"
            + "    - " + "${config.location}\n";

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        properties.put("p1", "a property");
        ClientConfig config = buildConfig(yaml, properties);
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    @Override
    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
        String networkConfig = ""
                + "hazelcast-client:\n"
                + "  network:\n"
                + "    cluster-members:\n"
                + "      - 192.168.100.100\n"
                + "      - 127.0.0.10\n"
                + "    smart-routing: false\n"
                + "    redo-operation: true\n"
                + "    socket-interceptor:\n"
                + "      enabled: true\n"
                + "      class-name: com.hazelcast.examples.MySocketInterceptor\n"
                + "      properties:\n"
                + "        foo: bar";
        String networkConfigPath = helper.givenConfigFileInWorkDir("foo.bar", networkConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}";

        ClientConfig config = buildConfig(yaml, "config.location", networkConfigPath);
        assertFalse(config.getNetworkConfig().isSmartRouting());
        assertTrue(config.getNetworkConfig().isRedoOperation());
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.100.100");
        assertContains(config.getNetworkConfig().getAddresses(), "127.0.0.10");
    }

    @Override
    @Test
    public void testImportedConfigVariableReplacement() throws IOException {
        String networkConfig = ""
                + "hazelcast-client:\n"
                + "  network:\n"
                + "    cluster-members:\n"
                + "      - ${ip.address}";
        String networkConfigPath = helper.givenConfigFileInWorkDir("foo.bar", networkConfig).getAbsolutePath();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}";

        Properties properties = new Properties();
        properties.setProperty("config.location", networkConfigPath);
        properties.setProperty("ip.address", "192.168.5.5");
        ClientConfig config = buildConfig(yaml, properties);
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.5.5");
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        String yamlWithCyclicImport = helper.createFilesWithCycleImports(
            this::contentWithImportResource,
            helper.givenConfigFileInWorkDir("hz1.yaml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz2.yaml", "").getAbsolutePath()
        );

        buildConfig(yamlWithCyclicImport);
    }

    private String contentWithImportResource(String path) {
        return ""
            + "hazelcast-client:\n"
            + "  import:\n"
            + "    - " + path;
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

        buildConfig(yamlWithCyclicImport);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        String emptyFilePath = helper.givenConfigFileInWorkDir("hz1.yaml", "").getAbsolutePath();
        String configYaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - " + emptyFilePath + "\"";
        buildConfig(configYaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceThrowsException() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - \"\"";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportNotExistingResourceThrowsException() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - notexisting.yaml";

        buildConfig(yaml);
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void testImportNotExistingUrlResourceThrowsException() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - file:///notexisting.yaml";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void testReplacers() throws Exception {
        String passwordFilePath = helper.givenConfigFileInWorkDir(getClass().getSimpleName() + ".pwd", "This is a password")
            .getAbsolutePath();
        String yaml = ""
                + "hazelcast-client:\n"
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
                + "  instance-name: $ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}";
        ClientConfig config = buildConfig(yaml, System.getProperties());
        assertEquals(System.getProperty("java.version") + " dev", config.getClusterName());
        assertEquals("My very secret secret", config.getInstanceName());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testMissingReplacement() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  config-replacers:\n"
                + "    replacers:\n"
                + "      - class-name: " + EncryptionReplacer.class.getName() + "\n"
                + "    cluster-name: $ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}\n";
        buildConfig(yaml, System.getProperties());
    }

    @Override
    @Test
    public void testReplacerProperties() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  config-replacers:\n"
                + "    fail-if-value-missing: false\n"
                + "    replacers:\n"
                + "      - class-name: " + TestReplacer.class.getName() + "\n"
                + "        properties:\n"
                + "          p1: a property\n"
                + "          p2: \"\"\n"
                + "          p3: another property\n"
                + "          p4: <test/>\n"
                + "  cluster-name: $T{p1} $T{p2} $T{p3} $T{p4} $T{p5}";
        ClientConfig config = buildConfig(yaml, System.getProperties());
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    @Override
    @Test
    public void testNoConfigReplacersMissingProperties() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: ${noSuchPropertyAvailable}";

        ClientConfig clientConfig = buildConfig(yaml, System.getProperties());
        assertEquals("${noSuchPropertyAvailable}", clientConfig.getClusterName());
    }

    @Override
    @Test
    public void testImportConfigFromClassPath() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - classpath:hazelcast-client-c1.yaml";

        ClientConfig config = buildConfig(yaml);
        assertEquals("cluster1", config.getClusterName());
    }

    @Override
    @Test
    public void testReplaceVariablesUseSystemProperties() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  properties:\n"
                + "    prop: ${variable}";

        System.setProperty("variable", "foobar");
        ClientConfig config = buildConfig(yaml);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithClasspathConfig() {
        Properties properties = new Properties();
        properties.put("variable", "foobar");
        ClientConfig config = new ClientClasspathYamlConfig("test-hazelcast-client-variable.yaml", properties);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Test
    public void testImportRedefinesSameConfigScalarThrows() throws Exception {
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: name1";
        String configPath = helper.givenConfigFileInWorkDir("foo.bar", importedYaml)
            .getAbsolutePath();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: name2";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/cluster-name"));

        buildConfig(yaml, "config.location", configPath);
    }

    @Test
    public void testImportSameScalarConfig() throws Exception {
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: name";
        String configPath = helper.givenConfigFileInWorkDir("foo.bar", importedYaml)
            .getAbsolutePath();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: name";

        ClientConfig config = buildConfig(yaml, "config.location", configPath);
        assertEquals("name", config.getClusterName());
    }

    @Test
    public void testImportNodeScalarVsSequenceThrows() throws Exception {
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: name1";
        String configPath = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name:\n"
                + "    - seqName: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/cluster-name"));

        buildConfig(yaml, "config.location", configPath);
    }

    @Test
    public void testImportNodeScalarVsMappingThrows() throws Exception {
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: name1";
        String configPath = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/cluster-name"));

        buildConfig(yaml, "config.location", configPath);
    }

    @Test
    public void testImportNodeSequenceVsMappingThrows() throws Exception {
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name:\n"
                + "    - seqname";
        String configPath = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/cluster-name"));

        buildConfig(yaml, "config.location", configPath);
    }

    @Test
    public void testImportNodeSequenceVsSequenceMerges() throws Exception {
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  client-labels:\n"
                + "    - label1\n";
        String configPath = helper.givenConfigFileInWorkDir("foo.bar", importedYaml).getAbsolutePath();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  client-labels:\n"
                + "    - label2\n";

        ClientConfig config = buildConfig(yaml, "config.location", configPath);
        Set<String> labels = config.getLabels();
        assertEquals(2, labels.size());
        assertTrue(labels.contains("label1"));
        assertTrue(labels.contains("label2"));
    }
}
