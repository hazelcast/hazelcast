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

package com.hazelcast.client.config;

import com.hazelcast.config.AbstractConfigImportVariableReplacementTest;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.replacer.EncryptionReplacer;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RootCauseMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.client.config.YamlClientConfigBuilderTest.buildConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class YamlClientConfigImportVariableReplacementTest extends AbstractClientConfigImportVariableReplacementTest {
    @Rule
    public ExpectedException rule = ExpectedException.none();

    @Override
    @Test
    public void testImportElementOnlyAppearsInTopLevel() throws IOException {
        File config1 = createConfigFile("hz1", ".yaml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Yaml = ""
                + "hazelcast-client:\n"
                + "  instance-name: my-instance";
        writeStringToStreamAndClose(os1, config1Yaml);
        String yaml = ""
                + "hazelcast-client:\n"
                + "  network:\n"
                + "    import:\n"
                + "      - file:///" + config1.getAbsolutePath() + "\"";

        ClientConfig clientConfig = buildConfig(yaml);

        // verify that instance-name is not set, because the import is not
        // processed when defined at this level
        assertNull(clientConfig.getInstanceName());
    }

    @Test
    public void testImportNoHazelcastClientRootNode() throws Exception {
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
        ClientConfig clientConfig = buildConfig(yaml, "file", file.getAbsolutePath());
        assertEquals("my-instance", clientConfig.getInstanceName());
        assertEquals("value1", clientConfig.getProperty("prop1"));
        assertEquals("value2", clientConfig.getProperty("prop2"));
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastElementOnlyAppearsOnce() {
        String yaml = ""
                + "hazelcast-client: {}\n"
                + "hazelcast-client: {}";

        buildConfig(yaml);
    }

    @Override
    @Test
    public void readVariables() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  executor-pool-size: ${executor.pool.size}";

        ClientConfig config = buildConfig(yaml, "executor.pool.size", "40");
        assertEquals(40, config.getExecutorPoolSize());
    }

    @Override
    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
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
        writeStringToStreamAndClose(os, networkConfig);

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}";

        ClientConfig config = buildConfig(yaml, "config.location", file.getAbsolutePath());
        assertFalse(config.getNetworkConfig().isSmartRouting());
        assertTrue(config.getNetworkConfig().isRedoOperation());
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.100.100");
        assertContains(config.getNetworkConfig().getAddresses(), "127.0.0.10");
    }

    @Override
    @Test
    public void testImportedConfigVariableReplacement() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = ""
                + "hazelcast-client:\n"
                + "  network:\n"
                + "    cluster-members:\n"
                + "      - ${ip.address}";
        writeStringToStreamAndClose(os, networkConfig);

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}";

        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        properties.setProperty("ip.address", "192.168.5.5");
        ClientConfig config = buildConfig(yaml, properties);
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.5.5");
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", ".yaml");
        File config2 = createConfigFile("hz2", ".yaml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        String config1Yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - file:///" + config2.getAbsolutePath() + "\"";
        String config2Yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - file:///" + config1.getAbsolutePath() + "\"";
        writeStringToStreamAndClose(os1, config1Yaml);
        writeStringToStreamAndClose(os2, config2Yaml);

        buildConfig(config1Yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", ".yaml");
        File config2 = createConfigFile("hz2", ".yaml");
        File config3 = createConfigFile("hz3", ".yaml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        FileOutputStream os3 = new FileOutputStream(config2);
        String config1Yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - file:///" + config2.getAbsolutePath() + "\"";
        String config2Yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - file:///" + config3.getAbsolutePath() + "\"";
        String config3Yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - file:///" + config1.getAbsolutePath() + "\"";
        writeStringToStreamAndClose(os1, config1Yaml);
        writeStringToStreamAndClose(os2, config2Yaml);
        writeStringToStreamAndClose(os3, config3Yaml);
        buildConfig(config1Yaml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        File config = createConfigFile("hz1", ".yaml");
        FileOutputStream os = new FileOutputStream(config);
        String configYaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - file:///" + config.getAbsolutePath() + "\"";
        writeStringToStreamAndClose(os, "");
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
                + "hazelcast-client:\n"
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
                + "      - class-name: " + AbstractConfigImportVariableReplacementTest.IdentityReplacer.class.getName() + "\n"
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
                + "      - class-name: " + AbstractConfigImportVariableReplacementTest.TestReplacer.class.getName() + "\n"
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
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: name1";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: name2";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/cluster-name"));

        buildConfig(yaml, "config.location", file.getAbsolutePath());
    }

    @Test
    public void testImportSameScalarConfig() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: name";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: name";

        ClientConfig config = buildConfig(yaml, "config.location", file.getAbsolutePath());
        assertEquals("name", config.getClusterName());
    }

    @Test
    public void testImportNodeScalarVsSequenceThrows() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: name1";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name:\n"
                + "    - seqName: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/cluster-name"));

        buildConfig(yaml, "config.location", file.getAbsolutePath());
    }

    @Test
    public void testImportNodeScalarVsMappingThrows() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: name1";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/cluster-name"));

        buildConfig(yaml, "config.location", file.getAbsolutePath());
    }

    @Test
    public void testImportNodeSequenceVsMappingThrows() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name:\n"
                + "    - seqname";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  cluster-name: {}";

        rule.expect(new RootCauseMatcher(InvalidConfigurationException.class, "hazelcast-client/cluster-name"));

        buildConfig(yaml, "config.location", file.getAbsolutePath());
    }

    @Test
    public void testImportNodeSequenceVsSequenceMerges() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String importedYaml = ""
                + "hazelcast-client:\n"
                + "  client-labels:\n"
                + "    - label1\n";
        writeStringToStreamAndClose(os, importedYaml);

        String yaml = ""
                + "hazelcast-client:\n"
                + "  import:\n"
                + "    - ${config.location}\n"
                + "  client-labels:\n"
                + "    - label2\n";

        ClientConfig config = buildConfig(yaml, "config.location", file.getAbsolutePath());
        Set<String> labels = config.getLabels();
        assertEquals(2, labels.size());
        assertTrue(labels.contains("label1"));
        assertTrue(labels.contains("label2"));
    }

}
