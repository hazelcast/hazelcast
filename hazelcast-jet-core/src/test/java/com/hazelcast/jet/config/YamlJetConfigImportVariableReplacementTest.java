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

package com.hazelcast.jet.config;

import com.hazelcast.config.AbstractConfigImportVariableReplacementTest.IdentityReplacer;
import com.hazelcast.config.AbstractConfigImportVariableReplacementTest.TestReplacer;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.replacer.EncryptionReplacer;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class YamlJetConfigImportVariableReplacementTest extends AbstractJetConfigImportVariableReplacementTest {

    private static final String TEST_YAML_JET_WITH_VARIABLES = "hazelcast-jet-with-variables.yaml";


    @Override
    public void testHazelcastJetElementOnlyAppearsOnce() {
        String yaml = ""
                + "hazelcast-jet:\n{}"
                + "hazelcast-jet:";

        rule.expect(InvalidConfigurationException.class);
        JetConfig.loadYamlFromString(yaml);
    }

    @Override
    public void readVariables() {
        // Given
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  metrics:\n"
                + "    collection-interval-seconds: ${interval}\n";

        Properties properties = new Properties();
        properties.setProperty("interval", "100");

        // When
        JetConfig config = JetConfig.loadYamlFromString(yaml, properties);

        // Then
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertEquals(100, metricsConfig.getCollectionIntervalSeconds());
    }

    @Override
    public void testImportConfigFromResourceVariables() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigStr = ""
                + "hazelcast-jet:\n"
                + "  metrics:\n"
                + "    enabled: true\n"
                + "    jmx-enabled: false\n"
                + "    collection-interval-seconds: 101\n";
        writeStringToStreamAndClose(os, metricsConfigStr);
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - ${config.location}\n";

        //When
        JetConfig config = buildConfig(yaml, "config.location", file.getAbsolutePath());

        //Then
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertTrue(metricsConfig.isEnabled());
        assertFalse(metricsConfig.isJmxEnabled());
        assertEquals(101, metricsConfig.getCollectionIntervalSeconds());
    }

    @Override
    public void testImportedConfigVariableReplacement() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigStr = ""
                + "hazelcast-jet:\n"
                + "  metrics:\n"
                + "    enabled: ${metrics.enabled}\n"
                + "    jmx-enabled: ${metrics.jmx.enabled}\n"
                + "    collection-interval-seconds: ${metrics.interval}\n";
        writeStringToStreamAndClose(os, metricsConfigStr);
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - ${config.location}\n";
        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        properties.setProperty("metrics.enabled", "false");
        properties.setProperty("metrics.jmx.enabled", "true");
        properties.setProperty("metrics.interval", "505");

        //When
        JetConfig config = JetConfig.loadYamlFromString(yaml, properties);

        //Then
        MetricsConfig metricsConfig = config.getMetricsConfig();
        assertFalse(metricsConfig.isEnabled());
        assertTrue(metricsConfig.isJmxEnabled());
        assertEquals(505, metricsConfig.getCollectionIntervalSeconds());
    }

    @Override
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("jet1", "yaml");
        File config2 = createConfigFile("jet2", "yaml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        String config1Yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///" + config2.getAbsolutePath();
        String config2Yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///" + config1.getAbsolutePath();
        writeStringToStreamAndClose(os1, config1Yaml);
        writeStringToStreamAndClose(os2, config2Yaml);
        rule.expect(InvalidConfigurationException.class);

        JetConfig.loadYamlFromString(config1Yaml);
    }

    @Override
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        String template = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///%s";
        File config1 = createConfigFile("jet1", "yaml");
        File config2 = createConfigFile("jet2", "yaml");
        File config3 = createConfigFile("jet3", "yaml");
        String config1Yaml = String.format(template, config2.getAbsolutePath());
        String config2Yaml = String.format(template, config3.getAbsolutePath());
        String config3Yaml = String.format(template, config1.getAbsolutePath());
        writeStringToStreamAndClose(new FileOutputStream(config1), config1Yaml);
        writeStringToStreamAndClose(new FileOutputStream(config2), config2Yaml);
        writeStringToStreamAndClose(new FileOutputStream(config3), config3Yaml);
        rule.expect(InvalidConfigurationException.class);

        JetConfig.loadYamlFromString(config1Yaml);
    }

    @Override
    public void testImportEmptyResourceContent() throws Exception {
        File config1 = createConfigFile("jet1", "yaml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///" + config1.getAbsolutePath();
        writeStringToStreamAndClose(os1, "%invalid-yaml");
        rule.expect(InvalidConfigurationException.class);

        JetConfig.loadYamlFromString(config1Yaml);

    }

    @Override
    public void testImportEmptyResourceThrowsException() {
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - \"\"";
        rule.expect(InvalidConfigurationException.class);

        JetConfig.loadYamlFromString(yaml);
    }

    @Override
    public void testImportNotExistingResourceThrowsException() {
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - notexisting.yaml";
        rule.expect(InvalidConfigurationException.class);

        JetConfig.loadYamlFromString(yaml);
    }

    @Test(expected = HazelcastException.class)
    public void testImportFromNonHazelcastJetConfigThrowsException() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigStr = ""
                + "non-jet:\n"
                + "  metrics:\n"
                + "    enabled: false\n"
                + "    jmx-enabled: false\n"
                + "    collection-interval-seconds: 101\n";
        writeStringToStreamAndClose(os, metricsConfigStr);

        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///" + file.getAbsolutePath();

        JetConfig config = JetConfig.loadYamlFromString(yaml);
        assertTrue(config.getMetricsConfig().isEnabled());
        assertTrue(config.getMetricsConfig().isJmxEnabled());
    }

    @Override
    public void testImportMetricsConfigFromFile() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigStr = ""
                + "hazelcast-jet:\n"
                + "  metrics:\n"
                + "    enabled: false\n"
                + "    jmx-enabled: false\n"
                + "    collection-interval-seconds: 101\n";
        writeStringToStreamAndClose(os, metricsConfigStr);

        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///" + file.getAbsolutePath();

        JetConfig config = JetConfig.loadYamlFromString(yaml);
        assertFalse(config.getMetricsConfig().isEnabled());
        assertFalse(config.getMetricsConfig().isJmxEnabled());
    }

    @Override
    public void testImportInstanceConfigFromFile() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigStr = ""
                + "hazelcast-jet:\n"
                + "  instance:\n"
                + "    cooperative-thread-count: 55\n"
                + "    flow-control-period: 50\n"
                + "    backup-count: 2\n"
                + "    scale-up-delay-millis: 1234\n"
                + "    lossless-restart-enabled: true\n";
        writeStringToStreamAndClose(os, metricsConfigStr);

        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///" + file.getAbsolutePath();

        JetConfig jetConfig = JetConfig.loadYamlFromString(yaml);
        InstanceConfig instanceConfig = jetConfig.getInstanceConfig();
        assertEquals("cooperativeThreadCount", 55, instanceConfig.getCooperativeThreadCount());
        assertEquals("backupCount", 2, instanceConfig.getBackupCount());
        assertEquals("flowControlMs", 50, instanceConfig.getFlowControlPeriodMs());
        assertEquals("scaleUpDelayMillis", 1234, instanceConfig.getScaleUpDelayMillis());
        assertTrue("losslessRestartEnabled", instanceConfig.isLosslessRestartEnabled());
    }

    @Override
    public void testImportEdgeConfigFromFile() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigStr = ""
                + "hazelcast-jet:\n"
                + "  edge-defaults:\n"
                + "    queue-size: 999\n"
                + "    packet-size-limit: 997\n"
                + "    receive-window-multiplier: 996\n";
        writeStringToStreamAndClose(os, metricsConfigStr);

        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///" + file.getAbsolutePath();

        JetConfig config = JetConfig.loadYamlFromString(yaml);
        EdgeConfig edgeConfig = config.getDefaultEdgeConfig();
        assertEquals(999, edgeConfig.getQueueSize());
        assertEquals(997, edgeConfig.getPacketSizeLimit());
        assertEquals(996, edgeConfig.getReceiveWindowMultiplier());
    }

    @Override
    public void testReplacers() throws Exception {
        File passwordFile = tempFolder.newFile(getClass().getSimpleName() + ".pwd");
        PrintWriter out = new PrintWriter(passwordFile);
        try {
            out.print("This is a password");
        } finally {
            IOUtil.closeResource(out);
        }
        String yaml = ""
                + "hazelcast-jet:\n"
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
                + "  properties:\n"
                + "    test: ${java.version} $ID{dev}\n"
                + "    pw: $ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}\n";
        Properties properties = JetConfig.loadYamlFromString(yaml, System.getProperties()).getProperties();
        assertEquals(System.getProperty("java.version") + " dev", properties.getProperty("test"));
        assertEquals("My very secret secret", properties.getProperty("pw"));

    }

    @Override
    @Test(expected = ConfigurationException.class)
    public void testMissingReplacement() throws Exception {
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  config-replacers:\n"
                + "    replacers:\n"
                + "      - class-name: " + EncryptionReplacer.class.getName() + "\n"
                + "  properties:\n"
                + "    name: $ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}";
        JetConfig.loadYamlFromString(yaml);
    }

    @Override
    public void testBadVariableSyntaxIsIgnored() throws Exception {
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  properties:\n"
                + "    name: ${noSuchPropertyAvailable]";
        Properties properties = JetConfig.loadYamlFromString(yaml, System.getProperties()).getProperties();
        assertEquals("${noSuchPropertyAvailable]", properties.getProperty("name"));
    }

    @Override
    public void testReplacerProperties() throws Exception {
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  config-replacers:\n"
                + "    fail-if-value-missing: false\n"
                + "    replacers:\n"
                + "      - class-name: " + TestReplacer.class.getName() + "\n"
                + "        properties:\n"
                + "          p1: a property\n"
                + "          p2: \"\"\n"
                + "          p3: another property\n"
                + "          p4: <test/>\n"
                + "  properties:\n"
                + "    name: $T{p1} $T{p2} $T{p3} $T{p4} $T{p5}\n";
        Properties properties = JetConfig.loadYamlFromString(yaml, System.getProperties()).getProperties();
        assertEquals("a property  another property <test/> $T{p5}", properties.getProperty("name"));
    }

    @Override
    public void testNoConfigReplacersMissingProperties() throws Exception {
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  properties:\n"
                + "    name: ${noSuchPropertyAvailable]";
        Properties properties = JetConfig.loadYamlFromString(yaml, System.getProperties()).getProperties();
        assertEquals("${noSuchPropertyAvailable]", properties.getProperty("name"));
    }

    @Override
    public void testVariableReplacementAsSubstring() {
        String yaml = ""
                + "hazelcast-jet:\n"
                + "  properties:\n"
                + "    ${env}-with-suffix: local-with-suffix\n"
                + "    with-prefix-${env}: with-prefix-local";
        Properties properties = buildConfig(yaml, "env", "local").getProperties();
        assertEquals(properties.getProperty("local-with-suffix"), "local-with-suffix");
        assertEquals(properties.getProperty("with-prefix-local"), "with-prefix-local");
    }

    @Override
    public void testImportWithVariableReplacementAsSubstring() throws Exception {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = ""
                + "hazelcast-jet:\n"
                + "  properties:\n"
                + "    prop1: value1\n"
                + "    prop2: value2\n";
        writeStringToStreamAndClose(os, networkConfig);

        String yaml = ""
                + "hazelcast-jet:\n"
                + "  import:\n"
                + "    - file:///" + "${file}";
        Properties properties = buildConfig(yaml, "file", file.getAbsolutePath()).getProperties();
        assertEquals(properties.getProperty("prop1"), "value1");
        assertEquals(properties.getProperty("prop2"), "value2");
    }

    @Override
    public void testReplaceVariablesWithFileSystemConfig() throws Exception {
        //Given
        Properties properties = getProperties();
        File tempFile = createConfigFile("jet", ".yaml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_YAML_JET_WITH_VARIABLES);
            os.write(Util.readFully(resourceAsStream));
        }

        //When
        JetConfig config = JetConfig.loadFromFile(tempFile, properties);

        //Then
        assertPropertiesOnConfig(config);
    }

    @Override
    public void testReplaceVariablesWithInMemoryConfig() {
        String configYaml = ""
                + "hazelcast-jet:\n"
                + "  properties:\n"
                + "    prop: ${variable}";

        Properties properties = new Properties();
        properties.put("variable", "foobar");
        JetConfig config = JetConfig.loadYamlFromString(configYaml, properties);

        assertEquals("foobar", config.getProperties().getProperty("prop"));
    }

    @Override
    public void testReplaceVariablesWithClasspathConfig() {
        //Given
        Properties properties = getProperties();

        //When
        JetConfig config = JetConfig.loadFromClasspath(getClass().getClassLoader(),
                TEST_YAML_JET_WITH_VARIABLES, properties);

        //Then
        assertPropertiesOnConfig(config);
    }

    @Override
    public void testReplaceVariablesWithUrlConfig() throws Exception {
        //Given
        Properties properties = getProperties();
        File tempFile = createConfigFile("jet", ".yaml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_YAML_JET_WITH_VARIABLES);
            os.write(Util.readFully(resourceAsStream));
        }

        //When
        InputStream inputStream = new URL("file:///" + tempFile.getPath()).openStream();
        JetConfig config = JetConfig.loadYamlFromStream(inputStream, properties);

        //Then
        assertPropertiesOnConfig(config);
    }

    @Override
    public void testReplaceVariablesUseSystemProperties() throws Exception {
        String configYaml = ""
                + "hazelcast-jet:\n"
                + "  properties:\n"
                + "    prop: ${variable}";

        System.setProperty("variable", "foobar");
        JetConfig config = JetConfig.loadYamlFromString(configYaml);

        assertEquals("foobar", config.getProperties().getProperty("prop"));
    }

    private static JetConfig buildConfig(String yaml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return JetConfig.loadYamlFromString(yaml, properties);
    }

}
