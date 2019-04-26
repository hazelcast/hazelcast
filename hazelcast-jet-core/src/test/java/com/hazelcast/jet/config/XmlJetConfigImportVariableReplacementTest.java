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
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.jet.config.XmlJetConfigBuilderTest.JET_END_TAG;
import static com.hazelcast.jet.config.XmlJetConfigBuilderTest.JET_START_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class XmlJetConfigImportVariableReplacementTest extends AbstractJetConfigImportVariableReplacementTest {

    private static final String TEST_XML_JET_WITH_VARIABLES = "hazelcast-jet-with-variables.xml";

    @Test(expected = InvalidConfigurationException.class)
    public void testImportElementOnlyAppearsInTopLevel() {
        String xml = JET_START_TAG
                + "   <properties>"
                + "        <import resource=\"\"/>\n"
                + "   </properties>"
                + JET_END_TAG;
        JetConfig.loadXmlFromString(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastJetElementOnlyAppearsOnce() {
        String xml = JET_START_TAG
                + "   <hazelcast-jet>"
                + "   </hazelcast-jet>"
                + JET_END_TAG;
        JetConfig.loadXmlFromString(xml);
    }

    @Override
    public void readVariables() {
        //Given
        String xml = JET_START_TAG +
                "   <metrics enabled=\"${metrics.enabled}\" jmxEnabled=\"${jmx.enabled}\">\n" +
                "        <collection-interval-seconds>${metrics.collection}</collection-interval-seconds>\n" +
                "    </metrics>\n\n" +
                JET_END_TAG;

        Properties properties = new Properties();
        properties.setProperty("metrics.enabled", "false");
        properties.setProperty("jmx.enabled", "false");
        properties.setProperty("metrics.collection", "6");

        //When
        JetConfig jetConfig = JetConfig.loadXmlFromString(xml, properties);

        //Then
        MetricsConfig metricsCfg = jetConfig.getMetricsConfig();
        assertFalse("isEnabled", metricsCfg.isEnabled());
        assertFalse("isJmxEnabled", metricsCfg.isJmxEnabled());
        assertEquals("metricsCollectionInterval", 6, metricsCfg.getCollectionIntervalSeconds());
    }

    @Override
    public void testImportConfigFromResourceVariables() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigXml = JET_START_TAG +
                "   <metrics enabled=\"true\" jmxEnabled=\"false\">\n" +
                "        <collection-interval-seconds>122</collection-interval-seconds>\n" +
                "    </metrics>\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os, metricsConfigXml);

        String xml = JET_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + JET_END_TAG;
        //When
        JetConfig jetConfig = buildConfig(xml, "config.location", file.getAbsolutePath());

        //Then
        MetricsConfig metricsCfg = jetConfig.getMetricsConfig();
        assertTrue("isEnabled", metricsCfg.isEnabled());
        assertFalse("isJmxEnabled", metricsCfg.isJmxEnabled());
        assertEquals("metricsCollectionInterval", 122, metricsCfg.getCollectionIntervalSeconds());
    }


    @Override
    public void testImportedConfigVariableReplacement() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigXml = JET_START_TAG +
                "   <metrics enabled=\"true\" jmxEnabled=\"false\">\n" +
                "        <collection-interval-seconds>${metrics.collection}</collection-interval-seconds>\n" +
                "    </metrics>\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os, metricsConfigXml);

        String xml = JET_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + JET_END_TAG;
        //When
        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        properties.setProperty("metrics.collection", "222");

        JetConfig jetConfig = JetConfig.loadXmlFromString(xml, properties);

        //Then
        MetricsConfig metricsCfg = jetConfig.getMetricsConfig();
        assertTrue("isEnabled", metricsCfg.isEnabled());
        assertFalse("isJmxEnabled", metricsCfg.isJmxEnabled());
        assertEquals("metricsCollectionInterval", 222, metricsCfg.getCollectionIntervalSeconds());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("jet1", "xml");
        File config2 = createConfigFile("jet2", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        String config1Xml = JET_START_TAG
                + "    <import resource=\"file:///" + config2.getAbsolutePath() + "\"/>\n"
                + JET_END_TAG;
        String config2Xml = JET_START_TAG
                + "    <import resource=\"file:///" + config1.getAbsolutePath() + "\"/>\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);
        JetConfig.loadXmlFromString(config1Xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        String template = JET_START_TAG
                + "    <import resource=\"file:///%s\"/>\n"
                + JET_END_TAG;
        File config1 = createConfigFile("jet1", "xml");
        File config2 = createConfigFile("jet2", "xml");
        File config3 = createConfigFile("jet3", "xml");
        String config1Xml = String.format(template, config2.getAbsolutePath());
        String config2Xml = String.format(template, config3.getAbsolutePath());
        String config3Xml = String.format(template, config1.getAbsolutePath());
        writeStringToStreamAndClose(new FileOutputStream(config1), config1Xml);
        writeStringToStreamAndClose(new FileOutputStream(config2), config2Xml);
        writeStringToStreamAndClose(new FileOutputStream(config3), config3Xml);
        JetConfig.loadXmlFromString(config1Xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        File config1 = createConfigFile("jet1", "xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        String config1Xml = JET_START_TAG
                + "    <import resource='file:///" + config1.getAbsolutePath() + "'/>\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os1, "");
        JetConfig.loadXmlFromString(config1Xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceThrowsException() {
        String xml = JET_START_TAG
                + "    <import resource=\"\"/>\n"
                + JET_END_TAG;
        JetConfig.loadXmlFromString(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportNotExistingResourceThrowsException() {
        String xml = JET_START_TAG
                + "    <import resource=\"notexisting.xml\"/>\n"
                + JET_END_TAG;
        JetConfig.loadXmlFromString(xml);
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void testImportFromNonHazelcastJetConfigThrowsException() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigXml = JET_START_TAG +
                "   <metrics enabled=\"true\" jmxEnabled=\"false\">\n" +
                "    </metrics>\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os, metricsConfigXml);

        String xml = "<non-hazelcast-jet>"
                + "    <import resource=\"${config.location}\"/>\n"
                + "</non-hazelcast-jet>";
        //When
        JetConfig.loadXmlFromString(xml);
    }

    @Override
    public void testImportMetricsConfigFromFile() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String metricsConfigXml = JET_START_TAG +
                "   <metrics enabled=\"false\" jmxEnabled=\"false\">\n" +
                "        <collection-interval-seconds>123</collection-interval-seconds>\n" +
                "        <retention-seconds>124</retention-seconds>\n" +
                "        <metrics-for-data-structures>true</metrics-for-data-structures>\n" +
                "    </metrics>\n\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os, metricsConfigXml);

        String xml = JET_START_TAG
                + "    <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n"
                + JET_END_TAG;
        //When
        JetConfig jetConfig = JetConfig.loadXmlFromString(xml);

        //Then
        MetricsConfig metricsCfg = jetConfig.getMetricsConfig();
        assertFalse("isEnabled", metricsCfg.isEnabled());
        assertFalse("isJmxEnabled", metricsCfg.isJmxEnabled());
        assertEquals("metricsRetentionSeconds", 124, metricsCfg.getRetentionSeconds());
        assertEquals("metricsCollectionInterval", 123, metricsCfg.getCollectionIntervalSeconds());
        assertTrue("metricsForDataStructures", metricsCfg.isMetricsForDataStructuresEnabled());
    }

    @Override
    public void testImportInstanceConfigFromFile() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String instanceConfigXml = JET_START_TAG +
                "    <instance>\n" +
                "        <cooperative-thread-count>66</cooperative-thread-count>\n" +
                "        <flow-control-period>51</flow-control-period>\n" +
                "        <backup-count>4</backup-count>\n" +
                "        <scale-up-delay-millis>1234</scale-up-delay-millis>\n" +
                "        <lossless-restart-enabled>true</lossless-restart-enabled>\n" +
                "    </instance>\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os, instanceConfigXml);

        String xml = JET_START_TAG
                + "    <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n"
                + JET_END_TAG;
        //When
        JetConfig jetConfig = JetConfig.loadXmlFromString(xml);

        //Then
        InstanceConfig instanceConfig = jetConfig.getInstanceConfig();
        assertEquals("cooperativeThreadCount", 66, instanceConfig.getCooperativeThreadCount());
        assertEquals("backupCount", 4, instanceConfig.getBackupCount());
        assertEquals("flowControlMs", 51, instanceConfig.getFlowControlPeriodMs());
        assertEquals("scaleUpDelayMillis", 1234, instanceConfig.getScaleUpDelayMillis());
        assertTrue("losslessRestartEnabled", instanceConfig.isLosslessRestartEnabled());

    }

    @Override
    public void testImportEdgeConfigFromFile() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String edgeConfigXml = JET_START_TAG +
                "    <edge-defaults>\n" +
                "       <queue-size>111</queue-size>\n" +
                "       <packet-size-limit>222</packet-size-limit>\n" +
                "       <receive-window-multiplier>333</receive-window-multiplier>\n" +
                "    </edge-defaults>\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os, edgeConfigXml);

        String xml = JET_START_TAG
                + "    <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n"
                + JET_END_TAG;
        //When
        JetConfig jetConfig = JetConfig.loadXmlFromString(xml);

        //Then
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        assertEquals("queueSize", 111, edgeConfig.getQueueSize());
        assertEquals("packetSizeLimit", 222, edgeConfig.getPacketSizeLimit());
        assertEquals("receiveWindowMultiplier", 333, edgeConfig.getReceiveWindowMultiplier());
    }

    @Override
    public void testReplacers() throws Exception {
        //Given
        File passwordFile = tempFolder.newFile(getClass().getSimpleName() + ".pwd");
        PrintWriter out = new PrintWriter(passwordFile);
        try {
            out.print("This is a password");
        } finally {
            IOUtil.closeResource(out);
        }
        String xml = JET_START_TAG
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
                + "    <properties>\n"
                + "        <property name=\"test\">${java.version} $ID{dev}</property>\n"
                + "        <property name=\"pw\">$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</property>\n"
                + "    </properties>\n"
                + JET_END_TAG;
        //When
        Properties properties = JetConfig.loadXmlFromString(xml, System.getProperties()).getProperties();

        //Then
        assertEquals(System.getProperty("java.version") + " dev", properties.getProperty("test"));
        assertEquals("My very secret secret", properties.getProperty("pw"));
    }

    @Override
    @Test(expected = ConfigurationException.class)
    public void testMissingReplacement() throws Exception {
        String xml = JET_START_TAG
                + "    <config-replacers>\n"
                + "        <replacer class-name='" + EncryptionReplacer.class.getName() + "'/>\n"
                + "    </config-replacers>\n"
                + "    <properties>\n"
                + "        <property name=\"pw\">$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</property>\n"
                + "    </properties>\n"
                + JET_END_TAG;
        JetConfig.loadXmlFromString(xml, System.getProperties());
    }

    @Override
    public void testBadVariableSyntaxIsIgnored() throws Exception {
        //Given
        String xml = JET_START_TAG
                + "    <properties>\n"
                + "        <property name=\"pw\">${noSuchPropertyAvailable]</property>\n"
                + "    </properties>\n"
                + JET_END_TAG;

        //When
        Properties properties = JetConfig.loadXmlFromString(xml, System.getProperties()).getProperties();

        //Then
        assertEquals("${noSuchPropertyAvailable]", properties.getProperty("pw"));
    }

    @Override
    public void testReplacerProperties() throws Exception {
        //Given
        String xml = JET_START_TAG
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
                + "    <properties>\n"
                + "        <property name=\"pw\">$T{p1} $T{p2} $T{p3} $T{p4} $T{p5}</property>\n"
                + "    </properties>\n"
                + JET_END_TAG;
        //When
        Properties properties = JetConfig.loadXmlFromString(xml, System.getProperties()).getProperties();

        //Then
        assertEquals("a property  another property <test/> $T{p5}", properties.getProperty("pw"));

    }

    @Override
    public void testNoConfigReplacersMissingProperties() throws Exception {
        //Given
        String xml = JET_START_TAG
                + "    <properties>\n"
                + "        <property name=\"pw\">${noSuchPropertyAvailable]</property>\n"
                + "    </properties>\n"
                + JET_END_TAG;

        //When
        Properties properties = JetConfig.loadXmlFromString(xml, System.getProperties()).getProperties();

        //Then
        assertEquals("${noSuchPropertyAvailable]", properties.getProperty("pw"));
    }

    @Override
    public void testVariableReplacementAsSubstring() {
        //Given
        String xml = JET_START_TAG
                + "    <properties>\n"
                + "        <property name=\"${env}-with-suffix\">local-with-suffix</property>\n"
                + "        <property name=\"with-prefix-${env}\">with-prefix-local</property>\n"
                + "    </properties>\n"
                + JET_END_TAG;

        //When
        Properties properties = buildConfig(xml, "env", "local").getProperties();

        //Then
        assertEquals(properties.getProperty("local-with-suffix"), "local-with-suffix");
        assertEquals(properties.getProperty("with-prefix-local"), "with-prefix-local");
    }

    @Override
    public void testImportWithVariableReplacementAsSubstring() throws Exception {
        //Given
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String edgeConfigXml = JET_START_TAG
                + "    <properties>\n"
                + "        <property name=\"${env}-with-suffix\">local-with-suffix</property>\n"
                + "        <property name=\"with-prefix-${env}\">with-prefix-local</property>\n"
                + "    </properties>\n"
                + JET_END_TAG;
        writeStringToStreamAndClose(os, edgeConfigXml);

        String xml = JET_START_TAG
                + "    <import resource=\"file:///" + file.getAbsolutePath() + "\"/>\n"
                + JET_END_TAG;
        //When
        Properties properties = buildConfig(xml, "env", "local").getProperties();

        //Then
        assertEquals(properties.getProperty("local-with-suffix"), "local-with-suffix");
        assertEquals(properties.getProperty("with-prefix-local"), "with-prefix-local");
    }

    @Override
    public void testReplaceVariablesWithFileSystemConfig() throws Exception {
        //Given
        Properties properties = getProperties();
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_JET_WITH_VARIABLES);
            os.write(Util.readFully(resourceAsStream));
        }

        //When
        JetConfig config = JetConfig.loadFromFile(tempFile, properties);

        //Then
        assertPropertiesOnConfig(config);
    }

    @Override
    public void testReplaceVariablesWithInMemoryConfig() {
        //Given
        String xml = JET_START_TAG +
                "    <properties>\n" +
                "       <property name=\"property\">${prop.value}</property>\n" +
                "    </properties>\n" +
                JET_END_TAG;

        //When
        Properties properties = new Properties();
        properties.put("prop.value", "foobar");
        JetConfig jetConfig = JetConfig.loadXmlFromString(xml, properties);

        //Then
        assertEquals("foobar", jetConfig.getProperties().getProperty("property"));
    }

    @Override
    public void testReplaceVariablesWithClasspathConfig() {
        //Given
        Properties properties = getProperties();

        //When
        JetConfig config = JetConfig.loadFromClasspath(getClass().getClassLoader(),
                TEST_XML_JET_WITH_VARIABLES, properties);

        //Then
        assertPropertiesOnConfig(config);
    }

    @Override
    public void testReplaceVariablesWithUrlConfig() throws Exception {
        //Given
        Properties properties = getProperties();
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_JET_WITH_VARIABLES);
            os.write(Util.readFully(resourceAsStream));
        }

        //When
        InputStream inputStream = new URL("file:///" + tempFile.getPath()).openStream();
        JetConfig config = JetConfig.loadXmlFromStream(inputStream, properties);

        //Then
        assertPropertiesOnConfig(config);
    }

    @Override
    public void testReplaceVariablesUseSystemProperties() throws Exception {
        //Given
        String xml = JET_START_TAG +
                "    <properties>\n" +
                "       <property name=\"property\">${prop.value}</property>\n" +
                "    </properties>\n" +
                JET_END_TAG;

        //When
        System.setProperty("prop.value", "foobar");
        JetConfig jetConfig = JetConfig.loadXmlFromString(xml);

        //Then
        assertEquals("foobar", jetConfig.getProperties().getProperty("property"));
    }

    private static JetConfig buildConfig(String yaml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return JetConfig.loadXmlFromString(yaml, properties);
    }

}
