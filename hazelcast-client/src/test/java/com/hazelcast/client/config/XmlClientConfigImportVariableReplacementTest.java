/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigImportVariableReplacementTest.IdentityReplacer;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import static com.hazelcast.client.config.XmlClientConfigBuilderTest.HAZELCAST_CLIENT_END_TAG;
import static com.hazelcast.client.config.XmlClientConfigBuilderTest.HAZELCAST_CLIENT_START_TAG;
import static com.hazelcast.client.config.XmlClientConfigBuilderTest.buildConfig;
import static com.hazelcast.config.AbstractConfigBuilder.HAZELCAST_CONFIG_REPLACER_CLASS;
import static com.hazelcast.config.AbstractConfigBuilder.HAZELCAST_CONFIG_REPLACER_FAIL_IF_MISSING;
import static com.hazelcast.nio.IOUtil.closeResource;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class XmlClientConfigImportVariableReplacementTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test(expected = InvalidConfigurationException.class)
    public void testImportElementOnlyAppersInTopLevel() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "   <network>\n"
                + "        <import resource=\"\"/>\n"
                + "   </network>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastElementOnlyAppearsOnce() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "   <hazelcast-client>"
                + "   </hazelcast-client>"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Test
    public void readVariables() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "<executor-pool-size>${executor.pool.size}</executor-pool-size>"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml, "executor.pool.size", "40");
        assertEquals(40, config.getExecutorPoolSize());
    }

    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = HAZELCAST_CLIENT_START_TAG
                + "<network>"
                + "    <cluster-members>"
                + "      <address>192.168.100.100</address>"
                + "      <address>127.0.0.10</address>"
                + "    </cluster-members>"
                + "    <smart-routing>false</smart-routing>"
                + "    <redo-operation>true</redo-operation>"
                + "    <socket-interceptor enabled=\"true\">"
                + "      <class-name>com.hazelcast.examples.MySocketInterceptor</class-name>"
                + "      <properties>"
                + "        <property name=\"foo\">bar</property>"
                + "      </properties>"
                + "    </socket-interceptor>"
                + "  </network>"
                + HAZELCAST_CLIENT_END_TAG;
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml, "config.location", file.getAbsolutePath());
        assertFalse(config.getNetworkConfig().isSmartRouting());
        assertTrue(config.getNetworkConfig().isRedoOperation());
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.100.100");
        assertContains(config.getNetworkConfig().getAddresses(), "127.0.0.10");
    }

    @Test
    public void testImportedConfigVariableReplacement() throws IOException {
        File file = createConfigFile("foo", "bar");
        FileOutputStream os = new FileOutputStream(file);
        String networkConfig = HAZELCAST_CLIENT_START_TAG
                + "  <network>"
                + "    <cluster-members>"
                + "      <address>${ip.address}</address>"
                + "    </cluster-members>"
                + "  </network>"
                + HAZELCAST_CLIENT_END_TAG;
        writeStringToStreamAndClose(os, networkConfig);

        String xml = HAZELCAST_CLIENT_START_TAG +
                "    <import resource=\"${config.location}\"/>\n" +
                HAZELCAST_CLIENT_END_TAG;

        Properties properties = new Properties();
        properties.setProperty("config.location", file.getAbsolutePath());
        properties.setProperty("ip.address", "192.168.5.5");
        ClientConfig config = buildConfig(xml, properties);
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.5.5");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", ".xml");
        File config2 = createConfigFile("hz2", ".xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        String config1Xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"file:///" + config2.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;
        String config2Xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"file:///" + config1.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);

        buildConfig(config1Xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testThreeResourceCyclicImportThrowsException() throws Exception {
        File config1 = createConfigFile("hz1", ".xml");
        File config2 = createConfigFile("hz2", ".xml");
        File config3 = createConfigFile("hz3", ".xml");
        FileOutputStream os1 = new FileOutputStream(config1);
        FileOutputStream os2 = new FileOutputStream(config2);
        FileOutputStream os3 = new FileOutputStream(config2);
        String config1Xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"file:///" + config2.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;
        String config2Xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"file:///" + config3.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;
        String config3Xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"file:///" + config1.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;
        writeStringToStreamAndClose(os1, config1Xml);
        writeStringToStreamAndClose(os2, config2Xml);
        writeStringToStreamAndClose(os3, config3Xml);
        buildConfig(config1Xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        File config = createConfigFile("hz1", ".xml");
        FileOutputStream os = new FileOutputStream(config);
        String configXml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"file:///" + config.getAbsolutePath() + "\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;
        writeStringToStreamAndClose(os, "");
        buildConfig(configXml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceThrowsException() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testImportNotExistingResourceThrowsException() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"notexisting.xml\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Test
    public void testEncryptionReplacer() throws Exception {
        File passwordFile = tempFolder.newFile(getClass().getSimpleName() + ".pwd");
        PrintWriter out = new PrintWriter(passwordFile);
        try {
            out.print("This is a password");
        } finally {
            IOUtil.closeResource(out);
        }
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <properties>\n"
                + "        <property name='" + HAZELCAST_CONFIG_REPLACER_CLASS
                + "'>com.hazelcast.config.replacer.EncryptionReplacer</property>\n"
                + "        <property name='com.hazelcast.config.replacer.EncryptionReplacer.passwordFile'>"
                + passwordFile.getAbsolutePath() + "</property>\n"
                + "        <property name='com.hazelcast.config.replacer.EncryptionReplacer.passwordUserProperties'>false</property>\n"
                + "        <property name='com.hazelcast.config.replacer.EncryptionReplacer.keyLengthBits'>64</property>\n"
                + "        <property name='com.hazelcast.config.replacer.EncryptionReplacer.saltLengthBytes'>8</property>\n"
                + "        <property name='com.hazelcast.config.replacer.EncryptionReplacer.cipherAlgorithm'>DES</property>\n"
                + "        <property name='com.hazelcast.config.replacer.EncryptionReplacer.secretKeyFactoryAlgorithm'>PBKDF2WithHmacSHA1</property>\n"
                + "        <property name='com.hazelcast.config.replacer.EncryptionReplacer.secretKeyAlgorithm'>DES</property>\n"
                + "    </properties>\n"
                + "    <group>\n"
                + "        <name>${java.version}</name>\n"
                + "        <password>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</password>\n"
                + "    </group>\n"
                + HAZELCAST_CLIENT_END_TAG;
        GroupConfig groupConfig = buildConfig(xml, System.getProperties()).getGroupConfig();
        assertEquals(System.getProperty("java.version"), groupConfig.getName());
        assertEquals("My very secret secret", groupConfig.getPassword());
    }

    @Test(expected = ConfigurationException.class)
    public void testReplacerFailFast() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <properties>\n"
                + "        <property name='" + HAZELCAST_CONFIG_REPLACER_CLASS
                + "'>com.hazelcast.config.replacer.EncryptionReplacer</property>\n"
                + "    </properties>\n"
                + "    <group>\n"
                + "        <password>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</password>\n"
                + "    </group>\n"
                + HAZELCAST_CLIENT_END_TAG;
        buildConfig(xml, System.getProperties());
    }

    @Test
    public void testReplacerDisabledFailFast() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <properties>\n"
                + "        <property name='" + HAZELCAST_CONFIG_REPLACER_CLASS
                + "'>com.hazelcast.config.replacer.EncryptionReplacer</property>\n"
                + "        <property name='" + HAZELCAST_CONFIG_REPLACER_FAIL_IF_MISSING + "'>false</property>\n"
                + "    </properties>\n"
                + "    <group>\n"
                + "        <name>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</name>\n"
                + "    </group>\n"
                + HAZELCAST_CLIENT_END_TAG;
        GroupConfig groupConfig = buildConfig(xml, System.getProperties()).getGroupConfig();
        assertEquals("$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}", groupConfig.getName());
    }

    @Test
    public void testCustomReplacer() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <properties>\n"
                + "        <property name='" + HAZELCAST_CONFIG_REPLACER_CLASS
                + "'>" + IdentityReplacer.class.getName() + "</property>\n"
                + "    </properties>\n"
                + "    <group>\n"
                + "        <name>$ID{my-&lt;test/&gt;-group}</name>\n"
                + "    </group>\n"
                + HAZELCAST_CLIENT_END_TAG;
        GroupConfig groupConfig = buildConfig(xml, System.getProperties()).getGroupConfig();
        assertEquals("my-<test/>-group", groupConfig.getName());
    }

    /**
     * Given: No replacer is used in the configuration file<br>
     * When: A property variable is used within the file<br>
     * Then: The configuration parsing doesn't fail and the variable string remains unchanged (i.e. backward compatible
     * behavior, as if {@code fail-if-value-missing} attribute is {@code false}).
     */
    @Test
    public void testNoConfigReplacersMissingProperties() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <group>\n"
                + "        <name>${noSuchPropertyAvailable}</name>\n"
                + "    </group>\n"
                + HAZELCAST_CLIENT_END_TAG;
        GroupConfig groupConfig = buildConfig(xml, System.getProperties()).getGroupConfig();
        assertEquals("${noSuchPropertyAvailable}", groupConfig.getName());
    }

    @Test
    public void testImportGroupConfigFromClassPath() throws Exception {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"classpath:hazelcast-client-c1.xml\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);
        GroupConfig groupConfig = config.getGroupConfig();
        assertEquals("cluster1", groupConfig.getName());
        assertEquals("cluster1pass", groupConfig.getPassword());
    }

    private File createConfigFile(String filename, String suffix) throws IOException {
        File file = createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    private void writeStringToStreamAndClose(FileOutputStream os, String string) throws IOException {
        os.write(string.getBytes());
        os.flush();
        closeResource(os);
    }
}
