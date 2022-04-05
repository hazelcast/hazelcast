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
import com.hazelcast.config.test.builders.ConfigReplacerBuilder;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.client.config.XmlClientConfigBuilderTest.HAZELCAST_CLIENT_END_TAG;
import static com.hazelcast.client.config.XmlClientConfigBuilderTest.HAZELCAST_CLIENT_START_TAG;
import static com.hazelcast.client.config.XmlClientConfigBuilderTest.buildConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientConfigImportVariableReplacementTest extends AbstractClientConfigImportVariableReplacementTest {

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
    @Test(expected = InvalidConfigurationException.class)
    public void testImportElementOnlyAppearsInTopLevel() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "   <network>\n"
                + "        <import resource=\"\"/>\n"
                + "   </network>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastElementOnlyAppearsOnce() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "   <hazelcast-client>"
                + "   </hazelcast-client>"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Test
    @Override
    public void testImportResourceWithConfigReplacers() throws IOException {
        String configReplacer = HAZELCAST_CLIENT_START_TAG
            + "    <config-replacers>\n"
            + "        <replacer class-name='" + IdentityReplacer.class.getName() + "'/>\n"
            + "    </config-replacers>\n"
            + HAZELCAST_CLIENT_END_TAG;
        String configLocation = helper.givenConfigFileInWorkDir("config-replacers.xml", configReplacer).getAbsolutePath();

        String xml = HAZELCAST_CLIENT_START_TAG
            + "    <import resource=\"${config.location}\"/>\n"
            + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
            + HAZELCAST_CLIENT_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", configLocation);
        ClientConfig groupConfig = buildConfig(xml, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Test
    @Override
    public void testImportResourceWithNestedImports() throws IOException {
        String configReplacer = HAZELCAST_CLIENT_START_TAG
            + "    <config-replacers>\n"
            + "        <replacer class-name='" + IdentityReplacer.class.getName() + "'/>\n"
            + "    </config-replacers>\n"
            + HAZELCAST_CLIENT_END_TAG;
        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacers.xml", configReplacer).getAbsolutePath();

        String clusterName = HAZELCAST_CLIENT_START_TAG
            + "    <import resource=\"" + "file:///" + configReplacerLocation + "\"/>\n"
            + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
            + HAZELCAST_CLIENT_END_TAG;

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.xml", clusterName).getAbsolutePath();

        String xml = HAZELCAST_CLIENT_START_TAG
            + "    <import resource=\"${config.location}\"/>\n"
            + HAZELCAST_CLIENT_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        ClientConfig groupConfig = buildConfig(xml, properties);
        assertEquals(System.getProperty("java.version") + " dev", groupConfig.getClusterName());
    }

    @Test
    @Override
    public void testImportResourceWithNestedImportsAndProperties() throws IOException {
        ConfigReplacerBuilder testReplacer = new ConfigReplacerBuilder()
            .withClass(TestReplacer.class)
            .addProperty("p1", "${p1}")
            .addProperty("p2", "")
            .addProperty("p3", "another property")
            .addProperty("p4", "&lt;test/&gt;");
        String configReplacer = HAZELCAST_CLIENT_START_TAG
            + "    <config-replacers fail-if-value-missing='false'>\n"
            + testReplacer.build()
            + "    </config-replacers>\n"
            + HAZELCAST_CLIENT_END_TAG;
        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.xml", configReplacer).getAbsolutePath();

        String clusterName = HAZELCAST_CLIENT_START_TAG
            + "    <import resource=\"" + "file:///" + configReplacerLocation + "\"/>\n"
            + "    <cluster-name>$T{p1} $T{p2} $T{p3} $T{p4} $T{p5}</cluster-name>\n"
            + HAZELCAST_CLIENT_END_TAG;

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.xml", clusterName).getAbsolutePath();

        String xml = HAZELCAST_CLIENT_START_TAG
            + "    <import resource=\"${config.location}\"/>\n"
            + HAZELCAST_CLIENT_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        properties.put("p1", "a property");
        ClientConfig config = buildConfig(xml, properties);
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    @Override
    @Test
    public void testImportConfigFromResourceVariables() throws IOException {
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
        String configLocationPath = helper.givenConfigFileInWorkDir("config-network.xml", networkConfig).getAbsolutePath();

        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml, "config.location", configLocationPath);
        assertFalse(config.getNetworkConfig().isSmartRouting());
        assertTrue(config.getNetworkConfig().isRedoOperation());
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.100.100");
        assertContains(config.getNetworkConfig().getAddresses(), "127.0.0.10");
    }

    @Override
    @Test
    public void testImportedConfigVariableReplacement() throws IOException {
        String networkConfig = HAZELCAST_CLIENT_START_TAG
                + "  <network>"
                + "    <cluster-members>"
                + "      <address>${ip.address}</address>"
                + "    </cluster-members>"
                + "  </network>"
                + HAZELCAST_CLIENT_END_TAG;
        String configLocationPath = helper.givenConfigFileInWorkDir("config-network.xml", networkConfig).getAbsolutePath();

        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        Properties properties = new Properties();
        properties.setProperty("config.location", configLocationPath);
        properties.setProperty("ip.address", "192.168.5.5");
        ClientConfig config = buildConfig(xml, properties);
        assertContains(config.getNetworkConfig().getAddresses(), "192.168.5.5");
    }

    private String xmlContentWithImportResource(String url) {
        return HAZELCAST_CLIENT_START_TAG
            + "    <import resource=\"" + url + "\"/>\n"
            + HAZELCAST_CLIENT_END_TAG;
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testTwoResourceCyclicImportThrowsException() throws Exception {
        String xmlWithCyclicImport = helper.createFilesWithCycleImports(
            this::xmlContentWithImportResource,
            helper.givenConfigFileInWorkDir("hz1.xml", "").getAbsolutePath(),
            helper.givenConfigFileInWorkDir("hz2.xml", "").getAbsolutePath()
        );

        buildConfig(xmlWithCyclicImport);
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

        buildConfig(xmlWithCyclicImport);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceContent() throws Exception {
        String pathToEmptyFile = createEmptyFile();
        String configXml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"file:///" + pathToEmptyFile + "\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;
        buildConfig(configXml);
    }

    private String createEmptyFile() throws Exception {
        return helper.givenConfigFileInWorkDir("hz1.xml", "").getAbsolutePath();
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportEmptyResourceThrowsException() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testImportNotExistingResourceThrowsException() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"notexisting.xml\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void testImportNotExistingUrlResourceThrowsException() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"file:///notexisting.xml\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        buildConfig(xml);
    }

    @Override
    @Test
    public void testReplacers() throws Exception {
        String pathToFileWithPassword = helper.givenConfigFileInWorkDir(
            getClass().getSimpleName() + ".pwd",
            "This is a password"
        ).getAbsolutePath();

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

        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <config-replacers>\n"
                + encryptionReplacer.build()
                + identityReplacer.build()
                + "    </config-replacers>\n"
                + "    <cluster-name>${java.version} $ID{dev}</cluster-name>\n"
                + "    <instance-name>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</instance-name>\n"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml, System.getProperties());
        assertEquals(System.getProperty("java.version") + " dev", config.getClusterName());
        assertEquals("My very secret secret", config.getInstanceName());
    }

    @Override
    @Test(expected = InvalidConfigurationException.class)
    public void testMissingReplacement() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <config-replacers>\n"
                + "        <replacer class-name='" + EncryptionReplacer.class.getName() + "'/>\n"
                + "    </config-replacers>\n"
                + "    <cluster-name>$ENC{7JX2r/8qVVw=:10000:Jk4IPtor5n/vCb+H8lYS6tPZOlCZMtZv}</cluster-name>\n"
                + HAZELCAST_CLIENT_END_TAG;
        buildConfig(xml, System.getProperties());
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

        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <config-replacers fail-if-value-missing='false'>\n"
                + testReplacer.build()
                + "    </config-replacers>\n"
                + "    <cluster-name>$T{p1} $T{p2} $T{p3} $T{p4} $T{p5}</cluster-name>\n"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig clientConfig = buildConfig(xml, System.getProperties());
        assertEquals("a property  another property <test/> $T{p5}", clientConfig.getClusterName());
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
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <cluster-name>${noSuchPropertyAvailable}</cluster-name>\n"
                + HAZELCAST_CLIENT_END_TAG;
        ClientConfig config = buildConfig(xml, System.getProperties());
        assertEquals("${noSuchPropertyAvailable}", config.getClusterName());
    }

    @Override
    @Test
    public void testImportConfigFromClassPath() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"classpath:hazelcast-client-c1.xml\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        ClientConfig config = buildConfig(xml);
        assertEquals("cluster1", config.getClusterName());
    }

    @Override
    @Test
    public void testReplaceVariablesUseSystemProperties() {
        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <properties>\n"
                + "        <property name=\"prop\">${variable}</property>\n"
                + "    </properties>\n"
                + HAZELCAST_CLIENT_END_TAG;

        System.setProperty("variable", "foobar");
        ClientConfig config = buildConfig(xml);

        assertEquals("foobar", config.getProperty("prop"));
    }

    @Override
    @Test
    public void testReplaceVariablesWithClasspathConfig() {
        System.setProperty("variable", "foobar");
        ClientConfig config = new ClientClasspathXmlConfig("test-hazelcast-client-variable.xml");

        assertEquals("foobar", config.getProperty("prop"));
    }
}
