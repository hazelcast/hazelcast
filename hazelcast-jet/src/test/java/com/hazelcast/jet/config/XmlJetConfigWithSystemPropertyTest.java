/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.impl.config.ConfigProvider;
import com.hazelcast.jet.impl.config.XmlJetConfigBuilder;
import com.hazelcast.jet.impl.util.IOUtil;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static com.hazelcast.jet.impl.config.JetDeclarativeConfigUtil.SYSPROP_JET_CONFIG;
import static java.io.File.createTempFile;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SerialTest.class})
public class XmlJetConfigWithSystemPropertyTest extends AbstractJetMemberConfigWithSystemPropertyTest {

    private static final String TEST_XML_1 = "hazelcast-jet-test.xml";
    private static final String TEST_XML_2 = "hazelcast-jet-member-test.xml";

    @Override
    @Test(expected = HazelcastException.class)
    public void when_filePathSpecifiedNonExistingFile_thenThrowsException() throws Exception {
        // Given
        File file = createTempFile("foo", ".xml");
        file.delete();
        System.setProperty(SYSPROP_JET_CONFIG, file.getAbsolutePath());

        // When
        new XmlJetConfigBuilder().build();
    }

    @Override
    @Test
    public void when_filePathSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_1);
            os.write(IOUtil.readFully(resourceAsStream));
        }
        System.setProperty(SYSPROP_JET_CONFIG, tempFile.getAbsolutePath());

        // When
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder();
        JetConfig jetConfig = builder.build();

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void when_filePathMemberSpecifiedNonExistingFile_thenThrowsException() throws Exception {
        // Given
        File file = createTempFile("foo", ".xml");
        file.delete();
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        // When
        new XmlJetConfigBuilder().build();
    }

    @Test(expected = HazelcastException.class)
    public void when_filePathMemberSpecifiedNonExistingNonXmlFile_thenThrowsException() throws Exception {
        // Given
        File file = createTempFile("foo", ".bar");
        file.delete();
        System.out.println("file = " + file.getAbsolutePath());
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        // When
        new XmlJetConfigBuilder().build();
    }


    @Override
    @Test
    public void when_filePathMemberSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("imdg", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_2);
            os.write(IOUtil.readFully(resourceAsStream));
        }
        System.setProperty(SYSPROP_MEMBER_CONFIG, tempFile.getAbsolutePath());

        // When
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder();
        JetConfig jetConfig = builder.build();

        // Then
        assertMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void when_classpathSpecifiedNonExistingFile_thenThrowsException() {
        // Given
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:non-existing.xml");

        // When
        new XmlJetConfigBuilder().build();
    }


    @Override
    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        // Given
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:" + TEST_XML_1);

        // When
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder();
        JetConfig jetConfig = builder.build();

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void when_classpathMemberSpecifiedNonExistingFile_thenThrowsException() {
        // Given
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:non-existing.xml");

        // When
        new XmlJetConfigBuilder().build();
    }

    @Test(expected = HazelcastException.class)
    public void when_classpathMemberSpecifiedNonExistingNonXmlFile_thenThrowsException() {
        // Given
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:non-existing.bar");

        // When
        new XmlJetConfigBuilder().build();
    }


    @Override
    @Test
    public void when_classpathMemberSpecified_usesSpecifiedResource() {
        // Given
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + TEST_XML_2);

        // When
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder();
        JetConfig jetConfig = builder.build();

        // Then
        assertMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test
    public void when_configHasVariable_variablesAreReplaced() {
        // Given
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:hazelcast-jet-with-variables.xml");
        Properties properties = new Properties();
        properties.put("thread.count", String.valueOf(55));
        properties.put("flow.control.period", "50");
        properties.put("backup.count", "2");
        properties.put("scale.up.delay.millis", "1234");
        properties.put("lossless.restart.enabled", "false");
        properties.put("metrics.enabled", "false");
        properties.put("metrics.jmxEnabled", "false");
        properties.put("metrics.retention", "124");
        properties.put("metrics.collection-interval", "123");
        properties.put("metrics.enabled-for-data-structures", "true");

        // When
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder().setProperties(properties);
        JetConfig jetConfig = builder.build();

        // Then
        assertConfig(jetConfig);
    }

    @Override
    @Test
    public void when_configMemberHasVariable_variablesAreReplaced() {
        // Given
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + TEST_XML_2);

        Properties properties = new Properties();
        properties.put("imdg.instance.name", INSTANCE_NAME);

        // When
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder().setProperties(properties);
        JetConfig jetConfig = builder.build();
        // Then
        assertMemberConfig(jetConfig.getHazelcastConfig());
        assertThat(jetConfig.getHazelcastConfig().getInstanceName(), equalTo(INSTANCE_NAME));
    }

    @Override
    @Test
    public void when_edgeDefaultsSpecified_usesSpecified() {
        // Given
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:" + TEST_XML_1);

        // When
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder();
        JetConfig jetConfig = builder.build();

        // Then
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        assertEquals("queueSize", 999, edgeConfig.getQueueSize());
        assertEquals("packetSizeLimit", 997, edgeConfig.getPacketSizeLimit());
        assertEquals("receiveWindowMultiplier", 996, edgeConfig.getReceiveWindowMultiplier());
    }

    @Test
    public void when_filePathSpecifiedNonXml_then_loadedAsXml() throws Exception {
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("hazelcast-jet-foo.bar");
            os.write(IOUtil.readFully(resourceAsStream));
        }
        System.setProperty(SYSPROP_JET_CONFIG, tempFile.getAbsolutePath());

        XmlJetConfigBuilder configBuilder = new XmlJetConfigBuilder();
        JetConfig config = configBuilder.build();
        assertEquals("bar", config.getProperties().getProperty("foo"));
    }

    @Test(expected = HazelcastException.class)
    public void when_classPathSpecifiedNonXml_then_throwsException() {
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:hazelcast-jet-foo.bar");

        XmlJetConfigBuilder configBuilder = new XmlJetConfigBuilder();
        configBuilder.build();
    }

    @Test(expected = HazelcastException.class)
    public void when_loadingThroughSystemPropertyViaLocator_nonXmlSuffix_then_throwsException() {
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:hazelcast-jet-foo.bar");
        JetConfig config = ConfigProvider.locateAndGetJetConfig();
    }
}
