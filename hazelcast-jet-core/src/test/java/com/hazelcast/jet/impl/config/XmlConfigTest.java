/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.config;

import com.hazelcast.config.Config;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.util.Util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.jet.config.InstanceConfig.DEFAULT_FLOW_CONTROL_PERIOD_MS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class XmlConfigTest {

    private static final String TEST_XML_1 = "hazelcast-jet-test.xml";
    private static final String TEST_XML_2 = "hazelcast-jet-member-test.xml";
    private static final String TEST_XML_2_GROUP_NAME = "imdg";
    private static final String WORLDS_MOST_COMMON_PASSWORD = "123456";

    @Test
    public void when_noConfigSpecified_usesDefaultConfig() {
        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(new Properties());

        // Then
        assertEquals(Runtime.getRuntime().availableProcessors(),
                jetConfig.getInstanceConfig().getCooperativeThreadCount());
        assertEquals(DEFAULT_FLOW_CONTROL_PERIOD_MS, jetConfig.getInstanceConfig().getFlowControlPeriodMs());
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_filePathSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_1);
            os.write(Util.readFully(resourceAsStream));
        }

        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, tempFile.getAbsolutePath());

        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(properties);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_filePathMemberSpecified_usesSpecifiedFile() throws IOException {
        // Given
        File tempFile = File.createTempFile("imdg", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_2);
            os.write(Util.readFully(resourceAsStream));
        }

        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_MEMBER_CONFIG_PROPERTY, tempFile.getAbsolutePath());

        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(properties);

        // Then
        assertXmlMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + TEST_XML_1);

        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(properties);

        // Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_classpathMemberSpecified_usesSpecifiedResource() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_MEMBER_CONFIG_PROPERTY, "classpath:" + TEST_XML_2);

        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(properties);

        // Then
        assertXmlMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Test
    public void when_configHasVariable_variablesAreReplaced() {
        // Given
        String dir = "/var/tmp";
        int threadCount = 55;
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:hazelcast-jet-with-variables.xml");
        properties.put("working.directory", dir);
        properties.put("thread.count", String.valueOf(threadCount));

        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(properties);

        // Then
        assertConfig(jetConfig);
    }

    @Ignore("To confirm if substitution in Jet should be visible to IMDG")
    @Test
    public void when_configMemberHasVariable_variablesAreReplaced() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_MEMBER_CONFIG_PROPERTY, "classpath:${my.filename}");
        properties.put("my.filename", TEST_XML_2);
        properties.put("imdg.pass", WORLDS_MOST_COMMON_PASSWORD);

        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(properties);

        // Then
        assertXmlMemberConfig(jetConfig.getHazelcastConfig());
        assertThat(jetConfig.getHazelcastConfig().getGroupConfig().getPassword(), equalTo(WORLDS_MOST_COMMON_PASSWORD));
    }

    @Test
    public void when_edgeDefaultsSpecified_usesSpecified() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + TEST_XML_1);

        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(properties);

        // Then
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        assertEquals("queueSize", 999, edgeConfig.getQueueSize());
        assertEquals("packetSizeLimit", 997, edgeConfig.getPacketSizeLimit());
        assertEquals("receiveWindowMultiplier", 996, edgeConfig.getReceiveWindowMultiplier());
    }

    @Test
    public void when_jobMetadataBackupCount_usesSpecified() {
        // Given
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + TEST_XML_1);

        // When
        JetConfig jetConfig = XmlJetConfigBuilder.getConfig(properties);

        // Then
        assertEquals("jobMetadataBackupCount", 4, jetConfig.getJobMetadataBackupCount());
    }

    private static void assertConfig(JetConfig jetConfig) {
        assertEquals(55, jetConfig.getInstanceConfig().getCooperativeThreadCount());
        assertEquals("/var/tmp", jetConfig.getInstanceConfig().getTempDir());

        assertEquals("value1", jetConfig.getProperties().getProperty("property1"));
        assertEquals("value2", jetConfig.getProperties().getProperty("property2"));
    }

    private static void assertDefaultMemberConfig(Config config) {
        assertThat(config, not(nullValue()));
        assertThat(config.getGroupConfig().getName(), not(equalTo(TEST_XML_2_GROUP_NAME)));
    }

    private static void assertXmlMemberConfig(Config config) {
        assertThat(config, not(nullValue()));
        assertThat(config.getGroupConfig().getName(), equalTo(TEST_XML_2_GROUP_NAME));
    }
}
