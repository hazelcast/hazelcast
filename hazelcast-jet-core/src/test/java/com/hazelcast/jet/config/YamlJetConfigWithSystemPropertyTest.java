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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.config.YamlJetConfigBuilder;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
public class YamlJetConfigWithSystemPropertyTest extends AbstractJetMemberConfigWithSystemPropertyTest {

    private static final String JET_TEST_YAML = "hazelcast-jet-test.yaml";
    private static final String JET_TEST_WITH_VARIABLES_YAML = "hazelcast-jet-with-variables.yaml";
    private static final String JET_MEMBER_TEST_YAML = "hazelcast-jet-member-test.yaml";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    @Test(expected = HazelcastException.class)
    public void when_filePathSpecifiedNonExistingFile_thenThrowsException() throws Exception {
        // Given
        File file = File.createTempFile("foo", ".yaml");
        file.delete();
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, file.getAbsolutePath());

        // When
        new YamlJetConfigBuilder().build();
    }

    @Test(expected = HazelcastException.class)
    public void when_filePathSpecifiedNonExistingNonYamlFile_thenThrowsException() throws Exception {
        // Given
        File file = File.createTempFile("foo", ".yaml");
        file.delete();
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, file.getAbsolutePath());

        // When
        new YamlJetConfigBuilder().build();
    }

    @Override
    @Test
    public void when_filePathSpecified_usesSpecifiedFile() throws IOException {
        //Given
        File tempFile = File.createTempFile("jet", ".yaml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(JET_TEST_YAML);
            os.write(Util.readFully(resourceAsStream));
        }
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, tempFile.getAbsolutePath());

        //When
        JetConfig jetConfig = new YamlJetConfigBuilder().build();


        //Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());

    }

    @Override
    @Test(expected = HazelcastException.class)
    public void when_filePathMemberSpecifiedNonExistingFile_thenThrowsException() throws Exception {
        // Given
        File file = File.createTempFile("foo", ".yaml");
        file.delete();
        System.setProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY, file.getAbsolutePath());

        // When
        new YamlJetConfigBuilder().build();
    }


    @Test(expected = HazelcastException.class)
    public void when_filePathMemberSpecifiedNonExistingNonYamlFile_thenThrowsException() throws Exception {
        // Given
        File file = File.createTempFile("foo", ".bar");
        file.delete();
        System.setProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY, file.getAbsolutePath());

        // When
        new YamlJetConfigBuilder().build();
    }

    @Override
    @Test
    public void when_filePathMemberSpecified_usesSpecifiedFile() throws IOException {
        //Given
        File tempFile = File.createTempFile("jet", ".yaml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(JET_MEMBER_TEST_YAML);
            os.write(Util.readFully(resourceAsStream));
        }
        System.setProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY, tempFile.getAbsolutePath());

        //When
        JetConfig jetConfig = new YamlJetConfigBuilder().build();


        //Then
        assertMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void when_classpathSpecifiedNonExistingFile_thenThrowsException() {
        // Given
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, "classpath:non-existing.yaml");

        //When
        new YamlJetConfigBuilder().build();
    }

    @Test(expected = HazelcastException.class)
    public void when_classpathSpecifiedNonExistingNonYamlFile_thenThrowsException() {
        // Given
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, "classpath:non-existing.bar");

        //When
        new YamlJetConfigBuilder().build();
    }

    @Override
    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        // Given
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + JET_TEST_YAML);

        //When
        JetConfig jetConfig = new YamlJetConfigBuilder().build();

        //Then
        assertConfig(jetConfig);
        assertDefaultMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test(expected = HazelcastException.class)
    public void when_classpathMemberSpecifiedNonExistingFile_thenThrowsException() {
        // Given
        System.setProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY, "classpath:non-existing.yaml");

        //When
        new YamlJetConfigBuilder().build();

    }

    @Test(expected = HazelcastException.class)
    public void when_classpathMemberSpecifiedNonExistingNonYamlFile_thenThrowsException() {
        // Given
        System.setProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY, "classpath:non-existing.bar");

        //When
        new YamlJetConfigBuilder().build();
    }

    @Override
    @Test
    public void when_classpathMemberSpecified_usesSpecifiedResource() {
        // Given
        System.setProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY, "classpath:" + JET_MEMBER_TEST_YAML);

        //When
        JetConfig jetConfig = new YamlJetConfigBuilder().build();

        //Then
        assertMemberConfig(jetConfig.getHazelcastConfig());
    }

    @Override
    @Test
    public void when_configHasVariable_variablesAreReplaced() {
        // Given
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + JET_TEST_WITH_VARIABLES_YAML);
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
        JetConfig jetConfig = new YamlJetConfigBuilder().setProperties(properties).build();

        // Then
        assertConfig(jetConfig);
    }

    @Override
    @Test
    public void when_configMemberHasVariable_variablesAreReplaced() {
        // Given
        System.setProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY, "classpath:" + JET_MEMBER_TEST_YAML);

        Properties properties = new Properties();
        properties.put("imdg.pass", PASSWORD);
        properties.put("imdg.instance.name", INSTANCE_NAME);

        // When
        JetConfig jetConfig = new YamlJetConfigBuilder().setProperties(properties).build();

        // Then
        assertMemberConfig(jetConfig.getHazelcastConfig());
        assertThat(jetConfig.getHazelcastConfig().getGroupConfig().getPassword(), equalTo(PASSWORD));
        assertThat(jetConfig.getHazelcastConfig().getInstanceName(), equalTo(INSTANCE_NAME));
    }

    @Override
    @Test
    public void when_edgeDefaultsSpecified_usesSpecified() {
        // Given
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + JET_TEST_YAML);

        // When
        JetConfig jetConfig = new YamlJetConfigBuilder().build();

        // Then
        EdgeConfig edgeConfig = jetConfig.getDefaultEdgeConfig();
        assertEquals("queueSize", 999, edgeConfig.getQueueSize());
        assertEquals("packetSizeLimit", 997, edgeConfig.getPacketSizeLimit());
        assertEquals("receiveWindowMultiplier", 996, edgeConfig.getReceiveWindowMultiplier());
    }

    @Test
    public void loadingThroughSystemPropertyWithLocator_nonYamlSuffix() {
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, "classpath:test-jet.foobar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("hazelcast.jet.config");
        expectedException.expectMessage("classpath:test-jet.foobar");
        expectedException.expectMessage("yaml, yml");

        new YamlJetConfigBuilder();
    }

    @Test
    public void loadingThroughSystemPropertyWithLocator_existingClasspathResource() {
        System.setProperty(HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + JET_TEST_YAML);

        JetInstance instance = Jet.newJetInstance();
        JetConfig config = instance.getConfig();
        instance.shutdown();

        assertConfig(config);
    }

}
