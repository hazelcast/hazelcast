/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.helpers.DeclarativeConfigFileHelper;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_FAILOVER_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES_STRING;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlClientFailoverConfigBuilderConfigResolutionTest {

    private static final String CONFIG_FILE_PREFIX = YamlClientFailoverConfigBuilderConfigResolutionTest
            .class.getSimpleName() + "foo";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    @After
    public void tearDown() throws Exception {
        System.clearProperty(SYSPROP_CLIENT_FAILOVER_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_yaml() throws Exception {
        helper.givenYamlClientFailoverConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".yaml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, CONFIG_FILE_PREFIX + ".yaml");

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();
        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".yaml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".yaml");

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();
        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_file_yml() throws Exception {
        helper.givenYamlClientFailoverConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".yml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, CONFIG_FILE_PREFIX + ".yml");

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();
        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".yml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".yml");

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();
        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.yaml");

        new YamlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("idontexist.yaml");

        new YamlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonYaml_throws() throws Exception {
        File file = helper.givenYamlClientFailoverConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".xml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_FAILOVER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonYaml_throws() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".xml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_FAILOVER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentNonYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, CONFIG_FILE_PREFIX + ".xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_FAILOVER_CONFIG);
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentNonYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_FAILOVER_CONFIG);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_noSuffix_throws() throws Exception {
        File file = helper.givenYamlClientFailoverConfigFileInWorkDir(CONFIG_FILE_PREFIX, 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_FAILOVER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX);
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_noSuffix_throws() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath(CONFIG_FILE_PREFIX, 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX);

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_FAILOVER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX);
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveFromWorkDirYamlButNotYml() throws Exception {
        helper.givenYamlClientFailoverConfigFileInWorkDir(42);

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveFromWorkDirYmlButNotYaml() throws Exception {
        helper.givenYmlClientFailoverConfigFileInWorkDir(42);

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveFromWorkDirYamlAndYaml() throws Exception {
        helper.givenYamlClientFailoverConfigFileInWorkDir(42);
        helper.givenYmlClientFailoverConfigFileInWorkDir(24);

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveFromClasspathYamlButNotYml() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath(42);

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveFromClasspathYmlButNotYaml() throws Exception {
        helper.givenYmlClientFailoverConfigFileOnClasspath(42);

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveFromClasspathYamlAndYml() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath(42);
        helper.givenYmlClientFailoverConfigFileOnClasspath(24);

        ClientFailoverConfig config = new YamlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveDefault() {
        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("Failed to load ClientFailoverConfig");

        new YamlClientFailoverConfigBuilder().build();
    }

}
