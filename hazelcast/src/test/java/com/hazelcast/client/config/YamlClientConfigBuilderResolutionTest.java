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

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES_STRING;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlClientConfigBuilderResolutionTest {

    private static final String CONFIG_FILE_PREFIX = YamlClientConfigBuilderResolutionTest.class.getSimpleName() + "foo";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    @After
    public void tearDown() throws Exception {
        System.clearProperty(SYSPROP_CLIENT_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_yaml() throws Exception {
        helper.givenYamlClientConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".yaml", "cluster-yaml-file");
        System.setProperty(SYSPROP_CLIENT_CONFIG, CONFIG_FILE_PREFIX + ".yaml");

        ClientConfig config = new YamlClientConfigBuilder().build();
        assertEquals("cluster-yaml-file", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".yaml", "cluster-yaml-classpath");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".yaml");

        ClientConfig config = new YamlClientConfigBuilder().build();
        assertEquals("cluster-yaml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yml() throws Exception {
        helper.givenYamlClientConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".yml", "cluster-yml-file");
        System.setProperty(SYSPROP_CLIENT_CONFIG, CONFIG_FILE_PREFIX + ".yml");

        ClientConfig config = new YamlClientConfigBuilder().build();
        assertEquals("cluster-yml-file", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".yml", "cluster-yml-classpath");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".yml");

        ClientConfig config = new YamlClientConfigBuilder().build();
        assertEquals("cluster-yml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.yaml");

        new YamlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("idontexist.yaml");

        new YamlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonYaml_throws() throws Exception {
        File file = helper.givenYamlClientConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".xml", "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonYaml_throws() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".xml", "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentNonYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, CONFIG_FILE_PREFIX + ".xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentNonYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_noSuffix_throws() throws Exception {
        File file = helper.givenYamlClientConfigFileInWorkDir(CONFIG_FILE_PREFIX, "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX);
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_noSuffix_throws() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath(CONFIG_FILE_PREFIX, "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:" + CONFIG_FILE_PREFIX);

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX);
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlClientConfigBuilder().build();
    }

    @Test
    public void testResolveFromWorkDirYamlButNotYml() throws Exception {
        helper.givenYamlClientConfigFileInWorkDir("cluster-yaml-workdir");

        ClientConfig config = new YamlClientConfigBuilder().build();

        assertEquals("cluster-yaml-workdir", config.getInstanceName());
    }

    @Test
    public void testResolveFromWorkDirYmlButNotYaml() throws Exception {
        helper.givenYmlClientConfigFileInWorkDir("cluster-yml-workdir");

        ClientConfig config = new YamlClientConfigBuilder().build();

        assertEquals("cluster-yml-workdir", config.getInstanceName());
    }

    @Test
    public void testResolveFromWorkDirYamlAndYml() throws Exception {
        helper.givenYamlClientConfigFileInWorkDir("cluster-yaml-workdir");
        helper.givenYmlClientConfigFileInWorkDir("cluster-yml-workdir");

        ClientConfig config = new YamlClientConfigBuilder().build();

        assertEquals("cluster-yaml-workdir", config.getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYamlButNotYml() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath("cluster-yaml-classpath");

        ClientConfig config = new YamlClientConfigBuilder().build();

        assertEquals("cluster-yaml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYmlButNotYaml() throws Exception {
        helper.givenYmlClientConfigFileOnClasspath("cluster-yml-classpath");

        ClientConfig config = new YamlClientConfigBuilder().build();

        assertEquals("cluster-yml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYamlAndYml() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath("cluster-yaml-classpath");
        helper.givenYmlClientConfigFileOnClasspath("cluster-yml-classpath");

        ClientConfig config = new YamlClientConfigBuilder().build();

        assertEquals("cluster-yaml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveDefault() {
        ClientConfig config = new YamlClientConfigBuilder().build();
        assertEquals("dev", config.getClusterName());
    }

}
