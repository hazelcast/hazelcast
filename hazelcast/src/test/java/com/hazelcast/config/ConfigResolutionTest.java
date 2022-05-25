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

package com.hazelcast.config;

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

import static com.hazelcast.internal.config.DeclarativeConfigUtil.ALL_ACCEPTED_SUFFIXES_STRING;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ConfigResolutionTest {

    private static final String CONFIG_FILE_PREFIX = ConfigResolutionTest.class.getSimpleName() + "foo";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    public void setUp() {
        System.clearProperty(SYSPROP_MEMBER_CONFIG);
    }

    @After
    public void tearDown() throws Exception {
        System.clearProperty(SYSPROP_MEMBER_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml_loadedAsXml() throws Exception {
        File file = helper.givenXmlConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".xml", "cluster-xml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml_loadedAsXml() throws Exception {
        helper.givenXmlConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".xml", "cluster-xml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".xml");

        Config config = Config.load();

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yaml_loadedAsYaml() throws Exception {
        File file = helper.givenYamlConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".yaml", "cluster-yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml_loadedAsYaml() throws Exception {
        helper.givenYamlConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".yaml", "cluster-yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".yaml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yml_loadedAsYaml() throws Exception {
        File file = helper.givenYamlConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".yml", "cluster-yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml_loadedAsYaml() throws Exception {
        helper.givenYamlConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".yml", "cluster-yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".yml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_bar_throws() throws Exception {
        File file = helper.givenXmlConfigFileInWorkDir(CONFIG_FILE_PREFIX + ".bar", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".bar");
        expectedException.expectMessage(ALL_ACCEPTED_SUFFIXES_STRING);

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_bar_throws() throws Exception {
        helper.givenXmlConfigFileOnClasspath(CONFIG_FILE_PREFIX + ".bar", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("classpath:" + CONFIG_FILE_PREFIX + ".bar");
        expectedException.expectMessage(ALL_ACCEPTED_SUFFIXES_STRING);

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, CONFIG_FILE_PREFIX + ".xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".xml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".xml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, CONFIG_FILE_PREFIX + ".yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".yaml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".yaml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, CONFIG_FILE_PREFIX + ".yml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".yml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".yml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".yml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentBar_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, CONFIG_FILE_PREFIX + ".bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".bar");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentBar_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX + ".bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage(CONFIG_FILE_PREFIX + ".bar");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_file_noSuffix_throws() throws Exception {
        File file = helper.givenXmlConfigFileInWorkDir(CONFIG_FILE_PREFIX, "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage(CONFIG_FILE_PREFIX);
        expectedException.expectMessage(ALL_ACCEPTED_SUFFIXES_STRING);

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_noSuffix_throws() throws Exception {
        helper.givenXmlConfigFileOnClasspath(CONFIG_FILE_PREFIX, "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:" + CONFIG_FILE_PREFIX);

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("classpath:" + CONFIG_FILE_PREFIX);
        expectedException.expectMessage(ALL_ACCEPTED_SUFFIXES_STRING);

        Config.load();
    }

    @Test
    public void testResolveWorkDir_xml() throws Exception {
        helper.givenXmlConfigFileInWorkDir("cluster-xml");

        Config config = Config.load();

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveClasspath_xml() throws Exception {
        helper.givenXmlConfigFileOnClasspath("cluster-xml");

        Config config = Config.load();

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveWorkDir_yaml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("cluster-yaml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveClasspath_yaml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("cluster-yaml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveDefault_xml() {
        getClass().getClassLoader().getResource("hazelcast-default.xml");

        Config config = Config.load();

        assertEquals("dev", config.getClusterName());
    }

}
