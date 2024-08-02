/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.ALL_ACCEPTED_SUFFIXES_STRING;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ConfigResolutionTest {


    private final DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

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
        File file = helper.givenXmlConfigFileInWorkDir("foo.xml", "cluster-xml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml_loadedAsXml() throws Exception {
        helper.givenXmlConfigFileOnClasspath("foo.xml", "cluster-xml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.xml");

        Config config = Config.load();

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yaml_loadedAsYaml() throws Exception {
        File file = helper.givenYamlConfigFileInWorkDir("foo.yaml", "cluster-yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml_loadedAsYaml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("foo.yaml", "cluster-yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.yaml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yml_loadedAsYaml() throws Exception {
        File file = helper.givenYamlConfigFileInWorkDir("foo.yml", "cluster-yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml_loadedAsYaml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("foo.yml", "cluster-yaml");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.yml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_bar_throws() throws Exception {
        File file = helper.givenXmlConfigFileInWorkDir("foo.bar", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_MEMBER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("foo.bar")
                .hasMessageContaining(ALL_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_classpath_bar_throws() throws Exception {
        helper.givenXmlConfigFileOnClasspath("foo.bar", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.bar");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_MEMBER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("classpath:foo.bar")
                .hasMessageContaining(ALL_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.xml");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("foo.xml");
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.xml");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("foo.xml");
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.yaml");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("foo.yaml");
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.yaml");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("foo.yaml");
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.yml");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("foo.yml");
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.yml");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("foo.yml");
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentBar_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.bar");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("foo.bar");
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentBar_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.bar");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("foo.bar");
    }

    @Test
    public void testResolveSystemProperty_file_noSuffix_throws() throws Exception {
        File file = helper.givenXmlConfigFileInWorkDir("foo", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_MEMBER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("foo")
                .hasMessageContaining(ALL_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_classpath_noSuffix_throws() throws Exception {
        helper.givenXmlConfigFileOnClasspath("foo", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo");

        assertThatThrownBy(Config::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_MEMBER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("classpath:foo")
                .hasMessageContaining(ALL_ACCEPTED_SUFFIXES_STRING);
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
