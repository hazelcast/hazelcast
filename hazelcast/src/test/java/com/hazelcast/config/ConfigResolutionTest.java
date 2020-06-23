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

import static com.hazelcast.test.HazelcastTestSupport.assumeThatJDK8OrHigher;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ConfigResolutionTest {

    private static final String SYSPROP_NAME = "hazelcast.config";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    public void setUp() {
        System.clearProperty(SYSPROP_NAME);
    }

    @After
    public void tearDown() throws Exception {
        System.clearProperty(SYSPROP_NAME);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml_loadedAsXml() throws Exception {
        File file = helper.givenXmlConfigFileInWorkDir("foo.xml", "cluster-xml");
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml_loadedAsXml() throws Exception {
        helper.givenXmlConfigFileOnClasspath("foo.xml", "cluster-xml");
        System.setProperty(SYSPROP_NAME, "classpath:foo.xml");

        Config config = Config.load();

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yaml_loadedAsYaml() throws Exception {
        assumeThatJDK8OrHigher();
        File file = helper.givenYamlConfigFileInWorkDir("foo.yaml", "cluster-yaml");
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml_loadedAsYaml() throws Exception {
        assumeThatJDK8OrHigher();
        helper.givenYamlConfigFileOnClasspath("foo.yaml", "cluster-yaml");
        System.setProperty(SYSPROP_NAME, "classpath:foo.yaml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yml_loadedAsYaml() throws Exception {
        assumeThatJDK8OrHigher();
        File file = helper.givenYamlConfigFileInWorkDir("foo.yml", "cluster-yaml");
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml_loadedAsYaml() throws Exception {
        assumeThatJDK8OrHigher();
        helper.givenYamlConfigFileOnClasspath("foo.yml", "cluster-yaml");
        System.setProperty(SYSPROP_NAME, "classpath:foo.yml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_bar_loadedAsXml() throws Exception {
        File file = helper.givenXmlConfigFileInWorkDir("foo.bar", "cluster-bar");
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        Config config = Config.load();

        assertEquals("cluster-bar", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_bar_loadedAsXml() throws Exception {
        helper.givenXmlConfigFileOnClasspath("foo.bar", "cluster-bar");
        System.setProperty(SYSPROP_NAME, "classpath:foo.bar");

        Config config = Config.load();

        assertEquals("cluster-bar", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_NAME, "foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.xml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.xml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_NAME, "foo.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.yaml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:foo.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.yaml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYml_throws() {
        System.setProperty(SYSPROP_NAME, "foo.yml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.yml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:foo.yml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.yml");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentBar_throws() {
        System.setProperty(SYSPROP_NAME, "foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.bar");

        Config.load();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentBar_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.bar");

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
        assumeThatJDK8OrHigher();
        helper.givenYamlConfigFileInWorkDir("cluster-yaml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveClasspath_yaml() throws Exception {
        assumeThatJDK8OrHigher();
        helper.givenYamlConfigFileOnClasspath("cluster-yaml");

        Config config = Config.load();

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveDefault_xml() {
        getClass().getClassLoader().getResource("hazelcast-default.xml");

        Config config = Config.load();

        assertEquals("dev", config.getGroupConfig().getName());
    }
}
