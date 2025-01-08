/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.ALL_ACCEPTED_SUFFIXES_STRING;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_FAILOVER_CONFIG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientFailoverConfigResolutionTest {

    private final DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    public void setUp() {
        System.clearProperty(SYSPROP_CLIENT_FAILOVER_CONFIG);
    }

    @After
    public void tearDown() throws Exception {
        System.clearProperty(SYSPROP_CLIENT_FAILOVER_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml_loadedAsXml() throws Exception {
        File file = helper.givenXmlClientFailoverConfigFileInWorkDir("foo.xml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, file.getAbsolutePath());

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml_loadedAsXml() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo.xml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.xml");

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_file_yaml_loadedAsYaml() throws Exception {
        File file = helper.givenYamlClientFailoverConfigFileInWorkDir("foo.yaml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, file.getAbsolutePath());

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml_loadedAsYaml() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath("foo.yaml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.yaml");

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_file_yml_loadedAsYaml() throws Exception {
        File file = helper.givenYamlClientFailoverConfigFileInWorkDir("foo.yml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, file.getAbsolutePath());

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml_loadedAsYaml() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath("foo.yml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.yml");

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_file_bar_loadedAsXml() throws Exception {
        File file = helper.givenXmlClientFailoverConfigFileInWorkDir("foo.bar", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, file.getAbsolutePath());

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_CLIENT_FAILOVER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("foo.bar")
                .hasMessageContaining(ALL_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_classpath_bar_throws() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo.bar", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.bar");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_CLIENT_FAILOVER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("foo.bar")
                .hasMessageContaining(ALL_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "foo.xml");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("foo.xml");

    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.xml");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("foo.xml");

    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "foo.yaml");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("foo.yaml");
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.yaml");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("foo.yaml");
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "foo.yml");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("foo.yml");
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.yml");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("foo.yml");
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentBar_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "foo.bar");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("foo.bar");
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentBar_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.bar");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("foo.bar");
    }

    @Test
    public void testResolveSystemProperty_file_noSuffix_loadedAsXml() throws Exception {
        File file = helper.givenXmlClientFailoverConfigFileInWorkDir("foo", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, file.getAbsolutePath());

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_CLIENT_FAILOVER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("foo")
                .hasMessageContaining(ALL_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_classpath_noSuffix_throws() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo");

        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_CLIENT_FAILOVER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("foo")
                .hasMessageContaining(ALL_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveWorkDir_xml() throws Exception {
        helper.givenXmlClientFailoverConfigFileInWorkDir(42);

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveClasspath_xml() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath(42);

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveWorkDir_yaml() throws Exception {
        helper.givenYamlClientFailoverConfigFileInWorkDir(42);

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveClasspath_yaml() throws Exception {
        helper.givenYamlClientFailoverConfigFileOnClasspath(42);

        ClientFailoverConfig config = ClientFailoverConfig.load();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveDefault_xml() {
        assertThatThrownBy(ClientFailoverConfig::load)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Failed to load ClientFailoverConfig");

    }
}
