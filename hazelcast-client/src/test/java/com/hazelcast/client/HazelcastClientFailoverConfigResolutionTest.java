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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.helpers.DeclarativeConfigFileHelper;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
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
public class HazelcastClientFailoverConfigResolutionTest {

    private static final String SYSPROP_NAME = "hazelcast.client.failover.config";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HazelcastInstance instance;
    private DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    public void setUp() {
        System.clearProperty(SYSPROP_NAME);
    }

    @After
    public void tearDown() {
        if (instance != null) {
            instance.shutdown();
        }
        System.clearProperty(SYSPROP_NAME);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml_loadedAsXml() throws Exception {
        File file = helper.givenXmlClientFailoverConfigFileInWorkDir("foo.xml", 42);
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml_loadedAsXml() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo.xml", 42);
        System.setProperty(SYSPROP_NAME, "classpath:foo.xml");

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_file_yaml_loadedAsYaml() throws Exception {
        assumeThatJDK8OrHigher();
        File file = helper.givenYamlClientFailoverConfigFileInWorkDir("foo.yaml", 42);
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml_loadedAsYaml() throws Exception {
        assumeThatJDK8OrHigher();
        helper.givenYamlClientFailoverConfigFileOnClasspath("foo.yaml", 42);
        System.setProperty(SYSPROP_NAME, "classpath:foo.yaml");

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_file_yml_loadedAsYaml() throws Exception {
        assumeThatJDK8OrHigher();
        File file = helper.givenYamlClientFailoverConfigFileInWorkDir("foo.yml", 42);
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml_loadedAsYaml() throws Exception {
        assumeThatJDK8OrHigher();
        helper.givenYamlClientFailoverConfigFileOnClasspath("foo.yml", 42);
        System.setProperty(SYSPROP_NAME, "classpath:foo.yml");

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_file_bar_loadedAsXml() throws Exception {
        File file = helper.givenXmlClientFailoverConfigFileInWorkDir("foo.bar", 42);
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_bar_loadedAsXml() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo.bar", 42);
        System.setProperty(SYSPROP_NAME, "classpath:foo.bar");

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_NAME, "foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.xml");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.xml");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_NAME, "foo.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.yaml");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:foo.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.yaml");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYml_throws() {
        System.setProperty(SYSPROP_NAME, "foo.yml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.yml");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:foo.yml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.yml");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentBar_throws() {
        System.setProperty(SYSPROP_NAME, "foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.bar");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentBar_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.bar");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    @Test
    public void testResolveWorkDir_xml() throws Exception {
        helper.givenXmlClientFailoverConfigFileInWorkDir(42);

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveClasspath_xml() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath(42);

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveWorkDir_yaml() throws Exception {
        assumeThatJDK8OrHigher();
        helper.givenYamlClientFailoverConfigFileInWorkDir(42);

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveClasspath_yaml() throws Exception {
        assumeThatJDK8OrHigher();
        helper.givenYamlClientFailoverConfigFileOnClasspath(42);

        instance = HazelcastClient.newHazelcastFailoverClient();
        ClientFailoverConfig config = getClientConfig(instance);

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveDefault_xml() {
        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("Failed to load ClientFailoverConfig");

        instance = HazelcastClient.newHazelcastFailoverClient();
    }

    private ClientFailoverConfig getClientConfig(HazelcastInstance instance) {
        return ((HazelcastClientProxy) instance).client.getFailoverConfig();
    }
}
