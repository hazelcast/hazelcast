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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.helpers.DeclarativeConfigFileHelper;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
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

import static com.hazelcast.config.DeclarativeConfigUtil.ALL_ACCEPTED_SUFFIXES_STRING;
import static com.hazelcast.config.DeclarativeConfigUtil.SYSPROP_CLIENT_CONFIG;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastClientConfigResolutionTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HazelcastInstance instance;
    private DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    public void setUp() {
        System.clearProperty(SYSPROP_CLIENT_CONFIG);
    }

    @After
    public void tearDown() throws Exception {
        if (instance != null) {
            instance.shutdown();
        }
        System.clearProperty(SYSPROP_CLIENT_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml_loadedAsXml() throws Exception {
        File file = helper.givenXmlClientConfigFileInWorkDir("foo.xml", "cluster-xml");
        System.setProperty(SYSPROP_CLIENT_CONFIG, file.getAbsolutePath());

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml_loadedAsXml() throws Exception {
        helper.givenXmlClientConfigFileOnClasspath("foo.xml", "cluster-xml");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.xml");

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yaml_loadedAsYaml() throws Exception {
        File file = helper.givenYamlClientConfigFileInWorkDir("foo.yaml", "cluster-yaml");
        System.setProperty(SYSPROP_CLIENT_CONFIG, file.getAbsolutePath());

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml_loadedAsYaml() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath("foo.yaml", "cluster-yaml");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.yaml");

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yml_loadedAsYaml() throws Exception {
        File file = helper.givenYamlClientConfigFileInWorkDir("foo.yml", "cluster-yaml");
        System.setProperty(SYSPROP_CLIENT_CONFIG, file.getAbsolutePath());

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml_loadedAsYaml() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath("foo.yml", "cluster-yaml");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.yml");

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_bar_throws() throws Exception {
        File file = helper.givenXmlClientConfigFileInWorkDir("foo.bar", "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo.bar");
        expectedException.expectMessage(ALL_ACCEPTED_SUFFIXES_STRING);

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_bar_throws() throws Exception {
        helper.givenXmlClientConfigFileOnClasspath("foo.bar", "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo.bar");
        expectedException.expectMessage(ALL_ACCEPTED_SUFFIXES_STRING);

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.xml");

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.xml");

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "foo.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.yaml");

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.yaml");

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "foo.yml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.yml");

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.yml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.yml");

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentBar_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.bar");

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentBar_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("foo.bar");

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_file_noSuffix_throws() throws Exception {
        File file = helper.givenXmlClientConfigFileInWorkDir("foo", "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo");
        expectedException.expectMessage(ALL_ACCEPTED_SUFFIXES_STRING);

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveSystemProperty_classpath_noSuffix_throws() throws Exception {
        helper.givenXmlClientConfigFileOnClasspath("foo", "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo");
        expectedException.expectMessage(ALL_ACCEPTED_SUFFIXES_STRING);

        instance = HazelcastClient.newHazelcastClient();
    }

    @Test
    public void testResolveWorkDir_xml() throws Exception {
        helper.givenXmlClientConfigFileInWorkDir("cluster-xml");

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveClasspath_xml() throws Exception {
        helper.givenXmlClientConfigFileOnClasspath("cluster-xml");

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-xml", config.getInstanceName());
    }

    @Test
    public void testResolveWorkDir_yaml() throws Exception {
        helper.givenYamlClientConfigFileInWorkDir("cluster-yaml");

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveClasspath_yaml() throws Exception {
        helper.givenYamlClientConfigFileOnClasspath("cluster-yaml");

        instance = HazelcastClient.newHazelcastClient();
        ClientConfig config = getClientConfig(instance);

        assertEquals("cluster-yaml", config.getInstanceName());
    }

    @Test
    public void testResolveDefault_xml() {
        // needed for the client
        HazelcastInstance member = HazelcastInstanceFactory.newHazelcastInstance(null);

        try {
            getClass().getClassLoader().getResource("hazelcast-client-default.xml");

            instance = HazelcastClient.newHazelcastClient();
            ClientConfig config = getClientConfig(instance);

            assertEquals("dev", config.getGroupConfig().getName());
        } finally {
            member.shutdown();
        }
    }

    private ClientConfig getClientConfig(HazelcastInstance instance) {
        return ((HazelcastClientProxy) instance).getClientConfig();
    }
}
