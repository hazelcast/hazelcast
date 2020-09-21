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

package com.hazelcast.jet.config;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.impl.config.YamlJetConfigBuilder;
import com.hazelcast.jet.test.JetDeclarativeConfigFileHelper;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES_STRING;
import static com.hazelcast.jet.impl.config.JetDeclarativeConfigUtil.SYSPROP_JET_CONFIG;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SerialTest.class})
public class YamlJetConfigResolutionTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final JetDeclarativeConfigFileHelper helper = new JetDeclarativeConfigFileHelper();

    @Before
    @After
    public void tearDown() {
        System.clearProperty(SYSPROP_JET_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_yaml() throws Exception {
        helper.givenYamlJetConfigFileOnWorkdir("foo.yaml", "aaa", "bbb");
        System.setProperty(SYSPROP_JET_CONFIG, "foo.yaml");

        JetConfig config = new YamlJetConfigBuilder().build();
        assertEquals("bbb", config.getProperties().get("aaa"));
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml() throws Exception {
        helper.givenYamlJetConfigFileOnClasspath("foo.yaml", "aaa", "bbb");
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:foo.yaml");

        JetConfig config = new YamlJetConfigBuilder().build();
        assertEquals("bbb", config.getProperties().get("aaa"));
    }

    @Test
    public void testResolveSystemProperty_file_yml() throws Exception {
        helper.givenYamlJetConfigFileOnWorkdir("foo.yml", "aaa", "bbb");
        System.setProperty(SYSPROP_JET_CONFIG, "foo.yml");

        JetConfig config = new YamlJetConfigBuilder().build();
        assertEquals("bbb", config.getProperties().get("aaa"));
    }

    @Test
    public void testResolveSystemProperty_classpath_yml() throws Exception {
        helper.givenYamlJetConfigFileOnClasspath("foo.yml", "aaa", "bbb");
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:foo.yml");

        JetConfig config = new YamlJetConfigBuilder().build();
        assertEquals("bbb", config.getProperties().get("aaa"));
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.yaml");

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_JET_CONFIG, "idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("idontexist.yaml");

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonYaml_throws() throws Exception {
        File file = helper.givenYamlJetConfigFileOnWorkdir("foo.xml", "aaa", "bbb");
        System.setProperty(SYSPROP_JET_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_JET_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonYaml_throws() throws Exception {
        helper.givenYamlJetConfigFileOnClasspath("foo.xml", "aaa", "bbb");
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_JET_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentNonYaml_throws() {
        System.setProperty(SYSPROP_JET_CONFIG, "foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_JET_CONFIG);
        expectedException.expectMessage("foo.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentNonYaml_throws() {
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_JET_CONFIG);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_noSuffix_throws() throws Exception {
        File file = helper.givenYamlJetConfigFileOnWorkdir("foo", "irrelevant", "irrelevant");
        System.setProperty(SYSPROP_JET_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_JET_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_noSuffix_throws() throws Exception {
        helper.givenYamlJetConfigFileOnClasspath("foo", "irrelevant", "irrelevant");
        System.setProperty(SYSPROP_JET_CONFIG, "classpath:foo");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_JET_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveFromWorkDirYamlButNotYml() throws Exception {
        helper.givenYamlJetConfigFileOnWorkdir("hazelcast-jet.yaml", "aaa", "bbb");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("bbb", config.getProperties().get("aaa"));
    }

    @Test
    public void testResolveFromWorkDirYmlButNotYaml() throws Exception {
        helper.givenYmlJetConfigFileOnWorkdir("hazelcast-jet.yml", "aaa", "bbb");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("bbb", config.getProperties().get("aaa"));
    }

    @Test
    public void testResolveFromWorkDirYamlAndYml() throws Exception {
        helper.givenYamlJetConfigFileOnWorkdir("hazelcast-jet.yaml", "bbb", "yaml-workdir");
        helper.givenYmlJetConfigFileOnWorkdir("hazelcast-jet.yml", "bbb", "yml-workdir");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("yaml-workdir", config.getProperties().get("bbb"));
    }

    @Test
    public void testResolveFromClasspathYamlButNotYml() throws Exception {
        helper.givenYamlJetConfigFileOnWorkdir("hazelcast-jet.yaml", "aaa", "bbb");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("bbb", config.getProperties().get("aaa"));
    }

    @Test
    public void testResolveFromClasspathYmlButNotYaml() throws Exception {
        helper.givenYmlJetConfigFileOnClasspath("hazelcast-jet.yml", "aaa", "bbb");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("bbb", config.getProperties().get("aaa"));
    }

    @Test
    public void testResolveFromClasspathYamlAndYml() throws Exception {
        helper.givenYamlJetConfigFileOnClasspath("hazelcast-jet.yaml", "vvv", "yaml");
        helper.givenYmlJetConfigFileOnClasspath("hazelcast-jet.yml", "vvv", "yml");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("yaml", config.getProperties().get("vvv"));
    }
}
