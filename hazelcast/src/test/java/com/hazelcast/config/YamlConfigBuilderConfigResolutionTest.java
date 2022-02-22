/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.config.DeclarativeConfigUtil;
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
import java.util.ArrayList;
import java.util.Collections;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES_STRING;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlConfigBuilderConfigResolutionTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    @After
    public void tearDown() throws Exception {
        System.clearProperty(SYSPROP_MEMBER_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_yaml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("foo.yaml", "cluster-yaml-file");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.yaml");

        Config config = new YamlConfigBuilder().build();
        assertEquals("cluster-yaml-file", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("foo.yaml", "cluster-yaml-classpath");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.yaml");

        Config config = new YamlConfigBuilder().build();
        assertEquals("cluster-yaml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("foo.yml", "cluster-yml-file");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.yml");

        Config config = new YamlConfigBuilder().build();
        assertEquals("cluster-yml-file", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("foo.yml", "cluster-yml-classpath");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.yml");

        Config config = new YamlConfigBuilder().build();
        assertEquals("cluster-yml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.yaml");

        new YamlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("idontexist.yaml");

        new YamlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonYaml_throws() throws Exception {
        File file = helper.givenYamlConfigFileInWorkDir("foo.xml", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonYaml_throws() throws Exception {
        helper.givenYamlConfigFileOnClasspath("foo.xml", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentNonYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("foo.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentNonYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.xml");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_noSuffix_throws() throws Exception {
        File file = helper.givenYamlConfigFileInWorkDir("foo", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_noSuffix_throws() throws Exception {
        helper.givenYamlConfigFileOnClasspath("foo", "irrelevant");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_MEMBER_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo");
        expectedException.expectMessage(YAML_ACCEPTED_SUFFIXES_STRING);

        new YamlConfigBuilder().build();
    }

    @Test
    public void testResolveFromWorkDirYamlButNotYml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("cluster-yaml-workdir");

        Config config = new YamlConfigBuilder().build();

        assertEquals("cluster-yaml-workdir", config.getInstanceName());
    }

    @Test
    public void testResolveFromWorkDirYmlButNotYaml() throws Exception {
        helper.givenYmlConfigFileInWorkDir("cluster-yml-workdir");

        Config config = new YamlConfigBuilder().build();

        assertEquals("cluster-yml-workdir", config.getInstanceName());
    }

    @Test
    public void testResolveFromWorkDirYamlAndYml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("cluster-yaml-workdir");
        helper.givenYmlConfigFileInWorkDir("cluster-yml-workdir");

        Config config = new YamlConfigBuilder().build();

        assertEquals("cluster-yaml-workdir", config.getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYamlButNotYml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("cluster-yaml-classpath");

        Config config = new YamlConfigBuilder().build();

        assertEquals("cluster-yaml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYmlButNotYaml() throws Exception {
        helper.givenYmlConfigFileOnClasspath("cluster-yml-classpath");

        Config config = new YamlConfigBuilder().build();

        assertEquals("cluster-yml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYamlAndYml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("cluster-yaml-classpath");
        helper.givenYmlConfigFileOnClasspath("cluster-yml-classpath");

        Config config = new YamlConfigBuilder().build();

        assertEquals("cluster-yaml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveDefault() {
        Config config = new YamlConfigBuilder().build();
        assertEquals("dev", config.getClusterName());
    }

    @Test
    public void testYamlSuffixSearchSequence() {
        ArrayList<String> YAML_ACCEPTED_SUFFIXES_SORTED =
            new ArrayList<>(DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES);
        Collections.sort(YAML_ACCEPTED_SUFFIXES_SORTED);

        assertEquals("YAML_ACCEPTED_SUFFIXES sequence", YAML_ACCEPTED_SUFFIXES_SORTED, DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES);
        assertEquals("YAML_ACCEPTED_SUFFIXES size", 2, YAML_ACCEPTED_SUFFIXES_SORTED.size());
    }

}
