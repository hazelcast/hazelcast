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

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_MEMBER_CONFIG;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SerialTest.class})
public class YamlJetMemberConfigResolutionTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private JetDeclarativeConfigFileHelper helper = new JetDeclarativeConfigFileHelper();

    @Before
    @After
    public void tearDown() throws Exception {
        System.clearProperty(SYSPROP_MEMBER_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_yaml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("foo.yaml", "foo");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.yaml");

        JetConfig config = new YamlJetConfigBuilder().build();
        assertEquals("foo", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yaml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("foo.yaml", "foo");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.yaml");

        JetConfig config = new YamlJetConfigBuilder().build();
        assertEquals("foo", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_file_yml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("foo.yml", "aaa");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "foo.yml");

        JetConfig config = new YamlJetConfigBuilder().build();
        assertEquals("aaa", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_yml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("foo.yml", "bbb");
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:foo.yml");

        JetConfig config = new YamlJetConfigBuilder().build();
        assertEquals("bbb", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "classpath:idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.yaml");

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentYaml_throws() {
        System.setProperty(SYSPROP_MEMBER_CONFIG, "idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("idontexist.yaml");

        new YamlJetConfigBuilder().build();
    }

    @Test
    public void testResolveFromWorkDirYamlButNotYml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("test");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("test", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveFromWorkDirYmlButNotYaml() throws Exception {
        helper.givenYmlConfigFileInWorkDir("yml");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("yml", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveFromWorkDirYamlAndYml() throws Exception {
        helper.givenYamlConfigFileInWorkDir("yaml");
        helper.givenYmlConfigFileInWorkDir("yml");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("yaml", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYamlButNotYml() throws Exception {
        helper.givenYmlConfigFileInWorkDir("yaml");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("yaml", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYmlButNotYaml() throws Exception {
        helper.givenYmlConfigFileOnClasspath("yml");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("yml", config.getHazelcastConfig().getInstanceName());
    }

    @Test
    public void testResolveFromClasspathYamlAndYml() throws Exception {
        helper.givenYamlConfigFileOnClasspath("yaml");
        helper.givenYmlConfigFileOnClasspath("yml");

        JetConfig config = new YamlJetConfigBuilder().build();

        assertEquals("yaml", config.getHazelcastConfig().getInstanceName());

    }

}
