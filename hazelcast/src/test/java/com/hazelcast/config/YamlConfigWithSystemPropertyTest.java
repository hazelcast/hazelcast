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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
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
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLDecoder;

import static com.hazelcast.test.HazelcastTestSupport.assumeThatJDK8OrHigher;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;

/**
 * These tests manipulate system properties, therefore they must be run in serial mode.
 *
 * @see XMLConfigWithSystemPropertyTest
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class YamlConfigWithSystemPropertyTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        assumeThatJDK8OrHigher();

        System.clearProperty("hazelcast.config");
    }

    @After
    public void tearDown() {
        System.clearProperty("hazelcast.config");
    }

    @Test
    public void testConfigurationWithFile() throws Exception {
        URL url = getClass().getClassLoader().getResource("hazelcast-default.yaml");
        String decodedURL = URLDecoder.decode(url.getFile(), "UTF-8");
        System.setProperty("hazelcast.config", decodedURL);
        Config config = new YamlConfigBuilder().build();
        URL file = new URL("file:");
        URL encodedURL = new URL(file, decodedURL);
        assertEquals(encodedURL, config.getConfigurationUrl());
    }

    @Test(expected = HazelcastException.class)
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void loadingThroughSystemProperty_nonExistingFile() throws Exception {
        File file = createTempFile("foo", ".yaml");
        file.delete();
        System.setProperty("hazelcast.config", file.getAbsolutePath());
        new YamlConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingFile() throws Exception {
        String yaml = ""
                + "hazelcast:\n"
                + "  group:\n"
                + "    name: foobar\n"
                + "    password: dev-pass";

        File file = createTempFile("foo", ".yaml");
        file.deleteOnExit();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(yaml);
        writer.close();

        System.setProperty("hazelcast.config", file.getAbsolutePath());

        YamlConfigBuilder configBuilder = new YamlConfigBuilder();
        Config config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() {
        System.setProperty("hazelcast.config", "classpath:idontexist.yaml");
        new YamlConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() {
        System.setProperty("hazelcast.config", "classpath:test-hazelcast.yaml");

        YamlConfigBuilder configBuilder = new YamlConfigBuilder();
        Config config = configBuilder.build();
        assertEquals("foobar-yaml", config.getGroupConfig().getName());
    }

    @Test
    public void loadingThroughSystemPropertyWithLocator_existingClasspathResource() {
        System.setProperty("hazelcast.config", "classpath:test-hazelcast.yaml");

        HazelcastInstance instance = HazelcastInstanceFactory.newHazelcastInstance(null);
        Config config = instance.getConfig();
        instance.shutdown();

        assertEquals("foobar-yaml", config.getGroupConfig().getName());
    }

    @Test
    public void loadingThroughSystemPropertyWithLocator_nonYamlSuffix() {
        System.setProperty("hazelcast.config", "classpath:test-hazelcast.foobar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("hazelcast.config");
        expectedException.expectMessage("classpath:test-hazelcast.foobar");
        expectedException.expectMessage("yaml, yml");

        new YamlConfigBuilder();
    }
}
