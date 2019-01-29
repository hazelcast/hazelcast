/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLDecoder;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_END_TAG;
import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_START_TAG;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * These tests manipulate system properties, therefore they must be run in serial mode.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XMLConfigWithSystemPropertyTest {

    @Before
    @After
    public void beforeAndAfter() {
        System.clearProperty("hazelcast.config");
    }

    @Test
    public void testConfigurationWithFile() throws Exception {
        URL url = getClass().getClassLoader().getResource("hazelcast-default.xml");
        assertNotNull(url);
        String decodedURL = URLDecoder.decode(url.getFile(), "UTF-8");
        System.setProperty("hazelcast.config", decodedURL);
        Config config = new XmlConfigBuilder().build();
        URL file = new URL("file:");
        URL encodedURL = new URL(file, decodedURL);
        assertEquals(encodedURL, config.getConfigurationUrl());
    }

    @Test(expected = HazelcastException.class)
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void loadingThroughSystemProperty_nonExistingFile() throws Exception {
        File file = createTempFile("foo", "bar");
        file.delete();
        System.setProperty("hazelcast.config", file.getAbsolutePath());
        new XmlConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingFile() throws Exception {
        String xml = HAZELCAST_START_TAG
                + "    <group>\n"
                + "        <name>foobar</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>"
                + HAZELCAST_END_TAG;

        File file = createTempFile("foo", "bar");
        file.deleteOnExit();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(xml);
        writer.close();

        System.setProperty("hazelcast.config", file.getAbsolutePath());

        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() {
        System.setProperty("hazelcast.config", "classpath:idontexist");
        new XmlConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() {
        System.setProperty("hazelcast.config", "classpath:test-hazelcast.xml");

        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
    }
}
