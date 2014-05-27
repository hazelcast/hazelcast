/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;

/**
 * This class tests the usage of {@link XmlClientConfigBuilder}
 */
//tests need to be executed sequentially because of system properties being set/unset
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientConfigBuilderTest {

    @After
    @Before
    public void after() {
        System.clearProperty("hazelcast.client.config");
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingFile() throws IOException {
        File file = File.createTempFile("foo", "bar");
        file.delete();
        System.setProperty("hazelcast.client.config", file.getAbsolutePath());

        new XmlClientConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingFile() throws IOException {
        String xml =
                "<hazelcast-client>\n" +
                        "    <group>\n" +
                        "        <name>foobar</name>\n" +
                        "        <password>dev-pass</password>\n" +
                        "    </group>" +
                        "</hazelcast-client>";

        File file = File.createTempFile("foo", "bar");
        file.deleteOnExit();
        PrintWriter writer = new PrintWriter(file, "UTF-8");
        writer.println(xml);
        writer.close();

        System.setProperty("hazelcast.client.config", file.getAbsolutePath());

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder();
        ClientConfig config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
    }

    @Test(expected = HazelcastException.class)
    public void loadingThroughSystemProperty_nonExistingClasspathResource() throws IOException {
        System.setProperty("hazelcast.client.config", "classpath:idontexist");
        new XmlClientConfigBuilder();
    }

    @Test
    public void loadingThroughSystemProperty_existingClasspathResource() throws IOException {
        System.setProperty("hazelcast.client.config", "classpath:test-hazelcast-client.xml");

        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder();
        ClientConfig config = configBuilder.build();
        assertEquals("foobar", config.getGroupConfig().getName());
    }

    @Test
    public void readClientExecutorPoolSize() {
        String xml =
                "<hazelcast-client>\n" +
                        "<executor-pool-size>18</executor-pool-size>" +
                        "</hazelcast-client>";
        final ClientConfig clientConfig = buildConfig(xml);
        assertEquals(18, clientConfig.getExecutorPoolSize());
    }

    @Test
    public void readProperties() {
        String xml =
                "<hazelcast-client>\n" +
                        "<properties>" +
                        "<property name=\"hazelcast.client.connection.timeout\">6000</property>" +
                        "<property name=\"hazelcast.client.retry.count\">8</property>" +
                        "</properties>" +
                        "</hazelcast-client>";
        final ClientConfig clientConfig = buildConfig(xml);
        assertEquals("6000", clientConfig.getProperty("hazelcast.client.connection.timeout"));
        assertEquals("8", clientConfig.getProperty("hazelcast.client.retry.count"));
    }

    private ClientConfig buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        return configBuilder.build();
    }
}
