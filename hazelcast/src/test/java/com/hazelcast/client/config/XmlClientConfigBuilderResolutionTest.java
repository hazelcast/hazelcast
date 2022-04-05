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

package com.hazelcast.client.config;

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

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.XML_ACCEPTED_SUFFIXES_STRING;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientConfigBuilderResolutionTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    @After
    public void beforeAndAfter() throws Exception {
        System.clearProperty(SYSPROP_CLIENT_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml() throws Exception {
        helper.givenXmlClientConfigFileInWorkDir("foo.xml", "cluster-xml-file");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "foo.xml");

        ClientConfig config = new XmlClientConfigBuilder().build();
        assertEquals("cluster-xml-file", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml() throws Exception {
        helper.givenXmlClientConfigFileOnClasspath("foo.xml", "cluster-xml-classpath");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.xml");

        ClientConfig config = new XmlClientConfigBuilder().build();
        assertEquals("cluster-xml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.xml");

        new XmlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("idontexist.xml");

        new XmlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonXml_throws() throws Exception {
        File file = helper.givenXmlClientConfigFileInWorkDir("foo.yaml", "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, file.getAbsolutePath());

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo.yaml");
        expectedException.expectMessage(XML_ACCEPTED_SUFFIXES_STRING);

        new XmlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonXml_throws() throws Exception {
        helper.givenXmlClientConfigFileOnClasspath("foo.yaml", "irrelevant");
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:foo.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("foo.yaml");
        expectedException.expectMessage(XML_ACCEPTED_SUFFIXES_STRING);

        new XmlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentNonXml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "classpath:idontexist.yaml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expectMessage("suffix");
        expectedException.expectMessage("idontexist.yaml");
        expectedException.expectMessage(XML_ACCEPTED_SUFFIXES_STRING);

        new XmlClientConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentNonXml_throws() {
        System.setProperty(SYSPROP_CLIENT_CONFIG, "foo.yaml");

        expectedException.expectMessage(SYSPROP_CLIENT_CONFIG);
        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.yaml");
        expectedException.expectMessage(XML_ACCEPTED_SUFFIXES_STRING);

        new XmlClientConfigBuilder().build();
    }

    @Test
    public void testResolveFromWorkDir() throws Exception {
        helper.givenXmlClientConfigFileInWorkDir("cluster-xml-workdir");

        ClientConfig config = new XmlClientConfigBuilder().build();

        assertEquals("cluster-xml-workdir", config.getInstanceName());
    }

    @Test
    public void testResolveFromClasspath() throws Exception {
        helper.givenXmlClientConfigFileOnClasspath("cluster-xml-classpath");

        ClientConfig config = new XmlClientConfigBuilder().build();

        assertEquals("cluster-xml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveDefault() {
        ClientConfig config = new XmlClientConfigBuilder().build();
        assertEquals("dev", config.getClusterName());
    }

}
