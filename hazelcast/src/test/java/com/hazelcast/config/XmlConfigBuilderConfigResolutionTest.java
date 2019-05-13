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

package com.hazelcast.config;

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

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlConfigBuilderConfigResolutionTest {

    private static final String SYSPROP_NAME = "hazelcast.config";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    @After
    public void beforeAndAfter() throws Exception {
        System.clearProperty(SYSPROP_NAME);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml() throws Exception {
        helper.givenXmlConfigFileInWorkDir("foo.xml", "cluster-xml-file");
        System.setProperty(SYSPROP_NAME, "foo.xml");

        Config config = new XmlConfigBuilder().build();
        assertEquals("cluster-xml-file", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml() throws Exception {
        helper.givenXmlConfigFileOnClasspath("foo.xml", "cluster-xml-classpath");
        System.setProperty(SYSPROP_NAME, "classpath:foo.xml");

        Config config = new XmlConfigBuilder().build();
        assertEquals("cluster-xml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.xml");

        new XmlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_NAME, "idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("idontexist.xml");

        new XmlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonXml_loadedAsXml() throws Exception {
        File file = helper.givenXmlConfigFileInWorkDir("foo.bar", "cluster-bar-file");
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        Config config = new XmlConfigBuilder().build();

        assertEquals("cluster-bar-file", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonXml_loadedAsXml() throws Exception {
        helper.givenXmlConfigFileOnClasspath("foo.bar", "cluster-bar-classpath");
        System.setProperty(SYSPROP_NAME, "classpath:foo.bar");

        Config config = new XmlConfigBuilder().build();
        assertEquals("cluster-bar-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentNonXml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:idontexist.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.bar");

        new XmlConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentNonXml_throws() {
        System.setProperty(SYSPROP_NAME, "foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.bar");

        new XmlConfigBuilder().build();
    }

    @Test
    public void testResolveFromWorkDir() throws Exception {
        helper.givenXmlConfigFileInWorkDir("cluster-xml-workdir");

        Config config = new XmlConfigBuilder().build();

        assertEquals("cluster-xml-workdir", config.getInstanceName());
    }

    @Test
    public void testResolveFromClasspath() throws Exception {
        helper.givenXmlConfigFileOnClasspath("cluster-xml-classpath");

        Config config = new XmlConfigBuilder().build();

        assertEquals("cluster-xml-classpath", config.getInstanceName());
    }

    @Test
    public void testResolveDefault() {
        Config config = new XmlConfigBuilder().build();
        assertEquals("dev", config.getGroupConfig().getName());
    }
}
