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
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientFailoverConfigBuilderConfigResolutionTest {

    private static final String SYSPROP_NAME = "hazelcast.client.failover.config";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    @After
    public void beforeAndAfter() throws IOException {
        System.clearProperty(SYSPROP_NAME);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml() throws Exception {
        helper.givenXmlClientFailoverConfigFileInWorkDir("foo.xml", 42);
        System.setProperty(SYSPROP_NAME, "foo.xml");

        ClientFailoverConfig config = new XmlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo.xml", 42);
        System.setProperty(SYSPROP_NAME, "classpath:foo.xml");

        ClientFailoverConfig config = new XmlClientFailoverConfigBuilder().build();
        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.xml");

        new XmlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_NAME, "idontexist.xml");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("idontexist.xml");

        new XmlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonXml_loadedAsXml() throws Exception {
        File file = helper.givenXmlClientFailoverConfigFileInWorkDir("foo.bar", 42);
        System.setProperty(SYSPROP_NAME, file.getAbsolutePath());

        ClientFailoverConfig config = new XmlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonXml_loadedAsXml() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo.bar", 42);
        System.setProperty(SYSPROP_NAME, "classpath:foo.bar");

        ClientFailoverConfig config = new XmlClientFailoverConfigBuilder().build();
        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentNonXml_throws() {
        System.setProperty(SYSPROP_NAME, "classpath:idontexist.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("classpath");
        expectedException.expectMessage("idontexist.bar");

        new XmlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentNonXml_throws() {
        System.setProperty(SYSPROP_NAME, "foo.bar");

        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("foo.bar");

        new XmlClientFailoverConfigBuilder().build();
    }

    @Test
    public void testResolveFromWorkDir() throws Exception {
        helper.givenXmlClientFailoverConfigFileInWorkDir(42);

        ClientFailoverConfig config = new XmlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveFromClasspath() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath(42);

        ClientFailoverConfig config = new XmlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveDefault() {
        expectedException.expect(HazelcastException.class);
        expectedException.expectMessage("Failed to load ClientFailoverConfig");

        new XmlClientFailoverConfigBuilder().build();
    }
}
