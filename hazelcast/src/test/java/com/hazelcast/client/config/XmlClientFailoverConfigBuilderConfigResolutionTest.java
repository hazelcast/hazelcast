/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_FAILOVER_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.XML_ACCEPTED_SUFFIXES_STRING;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class XmlClientFailoverConfigBuilderConfigResolutionTest {


    private final DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();

    @Before
    @After
    public void beforeAndAfter() {
        System.clearProperty(SYSPROP_CLIENT_FAILOVER_CONFIG);
        helper.ensureTestConfigDeleted();
    }

    @Test
    public void testResolveSystemProperty_file_xml() throws Exception {
        helper.givenXmlClientFailoverConfigFileInWorkDir("foo.xml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "foo.xml");

        ClientFailoverConfig config = new XmlClientFailoverConfigBuilder().build();

        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_xml() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo.xml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.xml");

        ClientFailoverConfig config = new XmlClientFailoverConfigBuilder().build();
        assertEquals(42, config.getTryCount());
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentXml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:idontexist.xml");

        assertThatThrownBy(XmlClientFailoverConfigBuilder::new)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("classpath")
                .hasMessageContaining("idontexist.xml");
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentXml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "idontexist.xml");

        assertThatThrownBy(XmlClientFailoverConfigBuilder::new)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("idontexist.xml");
    }

    @Test
    public void testResolveSystemProperty_file_nonXml_throws() throws Exception {
        File file = helper.givenXmlClientFailoverConfigFileInWorkDir("foo.yaml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, file.getAbsolutePath());

        assertThatThrownBy(XmlClientFailoverConfigBuilder::new)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_CLIENT_FAILOVER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("foo.yaml")
                .hasMessageContaining(XML_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_classpath_nonXml_throws() throws Exception {
        helper.givenXmlClientFailoverConfigFileOnClasspath("foo.yaml", 42);
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:foo.yaml");

        assertThatThrownBy(XmlClientFailoverConfigBuilder::new)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_CLIENT_FAILOVER_CONFIG)
                .hasMessageContaining("suffix")
                .hasMessageContaining("foo.yaml")
                .hasMessageContaining(XML_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_classpath_nonExistentNonXml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "classpath:idontexist.yaml");

        assertThatThrownBy(XmlClientFailoverConfigBuilder::new)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_CLIENT_FAILOVER_CONFIG)
                .hasMessageContaining("classpath")
                .hasMessageContaining("idontexist.yaml")
                .hasMessageContaining(XML_ACCEPTED_SUFFIXES_STRING);
    }

    @Test
    public void testResolveSystemProperty_file_nonExistentNonXml_throws() {
        System.setProperty(SYSPROP_CLIENT_FAILOVER_CONFIG, "foo.yaml");

        assertThatThrownBy(XmlClientFailoverConfigBuilder::new)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining(SYSPROP_CLIENT_FAILOVER_CONFIG)
                .hasMessageContaining("foo.yaml")
                .hasMessageContaining(XML_ACCEPTED_SUFFIXES_STRING);
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
        assertThatThrownBy(XmlClientFailoverConfigBuilder::new)
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Failed to load ClientFailoverConfig");
    }
}
