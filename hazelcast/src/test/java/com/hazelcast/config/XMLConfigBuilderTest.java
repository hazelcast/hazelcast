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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.xml.sax.SAXException;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class XMLConfigBuilderTest {

    @Test
    public void testCleanNodeName() {
        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        assertEquals("nocolon", configBuilder.cleanNodeName("noColon"));
        assertEquals("after", configBuilder.cleanNodeName("Before:After"));
        assertNull(configBuilder.cleanNodeName((String) null));
    }

    @Test
    public void readAwsConfig() {
        String xml =
                "<hazelcast>\n" +
                        "   <group>\n" +
                        "        <name>dev</name>\n" +
                        "        <password>dev-pass</password>\n" +
                        "    </group>\n" +
                        "    <network>\n" +
                        "        <port auto-increment=\"true\">5701</port>\n" +
                        "        <join>\n" +
                        "            <multicast enabled=\"false\">\n" +
                        "                <multicast-group>224.2.2.3</multicast-group>\n" +
                        "                <multicast-port>54327</multicast-port>\n" +
                        "            </multicast>\n" +
                        "            <tcp-ip enabled=\"false\">\n" +
                        "                <interface>127.0.0.1</interface>\n" +
                        "            </tcp-ip>\n" +
                        "            <aws enabled=\"true\" connection-timeout-seconds=\"10\" >\n" +
                        "                <access-key>access</access-key>\n" +
                        "                <secret-key>secret</secret-key>\n" +
                        "            </aws>\n" +
                        "        </join>\n" +
                        "        <interfaces enabled=\"false\">\n" +
                        "            <interface>10.10.1.*</interface>\n" +
                        "        </interfaces>\n" +
                        "    </network>\n" +
                        "</hazelcast>";
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
        AwsConfig awsConfig = config.getNetworkConfig().getJoin().getAwsConfig();
        assertTrue(awsConfig.isEnabled());
        assertEquals(10, config.getNetworkConfig().getJoin().getAwsConfig().getConnectionTimeoutSeconds());
        assertEquals("access", awsConfig.getAccessKey());
        assertEquals("secret", awsConfig.getSecretKey());
    }

    @Test
    public void readSemaphoreConfig() {
        String xml =
                "<hazelcast>\n" +
                        "    <semaphore name=\"default\">\n" +
                        "        <initial-permits>1</initial-permits>\n" +
                        "    </semaphore>" +
                        "    <semaphore name=\"custom\">\n" +
                        "        <initial-permits>10</initial-permits>\n" +
                        "        <semaphore-factory enabled=\"true\">" +
                        "             <class-name>com.acme.MySemaphore</class-name>\n" +
                        "        </semaphore-factory>" +
                        "    </semaphore>" +
                        "</hazelcast>";
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
        SemaphoreConfig defaultConfig = config.getSemaphoreConfig("default");
        SemaphoreConfig customConfig = config.getSemaphoreConfig("custom");
        assertEquals(1, defaultConfig.getInitialPermits());
        assertEquals(10, customConfig.getInitialPermits());
    }

    @Test
    public void testConfig2Xml2DefaultConfig() {
        testConfig2Xml2Config("hazelcast-default.xml");
    }

    @Test
    public void testConfig2Xml2FullConfig() {
        testConfig2Xml2Config("hazelcast-fullconfig.xml");
    }

    private void testConfig2Xml2Config(String fileName) {
        final Config config = new ClasspathXmlConfig(fileName);
        final String xml = new ConfigXmlGenerator(true).generate(config);
        final Config config2 = new InMemoryXmlConfig(xml);
        assertTrue(config.isCompatible(config2));
        assertTrue(config2.isCompatible(config));
    }

    @Test
    public void testXSDDefaultXML() throws SAXException, IOException {
        testXSDConfigXML("hazelcast-default.xml");
    }

    @Test
    public void testFullConfigXML() throws SAXException, IOException {
        testXSDConfigXML("hazelcast-fullconfig.xml");
    }

    private void testXSDConfigXML(String xmlResource) throws SAXException, IOException {
        SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        URL schemaUrl = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-config-3.0.xsd");
        URL xmlURL = XMLConfigBuilderTest.class.getClassLoader().getResource(xmlResource);
        File schemaFile = new File(schemaUrl.getFile());
        File defaultXML = new File(xmlURL.getFile());
        Schema schema = factory.newSchema(schemaFile);
        Validator validator = schema.newValidator();
        Source source = new StreamSource(defaultXML);
        try {
            validator.validate(source);
        } catch (SAXException ex) {
            fail(defaultXML + " is not valid because: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
