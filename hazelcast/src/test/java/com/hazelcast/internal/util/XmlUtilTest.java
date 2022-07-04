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

package com.hazelcast.internal.util;

import static com.hazelcast.internal.util.XmlUtil.SYSTEM_PROPERTY_IGNORE_XXE_PROTECTION_FAILURES;
import static com.hazelcast.internal.util.XmlUtil.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerFactory;
import javax.xml.validation.SchemaFactory;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.xml.sax.HandlerBase;
import org.xml.sax.SAXException;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category({ QuickTest.class })
public class XmlUtilTest {

    @Rule
    public OverridePropertyRule ignoreXxeFailureProp = OverridePropertyRule
            .clear(SYSTEM_PROPERTY_IGNORE_XXE_PROTECTION_FAILURES);

    private DummyServer server;

    @Before
    public void before() throws IOException {
        server = new DummyServer();
        server.start();
    }

    @After
    public void after() {
        server.stop();
    }

    @Test
    public void testUnprotectedXxe() throws Exception {
        DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        try {
            db.parse(new ByteArrayInputStream(server.getTestXml().getBytes(UTF_8)));
        } catch (Exception e) {
            // not important if it fails
        }
        assertThat(server.getHits(), Matchers.greaterThan(0));
    }

    @Test
    public void testFormat() throws Exception {
        assertEquals("<a> <b>c</b></a>", format("<a><b>c</b></a>", 1).replaceAll("[\r\n]", ""));
        assertEquals("<a>   <b>c</b></a>", format("<a><b>c</b></a>", 3).replaceAll("[\r\n]", ""));
        assertEquals("<a><b>c</b></a>", format("<a><b>c</b></a>", -21));

        assertThrows(IllegalArgumentException.class, () -> format("<a><b>c</b></a>", 0));

        // check if the XXE protection is enabled
        String xml = server.getTestXml();
        assertEquals(xml, format(xml, 1));
        assertEquals(0, server.getHits());

        // wrongly formatted XML
        assertEquals("<a><b>c</b><a>", format("<a><b>c</b><a>", 1));
    }

    @Test
    public void testGetSchemaFactory() throws Exception {
        SchemaFactory schemaFactory = XmlUtil.getSchemaFactory();
        assertNotNull(schemaFactory);
        assertThrows(SAXException.class, () -> XmlUtil.setProperty(schemaFactory, "test://no-such-property"));
        ignoreXxeFailureProp.setOrClearProperty("false");
        assertThrows(SAXException.class, () -> XmlUtil.setProperty(schemaFactory, "test://no-such-property"));
        ignoreXxeFailureProp.setOrClearProperty("true");
        XmlUtil.setProperty(schemaFactory, "test://no-such-property");
    }

    @Test
    public void testGetTransformerFactory() throws Exception {
        TransformerFactory transformerFactory = XmlUtil.getTransformerFactory();
        assertNotNull(transformerFactory);
        assertThrows(IllegalArgumentException.class, () -> XmlUtil.setAttribute(transformerFactory, "test://no-such-property"));
        ignoreXxeFailureProp.setOrClearProperty("false");
        assertThrows(IllegalArgumentException.class, () -> XmlUtil.setAttribute(transformerFactory, "test://no-such-property"));
        ignoreXxeFailureProp.setOrClearProperty("true");
        XmlUtil.setAttribute(transformerFactory, "test://no-such-property");
    }

    @Test
    public void testGetDocumentBuilderFactory() throws Exception {
        DocumentBuilderFactory dbf = XmlUtil.getNsAwareDocumentBuilderFactory();
        assertNotNull(dbf);
        assertThrows(ParserConfigurationException.class, () -> XmlUtil.setFeature(dbf, "test://no-such-feature"));
        ignoreXxeFailureProp.setOrClearProperty("false");
        assertThrows(ParserConfigurationException.class, () -> XmlUtil.setFeature(dbf, "test://no-such-feature"));
        ignoreXxeFailureProp.setOrClearProperty("true");
        XmlUtil.setFeature(dbf, "test://no-such-feature");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetSAXParserFactory() throws Exception {
        SAXParserFactory saxParserFactory = XmlUtil.getSAXParserFactory();
        assertNotNull(saxParserFactory);
        // check if the XXE protection is enabled
        SAXParser saxParser = saxParserFactory.newSAXParser();
        assertThrows(SAXException.class,
                () -> saxParser.parse(new ByteArrayInputStream(server.getTestXml().getBytes(UTF_8)), new HandlerBase()));
        assertEquals(0, server.getHits());

        assertThrows(SAXException.class, () -> XmlUtil.setFeature(saxParserFactory, "test://no-such-feature"));
        ignoreXxeFailureProp.setOrClearProperty("false");
        assertThrows(SAXException.class, () -> XmlUtil.setFeature(saxParserFactory, "test://no-such-feature"));
        ignoreXxeFailureProp.setOrClearProperty("true");
        XmlUtil.setFeature(saxParserFactory, "test://no-such-feature");
    }

    @Test
    public void testGetXmlInputFactory() throws Exception {
        XMLInputFactory xmlInputFactory = XmlUtil.getXMLInputFactory();
        assertNotNull(xmlInputFactory);
        // check if the XXE protection is enabled
        assertThrows(XMLStreamException.class,
                () -> staxReadEvents(xmlInputFactory.createXMLEventReader(new StringReader(server.getTestXml()))));
        assertEquals(0, server.getHits());

        assertThrows(IllegalArgumentException.class,
                () -> XmlUtil.setProperty(xmlInputFactory, "test://no-such-property", false));
        ignoreXxeFailureProp.setOrClearProperty("false");
        assertThrows(IllegalArgumentException.class,
                () -> XmlUtil.setProperty(xmlInputFactory, "test://no-such-property", false));
        ignoreXxeFailureProp.setOrClearProperty("true");
        XmlUtil.setProperty(xmlInputFactory, "test://no-such-feature", false);
    }

    private void staxReadEvents(XMLEventReader reader) throws XMLStreamException {
        try {
            while (reader.hasNext()) {
                reader.nextEvent();
            }
        } finally {
            reader.close();
        }
    }

    static class DummyServer implements Runnable {
        private static final String XXE_TEST_STR_TEMPLATE = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                + "  <!DOCTYPE test [\n" + "    <!ENTITY xxe SYSTEM \"%s\">\n" + "  ]>" + "<a><b>&xxe;</b></a>";

        private final ServerSocket serverSocket;
        private final AtomicInteger counter = new AtomicInteger();

        DummyServer() throws IOException {
            serverSocket = new ServerSocket(0, 5, InetAddress.getLoopbackAddress());
        }

        public void start() {
            new Thread(this, "DummyServer-acceptor").start();
        }

        public String getUrlString() {
            return "http://127.0.0.1:" + serverSocket.getLocalPort();
        }

        public String getTestXml() {
            return String.format(XXE_TEST_STR_TEMPLATE, getUrlString());
        }

        public int getHits() {
            return counter.get();
        }

        public void stop() {
            try {
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            while (true) {
                try (Socket socket = serverSocket.accept()) {
                    System.out.println(">> connection accepted: " + socket);
                    counter.incrementAndGet();
                } catch (Exception e) {
                    System.out.println(">> stopping the server. Cause: " + e.getClass().getName());
                    return;
                }
            }
        }
    }
}
