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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class IterableNodeListTest {

    private Document document;

    @Before
    public void setupNodeList() throws ParserConfigurationException, SAXException, IOException {
        final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        final String testXml = "<root><element></element><element></element><element></element></root>";
        document = builder.parse(new ByteArrayInputStream(testXml.getBytes()));
    }

    /**
     * Test method for {@link com.hazelcast.config.XmlConfigBuilder.IterableNodeList#IterableNodeList(org.w3c.dom.NodeList)}.
     */
    @Test
    public void testIterableNodeList() {
        NodeList nodeList = document.getFirstChild().getChildNodes();
        int count = 0;
        for (Node node : new XmlConfigBuilder.IterableNodeList(nodeList)) {
            count += (node != null) ? 1 : 0;
        }
        assertEquals(3, count);
    }

    @Test
    public void testHasNext() {
        NodeList nodeList = document.getChildNodes();
        assertTrue(new XmlConfigBuilder.IterableNodeList(nodeList).iterator().hasNext());
    }

    @Test
    public void testNext() {
        NodeList nodeList = document.getChildNodes();
        assertNotNull(new XmlConfigBuilder.IterableNodeList(nodeList).iterator().next());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        NodeList nodeList = document.getChildNodes();
        new XmlConfigBuilder.IterableNodeList(nodeList).iterator().remove();
    }
}
