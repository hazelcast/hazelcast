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

package com.hazelcast.internal.config.yaml;

import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.InputStream;

import static com.hazelcast.internal.config.yaml.EmptyNamedNodeMap.emptyNamedNodeMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * A modified copy of the of the main test case of {@link YamlTest} that
 * verifies the behavior of the W3C DOM adapters' supported methods.
 * <p>
 * These tests utilize that we work with the node adapters with which we
 * can access the mapping via getAttributes().
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class W3cDomTest extends HazelcastTestSupport {
    private static final int NOT_EXISTING = 42;

    @Test
    public void testW3cDomAdapter() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map-extended.yaml");
        YamlNode yamlRoot = YamlLoader.load(inputStream, "root-map");
        Node domRoot = W3cDomUtil.asW3cNode(yamlRoot);

        NamedNodeMap rootAttributes = domRoot.getAttributes();
        Node embeddedMap = rootAttributes.getNamedItem("embedded-map");
        NamedNodeMap embeddedMapAttributes = embeddedMap.getAttributes();
        String scalarString = embeddedMapAttributes.getNamedItem("scalar-str").getTextContent();
        int scalarInt = Integer.parseInt(embeddedMapAttributes.getNamedItem("scalar-int").getTextContent());
        double scalarDouble = Double.parseDouble(embeddedMapAttributes.getNamedItem("scalar-double").getTextContent());
        boolean scalarBool = Boolean.parseBoolean(embeddedMapAttributes.getNamedItem("scalar-bool").getTextContent());

        Node embeddedListNode = embeddedMapAttributes.getNamedItem("embedded-list");
        NodeList embeddedList = embeddedListNode.getChildNodes();
        String elItem0 = embeddedList.item(0).getTextContent();
        Node elItem0AsNode = embeddedList.item(0);
        int elItem1 = Integer.parseInt(embeddedList.item(1).getTextContent());
        double elItem2 = Double.parseDouble(embeddedList.item(2).getTextContent());
        boolean elItem3 = Boolean.parseBoolean(embeddedList.item(3).getTextContent());

        NodeList embeddedList2 = embeddedMapAttributes.getNamedItem("embedded-list2").getChildNodes();
        String el2Item0 = embeddedList2.item(0).getTextContent();
        double el2Item1 = Double.parseDouble(embeddedList2.item(1).getTextContent());

        String keysValue = embeddedList.item(4).getAttributes().getNamedItem("key").getTextContent();
        String multilineStr = domRoot.getAttributes().getNamedItem("multiline-str").getTextContent();

        assertEquals("embedded-map", embeddedMap.getNodeName());
        assertEquals("scalar-str", embeddedMap.getChildNodes().item(0).getNodeName());
        assertNull(embeddedMap.getChildNodes().item(NOT_EXISTING));
        assertNull(embeddedMap.getTextContent());
        assertEquals("embedded-list", embeddedMapAttributes.item(4).getNodeName());
        assertEquals("embedded-list", embeddedListNode.getNodeName());
        assertEquals("embedded-map", embeddedListNode.getParentNode().getNodeName());
        assertSame(emptyNamedNodeMap(), embeddedListNode.getAttributes());

        assertEquals(6, embeddedMap.getAttributes().getLength());

        // root-map/embedded-map/scalars
        assertEquals(6, embeddedMap.getChildNodes().getLength());
        assertEquals("h4z3lc4st", scalarString);
        assertEquals(123, scalarInt);
        assertEquals(123.12312D, scalarDouble, 10E-5);
        assertTrue(scalarBool);

        // root-map/embedded-map/embedded-list
        assertEquals("value1", elItem0);
        assertEquals(NOT_EXISTING, elItem1);
        assertEquals(42.42D, elItem2, 10E-2);
        assertFalse(elItem3);
        NodeList elItem0ChildNodes = elItem0AsNode.getChildNodes();
        assertEquals(1, elItem0ChildNodes.getLength());
        assertEquals("value1", elItem0ChildNodes.item(0).getNodeValue());
        assertEquals("value1", elItem0AsNode.getNodeValue());

        // root-map/embedded-map/embedded-list2
        assertEquals(2, embeddedList2.getLength());
        assertEquals("value2", el2Item0);
        assertEquals(1D, el2Item1, 10E-1);

        assertEquals("value", keysValue);

        assertEquals("Hazelcast IMDG\n"
                + "The Leading Open Source In-Memory Data Grid:\n"
                + "Distributed Computing, Simplified.\n", multilineStr);

        Element embeddedMapAsElement = (Element) embeddedMap;
        String scalarStrAttr = embeddedMapAsElement.getAttribute("scalar-str");
        assertEquals("h4z3lc4st", scalarStrAttr);
        assertEquals("embedded-map", embeddedMapAsElement.getTagName());
        assertTrue(embeddedMapAsElement.hasAttribute("scalar-str"));

        assertEquals("", embeddedMapAsElement.getAttribute("non-existing"));

        NodeList nodesByTagName = embeddedMapAsElement.getElementsByTagName("scalar-str");
        assertEquals(1, nodesByTagName.getLength());
        assertEquals("h4z3lc4st", nodesByTagName.item(0).getNodeValue());

    }
}
