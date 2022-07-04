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

package com.hazelcast.aws;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Helper class for parsing XML strings
 */
final class XmlNode {
    private Node node;

    private XmlNode(Node node) {
        this.node = node;
    }

    static XmlNode create(String xmlString) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            Document doc = dbf.newDocumentBuilder().parse(new ByteArrayInputStream(xmlString.getBytes(UTF_8)));
            return new XmlNode(doc.getDocumentElement());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    Node getNode() {
        return node;
    }

    List<XmlNode> getSubNodes(String name) {
        return StreamSupport.stream(childElements(node).spliterator(), false)
            .filter(e -> name.equals(cleanNodeName(e)))
            .map(XmlNode::new)
            .collect(Collectors.toList());
    }

    String getValue(String name) {
        return getSubNodes(name).stream()
            .map(XmlNode::getNode)
            .map(Node::getFirstChild)
            .map(Node::getNodeValue)
            .findFirst()
            .orElse(null);
    }
}
