/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.config;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static com.hazelcast.config.XmlElements.HAZELCAST;
import static com.hazelcast.config.XmlElements.IMPORT;

/**
 * Contains logic for replacing system variables in the XML file and importing XML files from different locations.
 */
public class XmlConfigPreProcessor {

    private static final ILogger LOGGER = Logger.getLogger(XmlConfigPreProcessor.class);

    private final XmlConfigBuilder xmlConfigBuilder;
    private Set<String> currentlyImportedFiles = new HashSet<String>();
    private XPathFactory xpathFactory = XPathFactory.newInstance();
    private XPath xpath = xpathFactory.newXPath();

    public XmlConfigPreProcessor(XmlConfigBuilder xmlConfigBuilder) {
        this.xmlConfigBuilder = xmlConfigBuilder;
    }


    void process(Node root) throws Exception {
        traverseChildsAndReplaceVariables(root);
        replaceImportStatementsWithActualFileContents(root);
    }

    private void replaceImportStatementsWithActualFileContents(Node root) throws Exception {
        Document document = root.getOwnerDocument();
        NodeList misplacedImports = (NodeList) xpath.evaluate("//" + IMPORT.name
                        + "/parent::*[not(name()=\"" + HAZELCAST.name + "\")]", document,
                XPathConstants.NODESET);
        if (misplacedImports.getLength() > 0) {
            throw new IllegalStateException("<import> element can appear only in the top level of the XML");
        }
        NodeList importTags = (NodeList) xpath.evaluate("/" + HAZELCAST.name + "/" + IMPORT.name, document,
                XPathConstants.NODESET);
        for (Node node : new AbstractXmlConfigHelper.IterableNodeList(importTags)) {
            NamedNodeMap attributes = node.getAttributes();
            Node resourceAtrribute = attributes.getNamedItem("resource");
            String resource = resourceAtrribute.getTextContent();
            URL url = ConfigLoader.locateConfig(resource);
            if (url == null) {
                throw new HazelcastException("Failed to load resource : " + resource);
            }
            if (!currentlyImportedFiles.add(url.getPath())) {
                throw new HazelcastException("Cyclic loading of resource " + url.getPath() + " is detected !");
            }
            Document doc = xmlConfigBuilder.parse(url.openStream());
            Element importedRoot = doc.getDocumentElement();
            traverseChildsAndReplaceVariables(importedRoot);
            replaceImportStatementsWithActualFileContents(importedRoot);
            for (Node fromImportedDoc : new AbstractXmlConfigHelper.IterableNodeList(importedRoot.getChildNodes())) {
                Node importedNode = root.getOwnerDocument().importNode(fromImportedDoc, true);
                root.insertBefore(importedNode, node);
            }
            root.removeChild(node);
        }
    }

    private void traverseChildsAndReplaceVariables(Node root) throws XPathExpressionException {
        NodeList misplacedHazelcastTag = (NodeList) xpath.evaluate("//" + HAZELCAST.name, root.getOwnerDocument(),
                XPathConstants.NODESET);
        if (misplacedHazelcastTag.getLength() > 1) {
            throw new IllegalStateException("<hazelcast> element can appear only once in the XML");
        }
        NamedNodeMap attributes = root.getAttributes();
        if (attributes != null) {
            for (int k = 0; k < attributes.getLength(); k++) {
                Node attribute = attributes.item(k);
                replaceVariables(attribute);
            }
        }
        if (root.getNodeValue() != null) {
            replaceVariables(root);
        }
        final NodeList childNodes = root.getChildNodes();
        for (int k = 0; k < childNodes.getLength(); k++) {
            Node child = childNodes.item(k);
            traverseChildsAndReplaceVariables(child);
        }
    }

    private void replaceVariables(Node node) {
        String value = node.getNodeValue();
        StringBuilder sb = new StringBuilder();
        int endIndex = -1;
        int startIndex = value.indexOf("${");
        while (startIndex > -1) {
            endIndex = value.indexOf('}', startIndex);
            if (endIndex == -1) {
                LOGGER.warning("Bad variable syntax. Could not find a closing curly bracket '}' on node: "
                        + node.getLocalName());
                break;
            }
            String variable = value.substring(startIndex + 2, endIndex);
            String variableReplacement = xmlConfigBuilder.getProperties().getProperty(variable);
            if (variableReplacement != null) {
                sb.append(variableReplacement);
            } else {
                sb.append(value.substring(startIndex, endIndex + 1));
                LOGGER.warning("Could not find a value for property  '" + variable + "' on node: "
                        + node.getLocalName());
            }
            startIndex = value.indexOf("${", endIndex);
        }
        sb.append(value.substring(endIndex + 1));
        node.setNodeValue(sb.toString());
    }
}
