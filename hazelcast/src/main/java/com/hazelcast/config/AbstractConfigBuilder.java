/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.replacer.PropertyReplacer;
import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.XmlElements.CONFIG_REPLACERS;
import static com.hazelcast.config.XmlElements.IMPORT;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;

/**
 * Contains logic for replacing system variables in the XML file and importing XML files from different locations.
 */
public abstract class AbstractConfigBuilder extends AbstractXmlConfigHelper {

    protected enum ConfigType {
        SERVER("hazelcast"),
        CLIENT("hazelcast-client"),
        JET("hazelcast-jet");

        final String name;

        ConfigType(String name) {
            this.name = name;
        }
    }

    private static final ILogger LOGGER = Logger.getLogger(AbstractConfigBuilder.class);

    private final Set<String> currentlyImportedFiles = new HashSet<String>();
    private final XPath xpath;

    public AbstractConfigBuilder() {
        XPathFactory fac = XPathFactory.newInstance();
        this.xpath = fac.newXPath();

        xpath.setNamespaceContext(new NamespaceContext() {
            @Override
            public String getNamespaceURI(String prefix) {
                return "hz".equals(prefix) ? xmlns : null;
            }

            @Override
            public String getPrefix(String namespaceURI) {
                return null;
            }

            @Override
            public Iterator getPrefixes(String namespaceURI) {
                return null;
            }
        });
    }

    protected void process(Node root) throws Exception {
        traverseChildrenAndReplaceVariables(root);
        replaceImportElementsWithActualFileContents(root);
    }

    private void replaceImportElementsWithActualFileContents(Node root) throws Exception {
        Document document = root.getOwnerDocument();
        NodeList misplacedImports = (NodeList) xpath.evaluate(
                format("//hz:%s/parent::*[not(self::hz:%s)]", IMPORT.name, getXmlType().name), document, XPathConstants.NODESET);
        if (misplacedImports.getLength() > 0) {
            throw new InvalidConfigurationException("<import> element can appear only in the top level of the XML");
        }
        NodeList importTags = (NodeList) xpath.evaluate(
                format("/hz:%s/hz:%s", getXmlType().name, IMPORT.name), document, XPathConstants.NODESET);
        for (Node node : asElementIterable(importTags)) {
            loadAndReplaceImportElement(root, node);
        }
    }

    private void loadAndReplaceImportElement(Node root, Node node) throws Exception {
        NamedNodeMap attributes = node.getAttributes();
        Node resourceAttribute = attributes.getNamedItem("resource");
        String resource = resourceAttribute.getTextContent();
        URL url = ConfigLoader.locateConfig(resource);
        if (url == null) {
            throw new InvalidConfigurationException("Failed to load resource: " + resource);
        }
        if (!currentlyImportedFiles.add(url.getPath())) {
            throw new InvalidConfigurationException("Cyclic loading of resource '" + url.getPath() + "' detected!");
        }
        Document doc = parse(url.openStream());
        Element importedRoot = doc.getDocumentElement();
        traverseChildrenAndReplaceVariables(importedRoot);
        replaceImportElementsWithActualFileContents(importedRoot);
        for (Node fromImportedDoc : childElements(importedRoot)) {
            Node importedNode = root.getOwnerDocument().importNode(fromImportedDoc, true);
            root.insertBefore(importedNode, node);
        }
        root.removeChild(node);
    }

    /**
     * Reads XML from InputStream and parses.
     *
     * @param inputStream {@link InputStream} to read from
     * @return Document after parsing XML
     * @throws Exception if the XML configuration cannot be parsed or is invalid
     */
    protected abstract Document parse(InputStream inputStream) throws Exception;

    /**
     * @return system properties
     */
    protected abstract Properties getProperties();

    /**
     * @return ConfigType of current config class as enum value
     */
    @Override
    protected abstract ConfigType getXmlType();

    private void traverseChildrenAndReplaceVariables(Node root) throws Exception {
        // if no config-replacer is defined, use backward compatible default behavior for missing properties
        boolean failFast = false;

        List<ConfigReplacer> replacers = new ArrayList<ConfigReplacer>();

        // Always use the Property replacer first.
        PropertyReplacer propertyReplacer = new PropertyReplacer();
        propertyReplacer.init(getProperties());
        replacers.add(propertyReplacer);

        // Add other replacers defined in the XML
        Node node = (Node) xpath.evaluate(format("/hz:%s/hz:%s", getXmlType().name, CONFIG_REPLACERS.name), root,
                XPathConstants.NODE);
        if (node != null) {
            String failFastAttr = getAttribute(node, "fail-if-value-missing");
            failFast = isNullOrEmpty(failFastAttr) ? true : Boolean.parseBoolean(failFastAttr);
            for (Node n : childElements(node)) {
                String value = cleanNodeName(n);
                if ("replacer".equals(value)) {
                    replacers.add(createReplacer(n));
                }
            }
        }

        // Use all the replacers on the XML content
        for (ConfigReplacer replacer : replacers) {
            traverseChildrenAndReplaceVariables(root, replacer, failFast);
        }
    }

    private ConfigReplacer createReplacer(Node node) throws Exception {
        String replacerClass = getAttribute(node, "class-name");
        Properties properties = new Properties();
        for (Node n : childElements(node)) {
            String value = cleanNodeName(n);
            if ("properties".equals(value)) {
                fillProperties(n, properties);
            }
        }
        ConfigReplacer replacer = (ConfigReplacer) Class.forName(replacerClass).newInstance();
        replacer.init(properties);
        return replacer;
    }

    private void traverseChildrenAndReplaceVariables(Node root, ConfigReplacer replacer, boolean failFast) {
        NamedNodeMap attributes = root.getAttributes();
        if (attributes != null) {
            for (int k = 0; k < attributes.getLength(); k++) {
                Node attribute = attributes.item(k);
                replaceVariables(attribute, replacer, failFast);
            }
        }
        if (root.getNodeValue() != null) {
            replaceVariables(root, replacer, failFast);
        }
        final NodeList childNodes = root.getChildNodes();
        for (int k = 0; k < childNodes.getLength(); k++) {
            Node child = childNodes.item(k);
            traverseChildrenAndReplaceVariables(child, replacer, failFast);
        }
    }

    private void replaceVariables(Node node, ConfigReplacer replacer, boolean failFast) {
        String value = node.getNodeValue();
        StringBuilder sb = new StringBuilder(value);
        String replacerPrefix = "$" + replacer.getPrefix() + "{";
        int endIndex = -1;
        int startIndex = sb.indexOf(replacerPrefix);
        while (startIndex > -1) {
            endIndex = sb.indexOf("}", startIndex);
            if (endIndex == -1) {
                LOGGER.warning("Bad variable syntax. Could not find a closing curly bracket '}' for prefix " + replacerPrefix
                        + " on node: " + node.getLocalName());
                break;
            }

            String variable = sb.substring(startIndex + replacerPrefix.length(), endIndex);
            String variableReplacement = replacer.getReplacement(variable);
            if (variableReplacement != null) {
                sb.replace(startIndex, endIndex + 1, variableReplacement);
                endIndex = startIndex + variableReplacement.length();
            } else {
                handleMissingVariable(sb.substring(startIndex, endIndex + 1), node.getLocalName(), failFast);
            }
            startIndex = sb.indexOf(replacerPrefix, endIndex);
        }
        node.setNodeValue(sb.toString());
    }

    private void handleMissingVariable(String variable, String nodeName, boolean failFast) throws ConfigurationException {
        String message = format("Could not find a replacement for '%s' on node '%s'", variable, nodeName);
        if (failFast) {
            throw new ConfigurationException(message);
        }
        LOGGER.warning(message);
    }
}
