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

package com.hazelcast.config;

import com.hazelcast.config.replacer.PropertyReplacer;
import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.internal.config.ConfigLoader;
import com.hazelcast.internal.config.ConfigReplacerHelper;
import com.hazelcast.internal.config.DomConfigHelper;
import com.hazelcast.internal.config.XmlDomVariableReplacer;
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

import static com.hazelcast.internal.config.ConfigSections.CONFIG_REPLACERS;
import static com.hazelcast.internal.config.ConfigSections.IMPORT;
import static com.hazelcast.internal.config.DomConfigHelper.asElementIterable;
import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;

/**
 * Contains logic for replacing system variables in the XML file and importing XML files from different locations.
 */
public abstract class AbstractXmlConfigBuilder extends AbstractXmlConfigHelper {

    private Properties properties = System.getProperties();

    protected enum ConfigType {
        SERVER("hazelcast"),
        CLIENT("hazelcast-client"),
        JET("hazelcast-jet"),
        CLIENT_FAILOVER("hazelcast-client-failover");

        final String name;

        ConfigType(String name) {
            this.name = name;
        }
    }


    private final Set<String> currentlyImportedFiles = new HashSet<>();
    private final XPath xpath;

    public AbstractXmlConfigBuilder() {
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
        replaceImports(root);
        replaceVariables(root);
    }

    private void replaceVariables(Node root) throws Exception {
        // if no config-replacer is defined, use backward compatible default behavior for missing properties
        boolean failFast = false;

        List<ConfigReplacer> replacers = new ArrayList<>();

        // Always use the Property replacer first.
        PropertyReplacer propertyReplacer = new PropertyReplacer();
        propertyReplacer.init(getProperties());
        replacers.add(propertyReplacer);

        // Add other replacers defined in the XML
        Node node = (Node) xpath.evaluate(format("/hz:%s/hz:%s", getConfigType().name, CONFIG_REPLACERS.getName()), root,
                                          XPathConstants.NODE);
        if (node != null) {
            String failFastAttr = getAttribute(node, "fail-if-value-missing");
            failFast = isNullOrEmpty(failFastAttr) || Boolean.parseBoolean(failFastAttr);
            for (Node n : childElements(node)) {
                String value = cleanNodeName(n);
                if ("replacer".equals(value)) {
                    replacers.add(createReplacer(n));
                }
            }
        }
        ConfigReplacerHelper.traverseChildrenAndReplaceVariables(root, replacers, failFast, new XmlDomVariableReplacer());
    }

    private void replaceImports(Node root) throws Exception {
        replaceVariables(root);
        Document document = root.getOwnerDocument();
        NodeList misplacedImports = (NodeList) xpath.evaluate(
            format("//hz:%s/parent::*[not(self::hz:%s)]", IMPORT.getName(), getConfigType().name), document,
            XPathConstants.NODESET);
        if (misplacedImports.getLength() > 0) {
            throw new InvalidConfigurationException("<import> element can appear only in the top level of the XML");
        }
        NodeList importTags = (NodeList) xpath.evaluate(
            format("/hz:%s/hz:%s", getConfigType().name, IMPORT.getName()), document, XPathConstants.NODESET);
        for (Node node : asElementIterable(importTags)) {
            NamedNodeMap attributes = node.getAttributes();
            Node resourceAttribute = attributes.getNamedItem("resource");
            String resource = resourceAttribute.getTextContent();
            URL url = ConfigLoader.locateConfig(resource);
            if (url == null) {
                throw new InvalidConfigurationException("Failed to load resource: " + resource);
            }
            if (!currentlyImportedFiles.add(url.getPath())) {
                throw new InvalidConfigurationException("Resource '" + url.getPath() + "' is already loaded! This can be due to"
                                                            + " duplicate or cyclic imports.");
            }
            Document doc = parse(url.openStream());
            Element importedRoot = doc.getDocumentElement();
            replaceImports(importedRoot);
            for (Node fromImportedDoc : childElements(importedRoot)) {
                Node importedNode = root.getOwnerDocument().importNode(fromImportedDoc, true);
                root.insertBefore(importedNode, node);
            }
            root.removeChild(node);
        }
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
     * Gets the current used properties. Can be null if no properties are set.
     *
     * @return the current used properties
     * @see #setPropertiesInternal(Properties)
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the used properties. Can be null if no properties should be used.
     * <p>
     * Properties are used to resolve ${variable} occurrences in the XML file.
     *
     * @param properties the new properties
     */
    protected void setPropertiesInternal(Properties properties) {
        this.properties = properties;
    }

    /**
     * @return ConfigType of current config class as enum value
     */
    @Override
    protected abstract ConfigType getConfigType();

    String getAttribute(Node node, String attName) {
        return DomConfigHelper.getAttribute(node, attName, domLevel3);
    }

    void fillProperties(Node node, Properties properties) {
        DomConfigHelper.fillProperties(node, properties, domLevel3);
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
}
