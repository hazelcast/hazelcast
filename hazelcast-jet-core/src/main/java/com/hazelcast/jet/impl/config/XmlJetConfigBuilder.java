/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.config;

import com.hazelcast.config.AbstractConfigBuilder;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.jet.JetConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.util.ExceptionUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.jet.impl.config.XmlJetConfigLocator.getConfigStream;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * Loads the {@link JetConfig} using XML.
 */
public class XmlJetConfigBuilder extends AbstractConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(XmlJetConfigBuilder.class);

    private final Set<String> occurrenceSet = new HashSet<>();

    private final Properties properties;

    private final JetConfig jetConfig = new JetConfig();

    public XmlJetConfigBuilder() {
        this(System.getProperties());
    }

    /**
     * Loads the jet config using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hazelcast.jet.config' is set. If it exist and it begins with
     * 'classpath:', then a classpath resource is loaded. Else it will assume it is a file reference</li>
     * <li>it checks if a hazelcast-jet.xml is available in the working dir</li>
     * <li>it checks if a hazelcast-jet.xml is available on the classpath</li>
     * <li>it loads the hazelcast-jet-default.xml</li>
     * </ol>
     */
    public XmlJetConfigBuilder(Properties properties) {
        this.properties = properties;
        InputStream in = getConfigStream(properties);
        try {
            parseAndBuildConfig(in);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    public JetConfig getJetConfig() {
        return jetConfig;
    }

    @Override
    protected Document parse(InputStream inputStream) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        DocumentBuilder builder = dbf.newDocumentBuilder();
        try {
            return builder.parse(inputStream);
        } catch (Exception e) {
            String msg = "Failed to parse config"
                    + LINE_SEPARATOR + "Exception: " + e.getMessage()
                    + LINE_SEPARATOR + "Hazelcast Jet startup interrupted.";
            LOGGER.severe(msg);
            throw new InvalidConfigurationException(e.getMessage(), e);
        } finally {
            IOUtil.closeResource(inputStream);
        }
    }

    @Override
    protected Properties getProperties() {
        return properties;
    }

    @Override
    protected ConfigType getXmlType() {
        return ConfigType.JET;
    }

    @Override
    protected String getReleaseVersion() {
        //TODO: replace with BuildInfoProvider
        return "0.3";
    }

    private void parseAndBuildConfig(InputStream in) throws Exception {
        Document doc = parse(in);
        Element root = doc.getDocumentElement();
        try {
            root.getTextContent();
        } catch (Throwable e) {
            domLevel3 = false;
        }
        process(root);
        schemaValidation(root.getOwnerDocument());
        handleConfig(root);
    }

    private void handleConfig(Element docElement) throws Exception {
        for (Node node : childElements(docElement)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException("Duplicate '" + nodeName + "' definition found in XML configuration. ");
            }
            handleXmlNode(node, nodeName);
            if (!JetXmlElements.canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }
    }

    private void handleXmlNode(Node node, String nodeName) throws Exception {
        if (JetXmlElements.EXECUTION_THREAD_COUNT.isEqual(nodeName)) {
            jetConfig.setExecutionThreadCount(Integer.parseInt(getTextContent(node)));
        } else if (JetXmlElements.RESOURCE_DIRECTORY.isEqual(nodeName)) {
            jetConfig.setResourceDirectory(getTextContent(node));
        }
        if (JetXmlElements.PROPERTIES.isEqual(nodeName)) {
            fillProperties(node, jetConfig.getProperties());
        }
    }

    private enum JetXmlElements {
        HAZELCAST_JET("hazelcast-jet", false),
        EXECUTION_THREAD_COUNT("execution-thread-count", false),
        RESOURCE_DIRECTORY("resource-directory", false),
        PROPERTIES("properties", false);

        final String name;
        final boolean multipleOccurrence;

        JetXmlElements(String name, boolean multipleOccurrence) {
            this.name = name;
            this.multipleOccurrence = multipleOccurrence;
        }

        public static boolean canOccurMultipleTimes(String name) {
            for (JetXmlElements element : values()) {
                if (name.equals(element.name)) {
                    return element.multipleOccurrence;
                }
            }
            return true;
        }

        public boolean isEqual(String name) {
            return this.name.equals(name);
        }

    }
}
