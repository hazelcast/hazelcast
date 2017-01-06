/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.AbstractConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.jet.EdgeConfig;
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
import java.util.Properties;

import static com.hazelcast.jet.impl.config.XmlJetConfigLocator.getClientConfigStream;
import static com.hazelcast.jet.impl.config.XmlJetConfigLocator.getJetConfigStream;
import static com.hazelcast.jet.impl.config.XmlJetConfigLocator.getMemberConfigStream;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * Loads the {@link JetConfig} using XML.
 */
public class XmlJetConfigBuilder extends AbstractConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(XmlJetConfigBuilder.class);

    private final Properties properties;

    private final JetConfig jetConfig = new JetConfig();

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
    XmlJetConfigBuilder(Properties properties, InputStream in) {
        this.properties = properties;
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
            String name = cleanNodeName(node);
            switch (name) {
                case "execution-thread-count":
                    jetConfig.setExecutionThreadCount(getIntValue(node));
                    break;
                case "resource-directory":
                    jetConfig.setResourceDirectory(getStringValue(node));
                    break;
                case "flow-control-period":
                    jetConfig.setFlowControlPeriodMs(getIntValue(node));
                    break;
                case "properties":
                    fillProperties(node, jetConfig.getProperties());
                    break;
                case "edge-defaults":
                    jetConfig.setDefaultEdgeConfig(parseEdgeDefaults(node));
                    break;
                default:
                    break;
            }
        }
    }

    private EdgeConfig parseEdgeDefaults(Node edgeNode) {
        EdgeConfig config = new EdgeConfig();
        for (Node child : childElements(edgeNode)) {
            String name = cleanNodeName(child);
            switch (name) {
                case "queue-size":
                    config.setQueueSize(getIntValue(child));
                    break;
                case "packet-size-limit":
                    config.setPacketSizeLimit(getIntValue(child));
                    break;
                case "high-water-mark":
                    config.setHighWaterMark(getIntValue(child));
                    break;
                case "receive-window-multiplier":
                    config.setReceiveWindowMultiplier(getIntValue(child));
                    break;
                default:
                    break;
            }
        }
        return config;
    }

    private int getIntValue(Node node) {
        return Integer.parseInt(getStringValue(node));
    }

    private String getStringValue(Node node) {
        return getTextContent(node);
    }

    static JetConfig getConfig(InputStream in, Properties properties) {
        JetConfig jetConfig = new XmlJetConfigBuilder(properties, in).getJetConfig();
        jetConfig.setHazelcastConfig(getMemberConfig(properties));
        return jetConfig;
    }

    public static JetConfig getConfig(Properties properties) {
        InputStream in = getJetConfigStream(properties);
        return getConfig(in, properties);
    }

    public static JetConfig getConfig() {
        Properties properties = System.getProperties();
        return getConfig(properties);
    }

    public static ClientConfig getClientConfig() {
        return getClientConfig(System.getProperties());
    }

    public static ClientConfig getClientConfig(Properties properties) {
        return new XmlClientConfigBuilder(getClientConfigStream(properties)).build();
    }

    public static Config getMemberConfig(Properties properties) {
        return new XmlConfigBuilder(getMemberConfigStream(properties)).build();
    }
}
