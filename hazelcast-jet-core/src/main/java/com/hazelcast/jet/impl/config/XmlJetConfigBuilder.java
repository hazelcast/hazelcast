/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.DomConfigHelper;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.MetricsConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.jet.impl.config.XmlJetConfigLocator.getClientConfigStream;
import static com.hazelcast.jet.impl.config.XmlJetConfigLocator.getJetConfigStream;
import static com.hazelcast.jet.impl.config.XmlJetConfigLocator.getMemberConfigStream;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * Loads the {@link JetConfig} using XML.
 */
public final class XmlJetConfigBuilder extends AbstractConfigBuilder {

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
    private XmlJetConfigBuilder(Properties properties, InputStream in) {
        this.properties = properties;
        try {
            parseAndBuildConfig(in);
        } catch (Exception e) {
            throw sneakyThrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    public static JetConfig loadConfig(@Nullable InputStream stream, @Nullable Properties properties) {
        if (properties == null) {
            properties = System.getProperties();
        }
        if (stream == null) {
            stream = getJetConfigStream(properties);
        }
        JetConfig cfg = new XmlJetConfigBuilder(properties, stream).jetConfig;
        cfg.setHazelcastConfig(getMemberConfig(properties));
        return cfg;
    }

    public static ClientConfig getClientConfig() {
        return getClientConfig(System.getProperties());
    }

    public static ClientConfig getClientConfig(Properties properties) {
        return new XmlClientConfigBuilder(getClientConfigStream(properties)).build();
    }

    private static Config getMemberConfig(Properties properties) {
        return new XmlConfigBuilder(getMemberConfigStream(properties)).build();
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

    @Override public String getNamespaceType() {
        return "jet-config";
    }

    @Override
    protected String getReleaseVersion() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        JetBuildInfo jetBuildInfo = buildInfo.getJetBuildInfo();
        return jetBuildInfo.getVersion().substring(0, 3);
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

    private void handleConfig(Element docElement) {
        for (Node node : childElements(docElement)) {
            String name = cleanNodeName(node);
            switch (name) {
                case "instance":
                    parseInstanceConfig(node);
                    break;
                case "properties":
                    fillProperties(node, jetConfig.getProperties());
                    break;
                case "edge-defaults":
                    parseEdgeDefaults(node);
                    break;
                case "metrics":
                    parseMetrics(node);
                    break;
                default:
                    throw new AssertionError("Unrecognized XML element: " + name);
            }
        }
    }

    private void parseInstanceConfig(Node instanceNode) {
        final InstanceConfig instanceConfig = jetConfig.getInstanceConfig();
        for (Node node : childElements(instanceNode)) {
            String name = cleanNodeName(node);
            switch (name) {
                case "cooperative-thread-count":
                    instanceConfig.setCooperativeThreadCount(intValue(node));
                    break;
                case "flow-control-period":
                    instanceConfig.setFlowControlPeriodMs(intValue(node));
                    break;
                case "backup-count":
                    instanceConfig.setBackupCount(intValue(node));
                    break;
                case "scale-up-delay-millis":
                    instanceConfig.setScaleUpDelayMillis(longValue(node));
                    break;
                default:
                    throw new AssertionError("Unrecognized XML element: " + name);
            }
        }
    }

    private void parseEdgeDefaults(Node edgeNode) {
        EdgeConfig config = jetConfig.getDefaultEdgeConfig();
        for (Node child : childElements(edgeNode)) {
            String name = cleanNodeName(child);
            switch (name) {
                case "queue-size":
                    config.setQueueSize(intValue(child));
                    break;
                case "packet-size-limit":
                    config.setPacketSizeLimit(intValue(child));
                    break;
                case "receive-window-multiplier":
                    config.setReceiveWindowMultiplier(intValue(child));
                    break;
                default:
                    throw new AssertionError("Unrecognized XML element: " + name);
            }
        }
    }

    private void parseMetrics(Node metricsNode) {
        MetricsConfig config = jetConfig.getMetricsConfig();

        getBooleanAttribute(metricsNode, "enabled").ifPresent(config::setEnabled);
        getBooleanAttribute(metricsNode, "jmxEnabled").ifPresent(config::setJmxEnabled);

        for (Node child : childElements(metricsNode)) {
            String name = cleanNodeName(child);
            switch (name) {
                case "retention-seconds":
                    config.setRetentionSeconds(intValue(child));
                    break;
                case "collection-interval-seconds":
                    config.setCollectionIntervalSeconds(intValue(child));
                    break;
                case "metrics-for-data-structures":
                    config.setMetricsForDataStructuresEnabled(booleanValue(child));
                    break;
                default:
                    throw new AssertionError("Unrecognized XML element: " + name);
            }
        }
    }

    private int intValue(Node node) {
        return Integer.parseInt(stringValue(node));
    }

    private int longValue(Node node) {
        return Integer.parseInt(stringValue(node));
    }

    private String stringValue(Node node) {
        return getTextContent(node);
    }

    private boolean booleanValue(Node node) {
        return Boolean.parseBoolean(getTextContent(node));
    }

    private Optional<Boolean> getBooleanAttribute(Node node, String name) {
        return Optional.ofNullable(node.getAttributes().getNamedItem(name)).map(this::booleanValue);
    }

    private String getTextContent(Node node) {
        return DomConfigHelper.getTextContent(node, domLevel3);
    }

    public void fillProperties(Node node, Properties properties) {
        DomConfigHelper.fillProperties(node, properties, domLevel3);
    }
}
