/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PartitionGroupConfig.MemberGroupType;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.spi.ServiceConfigurationParser;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.StringUtil;
import com.hazelcast.wan.impl.WanNoDelayReplication;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import static com.hazelcast.config.XmlElements.CACHE;
import static com.hazelcast.config.XmlElements.EXECUTOR_SERVICE;
import static com.hazelcast.config.XmlElements.GROUP;
import static com.hazelcast.config.XmlElements.IMPORT;
import static com.hazelcast.config.XmlElements.JOB_TRACKER;
import static com.hazelcast.config.XmlElements.LICENSE_KEY;
import static com.hazelcast.config.XmlElements.LIST;
import static com.hazelcast.config.XmlElements.LISTENERS;
import static com.hazelcast.config.XmlElements.MANAGEMENT_CENTER;
import static com.hazelcast.config.XmlElements.MAP;
import static com.hazelcast.config.XmlElements.MEMBER_ATTRIBUTES;
import static com.hazelcast.config.XmlElements.MULTIMAP;
import static com.hazelcast.config.XmlElements.NATIVE_MEMORY;
import static com.hazelcast.config.XmlElements.NETWORK;
import static com.hazelcast.config.XmlElements.PARTITION_GROUP;
import static com.hazelcast.config.XmlElements.PROPERTIES;
import static com.hazelcast.config.XmlElements.QUEUE;
import static com.hazelcast.config.XmlElements.QUORUM;
import static com.hazelcast.config.XmlElements.RELIABLE_TOPIC;
import static com.hazelcast.config.XmlElements.REPLICATED_MAP;
import static com.hazelcast.config.XmlElements.RINGBUFFER;
import static com.hazelcast.config.XmlElements.SECURITY;
import static com.hazelcast.config.XmlElements.SEMAPHORE;
import static com.hazelcast.config.XmlElements.SERIALIZATION;
import static com.hazelcast.config.XmlElements.SERVICES;
import static com.hazelcast.config.XmlElements.SET;
import static com.hazelcast.config.XmlElements.TOPIC;
import static com.hazelcast.config.XmlElements.WAN_REPLICATION;
import static com.hazelcast.config.XmlElements.canOccurMultipleTimes;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.util.StringUtil.upperCaseInternal;

/**
 * A XML {@link ConfigBuilder} implementation.
 */
public class XmlConfigBuilder extends AbstractConfigBuilder implements ConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(XmlConfigBuilder.class);

    private static final int DEFAULT_VALUE = 5;
    private static final int THOUSAND_FACTOR = 5;

    private Config config;
    private InputStream in;
    private File configurationFile;
    private URL configurationUrl;
    private Properties properties = System.getProperties();
    private Set<String> occurrenceSet = new HashSet<String>();
    private Element root;

    /**
     * Constructs a XmlConfigBuilder that reads from the provided XML file.
     *
     * @param xmlFileName the name of the XML file that the XmlConfigBuilder reads from
     * @throws FileNotFoundException if the file can't be found.
     */
    public XmlConfigBuilder(String xmlFileName) throws FileNotFoundException {
        this(new FileInputStream(xmlFileName));
        this.configurationFile = new File(xmlFileName);
    }

    /**
     * Constructs a XmlConfigBuilder that reads from the given InputStream.
     *
     * @param inputStream the InputStream containing the XML configuration.
     * @throws IllegalArgumentException if inputStream is null.
     */
    public XmlConfigBuilder(InputStream inputStream) {
        if (inputStream == null) {
            throw new IllegalArgumentException("inputStream can't be null");
        }
        this.in = inputStream;
    }

    /**
     * Constructs a XMLConfigBuilder that reads from the given URL.
     *
     * @param url the given url that the XMLConfigBuilder reads from
     * @throws IOException
     */
    public XmlConfigBuilder(URL url) throws IOException {
        checkNotNull(url, "URL is null!");
        in = url.openStream();
        this.configurationUrl = url;
    }

    /**
     * Constructs a XmlConfigBuilder that tries to find a usable XML configuration file.
     */
    public XmlConfigBuilder() {
        XmlConfigLocator locator = new XmlConfigLocator();
        this.in = locator.getIn();
        this.configurationFile = locator.getConfigurationFile();
        this.configurationUrl = locator.getConfigurationUrl();
    }

    /**
     * Gets the current used properties. Can be null if no properties are set.
     *
     * @return the current used properties.
     * @see #setProperties(java.util.Properties)
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the used properties. Can be null if no properties should be used.
     * <p/>
     * Properties are used to resolve ${variable} occurrences in the XML file.
     *
     * @param properties the new properties.
     * @return the XmlConfigBuilder
     */
    public XmlConfigBuilder setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    protected ConfigType getXmlType() {
        return ConfigType.SERVER;
    }

    @Override
    public Config build() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return build(config);
    }

    Config build(Config config) {
        config.setConfigurationFile(configurationFile);
        config.setConfigurationUrl(configurationUrl);
        try {
            parseAndBuildConfig(config);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return config;
    }

    private void parseAndBuildConfig(final Config config) throws Exception {
        this.config = config;
        Document doc = parse(in);
        root = doc.getDocumentElement();
        try {
            root.getTextContent();
        } catch (final Throwable e) {
            domLevel3 = false;
        }
        process(root);
        schemaValidation(root.getOwnerDocument());
        handleConfig(root);
    }

    @Override
    protected Document parse(InputStream is) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        final DocumentBuilder builder = dbf.newDocumentBuilder();
        Document doc;
        try {
            doc = builder.parse(is);
        } catch (final Exception e) {
            String lineSeparator = StringUtil.getLineSeperator();
            if (configurationFile != null) {
                String msg = "Failed to parse " + configurationFile
                        + lineSeparator + "Exception: " + e.getMessage()
                        + lineSeparator + "Hazelcast startup interrupted.";
                LOGGER.severe(msg);

            } else if (configurationUrl != null) {
                String msg = "Failed to parse " + configurationUrl
                        + lineSeparator + "Exception: " + e.getMessage()
                        + lineSeparator + "Hazelcast startup interrupted.";
                LOGGER.severe(msg);
            } else {
                String msg = "Failed to parse the inputstream"
                        + lineSeparator + "Exception: " + e.getMessage()
                        + lineSeparator + "Hazelcast startup interrupted.";
                LOGGER.severe(msg);
            }
            throw new InvalidConfigurationException(e.getMessage(), e);
        } finally {
            IOUtil.closeResource(is);
        }
        return doc;
    }

    private void handleConfig(final Element docElement) throws Exception {
        for (org.w3c.dom.Node node : new IterableNodeList(docElement.getChildNodes())) {
            final String nodeName = cleanNodeName(node.getNodeName());
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException("Duplicate '" + nodeName + "' definition found in XML configuration. ");
            }
            if (handleXmlNode(node, nodeName)) {
                continue;
            }
            if (!canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }
    }

    private boolean handleXmlNode(Node node, String nodeName) throws Exception {
        if (NETWORK.isEqual(nodeName)) {
            handleNetwork(node);
        } else if (IMPORT.isEqual(nodeName)) {
            throw new InvalidConfigurationException("<import> element can appear only in the top level of the XML");
        } else if (GROUP.isEqual(nodeName)) {
            handleGroup(node);
        } else if (PROPERTIES.isEqual(nodeName)) {
            fillProperties(node, config.getProperties());
        } else if (WAN_REPLICATION.isEqual(nodeName)) {
            handleWanReplication(node);
        } else if (EXECUTOR_SERVICE.isEqual(nodeName)) {
            handleExecutor(node);
        } else if (SERVICES.isEqual(nodeName)) {
            handleServices(node);
        } else if (QUEUE.isEqual(nodeName)) {
            handleQueue(node);
        } else if (MAP.isEqual(nodeName)) {
            handleMap(node);
        } else if (MULTIMAP.isEqual(nodeName)) {
            handleMultiMap(node);
        } else if (REPLICATED_MAP.isEqual(nodeName)) {
            handleReplicatedMap(node);
        } else if (LIST.isEqual(nodeName)) {
            handleList(node);
        } else if (SET.isEqual(nodeName)) {
            handleSet(node);
        } else if (TOPIC.isEqual(nodeName)) {
            handleTopic(node);
        } else if (RELIABLE_TOPIC.isEqual(nodeName)) {
            handleReliableTopic(node);
        } else if (CACHE.isEqual(nodeName)) {
            handleCache(node);
        } else if (NATIVE_MEMORY.isEqual(nodeName)) {
            fillNativeMemoryConfig(node, config.getNativeMemoryConfig());
        } else if (JOB_TRACKER.isEqual(nodeName)) {
            handleJobTracker(node);
        } else if (SEMAPHORE.isEqual(nodeName)) {
            handleSemaphore(node);
        } else if (RINGBUFFER.isEqual(nodeName)) {
            handleRingbuffer(node);
        } else if (LISTENERS.isEqual(nodeName)) {
            handleListeners(node);
        } else if (PARTITION_GROUP.isEqual(nodeName)) {
            handlePartitionGroup(node);
        } else if (SERIALIZATION.isEqual(nodeName)) {
            handleSerialization(node);
        } else if (SECURITY.isEqual(nodeName)) {
            handleSecurity(node);
        } else if (MEMBER_ATTRIBUTES.isEqual(nodeName)) {
            handleMemberAttributes(node);
        } else if (LICENSE_KEY.isEqual(nodeName)) {
            config.setLicenseKey(getTextContent(node));
        } else if (MANAGEMENT_CENTER.isEqual(nodeName)) {
            handleManagementCenterConfig(node);
        } else if (QUORUM.isEqual(nodeName)) {
            handleQuorum(node);
        } else {
            return true;
        }
        return false;
    }

    private void handleQuorum(final org.w3c.dom.Node node) {
        QuorumConfig quorumConfig = new QuorumConfig();
        final String name = getAttribute(node, "name");
        quorumConfig.setName(name);
        Node attrEnabled = node.getAttributes().getNamedItem("enabled");
        final boolean enabled = attrEnabled != null ? checkTrue(getTextContent(attrEnabled)) : false;
        quorumConfig.setEnabled(enabled);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("quorum-size".equals(nodeName)) {
                quorumConfig.setSize(getIntegerValue("quorum-size", value, 0));
            } else if ("quorum-listeners".equals(nodeName)) {
                for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
                    if ("quorum-listener".equals(cleanNodeName(listenerNode))) {
                        String listenerClass = getTextContent(listenerNode);
                        quorumConfig.addListenerConfig(new QuorumListenerConfig(listenerClass));
                    }
                }
            } else if ("quorum-type".equals(nodeName)) {
                quorumConfig.setType(QuorumType.valueOf(upperCaseInternal(value)));
            } else if ("quorum-function-class-name".equals(nodeName)) {
                quorumConfig.setQuorumFunctionClassName(value);
            }
        }
        this.config.addQuorumConfig(quorumConfig);
    }


    private void handleServices(final Node node) {
        final Node attDefaults = node.getAttributes().getNamedItem("enable-defaults");
        final boolean enableDefaults = attDefaults == null || checkTrue(getTextContent(attDefaults));
        ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.setEnableDefaults(enableDefaults);

        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("service".equals(nodeName)) {
                ServiceConfig serviceConfig = new ServiceConfig();
                String enabledValue = getAttribute(child, "enabled");
                boolean enabled = checkTrue(enabledValue);
                serviceConfig.setEnabled(enabled);

                for (org.w3c.dom.Node n : new IterableNodeList(child.getChildNodes())) {
                    final String value = cleanNodeName(n.getNodeName());
                    if ("name".equals(value)) {
                        String name = getTextContent(n);
                        serviceConfig.setName(name);
                    } else if ("class-name".equals(value)) {
                        String className = getTextContent(n);
                        serviceConfig.setClassName(className);
                    } else if ("properties".equals(value)) {
                        fillProperties(n, serviceConfig.getProperties());
                    } else if ("configuration".equals(value)) {
                        Node parserNode = n.getAttributes().getNamedItem("parser");
                        String parserClass = getTextContent(parserNode);
                        if (parserNode == null || parserClass == null) {
                            throw new InvalidConfigurationException("Parser is required!");
                        }
                        try {
                            ServiceConfigurationParser parser = ClassLoaderUtil.newInstance(config.getClassLoader(), parserClass);
                            Object obj = parser.parse((Element) n);
                            serviceConfig.setConfigObject(obj);
                        } catch (Exception e) {
                            ExceptionUtil.sneakyThrow(e);
                        }
                    }
                }
                servicesConfig.addServiceConfig(serviceConfig);
            }
        }
    }

    private void handleWanReplication(final Node node) throws Exception {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);

        final Node attSnapshotEnabled = node.getAttributes().getNamedItem("snapshot-enabled");
        final boolean snapshotEnabled = checkTrue(getTextContent(attSnapshotEnabled));

        final WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName(name);
        wanReplicationConfig.setSnapshotEnabled(snapshotEnabled);

        for (org.w3c.dom.Node nodeTarget : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(nodeTarget.getNodeName());
            if ("target-cluster".equals(nodeName)) {
                WanTargetClusterConfig wanTarget = new WanTargetClusterConfig();
                String groupName = getAttribute(nodeTarget, "group-name");
                String groupPassword = getAttribute(nodeTarget, "group-password");
                if (groupName != null) {
                    wanTarget.setGroupName(groupName);
                }
                if (groupPassword != null) {
                    wanTarget.setGroupPassword(groupPassword);
                }
                for (org.w3c.dom.Node targetChild : new IterableNodeList(nodeTarget.getChildNodes())) {
                    final String targetChildName = cleanNodeName(targetChild.getNodeName());
                    if ("replication-impl".equals(targetChildName)) {
                        String replicationImpl = getTextContent(targetChild);
                        if (WanNoDelayReplication.class.getName().equals(replicationImpl)
                                && wanReplicationConfig.isSnapshotEnabled()) {
                            throw new InvalidConfigurationException("snapshot-enabled property "
                                    + "only can be set to true when used with Enterprise Wan Batch Replication");
                        }
                        wanTarget.setReplicationImpl(getTextContent(targetChild));
                    } else if ("end-points".equals(targetChildName)) {
                        for (org.w3c.dom.Node address : new IterableNodeList(targetChild.getChildNodes())) {
                            final String addressNodeName = cleanNodeName(address.getNodeName());
                            if ("address".equals(addressNodeName)) {
                                String addressStr = getTextContent(address);
                                wanTarget.addEndpoint(addressStr);
                            }
                        }
                    }
                }
                wanReplicationConfig.addTargetClusterConfig(wanTarget);
            }
        }
        config.addWanReplicationConfig(wanReplicationConfig);
    }

    private void handleNetwork(final org.w3c.dom.Node node) throws Exception {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("reuse-address".equals(nodeName)) {
                String value = getTextContent(child).trim();
                config.getNetworkConfig().setReuseAddress(checkTrue(value));
            } else if ("port".equals(nodeName)) {
                handlePort(child);
            } else if ("outbound-ports".equals(nodeName)) {
                handleOutboundPorts(child);
            } else if ("public-address".equals(nodeName)) {
                final String address = getTextContent(child);
                config.getNetworkConfig().setPublicAddress(address);
            } else if ("join".equals(nodeName)) {
                handleJoin(child);
            } else if ("interfaces".equals(nodeName)) {
                handleInterfaces(child);
            } else if ("symmetric-encryption".equals(nodeName)) {
                handleViaReflection(child, config.getNetworkConfig(), new SymmetricEncryptionConfig());
            } else if ("ssl".equals(nodeName)) {
                handleSSLConfig(child);
            } else if ("socket-interceptor".equals(nodeName)) {
                handleSocketInterceptorConfig(child);
            }
        }
    }

    private void handleExecutor(final org.w3c.dom.Node node) throws Exception {
        final ExecutorConfig executorConfig = new ExecutorConfig();
        handleViaReflection(node, config, executorConfig);
    }

    private void handleGroup(final org.w3c.dom.Node node) {
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("name".equals(nodeName)) {
                config.getGroupConfig().setName(value);
            } else if ("password".equals(nodeName)) {
                config.getGroupConfig().setPassword(value);
            }
        }
    }

    private void handleInterfaces(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        final InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            if ("enabled".equals(att.getNodeName())) {
                final String value = att.getNodeValue();
                interfaces.setEnabled(checkTrue(value));
            }
        }
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            if ("interface".equalsIgnoreCase(cleanNodeName(n.getNodeName()))) {
                final String value = getTextContent(n).trim();
                interfaces.addInterface(value);
            }
        }
    }

    private void handleViaReflection(final org.w3c.dom.Node node, Object parent, Object target) throws Exception {
        final NamedNodeMap atts = node.getAttributes();
        if (atts != null) {
            for (int a = 0; a < atts.getLength(); a++) {
                final org.w3c.dom.Node att = atts.item(a);
                String methodName = "set" + getMethodName(att.getNodeName());
                Method method = getMethod(target, methodName, true);
                final String value = att.getNodeValue();
                invoke(target, method, value);
            }
        }
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            String methodName = "set" + getMethodName(cleanNodeName(n.getNodeName()));
            Method method = getMethod(target, methodName, true);
            invoke(target, method, value);
        }
        String mName = "set" + target.getClass().getSimpleName();
        Method method = getMethod(parent, mName, false);
        if (method == null) {
            mName = "add" + target.getClass().getSimpleName();
            method = getMethod(parent, mName, false);
        }
        method.invoke(parent, target);
    }

    private void invoke(Object target, Method method, String value) {
        if (method == null) {
            return;
        }
        Class<?>[] args = method.getParameterTypes();
        if (args == null || args.length == 0) {
            return;
        }
        Class<?> arg = method.getParameterTypes()[0];
        try {
            if (arg == String.class) {
                method.invoke(target, value);
            } else if (arg == int.class) {
                method.invoke(target, Integer.parseInt(value));
            } else if (arg == long.class) {
                method.invoke(target, Long.parseLong(value));
            } else if (arg == boolean.class) {
                method.invoke(target, Boolean.parseBoolean(value));
            }
        } catch (Exception e) {
            LOGGER.warning(e);
        }
    }

    private Method getMethod(Object target, String methodName, boolean requiresArg) {
        Method[] methods = target.getClass().getMethods();
        for (Method method : methods) {
            if (method.getName().equalsIgnoreCase(methodName)) {
                if (requiresArg) {
                    Class<?>[] args = method.getParameterTypes();
                    if (args == null || args.length == 0) {
                        continue;
                    }
                    Class<?> arg = method.getParameterTypes()[0];
                    // this list has to match the options in invoke(Object, Method, String)
                    if (arg == String.class || arg == int.class || arg == long.class || arg == boolean.class) {
                        return method;
                    }
                } else {
                    return method;
                }
            }
        }
        return null;
    }

    private String getMethodName(String element) {
        StringBuilder sb = new StringBuilder();
        char[] chars = element.toCharArray();
        boolean upper = true;
        for (char c : chars) {
            if (c == '_' || c == '-' || c == '.') {
                upper = true;
            } else {
                if (upper) {
                    sb.append(Character.toUpperCase(c));
                    upper = false;
                } else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    private void handleJoin(final org.w3c.dom.Node node) {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child.getNodeName());
            if ("multicast".equals(name)) {
                handleMulticast(child);
            } else if ("tcp-ip".equals(name)) {
                handleTcpIp(child);
            } else if ("aws".equals(name)) {
                handleAWS(child);
            } else if ("discovery-strategies".equals(name)) {
                handleDiscoveryStrategies(child);
            }
        }

        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.verify();
    }

    private void handleDiscoveryStrategies(Node node) {
        final JoinConfig join = config.getNetworkConfig().getJoin();
        final DiscoveryStrategiesConfig discoveryStrategiesConfig = join.getDiscoveryStrategiesConfig();
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child.getNodeName());
            if ("discovery-strategy".equals(name)) {
                handleDiscoveryStrategy(child, discoveryStrategiesConfig);
            } else if ("node-filter".equals(name)) {
                handleDiscoveryNodeFilter(child, discoveryStrategiesConfig);
            }
        }
    }

    private void handleDiscoveryNodeFilter(Node node, DiscoveryStrategiesConfig discoveryStrategiesConfig) {
        final NamedNodeMap atts = node.getAttributes();

        final Node att = atts.getNamedItem("class");
        if (att != null) {
            discoveryStrategiesConfig.setNodeFilterClass(getTextContent(att).trim());
        }
    }

    private void handleDiscoveryStrategy(Node node, DiscoveryStrategiesConfig discoveryStrategiesConfig) {
        final NamedNodeMap atts = node.getAttributes();

        boolean enabled = false;
        String clazz = null;

        for (int a = 0; a < atts.getLength(); a++) {
            final Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if ("enabled".equalsIgnoreCase(att.getNodeName())) {
                enabled = checkTrue(value);
            } else if ("class".equals(att.getNodeName())) {
                clazz = value;
            }
        }

        if (!enabled || clazz == null) {
            return;
        }

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child.getNodeName());
            if ("properties".equals(name)) {
                fillProperties(child, properties);
            }
        }

        discoveryStrategiesConfig.addDiscoveryProviderConfig(new DiscoveryStrategyConfig(clazz, properties));
    }

    private void handleAWS(Node node) {
        final JoinConfig join = config.getNetworkConfig().getJoin();
        final NamedNodeMap atts = node.getAttributes();
        final AwsConfig awsConfig = join.getAwsConfig();
        for (int a = 0; a < atts.getLength(); a++) {
            final Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if ("enabled".equalsIgnoreCase(att.getNodeName())) {
                awsConfig.setEnabled(checkTrue(value));
            } else if (att.getNodeName().equals("connection-timeout-seconds")) {
                awsConfig.setConnectionTimeoutSeconds(getIntegerValue("connection-timeout-seconds", value, DEFAULT_VALUE));
            }
        }
        for (Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            if ("secret-key".equals(cleanNodeName(n.getNodeName()))) {
                awsConfig.setSecretKey(value);
            } else if ("access-key".equals(cleanNodeName(n.getNodeName()))) {
                awsConfig.setAccessKey(value);
            } else if ("region".equals(cleanNodeName(n.getNodeName()))) {
                awsConfig.setRegion(value);
            } else if ("host-header".equals(cleanNodeName(n.getNodeName()))) {
                awsConfig.setHostHeader(value);
            } else if ("security-group-name".equals(cleanNodeName(n.getNodeName()))) {
                awsConfig.setSecurityGroupName(value);
            } else if ("tag-key".equals(cleanNodeName(n.getNodeName()))) {
                awsConfig.setTagKey(value);
            } else if ("tag-value".equals(cleanNodeName(n.getNodeName()))) {
                awsConfig.setTagValue(value);
            } else if ("iam-role".equals(cleanNodeName(n.getNodeName()))) {
                awsConfig.setIamRole(value);
            }
        }
    }

    private void handleMulticast(final org.w3c.dom.Node node) {
        final JoinConfig join = config.getNetworkConfig().getJoin();
        final NamedNodeMap atts = node.getAttributes();
        final MulticastConfig multicastConfig = join.getMulticastConfig();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if ("enabled".equalsIgnoreCase(att.getNodeName())) {
                multicastConfig.setEnabled(checkTrue(value));
            } else if ("loopbackModeEnabled".equalsIgnoreCase(att.getNodeName())) {
                multicastConfig.setLoopbackModeEnabled(checkTrue(value));
            }
        }
        for (Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            if ("multicast-group".equals(cleanNodeName(n.getNodeName()))) {
                multicastConfig.setMulticastGroup(value);
            } else if ("multicast-port".equals(cleanNodeName(n.getNodeName()))) {
                multicastConfig.setMulticastPort(Integer.parseInt(value));
            } else if ("multicast-timeout-seconds".equals(cleanNodeName(n.getNodeName()))) {
                multicastConfig.setMulticastTimeoutSeconds(Integer.parseInt(value));
            } else if ("multicast-time-to-live-seconds".equals(cleanNodeName(n.getNodeName()))) {
                //we need this line for the time being to prevent not reading the multicast-time-to-live-seconds property
                //for more info see: https://github.com/hazelcast/hazelcast/issues/752
                multicastConfig.setMulticastTimeToLive(Integer.parseInt(value));
            } else if ("multicast-time-to-live".equals(cleanNodeName(n.getNodeName()))) {
                multicastConfig.setMulticastTimeToLive(Integer.parseInt(value));
            } else if ("trusted-interfaces".equals(cleanNodeName(n.getNodeName()))) {
                for (org.w3c.dom.Node child : new IterableNodeList(n.getChildNodes())) {
                    if ("interface".equalsIgnoreCase(cleanNodeName(child.getNodeName()))) {
                        multicastConfig.addTrustedInterface(getTextContent(child).trim());
                    }
                }
            }
        }
    }

    private void handleTcpIp(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        final JoinConfig join = config.getNetworkConfig().getJoin();
        final TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if (att.getNodeName().equals("enabled")) {
                tcpIpConfig.setEnabled(checkTrue(value));
            } else if (att.getNodeName().equals("connection-timeout-seconds")) {
                tcpIpConfig.setConnectionTimeoutSeconds(getIntegerValue("connection-timeout-seconds", value, DEFAULT_VALUE));
            }
        }
        final NodeList nodelist = node.getChildNodes();
        final Set<String> memberTags = new HashSet<String>(Arrays.asList("interface", "member", "members"));
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (cleanNodeName(n.getNodeName()).equals("member-list")) {
                handleMemberList(n);
            } else if (cleanNodeName(n.getNodeName()).equals("required-member")) {
                if (tcpIpConfig.getRequiredMember() != null) {
                    throw new InvalidConfigurationException("Duplicate required-member"
                            + " definition found in XML configuration. ");
                }
                tcpIpConfig.setRequiredMember(value);
            } else if (memberTags.contains(cleanNodeName(n.getNodeName()))) {
                tcpIpConfig.addMember(value);
            }
        }
    }

    private void handleMemberList(final Node node) {
        final JoinConfig join = config.getNetworkConfig().getJoin();
        final TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("member".equals(nodeName)) {
                final String value = getTextContent(n).trim();
                tcpIpConfig.addMember(value);
            }
        }
    }

    private void handlePort(final Node node) {
        final String portStr = getTextContent(node).trim();
        final NetworkConfig networkConfig = config.getNetworkConfig();
        if (portStr != null && portStr.length() > 0) {
            networkConfig.setPort(Integer.parseInt(portStr));
        }
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();

            if ("auto-increment".equals(att.getNodeName())) {
                networkConfig.setPortAutoIncrement(checkTrue(value));
            } else if ("port-count".equals(att.getNodeName())) {
                int portCount = Integer.parseInt(value);
                networkConfig.setPortCount(portCount);
            }
        }
    }

    private void handleOutboundPorts(final Node child) {
        final NetworkConfig networkConfig = config.getNetworkConfig();
        for (Node n : new IterableNodeList(child.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("ports".equals(nodeName)) {
                final String value = getTextContent(n);
                networkConfig.addOutboundPortDefinition(value);
            }
        }
    }

    private void handleQueue(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final QueueConfig qConfig = new QueueConfig();
        qConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("max-size".equals(nodeName)) {
                qConfig.setMaxSize(getIntegerValue("max-size", value, QueueConfig.DEFAULT_MAX_SIZE));
            } else if ("backup-count".equals(nodeName)) {
                qConfig.setBackupCount(getIntegerValue("backup-count", value, QueueConfig.DEFAULT_SYNC_BACKUP_COUNT));
            } else if ("async-backup-count".equals(nodeName)) {
                qConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value, QueueConfig.DEFAULT_ASYNC_BACKUP_COUNT));
            } else if ("item-listeners".equals(nodeName)) {
                for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
                    if ("item-listener".equals(cleanNodeName(listenerNode))) {
                        final NamedNodeMap attrs = listenerNode.getAttributes();
                        boolean incValue = checkTrue(getTextContent(attrs.getNamedItem("include-value")));
                        String listenerClass = getTextContent(listenerNode);
                        qConfig.addItemListenerConfig(new ItemListenerConfig(listenerClass, incValue));
                    }
                }
            } else if ("statistics-enabled".equals(nodeName)) {
                qConfig.setStatisticsEnabled(checkTrue(value));
            } else if ("queue-store".equals(nodeName)) {
                final QueueStoreConfig queueStoreConfig = createQueueStoreConfig(n);
                qConfig.setQueueStoreConfig(queueStoreConfig);
            } else if ("empty-queue-ttl".equals(nodeName)) {
                qConfig.setEmptyQueueTtl(getIntegerValue("empty-queue-ttl", value, QueueConfig.DEFAULT_EMPTY_QUEUE_TTL));
            }
        }
        this.config.addQueueConfig(qConfig);
    }

    private void handleList(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final ListConfig lConfig = new ListConfig();
        lConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("max-size".equals(nodeName)) {
                lConfig.setMaxSize(getIntegerValue("max-size", value, ListConfig.DEFAULT_MAX_SIZE));
            } else if ("backup-count".equals(nodeName)) {
                lConfig.setBackupCount(getIntegerValue("backup-count", value, ListConfig.DEFAULT_SYNC_BACKUP_COUNT));
            } else if ("async-backup-count".equals(nodeName)) {
                lConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value, ListConfig.DEFAULT_ASYNC_BACKUP_COUNT));
            } else if ("item-listeners".equals(nodeName)) {
                for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
                    if ("item-listener".equals(cleanNodeName(listenerNode))) {
                        final NamedNodeMap attrs = listenerNode.getAttributes();
                        boolean incValue = checkTrue(getTextContent(attrs.getNamedItem("include-value")));
                        String listenerClass = getTextContent(listenerNode);
                        lConfig.addItemListenerConfig(new ItemListenerConfig(listenerClass, incValue));
                    }
                }
            } else if ("statistics-enabled".equals(nodeName)) {
                lConfig.setStatisticsEnabled(checkTrue(value));
            }
        }
        this.config.addListConfig(lConfig);
    }

    private void handleSet(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final SetConfig sConfig = new SetConfig();
        sConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("max-size".equals(nodeName)) {
                sConfig.setMaxSize(getIntegerValue("max-size", value, SetConfig.DEFAULT_MAX_SIZE));
            } else if ("backup-count".equals(nodeName)) {
                sConfig.setBackupCount(getIntegerValue("backup-count", value, SetConfig.DEFAULT_SYNC_BACKUP_COUNT));
            } else if ("async-backup-count".equals(nodeName)) {
                sConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value, SetConfig.DEFAULT_ASYNC_BACKUP_COUNT));
            } else if ("item-listeners".equals(nodeName)) {
                for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
                    if ("item-listener".equals(cleanNodeName(listenerNode))) {
                        final NamedNodeMap attrs = listenerNode.getAttributes();
                        boolean incValue = checkTrue(getTextContent(attrs.getNamedItem("include-value")));
                        String listenerClass = getTextContent(listenerNode);
                        sConfig.addItemListenerConfig(new ItemListenerConfig(listenerClass, incValue));
                    }
                }
            } else if ("statistics-enabled".equals(nodeName)) {
                sConfig.setStatisticsEnabled(checkTrue(value));
            }
        }
        this.config.addSetConfig(sConfig);
    }

    private void handleMultiMap(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("value-collection-type".equals(nodeName)) {
                multiMapConfig.setValueCollectionType(value);
            } else if ("backup-count".equals(nodeName)) {
                multiMapConfig.setBackupCount(getIntegerValue("backup-count"
                        , value, MultiMapConfig.DEFAULT_SYNC_BACKUP_COUNT));
            } else if ("async-backup-count".equals(nodeName)) {
                multiMapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count"
                        , value, MultiMapConfig.DEFAULT_ASYNC_BACKUP_COUNT));
            } else if ("entry-listeners".equals(nodeName)) {
                for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
                    if ("entry-listener".equals(cleanNodeName(listenerNode))) {
                        final NamedNodeMap attrs = listenerNode.getAttributes();
                        boolean incValue = checkTrue(getTextContent(attrs.getNamedItem("include-value")));
                        boolean local = checkTrue(getTextContent(attrs.getNamedItem("local")));
                        String listenerClass = getTextContent(listenerNode);
                        multiMapConfig.addEntryListenerConfig(new EntryListenerConfig(listenerClass, local, incValue));
                    }
                }
            } else if ("statistics-enabled".equals(nodeName)) {
                multiMapConfig.setStatisticsEnabled(checkTrue(value));
            }
        }
        this.config.addMultiMapConfig(multiMapConfig);
    }

    private void handleReplicatedMap(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
        replicatedMapConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("concurrency-level".equals(nodeName)) {
                replicatedMapConfig.setConcurrencyLevel(getIntegerValue("concurrency-level"
                        , value, ReplicatedMapConfig.DEFAULT_CONCURRENCY_LEVEL));
            } else if ("in-memory-format".equals(nodeName)) {
                replicatedMapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("replication-delay-millis".equals(nodeName)) {
                replicatedMapConfig.setReplicationDelayMillis(getIntegerValue("replication-delay-millis"
                        , value, ReplicatedMapConfig.DEFAULT_REPLICATION_DELAY_MILLIS));
            } else if ("async-fillup".equals(nodeName)) {
                replicatedMapConfig.setAsyncFillup(checkTrue(value));
            } else if ("statistics-enabled".equals(nodeName)) {
                replicatedMapConfig.setStatisticsEnabled(checkTrue(value));
            } else if ("entry-listeners".equals(nodeName)) {
                for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
                    if ("entry-listener".equals(cleanNodeName(listenerNode))) {
                        final NamedNodeMap attrs = listenerNode.getAttributes();
                        boolean incValue = checkTrue(getTextContent(attrs.getNamedItem("include-value")));
                        boolean local = checkTrue(getTextContent(attrs.getNamedItem("local")));
                        String listenerClass = getTextContent(listenerNode);
                        replicatedMapConfig.addEntryListenerConfig(new EntryListenerConfig(listenerClass, local, incValue));
                    }
                }
            } else if ("merge-policy".equals(nodeName)) {
                replicatedMapConfig.setMergePolicy(value);
            }
        }
        this.config.addReplicatedMapConfig(replicatedMapConfig);
    }

    //CHECKSTYLE:OFF
    private void handleMap(final org.w3c.dom.Node parentNode) throws Exception {
        final String name = getAttribute(parentNode, "name");
        final MapConfig mapConfig = new MapConfig();
        mapConfig.setName(name);
        for (org.w3c.dom.Node node : new IterableNodeList(parentNode.getChildNodes())) {
            final String nodeName = cleanNodeName(node.getNodeName());
            final String value = getTextContent(node).trim();
            if ("backup-count".equals(nodeName)) {
                mapConfig.setBackupCount(getIntegerValue("backup-count", value, MapConfig.DEFAULT_BACKUP_COUNT));
            } else if ("in-memory-format".equals(nodeName)) {
                mapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("async-backup-count".equals(nodeName)) {
                mapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value, MapConfig.MIN_BACKUP_COUNT));
            } else if ("eviction-policy".equals(nodeName)) {
                mapConfig.setEvictionPolicy(EvictionPolicy.valueOf(upperCaseInternal(value)));
            } else if ("max-size".equals(nodeName)) {
                final MaxSizeConfig msc = mapConfig.getMaxSizeConfig();
                final Node maxSizePolicy = node.getAttributes().getNamedItem("policy");
                if (maxSizePolicy != null) {
                    msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.valueOf(
                            upperCaseInternal(getTextContent(maxSizePolicy))));
                }
                final int size = sizeParser(value);
                msc.setSize(size);
            } else if ("eviction-percentage".equals(nodeName)) {
                mapConfig.setEvictionPercentage(getIntegerValue("eviction-percentage", value,
                        MapConfig.DEFAULT_EVICTION_PERCENTAGE));
            } else if ("min-eviction-check-millis".equals(nodeName)) {
                mapConfig.setMinEvictionCheckMillis(getLongValue("min-eviction-check-millis", value,
                        MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS));
            } else if ("time-to-live-seconds".equals(nodeName)) {
                mapConfig.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", value,
                        MapConfig.DEFAULT_TTL_SECONDS));
            } else if ("max-idle-seconds".equals(nodeName)) {
                mapConfig.setMaxIdleSeconds(getIntegerValue("max-idle-seconds", value,
                        MapConfig.DEFAULT_MAX_IDLE_SECONDS));
            } else if ("map-store".equals(nodeName)) {
                MapStoreConfig mapStoreConfig = createMapStoreConfig(node);
                mapConfig.setMapStoreConfig(mapStoreConfig);
            } else if ("near-cache".equals(nodeName)) {
                handleViaReflection(node, mapConfig, new NearCacheConfig());
            } else if ("merge-policy".equals(nodeName)) {
                mapConfig.setMergePolicy(value);
            } else if ("read-backup-data".equals(nodeName)) {
                mapConfig.setReadBackupData(checkTrue(value));
            } else if ("statistics-enabled".equals(nodeName)) {
                mapConfig.setStatisticsEnabled(checkTrue(value));
            } else if ("optimize-queries".equals(nodeName)) {
                mapConfig.setOptimizeQueries(checkTrue(value));
            } else if ("wan-replication-ref".equals(nodeName)) {
                mapWanReplicationRefHandle(node, mapConfig);
            } else if ("indexes".equals(nodeName)) {
                mapIndexesHandle(node, mapConfig);
            } else if ("entry-listeners".equals(nodeName)) {
                mapEntryListenerHandle(node, mapConfig);
            } else if ("partition-lost-listeners".equals(nodeName)) {
                mapPartitionLostListenerHandle(node, mapConfig);
            } else if ("partition-strategy".equals(nodeName)) {
                mapConfig.setPartitioningStrategyConfig(new PartitioningStrategyConfig(value));
            } else if ("quorum-ref".equals(nodeName)) {
                mapConfig.setQuorumName(value);
            } else if ("query-caches".equals(nodeName)) {
                mapQueryCacheHandler(node, mapConfig);
            }
        }
        this.config.addMapConfig(mapConfig);
    }
    //CHECKSTYLE:ON

    private void handleCache(final org.w3c.dom.Node node)
            throws Exception {
        final String name = getAttribute(node, "name");
        final CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("key-type".equals(nodeName)) {
                cacheConfig.setKeyType(getAttribute(n, "class-name"));
            } else if ("value-type".equals(nodeName)) {
                cacheConfig.setValueType(getAttribute(n, "class-name"));
            } else if ("statistics-enabled".equals(nodeName)) {
                cacheConfig.setStatisticsEnabled(checkTrue(value));
            } else if ("management-enabled".equals(nodeName)) {
                cacheConfig.setManagementEnabled(checkTrue(value));
            } else if ("read-through".equals(nodeName)) {
                cacheConfig.setReadThrough(checkTrue(value));
            } else if ("write-through".equals(nodeName)) {
                cacheConfig.setWriteThrough(checkTrue(value));
            } else if ("cache-loader-factory".equals(nodeName)) {
                cacheConfig.setCacheLoaderFactory(getAttribute(n, "class-name"));
            } else if ("cache-writer-factory".equals(nodeName)) {
                cacheConfig.setCacheWriterFactory(getAttribute(n, "class-name"));
            } else if ("expiry-policy-factory".equals(nodeName)) {
                cacheConfig.setExpiryPolicyFactoryConfig(getExpiryPolicyFactoryConfig(n));
            } else if ("cache-entry-listeners".equals(nodeName)) {
                cacheListenerHandle(n, cacheConfig);
            } else if ("in-memory-format".equals(nodeName)) {
                cacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("backup-count".equals(nodeName)) {
                cacheConfig.setBackupCount(getIntegerValue("backup-count", value, CacheSimpleConfig.DEFAULT_BACKUP_COUNT));
            } else if ("async-backup-count".equals(nodeName)) {
                cacheConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value, CacheSimpleConfig.MIN_BACKUP_COUNT));
            } else if ("wan-replication-ref".equals(nodeName)) {
                cacheWanReplicationRefHandle(n, cacheConfig);
            } else if ("eviction".equals(nodeName)) {
                EvictionConfig evictionConfig = getEvictionConfig(n);
                EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
                if (evictionPolicy == null || evictionPolicy == EvictionPolicy.NONE) {
                    throw new InvalidConfigurationException("Eviction policy of cache cannot be null or \"NONE\"");
                }
                cacheConfig.setEvictionConfig(evictionConfig);
            } else if ("quorum-ref".equals(nodeName)) {
                cacheConfig.setQuorumName(value);
            } else if ("partition-lost-listeners".equals(nodeName)) {
                cachePartitionLostListenerHandle(n, cacheConfig);
            } else if ("merge-policy".equals(nodeName)) {
                cacheConfig.setMergePolicy(value);
            }
        }
        this.config.addCacheConfig(cacheConfig);
    }

    private ExpiryPolicyFactoryConfig getExpiryPolicyFactoryConfig(final org.w3c.dom.Node node) {
        final String className = getAttribute(node, "class-name");
        if (!StringUtil.isNullOrEmpty(className)) {
            return new ExpiryPolicyFactoryConfig(className);
        } else {
            TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig = null;
            for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(n.getNodeName());
                if ("timed-expiry-policy-factory".equals(nodeName)) {
                    timedExpiryPolicyFactoryConfig = getTimedExpiryPolicyFactoryConfig(n);
                }
            }
            if (timedExpiryPolicyFactoryConfig == null) {
                throw new InvalidConfigurationException(
                        "One of the \"class-name\" or \"timed-expire-policy-factory\" configuration "
                                + "is needed for expiry policy factory configuration");
            } else {
                return new ExpiryPolicyFactoryConfig(timedExpiryPolicyFactoryConfig);
            }
        }
    }

    private TimedExpiryPolicyFactoryConfig getTimedExpiryPolicyFactoryConfig(final org.w3c.dom.Node node) {
        final String expiryPolicyTypeStr = getAttribute(node, "expiry-policy-type");
        final String durationAmountStr = getAttribute(node, "duration-amount");
        final String timeUnitStr = getAttribute(node, "time-unit");
        final ExpiryPolicyType expiryPolicyType =
                ExpiryPolicyType.valueOf(upperCaseInternal(expiryPolicyTypeStr));
        if (expiryPolicyType != ExpiryPolicyType.ETERNAL
                && (StringUtil.isNullOrEmpty(durationAmountStr)
                || StringUtil.isNullOrEmpty(timeUnitStr))) {
            throw new InvalidConfigurationException(
                    "Both of the \"duration-amount\" or \"time-unit\" attributes "
                            + "are required for expiry policy factory configuration "
                            + "(except \"ETERNAL\" expiry policy type)");
        }
        DurationConfig durationConfig = null;
        if (expiryPolicyType != ExpiryPolicyType.ETERNAL) {
            long durationAmount;
            try {
                durationAmount = Long.parseLong(durationAmountStr);
            } catch (NumberFormatException e) {
                throw new InvalidConfigurationException(
                        "Invalid value for duration amount: " + durationAmountStr, e);
            }
            if (durationAmount <= 0) {
                throw new InvalidConfigurationException(
                        "Duration amount must be positive: " + durationAmount);
            }
            TimeUnit timeUnit;
            try {
                timeUnit = TimeUnit.valueOf(upperCaseInternal(timeUnitStr));
            } catch (IllegalArgumentException e) {
                throw new InvalidConfigurationException(
                        "Invalid value for time unit: " + timeUnitStr, e);
            }
            durationConfig = new DurationConfig(durationAmount, timeUnit);
        }
        return new TimedExpiryPolicyFactoryConfig(expiryPolicyType, durationConfig);
    }

    private EvictionConfig getEvictionConfig(final org.w3c.dom.Node node) {
        final EvictionConfig evictionConfig = new EvictionConfig();
        final Node size = node.getAttributes().getNamedItem("size");
        final Node maxSizePolicy = node.getAttributes().getNamedItem("max-size-policy");
        final Node evictionPolicy = node.getAttributes().getNamedItem("eviction-policy");
        if (size != null) {
            evictionConfig.setSize(Integer.parseInt(getTextContent(size)));
        }
        if (maxSizePolicy != null) {
            evictionConfig.setMaximumSizePolicy(
                    EvictionConfig.MaxSizePolicy.valueOf(
                            upperCaseInternal(getTextContent(maxSizePolicy)))
            );
        }
        if (evictionPolicy != null) {
            evictionConfig.setEvictionPolicy(
                    EvictionPolicy.valueOf(
                            upperCaseInternal(getTextContent(evictionPolicy)))
            );
        }
        return evictionConfig;
    }

    private void cacheWanReplicationRefHandle(Node n, CacheSimpleConfig cacheConfig) {
        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        final String wanName = getAttribute(n, "name");
        wanReplicationRef.setName(wanName);
        for (org.w3c.dom.Node wanChild : new IterableNodeList(n.getChildNodes())) {
            final String wanChildName = cleanNodeName(wanChild.getNodeName());
            final String wanChildValue = getTextContent(wanChild);
            if ("merge-policy".equals(wanChildName)) {
                wanReplicationRef.setMergePolicy(wanChildValue);
            } else if ("republishing-enabled".equals(wanChildName)) {
                wanReplicationRef.setRepublishingEnabled(checkTrue(wanChildValue));
            }
        }
        cacheConfig.setWanReplicationRef(wanReplicationRef);
    }

    private void cachePartitionLostListenerHandle(Node n, CacheSimpleConfig cacheConfig) {
        for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
            if ("partition-lost-listener".equals(cleanNodeName(listenerNode))) {
                String listenerClass = getTextContent(listenerNode);
                cacheConfig.addCachePartitionLostListenerConfig(
                        new CachePartitionLostListenerConfig(listenerClass));
            }
        }
    }

    private void cacheListenerHandle(Node n, CacheSimpleConfig cacheSimpleConfig) {
        for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
            if ("cache-entry-listener".equals(cleanNodeName(listenerNode))) {
                CacheSimpleEntryListenerConfig listenerConfig = new CacheSimpleEntryListenerConfig();
                for (org.w3c.dom.Node listenerChildNode : new IterableNodeList(listenerNode.getChildNodes())) {
                    if ("cache-entry-listener-factory".equals(cleanNodeName(listenerChildNode))) {
                        listenerConfig.setCacheEntryListenerFactory(getAttribute(listenerChildNode, "class-name"));
                    }
                    if ("cache-entry-event-filter-factory".equals(cleanNodeName(listenerChildNode))) {
                        listenerConfig.setCacheEntryEventFilterFactory(getAttribute(listenerChildNode, "class-name"));
                    }
                }
                final NamedNodeMap attrs = listenerNode.getAttributes();
                listenerConfig.setOldValueRequired(checkTrue(getTextContent(attrs.getNamedItem("old-value-required"))));
                listenerConfig.setSynchronous(checkTrue(getTextContent(attrs.getNamedItem("synchronous"))));
                cacheSimpleConfig.addEntryListenerConfig(listenerConfig);
            }
        }
    }

    private void mapWanReplicationRefHandle(Node n, MapConfig mapConfig) {
        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        final String wanName = getAttribute(n, "name");
        wanReplicationRef.setName(wanName);
        for (org.w3c.dom.Node wanChild : new IterableNodeList(n.getChildNodes())) {
            final String wanChildName = cleanNodeName(wanChild.getNodeName());
            final String wanChildValue = getTextContent(wanChild);
            if ("merge-policy".equals(wanChildName)) {
                wanReplicationRef.setMergePolicy(wanChildValue);
            } else if ("republishing-enabled".equals(wanChildName)) {
                wanReplicationRef.setRepublishingEnabled(checkTrue(wanChildValue));
            }
        }
        mapConfig.setWanReplicationRef(wanReplicationRef);
    }

    private void mapIndexesHandle(Node n, MapConfig mapConfig) {
        for (org.w3c.dom.Node indexNode : new IterableNodeList(n.getChildNodes())) {
            if ("index".equals(cleanNodeName(indexNode))) {
                final NamedNodeMap attrs = indexNode.getAttributes();
                boolean ordered = checkTrue(getTextContent(attrs.getNamedItem("ordered")));
                String attribute = getTextContent(indexNode);
                mapConfig.addMapIndexConfig(new MapIndexConfig(attribute, ordered));
            }
        }
    }

    private void queryCacheIndexesHandle(Node n, QueryCacheConfig queryCacheConfig) {
        for (org.w3c.dom.Node indexNode : new IterableNodeList(n.getChildNodes())) {
            if ("index".equals(cleanNodeName(indexNode))) {
                final NamedNodeMap attrs = indexNode.getAttributes();
                boolean ordered = checkTrue(getTextContent(attrs.getNamedItem("ordered")));
                String attribute = getTextContent(indexNode);
                queryCacheConfig.addIndexConfig(new MapIndexConfig(attribute, ordered));
            }
        }
    }

    private void mapEntryListenerHandle(Node n, MapConfig mapConfig) {
        for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
            if ("entry-listener".equals(cleanNodeName(listenerNode))) {
                final NamedNodeMap attrs = listenerNode.getAttributes();
                boolean incValue = checkTrue(getTextContent(attrs.getNamedItem("include-value")));
                boolean local = checkTrue(getTextContent(attrs.getNamedItem("local")));
                String listenerClass = getTextContent(listenerNode);
                mapConfig.addEntryListenerConfig(new EntryListenerConfig(listenerClass, local, incValue));
            }
        }
    }

    private void mapPartitionLostListenerHandle(Node n, MapConfig mapConfig) {
        for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
            if ("partition-lost-listener".equals(cleanNodeName(listenerNode))) {
                String listenerClass = getTextContent(listenerNode);
                mapConfig.addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig(listenerClass));
            }
        }
    }

    private void mapQueryCacheHandler(Node n, MapConfig mapConfig) {
        for (org.w3c.dom.Node queryCacheNode : new IterableNodeList(n.getChildNodes())) {
            if ("query-cache".equals(cleanNodeName(queryCacheNode))) {
                NamedNodeMap attrs = queryCacheNode.getAttributes();
                String cacheName = getTextContent(attrs.getNamedItem("name"));
                QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
                for (org.w3c.dom.Node childNode : new IterableNodeList(queryCacheNode.getChildNodes())) {
                    String nodeName = cleanNodeName(childNode);
                    if ("entry-listeners".equals(nodeName)) {
                        for (org.w3c.dom.Node listenerNode : new IterableNodeList(childNode.getChildNodes())) {
                            if ("entry-listener".equals(cleanNodeName(listenerNode))) {
                                NamedNodeMap listenerNodeAttributes = listenerNode.getAttributes();
                                boolean incValue = checkTrue(
                                        getTextContent(listenerNodeAttributes.getNamedItem("include-value")));
                                boolean local = checkTrue(getTextContent(listenerNodeAttributes.getNamedItem("local")));
                                String listenerClass = getTextContent(listenerNode);
                                queryCacheConfig.addEntryListenerConfig(
                                        new EntryListenerConfig(listenerClass, local, incValue));
                            }
                        }
                    } else {
                        String textContent = getTextContent(childNode);
                        if ("include-value".equals(nodeName)) {
                            boolean includeValue = checkTrue(textContent);
                            queryCacheConfig.setIncludeValue(includeValue);
                        } else if ("batch-size".equals(nodeName)) {
                            int batchSize = getIntegerValue("batch-size", textContent.trim(),
                                    QueryCacheConfig.DEFAULT_BATCH_SIZE);
                            queryCacheConfig.setBatchSize(batchSize);
                        } else if ("buffer-size".equals(nodeName)) {
                            int bufferSize = getIntegerValue("buffer-size", textContent.trim(),
                                    QueryCacheConfig.DEFAULT_BUFFER_SIZE);
                            queryCacheConfig.setBufferSize(bufferSize);
                        } else if ("delay-seconds".equals(nodeName)) {
                            int delaySeconds = getIntegerValue("delay-seconds", textContent.trim(),
                                    QueryCacheConfig.DEFAULT_DELAY_SECONDS);
                            queryCacheConfig.setDelaySeconds(delaySeconds);
                        } else if ("in-memory-format".equals(nodeName)) {
                            String value = textContent.trim();
                            queryCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
                        } else if ("coalesce".equals(nodeName)) {
                            boolean coalesce = checkTrue(textContent);
                            queryCacheConfig.setCoalesce(coalesce);
                        } else if ("populate".equals(nodeName)) {
                            boolean populate = checkTrue(textContent);
                            queryCacheConfig.setPopulate(populate);
                        } else if ("indexes".equals(nodeName)) {
                            queryCacheIndexesHandle(childNode, queryCacheConfig);
                        } else if ("predicate".equals(nodeName)) {
                            queryCachePredicateHandler(childNode, queryCacheConfig);
                        } else if ("eviction".equals(nodeName)) {
                            queryCacheConfig.setEvictionConfig(getEvictionConfig(childNode));
                        }
                    }
                }
                mapConfig.addQueryCacheConfig(queryCacheConfig);
            }
        }
    }

    private void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig) {
        NamedNodeMap predicateAttributes = childNode.getAttributes();
        String predicateType = getTextContent(predicateAttributes.getNamedItem("type"));
        String textContent = getTextContent(childNode);
        PredicateConfig predicateConfig = new PredicateConfig();
        if ("class-name".equals(predicateType)) {
            predicateConfig.setClassName(textContent);
        } else if ("sql".equals(predicateType)) {
            predicateConfig.setSql(textContent);
        }
        queryCacheConfig.setPredicateConfig(predicateConfig);
    }


    private int sizeParser(String value) {
        int size;
        if (value.length() < 2) {
            size = Integer.parseInt(value);
        } else {
            char last = value.charAt(value.length() - 1);
            int type = 0;
            if (last == 'g' || last == 'G') {
                type = 1;
            } else if (last == 'm' || last == 'M') {
                type = 2;
            }
            if (type == 0) {
                size = Integer.parseInt(value);
            } else if (type == 1) {
                size = Integer.parseInt(value.substring(0, value.length() - 1)) * THOUSAND_FACTOR;
            } else {
                size = Integer.parseInt(value.substring(0, value.length() - 1));
            }
        }
        return size;
    }

    private MapStoreConfig createMapStoreConfig(final org.w3c.dom.Node node) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if ("enabled".equals(att.getNodeName())) {
                mapStoreConfig.setEnabled(checkTrue(value));
            } else if ("initial-mode".equals(att.getNodeName())) {
                final InitialLoadMode mode = InitialLoadMode.valueOf(upperCaseInternal(getTextContent(att)));
                mapStoreConfig.setInitialLoadMode(mode);
            }
        }
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("class-name".equals(nodeName)) {
                mapStoreConfig.setClassName(getTextContent(n).trim());
            } else if ("factory-class-name".equals(nodeName)) {
                mapStoreConfig.setFactoryClassName(getTextContent(n).trim());
            } else if ("write-delay-seconds".equals(nodeName)) {
                mapStoreConfig.setWriteDelaySeconds(getIntegerValue("write-delay-seconds", getTextContent(n).trim(),
                        MapStoreConfig.DEFAULT_WRITE_DELAY_SECONDS));
            } else if ("write-batch-size".equals(nodeName)) {
                mapStoreConfig.setWriteBatchSize(getIntegerValue("write-batch-size", getTextContent(n).trim(),
                        MapStoreConfig.DEFAULT_WRITE_BATCH_SIZE));
            } else if ("write-coalescing".equals(nodeName)) {
                final String writeCoalescing = getTextContent(n).trim();
                if (isNullOrEmpty(writeCoalescing)) {
                    mapStoreConfig.setWriteCoalescing(MapStoreConfig.DEFAULT_WRITE_COALESCING);
                } else {
                    mapStoreConfig.setWriteCoalescing(checkTrue(writeCoalescing));
                }

            } else if ("properties".equals(nodeName)) {
                fillProperties(n, mapStoreConfig.getProperties());
            }
        }
        return mapStoreConfig;
    }

    private QueueStoreConfig createQueueStoreConfig(final org.w3c.dom.Node node) {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig();
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if (att.getNodeName().equals("enabled")) {
                queueStoreConfig.setEnabled(checkTrue(value));
            }
        }
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("class-name".equals(nodeName)) {
                queueStoreConfig.setClassName(getTextContent(n).trim());
            } else if ("factory-class-name".equals(nodeName)) {
                queueStoreConfig.setFactoryClassName(getTextContent(n).trim());
            } else if ("properties".equals(nodeName)) {
                fillProperties(n, queueStoreConfig.getProperties());
            }
        }
        return queueStoreConfig;
    }

    private void handleSSLConfig(final org.w3c.dom.Node node) {
        SSLConfig sslConfig = new SSLConfig();
        final NamedNodeMap atts = node.getAttributes();
        final Node enabledNode = atts.getNamedItem("enabled");
        final boolean enabled = enabledNode != null && checkTrue(getTextContent(enabledNode).trim());
        sslConfig.setEnabled(enabled);

        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("factory-class-name".equals(nodeName)) {
                sslConfig.setFactoryClassName(getTextContent(n).trim());
            } else if ("properties".equals(nodeName)) {
                fillProperties(n, sslConfig.getProperties());
            }
        }
        config.getNetworkConfig().setSSLConfig(sslConfig);
    }

    private void handleSocketInterceptorConfig(final org.w3c.dom.Node node) {
        SocketInterceptorConfig socketInterceptorConfig = parseSocketInterceptorConfig(node);
        config.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig);
    }

    private void handleTopic(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final TopicConfig tConfig = new TopicConfig();
        tConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if (nodeName.equals("global-ordering-enabled")) {
                tConfig.setGlobalOrderingEnabled(checkTrue(getTextContent(n)));
            } else if ("message-listeners".equals(nodeName)) {
                for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
                    if ("message-listener".equals(cleanNodeName(listenerNode))) {
                        tConfig.addMessageListenerConfig(new ListenerConfig(getTextContent(listenerNode)));
                    }
                }
            } else if ("statistics-enabled".equals(nodeName)) {
                tConfig.setStatisticsEnabled(checkTrue(getTextContent(n)));
            }
        }
        config.addTopicConfig(tConfig);
    }

    private void handleReliableTopic(final Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final ReliableTopicConfig topicConfig = new ReliableTopicConfig(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("read-batch-size".equals(cleanNodeName(nodeName))) {
                String batchSize = getTextContent(n);
                topicConfig.setReadBatchSize(
                        getIntegerValue("read-batch-size", batchSize, ReliableTopicConfig.DEFAULT_READ_BATCH_SIZE));
            } else if ("statistics-enabled".equals(nodeName)) {
                topicConfig.setStatisticsEnabled(checkTrue(getTextContent(n)));
            } else if ("topic-overload-policy".equals(nodeName)) {
                TopicOverloadPolicy topicOverloadPolicy = TopicOverloadPolicy.valueOf(upperCaseInternal(getTextContent(n)));
                topicConfig.setTopicOverloadPolicy(topicOverloadPolicy);
            } else if ("message-listeners".equals(nodeName)) {
                for (org.w3c.dom.Node listenerNode : new IterableNodeList(n.getChildNodes())) {
                    if ("message-listener".equals(cleanNodeName(listenerNode))) {
                        topicConfig.addMessageListenerConfig(new ListenerConfig(getTextContent(listenerNode)));
                    }
                }
            }
        }
        config.addReliableTopicConfig(topicConfig);
    }

    private void handleJobTracker(final Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final JobTrackerConfig jConfig = new JobTrackerConfig();
        jConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("max-thread-size".equals(nodeName)) {
                jConfig.setMaxThreadSize(getIntegerValue("max-thread-size", value, JobTrackerConfig.DEFAULT_MAX_THREAD_SIZE));
            } else if ("queue-size".equals(nodeName)) {
                jConfig.setQueueSize(getIntegerValue("queue-size", value, JobTrackerConfig.DEFAULT_QUEUE_SIZE));
            } else if ("retry-count".equals(nodeName)) {
                jConfig.setRetryCount(getIntegerValue("retry-count", value, JobTrackerConfig.DEFAULT_RETRY_COUNT));
            } else if ("chunk-size".equals(nodeName)) {
                jConfig.setChunkSize(getIntegerValue("chunk-size", value, JobTrackerConfig.DEFAULT_CHUNK_SIZE));
            } else if ("communicate-stats".equals(nodeName)) {
                jConfig.setCommunicateStats(value == null || value.length() == 0
                        ? JobTrackerConfig.DEFAULT_COMMUNICATE_STATS : Boolean.parseBoolean(value));
            } else if ("topology-changed-stategy".equals(nodeName)) {
                TopologyChangedStrategy topologyChangedStrategy = JobTrackerConfig.DEFAULT_TOPOLOGY_CHANGED_STRATEGY;
                for (TopologyChangedStrategy temp : TopologyChangedStrategy.values()) {
                    if (temp.name().equals(value)) {
                        topologyChangedStrategy = temp;
                    }
                }
                jConfig.setTopologyChangedStrategy(topologyChangedStrategy);
            }
        }
        config.addJobTrackerConfig(jConfig);
    }

    private void handleSemaphore(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final SemaphoreConfig sConfig = new SemaphoreConfig();
        sConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("initial-permits".equals(nodeName)) {
                sConfig.setInitialPermits(getIntegerValue("initial-permits", value, 0));
            } else if ("backup-count".equals(nodeName)) {
                sConfig.setBackupCount(getIntegerValue("backup-count"
                        , value, SemaphoreConfig.DEFAULT_SYNC_BACKUP_COUNT));
            } else if ("async-backup-count".equals(nodeName)) {
                sConfig.setAsyncBackupCount(getIntegerValue("async-backup-count"
                        , value, SemaphoreConfig.DEFAULT_ASYNC_BACKUP_COUNT));
            }
        }
        config.addSemaphoreConfig(sConfig);
    }

    private void handleRingbuffer(Node node) {
        Node attName = node.getAttributes().getNamedItem("name");
        String name = getTextContent(attName);
        RingbufferConfig rbConfig = new RingbufferConfig(name);
        for (Node n : new IterableNodeList(node.getChildNodes())) {
            String nodeName = cleanNodeName(n.getNodeName());
            String value = getTextContent(n).trim();
            if ("capacity".equals(nodeName)) {
                int capacity = getIntegerValue("capacity", value, RingbufferConfig.DEFAULT_CAPACITY);
                rbConfig.setCapacity(capacity);
            } else if ("backup-count".equals(nodeName)) {
                int backupCount = getIntegerValue("backup-count", value, RingbufferConfig.DEFAULT_SYNC_BACKUP_COUNT);
                rbConfig.setBackupCount(backupCount);
            } else if ("async-backup-count".equals(nodeName)) {
                int asyncBackupCount = getIntegerValue("async-backup-count", value, RingbufferConfig.DEFAULT_ASYNC_BACKUP_COUNT);
                rbConfig.setAsyncBackupCount(asyncBackupCount);
            } else if ("time-to-live-seconds".equals(nodeName)) {
                int timeToLiveSeconds = getIntegerValue("time-to-live-seconds", value, RingbufferConfig.DEFAULT_TTL_SECONDS);
                rbConfig.setTimeToLiveSeconds(timeToLiveSeconds);
            } else if ("in-memory-format".equals(nodeName)) {
                InMemoryFormat inMemoryFormat = InMemoryFormat.valueOf(upperCaseInternal(value));
                rbConfig.setInMemoryFormat(inMemoryFormat);
            }
        }
        config.addRingBufferConfig(rbConfig);
    }

    private void handleListeners(final org.w3c.dom.Node node) throws Exception {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            if ("listener".equals(cleanNodeName(child))) {
                String listenerClass = getTextContent(child);
                config.addListenerConfig(new ListenerConfig(listenerClass));
            }
        }
    }

    private void handlePartitionGroup(Node node) {
        final NamedNodeMap atts = node.getAttributes();
        final Node enabledNode = atts.getNamedItem("enabled");
        final boolean enabled = enabledNode != null ? checkTrue(getTextContent(enabledNode)) : false;
        config.getPartitionGroupConfig().setEnabled(enabled);
        final Node groupTypeNode = atts.getNamedItem("group-type");
        final MemberGroupType groupType = groupTypeNode != null
                ? MemberGroupType.valueOf(upperCaseInternal(getTextContent(groupTypeNode)))
                : MemberGroupType.PER_MEMBER;
        config.getPartitionGroupConfig().setGroupType(groupType);
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            if ("member-group".equals(cleanNodeName(child))) {
                handleMemberGroup(child);
            }
        }
    }

    private void handleMemberGroup(Node node) {
        MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            if ("interface".equals(cleanNodeName(child))) {
                String value = getTextContent(child);
                memberGroupConfig.addInterface(value);
            }
        }
        config.getPartitionGroupConfig().addMemberGroupConfig(memberGroupConfig);
    }

    private void handleSerialization(final Node node) {
        SerializationConfig serializationConfig = parseSerialization(node);
        config.setSerializationConfig(serializationConfig);
    }

    private void handleManagementCenterConfig(final Node node) {
        NamedNodeMap attrs = node.getAttributes();

        final Node enabledNode = attrs.getNamedItem("enabled");
        boolean enabled = enabledNode != null && checkTrue(getTextContent(enabledNode));

        final Node intervalNode = attrs.getNamedItem("update-interval");
        final int interval = intervalNode != null ? getIntegerValue("update-interval",
                getTextContent(intervalNode), ManagementCenterConfig.UPDATE_INTERVAL) : ManagementCenterConfig.UPDATE_INTERVAL;

        final String url = getTextContent(node);
        ManagementCenterConfig managementCenterConfig = config.getManagementCenterConfig();
        managementCenterConfig.setEnabled(enabled);
        managementCenterConfig.setUpdateInterval(interval);
        managementCenterConfig.setUrl("".equals(url) ? null : url);
    }

    private void handleSecurity(final org.w3c.dom.Node node) throws Exception {
        final NamedNodeMap atts = node.getAttributes();
        final Node enabledNode = atts.getNamedItem("enabled");
        final boolean enabled = enabledNode != null && checkTrue(getTextContent(enabledNode));
        config.getSecurityConfig().setEnabled(enabled);
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("member-credentials-factory".equals(nodeName)) {
                handleCredentialsFactory(child);
            } else if ("member-login-modules".equals(nodeName)) {
                handleLoginModules(child, true);
            } else if ("client-login-modules".equals(nodeName)) {
                handleLoginModules(child, false);
            } else if ("client-permission-policy".equals(nodeName)) {
                handlePermissionPolicy(child);
            } else if ("client-permissions".equals(nodeName)) {
                handleSecurityPermissions(child);
            } else if ("security-interceptors".equals(nodeName)) {
                handleSecurityInterceptors(child);
            }
        }
    }

    private void handleSecurityInterceptors(final org.w3c.dom.Node node) throws Exception {
        final SecurityConfig cfg = config.getSecurityConfig();
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("interceptor".equals(nodeName)) {
                final NamedNodeMap attrs = child.getAttributes();
                Node classNameNode = attrs.getNamedItem("class-name");
                String className = getTextContent(classNameNode);
                cfg.addSecurityInterceptorConfig(new SecurityInterceptorConfig(className));
            }
        }
    }

    private void handleMemberAttributes(final Node node) {
        for (Node n : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
            final String name = cleanNodeName(n.getNodeName());
            if (!"attribute".equals(name)) {
                continue;
            }
            final String attributeName = getTextContent(n.getAttributes().getNamedItem("name"));
            final String attributeType = getTextContent(n.getAttributes().getNamedItem("type"));
            final String value = getTextContent(n);
            if ("string".equals(attributeType)) {
                config.getMemberAttributeConfig().setStringAttribute(attributeName, value);
            } else if ("boolean".equals(attributeType)) {
                config.getMemberAttributeConfig().setBooleanAttribute(attributeName, Boolean.parseBoolean(value));
            } else if ("byte".equals(attributeType)) {
                config.getMemberAttributeConfig().setByteAttribute(attributeName, Byte.parseByte(value));
            } else if ("double".equals(attributeType)) {
                config.getMemberAttributeConfig().setDoubleAttribute(attributeName, Double.parseDouble(value));
            } else if ("float".equals(attributeType)) {
                config.getMemberAttributeConfig().setFloatAttribute(attributeName, Float.parseFloat(value));
            } else if ("int".equals(attributeType)) {
                config.getMemberAttributeConfig().setIntAttribute(attributeName, Integer.parseInt(value));
            } else if ("long".equals(attributeType)) {
                config.getMemberAttributeConfig().setLongAttribute(attributeName, Long.parseLong(value));
            } else if ("short".equals(attributeType)) {
                config.getMemberAttributeConfig().setShortAttribute(attributeName, Short.parseShort(value));
            } else {
                config.getMemberAttributeConfig().setStringAttribute(attributeName, value);
            }
        }
    }

    private void handleCredentialsFactory(final org.w3c.dom.Node node) throws Exception {
        final NamedNodeMap attrs = node.getAttributes();
        Node classNameNode = attrs.getNamedItem("class-name");
        String className = getTextContent(classNameNode);
        final SecurityConfig cfg = config.getSecurityConfig();
        final CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig(className);
        cfg.setMemberCredentialsConfig(credentialsFactoryConfig);
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("properties".equals(nodeName)) {
                fillProperties(child, credentialsFactoryConfig.getProperties());
                break;
            }
        }
    }

    private void handleLoginModules(final org.w3c.dom.Node node, boolean member) throws Exception {
        final SecurityConfig cfg = config.getSecurityConfig();
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("login-module".equals(nodeName)) {
                LoginModuleConfig lm = handleLoginModule(child);
                if (member) {
                    cfg.addMemberLoginModuleConfig(lm);
                } else {
                    cfg.addClientLoginModuleConfig(lm);
                }
            }
        }
    }

    private LoginModuleConfig handleLoginModule(final org.w3c.dom.Node node) throws Exception {
        final NamedNodeMap attrs = node.getAttributes();
        Node classNameNode = attrs.getNamedItem("class-name");
        String className = getTextContent(classNameNode);
        Node usageNode = attrs.getNamedItem("usage");
        LoginModuleUsage usage = usageNode != null ? LoginModuleUsage.get(getTextContent(usageNode))
                : LoginModuleUsage.REQUIRED;
        final LoginModuleConfig moduleConfig = new LoginModuleConfig(className, usage);
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("properties".equals(nodeName)) {
                fillProperties(child, moduleConfig.getProperties());
                break;
            }
        }
        return moduleConfig;
    }

    private void handlePermissionPolicy(final org.w3c.dom.Node node) throws Exception {
        final NamedNodeMap attrs = node.getAttributes();
        Node classNameNode = attrs.getNamedItem("class-name");
        String className = getTextContent(classNameNode);
        final SecurityConfig cfg = config.getSecurityConfig();
        final PermissionPolicyConfig policyConfig = new PermissionPolicyConfig(className);
        cfg.setClientPolicyConfig(policyConfig);
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("properties".equals(nodeName)) {
                fillProperties(child, policyConfig.getProperties());
                break;
            }
        }
    }

    private void handleSecurityPermissions(final org.w3c.dom.Node node) throws Exception {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            PermissionType type;
            if ("map-permission".equals(nodeName)) {
                type = PermissionType.MAP;
            } else if ("queue-permission".equals(nodeName)) {
                type = PermissionType.QUEUE;
            } else if ("multimap-permission".equals(nodeName)) {
                type = PermissionType.MULTIMAP;
            } else if ("topic-permission".equals(nodeName)) {
                type = PermissionType.TOPIC;
            } else if ("list-permission".equals(nodeName)) {
                type = PermissionType.LIST;
            } else if ("set-permission".equals(nodeName)) {
                type = PermissionType.SET;
            } else if ("lock-permission".equals(nodeName)) {
                type = PermissionType.LOCK;
            } else if ("atomic-long-permission".equals(nodeName)) {
                type = PermissionType.ATOMIC_LONG;
            } else if ("countdown-latch-permission".equals(nodeName)) {
                type = PermissionType.COUNTDOWN_LATCH;
            } else if ("semaphore-permission".equals(nodeName)) {
                type = PermissionType.SEMAPHORE;
            } else if ("id-generator-permission".equals(nodeName)) {
                type = PermissionType.ID_GENERATOR;
            } else if ("executor-service-permission".equals(nodeName)) {
                type = PermissionType.EXECUTOR_SERVICE;
            } else if ("transaction-permission".equals(nodeName)) {
                type = PermissionType.TRANSACTION;
            } else if ("all-permissions".equals(nodeName)) {
                type = PermissionType.ALL;
            } else {
                continue;
            }
            handleSecurityPermission(child, type);
        }
    }

    private void handleSecurityPermission(final org.w3c.dom.Node node, PermissionType type) throws Exception {
        final SecurityConfig cfg = config.getSecurityConfig();
        final NamedNodeMap attrs = node.getAttributes();
        Node nameNode = attrs.getNamedItem("name");
        String name = nameNode != null ? getTextContent(nameNode) : "*";
        Node principalNode = attrs.getNamedItem("principal");
        String principal = principalNode != null ? getTextContent(principalNode) : "*";
        final PermissionConfig permConfig = new PermissionConfig(type, name, principal);
        cfg.addClientPermissionConfig(permConfig);
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("endpoints".equals(nodeName)) {
                handleSecurityPermissionEndpoints(child, permConfig);
            } else if ("actions".equals(nodeName)) {
                handleSecurityPermissionActions(child, permConfig);
            }
        }
    }

    private void handleSecurityPermissionEndpoints(final org.w3c.dom.Node node, PermissionConfig permConfig)
            throws Exception {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("endpoint".equals(nodeName)) {
                permConfig.addEndpoint(getTextContent(child).trim());
            }
        }
    }

    private void handleSecurityPermissionActions(final org.w3c.dom.Node node, PermissionConfig permConfig)
            throws Exception {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("action".equals(nodeName)) {
                permConfig.addAction(getTextContent(child).trim());
            }
        }
    }

}
