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

import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PartitionGroupConfig.MemberGroupType;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.ServiceConfigurationParser;
import com.hazelcast.util.ExceptionUtil;
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
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import static com.hazelcast.util.StringUtil.upperCaseInternal;

/**
 * A XML {@link ConfigBuilder} implementation.
 */
public class XmlConfigBuilder extends AbstractXmlConfigHelper implements ConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(XmlConfigBuilder.class);

    private static final int DEFAULT_VALUE = 5;
    private static final int THOUSAND_FACTOR = 5;
    private static final int FIVE = 5;

    private Config config;
    private InputStream in;
    private File configurationFile;
    private URL configurationUrl;
    private Properties properties = System.getProperties();

    /**
     * Constructs a XmlConfigBuilder that reads from the provided file.
     *
     * @param xmlFileName the name of the XML file
     * @throws FileNotFoundException if the file can't be found.
     */
    public XmlConfigBuilder(String xmlFileName) throws FileNotFoundException {
        this(new FileInputStream(xmlFileName));
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
     * @return the used properties.
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
    public Config build() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return build(config);
    }

    Config build(Config config) {
        config.setConfigurationFile(configurationFile);
        config.setConfigurationUrl(configurationUrl);
        try {
            parse(config);
        } catch (Exception e) {
            throw new HazelcastException(e.getMessage(), e);
        }
        return config;
    }

    private void parse(final Config config) throws Exception {
        this.config = config;
        final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc;
        try {
            doc = builder.parse(in);
        } catch (final Exception e) {
            if (configurationFile != null) {
                String msg = "Failed to parse " + configurationFile
                        + "\nException: " + e.getMessage()
                        + "\nHazelcast startup interrupted.";
                LOGGER.severe(msg);

            } else if (configurationUrl != null) {
                String msg = "Failed to parse " + configurationUrl
                        + "\nException: " + e.getMessage()
                        + "\nHazelcast startup interrupted.";
                LOGGER.severe(msg);
            } else {
                String msg = "Failed to parse the inputstream"
                        + "\nException: " + e.getMessage()
                        + "\nHazelcast startup interrupted.";
                LOGGER.severe(msg);

            }
            throw e;
        } finally {
            IOUtil.closeResource(in);
        }
        Element element = doc.getDocumentElement();
        try {
            element.getTextContent();
        } catch (final Throwable e) {
            domLevel3 = false;
        }
        preprocess(element);
        handleConfig(element);
    }

    private void preprocess(Node root) {
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
            preprocess(child);
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
                LOGGER.warning("Bad variable syntax. Could not find a closing curly bracket '}' on node: " + node.getLocalName());
                break;
            }
            String variable = value.substring(startIndex + 2, endIndex);
            String variableReplacement = properties.getProperty(variable);
            if (variableReplacement != null) {
                sb.append(variableReplacement);
            } else {
                sb.append(value.substring(startIndex, endIndex + 1));
                LOGGER.warning("Could not find a value for property  '" + variable + "' on node: " + node.getLocalName());
            }

            startIndex = value.indexOf("${", endIndex);
        }

        sb.append(value.substring(endIndex + 1));
        node.setNodeValue(sb.toString());
    }

    private void handleConfig(final Element docElement) throws Exception {
        for (org.w3c.dom.Node node : new IterableNodeList(docElement.getChildNodes())) {
            final String nodeName = cleanNodeName(node.getNodeName());
            if ("network".equals(nodeName)) {
                handleNetwork(node);
            } else if ("group".equals(nodeName)) {
                handleGroup(node);
            } else if ("properties".equals(nodeName)) {
                fillProperties(node, config.getProperties());
            } else if ("wan-replication".equals(nodeName)) {
                handleWanReplication(node);
            } else if ("executor-service".equals(nodeName)) {
                handleExecutor(node);
            } else if ("services".equals(nodeName)) {
                handleServices(node);
            } else if ("queue".equals(nodeName)) {
                handleQueue(node);
            } else if ("map".equals(nodeName)) {
                handleMap(node);
            } else if ("multimap".equals(nodeName)) {
                handleMultiMap(node);
            } else if ("replicatedmap".equals(nodeName)) {
                handleReplicatedMap(node);
            } else if ("list".equals(nodeName)) {
                handleList(node);
            } else if ("set".equals(nodeName)) {
                handleSet(node);
            } else if ("topic".equals(nodeName)) {
                handleTopic(node);
            } else if ("jobtracker".equals(nodeName)) {
                handleJobTracker(node);
            } else if ("semaphore".equals(nodeName)) {
                handleSemaphore(node);
            } else if ("listeners".equals(nodeName)) {
                handleListeners(node);
            } else if ("partition-group".equals(nodeName)) {
                handlePartitionGroup(node);
            } else if ("serialization".equals(nodeName)) {
                handleSerialization(node);
            } else if ("security".equals(nodeName)) {
                handleSecurity(node);
            } else if ("member-attributes".equals(nodeName)) {
                handleMemberAttributes(node);
            } else if ("license-key".equals(nodeName)) {
                config.setLicenseKey(getTextContent(node));
            } else if ("management-center".equals(nodeName)) {
                handleManagementCenterConfig(node);
            }
        }
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
                            throw new IllegalArgumentException("Parser is required!");
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
        final WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName(name);
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
            }
        }

        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.verify();
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
            }
            if ("clientDiscoveryEnabled".equalsIgnoreCase(att.getNodeName())) {
                multicastConfig.setClientDiscoveryEnabled(checkTrue(value));
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
            if (cleanNodeName(n.getNodeName()).equals("required-member")) {
                tcpIpConfig.setRequiredMember(value);
            } else if (memberTags.contains(cleanNodeName(n.getNodeName()))) {
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
            }
        }
        this.config.addReplicatedMapConfig(replicatedMapConfig);
    }

    private void handleMap(final org.w3c.dom.Node node) throws Exception {
        final String name = getAttribute(node, "name");
        final MapConfig mapConfig = new MapConfig();
        mapConfig.setName(name);
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            final String value = getTextContent(n).trim();
            if ("backup-count".equals(nodeName)) {
                mapConfig.setBackupCount(getIntegerValue("backup-count", value, MapConfig.DEFAULT_BACKUP_COUNT));
            } else if ("in-memory-format".equals(nodeName)) {
                mapConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("async-backup-count".equals(nodeName)) {
                mapConfig.setAsyncBackupCount(getIntegerValue("async-backup-count", value, MapConfig.MIN_BACKUP_COUNT));
            } else if ("eviction-policy".equals(nodeName)) {
                mapConfig.setEvictionPolicy(MapConfig.EvictionPolicy.valueOf(upperCaseInternal(value)));
            } else if ("max-size".equals(nodeName)) {
                final MaxSizeConfig msc = mapConfig.getMaxSizeConfig();
                final Node maxSizePolicy = n.getAttributes().getNamedItem("policy");
                if (maxSizePolicy != null) {
                    msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.valueOf(upperCaseInternal(getTextContent(maxSizePolicy))));
                }
                int size = sizeParser(value);
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
                MapStoreConfig mapStoreConfig = createMapStoreConfig(n);
                mapConfig.setMapStoreConfig(mapStoreConfig);
            } else if ("near-cache".equals(nodeName)) {
                handleViaReflection(n, mapConfig, new NearCacheConfig());
            } else if ("merge-policy".equals(nodeName)) {
                mapConfig.setMergePolicy(value);
            } else if ("read-backup-data".equals(nodeName)) {
                mapConfig.setReadBackupData(checkTrue(value));
            } else if ("statistics-enabled".equals(nodeName)) {
                mapConfig.setStatisticsEnabled(checkTrue(value));
            } else if ("wan-replication-ref".equals(nodeName)) {
                mapWanReplicationRefHandle(n, mapConfig);
            } else if ("indexes".equals(nodeName)) {
                mapIndexesHandle(n, mapConfig);
            } else if ("entry-listeners".equals(nodeName)) {
                mapEntryListenerHandle(n, mapConfig);
            } else if ("partition-strategy".equals(nodeName)) {
                mapConfig.setPartitioningStrategyConfig(new PartitioningStrategyConfig(value));
            }
        }
        this.config.addMapConfig(mapConfig);
    }

    private void mapWanReplicationRefHandle(Node n, MapConfig mapConfig) {
        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        final String wanName = getAttribute(n, "name");
        wanReplicationRef.setName(wanName);
        for (org.w3c.dom.Node wanChild : new IterableNodeList(n.getChildNodes())) {
            final String wanChildName = cleanNodeName(wanChild.getNodeName());
            final String wanChildValue = getTextContent(n);
            if ("merge-policy".equals(wanChildName)) {
                wanReplicationRef.setMergePolicy(wanChildValue);
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
                getTextContent(intervalNode), DEFAULT_VALUE) : FIVE;

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
