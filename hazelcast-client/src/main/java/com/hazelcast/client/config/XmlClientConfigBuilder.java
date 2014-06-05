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

package com.hazelcast.client.config;

import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.AbstractXmlConfigHelper;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigLoader;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.util.ExceptionUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Loads the {@link com.hazelcast.client.config.ClientConfig} using XML.
 */
public class XmlClientConfigBuilder extends AbstractXmlConfigHelper {

    private static final ILogger logger = Logger.getLogger(XmlClientConfigBuilder.class);

    private ClientConfig clientConfig;
    private InputStream in;

    public XmlClientConfigBuilder(String resource) throws IOException {
        URL url = ConfigLoader.locateConfig(resource);
        if (url == null) {
            throw new IllegalArgumentException("Could not load " + resource);
        }
        this.in = url.openStream();
    }

    public XmlClientConfigBuilder(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("File is null!");
        }
        in = new FileInputStream(file);
    }

    public XmlClientConfigBuilder(URL url) throws IOException {
        if (url == null) {
            throw new NullPointerException("URL is null!");
        }
        in = url.openStream();
    }

    public XmlClientConfigBuilder(InputStream in) {
        this.in = in;
    }

    /**
     * Loads the client config using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hazelcast.client.config' is set. If it exist and it begins with
     * 'classpath:', then a classpath resource is loaded. Else it will assume it is a file reference</li>
     * <li>it checks if a hazelcast-client.xml is available in the working dir</li>
     * <li>it checks if a hazelcast-client.xml is available on the classpath</li>
     * <li>it loads the hazelcast-client-default.xml</li>
     * </ol>
     */
    public XmlClientConfigBuilder() {
        try {
            if (loadFromSystemProperty()) {
                return;
            }

            if (loadFromWorkingDirectory()) {
                return;
            }

            if (loadClientHazelcastXmlFromClasspath()) {
                return;
            }

            loadDefaultConfigurationFromClasspath();
        } catch (final RuntimeException e) {
            throw new HazelcastException("Failed to load ClientConfig", e);
        }
    }

    private void loadDefaultConfigurationFromClasspath() {
        logger.info("Loading 'hazelcast-client-default.xml' from classpath.");

        in = Config.class.getClassLoader().getResourceAsStream("hazelcast-client-default.xml");
        if (in == null) {
            throw new HazelcastException("Could not load 'hazelcast-client-default.xml' from classpath");
        }
    }

    private boolean loadClientHazelcastXmlFromClasspath() {
        URL url = Config.class.getClassLoader().getResource("hazelcast-client.xml");
        if (url == null) {
            logger.finest("Could not find 'hazelcast-client.xml' in classpath.");
            return false;
        }

        logger.info("Loading 'hazelcast-client.xml' from classpath.");

        in = Config.class.getClassLoader().getResourceAsStream("hazelcast-client.xml");
        if (in == null) {
            throw new HazelcastException("Could not load 'hazelcast-client.xml' from classpath");
        }
        return true;
    }

    private boolean loadFromWorkingDirectory() {
        File file = new File("hazelcast-client.xml");
        if (!file.exists()) {
            logger.finest("Could not find 'hazelcast-client.xml' in working directory.");
            return false;
        }

        logger.info("Loading 'hazelcast-client.xml' from working directory.");
        try {
            in = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new HazelcastException("Failed to open file: " + file.getAbsolutePath(), e);
        }
        return true;
    }

    private boolean loadFromSystemProperty() {
        String configSystemProperty = System.getProperty("hazelcast.client.config");

        if (configSystemProperty == null) {
            logger.finest("Could not 'hazelcast.client.config' System property");
            return false;
        }

        logger.info("Loading configuration " + configSystemProperty + " from System property 'hazelcast.client.config'");

        if (configSystemProperty.startsWith("classpath:")) {
            loadSystemPropertyClassPathResource(configSystemProperty);
        } else {
            loadSystemPropertyFileResource(configSystemProperty);
        }
        return true;
    }

    private void loadSystemPropertyFileResource(String configSystemProperty) {
        //it is a file.
        File configurationFile = new File(configSystemProperty);
        logger.info("Using configuration file at " + configurationFile.getAbsolutePath());

        if (!configurationFile.exists()) {
            String msg = "Config file at '" + configurationFile.getAbsolutePath() + "' doesn't exist.";
            throw new HazelcastException(msg);
        }

        try {
            in = new FileInputStream(configurationFile);
        } catch (FileNotFoundException e) {
            throw new HazelcastException("Failed to open file: " + configurationFile.getAbsolutePath(), e);
        }
    }

    private void loadSystemPropertyClassPathResource(String configSystemProperty) {
        //it is a explicit configured classpath resource.
        String resource = configSystemProperty.substring("classpath:".length());

        logger.info("Using classpath resource at " + resource);

        if (resource.isEmpty()) {
            throw new HazelcastException("classpath resource can't be empty");
        }

        in = Config.class.getClassLoader().getResourceAsStream(resource);
        if (in == null) {
            throw new HazelcastException("Could not load classpath resource: " + resource);
        }
    }


    public ClientConfig build() {
        return build(Thread.currentThread().getContextClassLoader());
    }

    public ClientConfig build(ClassLoader classLoader) {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClassLoader(classLoader);
        try {
            parse(clientConfig);
            return clientConfig;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private void parse(ClientConfig clientConfig) throws Exception {
        this.clientConfig = clientConfig;
        final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc;
        try {
            doc = builder.parse(in);
        } catch (final Exception e) {
            throw new IllegalStateException("Could not parse configuration file, giving up.");
        }
        Element element = doc.getDocumentElement();
        try {
            element.getTextContent();
        } catch (final Throwable e) {
            domLevel3 = false;
        }
        handleConfig(element);
    }

    private void handleConfig(final Element docElement) throws Exception {
        for (Node node : new IterableNodeList(docElement.getChildNodes())) {
            final String nodeName = cleanNodeName(node.getNodeName());
            if ("security".equals(nodeName)) {
                handleSecurity(node);
            } else if ("proxy-factories".equals(nodeName)) {
                handleProxyFactories(node);
            } else if ("properties".equals(nodeName)) {
                fillProperties(node, clientConfig.getProperties());
            } else if ("serialization".equals(nodeName)) {
                handleSerialization(node);
            } else if ("group".equals(nodeName)) {
                handleGroup(node);
            } else if ("listeners".equals(nodeName)) {
                handleListeners(node);
            } else if ("network".equals(nodeName)) {
                handleNetwork(node);
            } else if ("load-balancer".equals(nodeName)) {
                handleLoadBalancer(node);
            } else if ("near-cache".equals(nodeName)) {
                handleNearCache(node);
            } else if ("executor-pool-size".equals(nodeName)) {
                final int poolSize = Integer.parseInt(getTextContent(node));
                clientConfig.setExecutorPoolSize(poolSize);
            }
        }
    }

    private void handleNearCache(Node node) {
        final String name = getAttribute(node, "name");
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child);
            if ("max-size".equals(nodeName)) {
                nearCacheConfig.setMaxSize(Integer.parseInt(getTextContent(child)));
            } else if ("time-to-live-seconds".equals(nodeName)) {
                nearCacheConfig.setTimeToLiveSeconds(Integer.parseInt(getTextContent(child)));
            } else if ("max-idle-seconds".equals(nodeName)) {
                nearCacheConfig.setMaxIdleSeconds(Integer.parseInt(getTextContent(child)));
            } else if ("eviction-policy".equals(nodeName)) {
                nearCacheConfig.setEvictionPolicy(getTextContent(child));
            } else if ("in-memory-format".equals(nodeName)) {
                nearCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(getTextContent(child)));
            } else if ("invalidate-on-change".equals(nodeName)) {
                nearCacheConfig.setInvalidateOnChange(Boolean.parseBoolean(getTextContent(child)));
            } else if ("cache-local-entries".equals(nodeName)) {
                nearCacheConfig.setCacheLocalEntries(Boolean.parseBoolean(getTextContent(child)));
            }
        }
        clientConfig.addNearCacheConfig(name, nearCacheConfig);
    }

    private void handleLoadBalancer(Node node) {
        final String type = getAttribute(node, "type");
        if ("random".equals(type)) {
            clientConfig.setLoadBalancer(new RandomLB());
        } else if ("round-robin".equals(type)) {
            clientConfig.setLoadBalancer(new RoundRobinLB());
        }
    }

    private void handleNetwork(Node node) {
        final ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child);
            if ("cluster-members".equals(nodeName)) {
                handleClusterMembers(child, clientNetworkConfig);
            } else if ("smart-routing".equals(nodeName)) {
                clientNetworkConfig.setSmartRouting(Boolean.parseBoolean(getTextContent(child)));
            } else if ("redo-operation".equals(nodeName)) {
                clientNetworkConfig.setRedoOperation(Boolean.parseBoolean(getTextContent(child)));
            } else if ("connection-timeout".equals(nodeName)) {
                clientNetworkConfig.setConnectionTimeout(Integer.parseInt(getTextContent(child)));
            } else if ("connection-attempt-period".equals(nodeName)) {
                clientNetworkConfig.setConnectionAttemptPeriod(Integer.parseInt(getTextContent(child)));
            } else if ("connection-attempt-limit".equals(nodeName)) {
                clientNetworkConfig.setConnectionAttemptLimit(Integer.parseInt(getTextContent(child)));
            } else if ("socket-options".equals(nodeName)) {
                handleSocketOptions(child, clientNetworkConfig);
            } else if ("socket-interceptor".equals(nodeName)) {
                handleSocketInterceptorConfig(child, clientNetworkConfig);
            } else if ("ssl".equals(nodeName)) {
                handleSSLConfig(child, clientNetworkConfig);
            } else if ("aws".equals(nodeName)) {
                handleAWS(child, clientNetworkConfig);
            }
        }
        clientConfig.setNetworkConfig(clientNetworkConfig);
    }

    private void handleAWS(Node node, ClientNetworkConfig clientNetworkConfig) {
        final NamedNodeMap atts = node.getAttributes();
        final ClientAwsConfig clientAwsConfig = new ClientAwsConfig();
        for (int a = 0; a < atts.getLength(); a++) {
            final Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if ("enabled".equalsIgnoreCase(att.getNodeName())) {
                clientAwsConfig.setEnabled(checkTrue(value));
            } else if (att.getNodeName().equals("connection-timeout-seconds")) {
                clientAwsConfig.setConnectionTimeoutSeconds(getIntegerValue("connection-timeout-seconds", value, 5));
            }
        }
        for (Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            if ("secret-key".equals(cleanNodeName(n.getNodeName()))) {
                clientAwsConfig.setSecretKey(value);
            } else if ("access-key".equals(cleanNodeName(n.getNodeName()))) {
                clientAwsConfig.setAccessKey(value);
            } else if ("region".equals(cleanNodeName(n.getNodeName()))) {
                clientAwsConfig.setRegion(value);
            } else if ("host-header".equals(cleanNodeName(n.getNodeName()))) {
                clientAwsConfig.setHostHeader(value);
            } else if ("security-group-name".equals(cleanNodeName(n.getNodeName()))) {
                clientAwsConfig.setSecurityGroupName(value);
            } else if ("tag-key".equals(cleanNodeName(n.getNodeName()))) {
                clientAwsConfig.setTagKey(value);
            } else if ("tag-value".equals(cleanNodeName(n.getNodeName()))) {
                clientAwsConfig.setTagValue(value);
            } else if ("inside-aws".equals(cleanNodeName(n.getNodeName()))) {
                clientAwsConfig.setInsideAws(checkTrue(value));
            }
        }
        clientNetworkConfig.setAwsConfig(clientAwsConfig);
    }

    private void handleSSLConfig(final org.w3c.dom.Node node, ClientNetworkConfig clientNetworkConfig) {
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
        clientNetworkConfig.setSSLConfig(sslConfig);
    }

    private void handleSocketOptions(Node node, ClientNetworkConfig clientNetworkConfig) {
        SocketOptions socketOptions = clientConfig.getSocketOptions();
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child);
            if ("tcp-no-delay".equals(nodeName)) {
                socketOptions.setTcpNoDelay(Boolean.parseBoolean(getTextContent(child)));
            } else if ("keep-alive".equals(nodeName)) {
                socketOptions.setKeepAlive(Boolean.parseBoolean(getTextContent(child)));
            } else if ("reuse-address".equals(nodeName)) {
                socketOptions.setReuseAddress(Boolean.parseBoolean(getTextContent(child)));
            } else if ("linger-seconds".equals(nodeName)) {
                socketOptions.setLingerSeconds(Integer.parseInt(getTextContent(child)));
            } else if ("buffer-size".equals(nodeName)) {
                socketOptions.setBufferSize(Integer.parseInt(getTextContent(child)));
            }
        }
        clientNetworkConfig.setSocketOptions(socketOptions);
    }

    private void handleClusterMembers(Node node, ClientNetworkConfig clientNetworkConfig) {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            if ("address".equals(cleanNodeName(child))) {
                clientNetworkConfig.addAddress(getTextContent(child));
            }
        }
    }

    private void handleListeners(Node node) throws Exception {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            if ("listener".equals(cleanNodeName(child))) {
                String className = getTextContent(child);
                clientConfig.addListenerConfig(new ListenerConfig(className));
            }
        }
    }

    private void handleGroup(Node node) {
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("name".equals(nodeName)) {
                clientConfig.getGroupConfig().setName(value);
            } else if ("password".equals(nodeName)) {
                clientConfig.getGroupConfig().setPassword(value);
            }
        }
    }

    private void handleSerialization(Node node) {
        SerializationConfig serializationConfig = parseSerialization(node);
        clientConfig.setSerializationConfig(serializationConfig);
    }


    private void handleProxyFactories(Node node) throws Exception {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("proxy-factory".equals(nodeName)) {
                handleProxyFactory(child);
            }
        }
    }

    private void handleProxyFactory(Node node) throws Exception {
        final String service = getAttribute(node, "service");
        final String className = getAttribute(node, "class-name");

        final ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig(className, service);
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);
    }

    private void handleSocketInterceptorConfig(final org.w3c.dom.Node node, ClientNetworkConfig clientNetworkConfig) {
        SocketInterceptorConfig socketInterceptorConfig = parseSocketInterceptorConfig(node);
        clientNetworkConfig.setSocketInterceptorConfig(socketInterceptorConfig);
    }

    private void handleSecurity(Node node) throws Exception {
        ClientSecurityConfig clientSecurityConfig = new ClientSecurityConfig();
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("credentials".equals(nodeName)) {
                String className = getTextContent(child);
                clientSecurityConfig.setCredentialsClassname(className);
            }
        }
        clientConfig.setSecurityConfig(clientSecurityConfig);
    }

}
