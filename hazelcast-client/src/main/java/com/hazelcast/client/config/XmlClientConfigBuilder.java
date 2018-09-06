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

package com.hazelcast.client.config;

import com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.AbstractConfigBuilder;
import com.hazelcast.config.ConfigLoader;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AliasedDiscoveryConfigMapper;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.util.ExceptionUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.client.config.ClientXmlElements.CONNECTION_STRATEGY;
import static com.hazelcast.client.config.ClientXmlElements.EXECUTOR_POOL_SIZE;
import static com.hazelcast.client.config.ClientXmlElements.FLAKE_ID_GENERATOR;
import static com.hazelcast.client.config.ClientXmlElements.GROUP;
import static com.hazelcast.client.config.ClientXmlElements.INSTANCE_NAME;
import static com.hazelcast.client.config.ClientXmlElements.LICENSE_KEY;
import static com.hazelcast.client.config.ClientXmlElements.LISTENERS;
import static com.hazelcast.client.config.ClientXmlElements.LOAD_BALANCER;
import static com.hazelcast.client.config.ClientXmlElements.NATIVE_MEMORY;
import static com.hazelcast.client.config.ClientXmlElements.NEAR_CACHE;
import static com.hazelcast.client.config.ClientXmlElements.NETWORK;
import static com.hazelcast.client.config.ClientXmlElements.PROPERTIES;
import static com.hazelcast.client.config.ClientXmlElements.PROXY_FACTORIES;
import static com.hazelcast.client.config.ClientXmlElements.QUERY_CACHES;
import static com.hazelcast.client.config.ClientXmlElements.RELIABLE_TOPIC;
import static com.hazelcast.client.config.ClientXmlElements.SECURITY;
import static com.hazelcast.client.config.ClientXmlElements.SERIALIZATION;
import static com.hazelcast.client.config.ClientXmlElements.USER_CODE_DEPLOYMENT;
import static com.hazelcast.client.config.ClientXmlElements.canOccurMultipleTimes;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static com.hazelcast.util.StringUtil.upperCaseInternal;

/**
 * Loads the {@link com.hazelcast.client.config.ClientConfig} using XML.
 */
@SuppressWarnings("checkstyle:methodcount")
public class XmlClientConfigBuilder extends AbstractConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(XmlClientConfigBuilder.class);

    private final QueryCacheConfigBuilderHelper queryCacheConfigBuilderHelper = new QueryCacheConfigBuilderHelper();

    private final Set<String> occurrenceSet = new HashSet<String>();
    private final InputStream in;

    private Properties properties = System.getProperties();
    private ClientConfig clientConfig;

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
        this.in = new FileInputStream(file);
    }

    public XmlClientConfigBuilder(URL url) throws IOException {
        if (url == null) {
            throw new NullPointerException("URL is null!");
        }
        this.in = url.openStream();
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
        XmlClientConfigLocator locator = new XmlClientConfigLocator();
        this.in = locator.getIn();
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
            String msg = "Failed to parse Config Stream"
                    + LINE_SEPARATOR + "Exception: " + e.getMessage()
                    + LINE_SEPARATOR + "HazelcastClient startup interrupted.";
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

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    protected ConfigType getXmlType() {
        return ConfigType.CLIENT;
    }

    public ClientConfig build() {
        return build(Thread.currentThread().getContextClassLoader());
    }

    public ClientConfig build(ClassLoader classLoader) {
        ClientConfig clientConfig = new ClientConfig();
        build(clientConfig, classLoader);
        return clientConfig;
    }

    void build(ClientConfig clientConfig, ClassLoader classLoader) {
        clientConfig.setClassLoader(classLoader);
        try {
            parseAndBuildConfig(clientConfig);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private void parseAndBuildConfig(ClientConfig clientConfig) throws Exception {
        this.clientConfig = clientConfig;
        Document doc = parse(in);
        Element root = doc.getDocumentElement();
        checkRootElement(root);
        try {
            root.getTextContent();
        } catch (Throwable e) {
            domLevel3 = false;
        }
        process(root);
        schemaValidation(root.getOwnerDocument());
        handleConfig(root);
    }

    private void checkRootElement(Element root) {
        String rootNodeName = root.getNodeName();
        if (!ClientXmlElements.HAZELCAST_CLIENT.isEqual(rootNodeName)) {
            throw new InvalidConfigurationException("Invalid root element in xml configuration! "
                    + "Expected: <" + ClientXmlElements.HAZELCAST_CLIENT.name + ">, Actual: <" + rootNodeName + ">.");
        }
    }

    private void handleConfig(Element docElement) throws Exception {
        for (Node node : childElements(docElement)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException("Duplicate '" + nodeName + "' definition found in XML configuration");
            }
            handleXmlNode(node, nodeName);
            if (!canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }
    }

    private void handleXmlNode(Node node, String nodeName) throws Exception {
        if (SECURITY.isEqual(nodeName)) {
            handleSecurity(node);
        } else if (PROXY_FACTORIES.isEqual(nodeName)) {
            handleProxyFactories(node);
        } else if (PROPERTIES.isEqual(nodeName)) {
            fillProperties(node, clientConfig.getProperties());
        } else if (SERIALIZATION.isEqual(nodeName)) {
            handleSerialization(node);
        } else if (NATIVE_MEMORY.isEqual(nodeName)) {
            fillNativeMemoryConfig(node, clientConfig.getNativeMemoryConfig());
        } else if (GROUP.isEqual(nodeName)) {
            handleGroup(node);
        } else if (LISTENERS.isEqual(nodeName)) {
            handleListeners(node);
        } else if (NETWORK.isEqual(nodeName)) {
            handleNetwork(node);
        } else if (LOAD_BALANCER.isEqual(nodeName)) {
            handleLoadBalancer(node);
        } else if (NEAR_CACHE.isEqual(nodeName)) {
            handleNearCache(node);
        } else if (QUERY_CACHES.isEqual(nodeName)) {
            queryCacheConfigBuilderHelper.handleQueryCache(clientConfig, node);
        } else if (EXECUTOR_POOL_SIZE.isEqual(nodeName)) {
            handleExecutorPoolSize(node);
        } else if (LICENSE_KEY.isEqual(nodeName)) {
            clientConfig.setLicenseKey(getTextContent(node));
        } else if (INSTANCE_NAME.isEqual(nodeName)) {
            clientConfig.setInstanceName(getTextContent(node));
        } else if (CONNECTION_STRATEGY.isEqual(nodeName)) {
            handleConnectionStrategy(node);
        } else if (USER_CODE_DEPLOYMENT.isEqual(nodeName)) {
            handleUserCodeDeployment(node);
        } else if (FLAKE_ID_GENERATOR.isEqual(nodeName)) {
            handleFlakeIdGenerator(node);
        } else if (RELIABLE_TOPIC.isEqual(nodeName)) {
            handleReliableTopic(node);
        }
    }

    private void handleConnectionStrategy(Node node) {
        ClientConnectionStrategyConfig strategyConfig = new ClientConnectionStrategyConfig();
        String attrValue = getAttribute(node, "async-start");
        strategyConfig.setAsyncStart(attrValue != null && getBooleanValue(attrValue.trim()));
        attrValue = getAttribute(node, "reconnect-mode");
        if (attrValue != null) {
            strategyConfig.setReconnectMode(ReconnectMode.valueOf(upperCaseInternal(attrValue.trim())));
        }
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("connection-retry".equals(nodeName)) {
                handleConnectionRetry(child, strategyConfig);
            }
        }
        clientConfig.setConnectionStrategyConfig(strategyConfig);
    }

    private void handleConnectionRetry(Node node, ClientConnectionStrategyConfig strategyConfig) {
        Node enabledNode = node.getAttributes().getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        if (!enabled) {
            LOGGER.warning("Exponential Connection Strategy is not enabled.");
        }
        ConnectionRetryConfig connectionRetryConfig = new ConnectionRetryConfig();
        connectionRetryConfig.setEnabled(enabled);

        String initialBackoffMillis = "initial-backoff-millis";
        String maxBackoffMillis = "max-backoff-millis";
        String multiplier = "multiplier";
        String jitter = "jitter";
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if (initialBackoffMillis.equals(nodeName)) {
                connectionRetryConfig.setInitialBackoffMillis(getIntegerValue(initialBackoffMillis, value));
            } else if (maxBackoffMillis.equals(nodeName)) {
                connectionRetryConfig.setMaxBackoffMillis(getIntegerValue(maxBackoffMillis, value));
            } else if (multiplier.equals(nodeName)) {
                connectionRetryConfig.setMultiplier(getIntegerValue(multiplier, value));
            } else if ("fail-on-max-backoff".equals(nodeName)) {
                connectionRetryConfig.setFailOnMaxBackoff(getBooleanValue(value));
            } else if (jitter.equals(nodeName)) {
                connectionRetryConfig.setJitter(getDoubleValue(jitter, value));
            }
        }
        strategyConfig.setConnectionRetryConfig(connectionRetryConfig);
    }

    private void handleUserCodeDeployment(Node node) {
        NamedNodeMap atts = node.getAttributes();
        Node enabledNode = atts.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        ClientUserCodeDeploymentConfig userCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        userCodeDeploymentConfig.setEnabled(enabled);

        for (Node child : childElements(node)) {
            String childNodeName = cleanNodeName(child);
            if ("classnames".equals(childNodeName)) {
                for (Node classNameNode : childElements(child)) {
                    userCodeDeploymentConfig.addClass(getTextContent(classNameNode));
                }
            } else if ("jarpaths".equals(childNodeName)) {
                for (Node jarPathNode : childElements(child)) {
                    userCodeDeploymentConfig.addJar(getTextContent(jarPathNode));
                }
            } else {
                throw new InvalidConfigurationException("User code deployement can either be className or jarPath. "
                        + childNodeName + " is invalid");
            }
        }
        clientConfig.setUserCodeDeploymentConfig(userCodeDeploymentConfig);
    }

    private void handleExecutorPoolSize(Node node) {
        int poolSize = Integer.parseInt(getTextContent(node));
        clientConfig.setExecutorPoolSize(poolSize);
    }

    private void handleNearCache(Node node) {
        String name = getAttribute(node, "name");
        NearCacheConfig nearCacheConfig = new NearCacheConfig(name);
        Boolean serializeKeys = null;
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if ("max-size".equals(nodeName)) {
                nearCacheConfig.setMaxSize(Integer.parseInt(value));
                LOGGER.warning("The element <max-size/> for <near-cache/> is deprecated, please use <eviction/> instead!");
            } else if ("time-to-live-seconds".equals(nodeName)) {
                nearCacheConfig.setTimeToLiveSeconds(Integer.parseInt(value));
            } else if ("max-idle-seconds".equals(nodeName)) {
                nearCacheConfig.setMaxIdleSeconds(Integer.parseInt(value));
            } else if ("eviction-policy".equals(nodeName)) {
                nearCacheConfig.setEvictionPolicy(value);
                LOGGER.warning("The element <eviction-policy/> for <near-cache/> is deprecated, please use <eviction/> instead!");
            } else if ("in-memory-format".equals(nodeName)) {
                nearCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("serialize-keys".equals(nodeName)) {
                serializeKeys = Boolean.parseBoolean(value);
                nearCacheConfig.setSerializeKeys(serializeKeys);
            } else if ("invalidate-on-change".equals(nodeName)) {
                nearCacheConfig.setInvalidateOnChange(Boolean.parseBoolean(value));
            } else if ("cache-local-entries".equals(nodeName)) {
                nearCacheConfig.setCacheLocalEntries(Boolean.parseBoolean(value));
            } else if ("local-update-policy".equals(nodeName)) {
                nearCacheConfig.setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.valueOf(value));
            } else if ("eviction".equals(nodeName)) {
                nearCacheConfig.setEvictionConfig(getEvictionConfig(child));
            } else if ("preloader".equals(nodeName)) {
                nearCacheConfig.setPreloaderConfig(getNearCachePreloaderConfig(child));
            }
        }
        if (serializeKeys != null && !serializeKeys && nearCacheConfig.getInMemoryFormat() == InMemoryFormat.NATIVE) {
            LOGGER.warning("The Near Cache doesn't support keys by-reference with NATIVE in-memory-format."
                    + " This setting will have no effect!");
        }
        clientConfig.addNearCacheConfig(nearCacheConfig);
    }

    private void handleFlakeIdGenerator(Node node) {
        String name = getAttribute(node, "name");
        ClientFlakeIdGeneratorConfig config = new ClientFlakeIdGeneratorConfig(name);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if ("prefetch-count".equals(nodeName)) {
                config.setPrefetchCount(Integer.parseInt(value));
            } else if ("prefetch-validity-millis".equalsIgnoreCase(nodeName)) {
                config.setPrefetchValidityMillis(Long.parseLong(value));
            }
        }
        clientConfig.addFlakeIdGeneratorConfig(config);
    }

    private void handleReliableTopic(Node node) {
        String name = getAttribute(node, "name");
        ClientReliableTopicConfig config = new ClientReliableTopicConfig(name);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if ("topic-overload-policy".equals(nodeName)) {
                config.setTopicOverloadPolicy(TopicOverloadPolicy.valueOf(value));
            } else if ("read-batch-size".equalsIgnoreCase(nodeName)) {
                config.setReadBatchSize(Integer.parseInt(value));
            }
        }
        clientConfig.addReliableTopicConfig(config);
    }

    private EvictionConfig getEvictionConfig(Node node) {
        EvictionConfig evictionConfig = new EvictionConfig();
        Node size = node.getAttributes().getNamedItem("size");
        Node maxSizePolicy = node.getAttributes().getNamedItem("max-size-policy");
        Node evictionPolicy = node.getAttributes().getNamedItem("eviction-policy");
        if (size != null) {
            evictionConfig.setSize(Integer.parseInt(getTextContent(size)));
        }
        if (maxSizePolicy != null) {
            evictionConfig.setMaximumSizePolicy(MaxSizePolicy.valueOf(upperCaseInternal(getTextContent(maxSizePolicy)))
            );
        }
        if (evictionPolicy != null) {
            evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(upperCaseInternal(getTextContent(evictionPolicy)))
            );
        }
        return evictionConfig;
    }

    private NearCachePreloaderConfig getNearCachePreloaderConfig(Node node) {
        NearCachePreloaderConfig preloaderConfig = new NearCachePreloaderConfig();
        String enabled = getAttribute(node, "enabled");
        String directory = getAttribute(node, "directory");
        String storeInitialDelaySeconds = getAttribute(node, "store-initial-delay-seconds");
        String storeIntervalSeconds = getAttribute(node, "store-interval-seconds");
        if (enabled != null) {
            preloaderConfig.setEnabled(getBooleanValue(enabled));
        }
        if (directory != null) {
            preloaderConfig.setDirectory(directory);
        }
        if (storeInitialDelaySeconds != null) {
            preloaderConfig.setStoreInitialDelaySeconds(getIntegerValue("storage-initial-delay-seconds",
                    storeInitialDelaySeconds));
        }
        if (storeIntervalSeconds != null) {
            preloaderConfig.setStoreIntervalSeconds(getIntegerValue("storage-interval-seconds", storeIntervalSeconds));
        }
        return preloaderConfig;
    }

    private void handleLoadBalancer(Node node) {
        String type = getAttribute(node, "type");
        if ("random".equals(type)) {
            clientConfig.setLoadBalancer(new RandomLB());
        } else if ("round-robin".equals(type)) {
            clientConfig.setLoadBalancer(new RoundRobinLB());
        }
    }

    private void handleNetwork(Node node) {
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
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
            } else if (AliasedDiscoveryConfigMapper.supports(nodeName)) {
                handleAliasedDiscoveryStrategy(child, clientNetworkConfig, nodeName);
            } else if ("discovery-strategies".equals(nodeName)) {
                handleDiscoveryStrategies(child, clientNetworkConfig);
            } else if ("outbound-ports".equals(nodeName)) {
                handleOutboundPorts(child, clientNetworkConfig);
            } else if ("icmp-ping".equals(nodeName)) {
                handleIcmpPing(child, clientNetworkConfig);
            } else if ("hazelcast-cloud".equals(nodeName)) {
                handleHazelcastCloud(child, clientNetworkConfig);
            }
        }
        clientConfig.setNetworkConfig(clientNetworkConfig);
    }

    private void handleHazelcastCloud(Node node, ClientNetworkConfig clientNetworkConfig) {
        ClientCloudConfig cloudConfig = clientNetworkConfig.getCloudConfig();

        NamedNodeMap atts = node.getAttributes();
        Node enabledNode = atts.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        cloudConfig.setEnabled(enabled);

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("discovery-token".equals(nodeName)) {
                cloudConfig.setDiscoveryToken(getTextContent(child));
            }
        }
    }

    private void handleIcmpPing(Node node, ClientNetworkConfig clientNetworkConfig) {
        ClientIcmpPingConfig icmpPingConfig = clientNetworkConfig.getClientIcmpPingConfig();

        NamedNodeMap atts = node.getAttributes();
        Node enabledNode = atts.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        icmpPingConfig.setEnabled(enabled);

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("timeout-milliseconds".equals(nodeName)) {
                icmpPingConfig.setTimeoutMilliseconds(Integer.parseInt(getTextContent(child)));
            } else if ("interval-milliseconds".equals(nodeName)) {
                icmpPingConfig.setIntervalMilliseconds(Integer.parseInt(getTextContent(child)));
            } else if ("ttl".equals(nodeName)) {
                icmpPingConfig.setTtl(Integer.parseInt(getTextContent(child)));
            } else if ("max-attempts".equals(nodeName)) {
                icmpPingConfig.setMaxAttempts(Integer.parseInt(getTextContent(child)));
            } else if ("echo-fail-fast-on-startup".equals(nodeName)) {
                icmpPingConfig.setEchoFailFastOnStartup(Boolean.parseBoolean(getTextContent(child)));
            }
        }
        clientNetworkConfig.setClientIcmpPingConfig(icmpPingConfig);
    }

    private void handleDiscoveryStrategies(Node node, ClientNetworkConfig clientNetworkConfig) {
        DiscoveryConfig discoveryConfig = clientNetworkConfig.getDiscoveryConfig();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if ("discovery-strategy".equals(name)) {
                handleDiscoveryStrategy(child, discoveryConfig);
            } else if ("node-filter".equals(name)) {
                handleDiscoveryNodeFilter(child, discoveryConfig);
            }
        }
    }

    private void handleDiscoveryNodeFilter(Node node, DiscoveryConfig discoveryConfig) {
        NamedNodeMap atts = node.getAttributes();
        Node att = atts.getNamedItem("class");
        if (att != null) {
            discoveryConfig.setNodeFilterClass(getTextContent(att).trim());
        }
    }

    private void handleDiscoveryStrategy(Node node, DiscoveryConfig discoveryConfig) {
        NamedNodeMap atts = node.getAttributes();

        boolean enabled = false;
        String clazz = null;

        for (int a = 0; a < atts.getLength(); a++) {
            Node att = atts.item(a);
            String value = getTextContent(att).trim();
            if ("enabled".equalsIgnoreCase(att.getNodeName())) {
                enabled = getBooleanValue(value);
            } else if ("class".equals(att.getNodeName())) {
                clazz = value;
            }
        }

        if (!enabled || clazz == null) {
            return;
        }

        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if ("properties".equals(name)) {
                fillProperties(child, properties);
            }
        }

        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(clazz, properties));
    }

    private void handleAliasedDiscoveryStrategy(Node node, ClientNetworkConfig clientNetworkConfig, String tag) {
        AliasedDiscoveryConfig config = new AliasedDiscoveryConfig();
        config.setEnvironment(tag);
        NamedNodeMap atts = node.getAttributes();
        for (int i = 0; i < atts.getLength(); i++) {
            Node att = atts.item(i);
            String value = getTextContent(att).trim();
            if ("enabled".equalsIgnoreCase(att.getNodeName())) {
                config.setEnabled(getBooleanValue(value));
            } else if (att.getNodeName().equals("connection-timeout-seconds")) {
                config.addProperty("connection-timeout-seconds", value);
            }
        }
        for (Node n : childElements(node)) {
            String key = cleanNodeName(n);
            String value = getTextContent(n).trim();
            config.addProperty(key, value);
        }
        clientNetworkConfig.addAliasedDiscoveryConfig(config);
    }

    private void handleSSLConfig(Node node, ClientNetworkConfig clientNetworkConfig) {
        SSLConfig sslConfig = new SSLConfig();
        NamedNodeMap atts = node.getAttributes();
        Node enabledNode = atts.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        sslConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
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
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
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
        for (Node child : childElements(node)) {
            if ("address".equals(cleanNodeName(child))) {
                clientNetworkConfig.addAddress(getTextContent(child));
            }
        }
    }

    private void handleListeners(Node node) throws Exception {
        for (Node child : childElements(node)) {
            if ("listener".equals(cleanNodeName(child))) {
                String className = getTextContent(child);
                clientConfig.addListenerConfig(new ListenerConfig(className));
            }
        }
    }

    private void handleGroup(Node node) {
        for (Node n : childElements(node)) {
            String value = getTextContent(n).trim();
            String nodeName = cleanNodeName(n);
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
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("proxy-factory".equals(nodeName)) {
                handleProxyFactory(child);
            }
        }
    }

    private void handleProxyFactory(Node node) throws Exception {
        String service = getAttribute(node, "service");
        String className = getAttribute(node, "class-name");

        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig(className, service);
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);
    }

    private void handleSocketInterceptorConfig(Node node, ClientNetworkConfig clientNetworkConfig) {
        SocketInterceptorConfig socketInterceptorConfig = parseSocketInterceptorConfig(node);
        clientNetworkConfig.setSocketInterceptorConfig(socketInterceptorConfig);
    }

    private void handleSecurity(Node node) throws Exception {
        ClientSecurityConfig clientSecurityConfig = new ClientSecurityConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("credentials".equals(nodeName)) {
                String className = getTextContent(child);
                clientSecurityConfig.setCredentialsClassname(className);
            } else if ("credentials-factory".equals(nodeName)) {
                handleCredentialsFactory(child, clientSecurityConfig);
            }
        }
        clientConfig.setSecurityConfig(clientSecurityConfig);
    }

    private void handleCredentialsFactory(Node node, ClientSecurityConfig clientSecurityConfig) {
        NamedNodeMap attrs = node.getAttributes();
        Node classNameNode = attrs.getNamedItem("class-name");
        String className = getTextContent(classNameNode);
        CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig(className);
        clientSecurityConfig.setCredentialsFactoryConfig(credentialsFactoryConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("properties".equals(nodeName)) {
                fillProperties(child, credentialsFactoryConfig.getProperties());
                break;
            }
        }
    }

    private void handleOutboundPorts(Node child, ClientNetworkConfig clientNetworkConfig) {
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if ("ports".equals(nodeName)) {
                String value = getTextContent(n);
                clientNetworkConfig.addOutboundPortDefinition(value);
            }
        }
    }

}
