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

package com.hazelcast.client.config.impl;

import com.hazelcast.client.config.ClientCloudConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.config.ClientIcmpPingConfig;
import com.hazelcast.client.config.ClientMetricsConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientReliableTopicConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AutoDetectionConfig;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MetricsJmxConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.internal.config.AbstractDomConfigProcessor;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.client.config.impl.ClientConfigSections.BACKUP_ACK_TO_CLIENT;
import static com.hazelcast.client.config.impl.ClientConfigSections.CLUSTER_NAME;
import static com.hazelcast.client.config.impl.ClientConfigSections.CONNECTION_STRATEGY;
import static com.hazelcast.client.config.impl.ClientConfigSections.FLAKE_ID_GENERATOR;
import static com.hazelcast.client.config.impl.ClientConfigSections.INSTANCE_NAME;
import static com.hazelcast.client.config.impl.ClientConfigSections.INSTANCE_TRACKING;
import static com.hazelcast.client.config.impl.ClientConfigSections.LABELS;
import static com.hazelcast.client.config.impl.ClientConfigSections.LISTENERS;
import static com.hazelcast.client.config.impl.ClientConfigSections.LOAD_BALANCER;
import static com.hazelcast.client.config.impl.ClientConfigSections.METRICS;
import static com.hazelcast.client.config.impl.ClientConfigSections.NATIVE_MEMORY;
import static com.hazelcast.client.config.impl.ClientConfigSections.NEAR_CACHE;
import static com.hazelcast.client.config.impl.ClientConfigSections.NETWORK;
import static com.hazelcast.client.config.impl.ClientConfigSections.PROPERTIES;
import static com.hazelcast.client.config.impl.ClientConfigSections.PROXY_FACTORIES;
import static com.hazelcast.client.config.impl.ClientConfigSections.QUERY_CACHES;
import static com.hazelcast.client.config.impl.ClientConfigSections.RELIABLE_TOPIC;
import static com.hazelcast.client.config.impl.ClientConfigSections.SECURITY;
import static com.hazelcast.client.config.impl.ClientConfigSections.SERIALIZATION;
import static com.hazelcast.client.config.impl.ClientConfigSections.USER_CODE_DEPLOYMENT;
import static com.hazelcast.client.config.impl.ClientConfigSections.canOccurMultipleTimes;
import static com.hazelcast.config.security.TokenEncoding.getTokenEncoding;
import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getDoubleValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.DomConfigHelper.getLongValue;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

@SuppressWarnings({
        "checkstyle:cyclomaticcomplexity",
        "checkstyle:classfanoutcomplexity",
        "checkstyle:classdataabstractioncoupling",
        "checkstyle:methodcount",
        "checkstyle:methodlength"})
public class ClientDomConfigProcessor extends AbstractDomConfigProcessor {

    private static final ILogger LOGGER = Logger.getLogger(ClientDomConfigProcessor.class);

    protected final ClientConfig clientConfig;

    protected final QueryCacheConfigBuilderHelper queryCacheConfigBuilderHelper;

    public ClientDomConfigProcessor(boolean domLevel3, ClientConfig clientConfig) {
        this(domLevel3, clientConfig, new QueryCacheXmlConfigBuilderHelper(domLevel3));
    }

    public ClientDomConfigProcessor(boolean domLevel3, ClientConfig clientConfig, boolean strict) {
        this(domLevel3, clientConfig, new QueryCacheXmlConfigBuilderHelper(domLevel3), strict);
    }

    ClientDomConfigProcessor(boolean domLevel3, ClientConfig clientConfig, QueryCacheConfigBuilderHelper
            queryCacheConfigBuilderHelper) {
        super(domLevel3);
        this.clientConfig = clientConfig;
        this.queryCacheConfigBuilderHelper = queryCacheConfigBuilderHelper;
    }

    ClientDomConfigProcessor(boolean domLevel3, ClientConfig clientConfig, QueryCacheConfigBuilderHelper
      queryCacheConfigBuilderHelper, boolean strict) {
        super(domLevel3, strict);
        this.clientConfig = clientConfig;
        this.queryCacheConfigBuilderHelper = queryCacheConfigBuilderHelper;
    }

    @Override
    public void buildConfig(Node rootNode) {
        for (Node node : childElements(rootNode)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException("Duplicate '" + nodeName + "' definition found in the configuration");
            }
            handleNode(node, nodeName);
            if (!canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }
    }

    private void handleNode(Node node, String nodeName) {
        if (matches(SECURITY.getName(), nodeName)) {
            handleSecurity(node);
        } else if (matches(PROXY_FACTORIES.getName(), nodeName)) {
            handleProxyFactories(node);
        } else if (matches(PROPERTIES.getName(), nodeName)) {
            fillProperties(node, clientConfig.getProperties());
        } else if (matches(SERIALIZATION.getName(), nodeName)) {
            handleSerialization(node);
        } else if (matches(NATIVE_MEMORY.getName(), nodeName)) {
            fillNativeMemoryConfig(node, clientConfig.getNativeMemoryConfig());
        } else if (matches(LISTENERS.getName(), nodeName)) {
            handleListeners(node);
        } else if (matches(NETWORK.getName(), nodeName)) {
            handleNetwork(node);
        } else if (matches(LOAD_BALANCER.getName(), nodeName)) {
            handleLoadBalancer(node);
        } else if (matches(NEAR_CACHE.getName(), nodeName)) {
            handleNearCache(node);
        } else if (matches(QUERY_CACHES.getName(), nodeName)) {
            queryCacheConfigBuilderHelper.handleQueryCache(clientConfig, node);
        } else if (matches(INSTANCE_NAME.getName(), nodeName)) {
            clientConfig.setInstanceName(getTextContent(node));
        } else if (matches(CONNECTION_STRATEGY.getName(), nodeName)) {
            handleConnectionStrategy(node);
        } else if (matches(USER_CODE_DEPLOYMENT.getName(), nodeName)) {
            handleUserCodeDeployment(node);
        } else if (matches(FLAKE_ID_GENERATOR.getName(), nodeName)) {
            handleFlakeIdGenerator(node);
        } else if (matches(RELIABLE_TOPIC.getName(), nodeName)) {
            handleReliableTopic(node);
        } else if (matches(LABELS.getName(), nodeName)) {
            handleLabels(node);
        } else if (matches(BACKUP_ACK_TO_CLIENT.getName(), nodeName)) {
            handleBackupAckToClient(node);
        } else if (matches(CLUSTER_NAME.getName(), nodeName)) {
            clientConfig.setClusterName(getTextContent(node));
        } else if (matches(METRICS.getName(), nodeName)) {
            handleMetrics(node);
        } else if (matches(INSTANCE_TRACKING.getName(), nodeName)) {
            handleInstanceTracking(node, clientConfig.getInstanceTrackingConfig());
        }
    }

    private void handleBackupAckToClient(Node node) {
        boolean enabled = Boolean.parseBoolean(getTextContent(node));
        clientConfig.setBackupAckToClientEnabled(enabled);
    }

    private void handleLabels(Node node) {
        for (Node n : childElements(node)) {
            String value = getTextContent(n);
            clientConfig.addLabel(value);
        }
    }

    private void handleConnectionStrategy(Node node) {
        ClientConnectionStrategyConfig strategyConfig = clientConfig.getConnectionStrategyConfig();
        String attrValue = getAttribute(node, "async-start");
        strategyConfig.setAsyncStart(attrValue != null && getBooleanValue(attrValue.trim()));
        attrValue = getAttribute(node, "reconnect-mode");
        if (attrValue != null) {
            strategyConfig
                    .setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.valueOf(upperCaseInternal(attrValue.trim())));
        }
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("connection-retry", nodeName)) {
                handleConnectionRetry(child, strategyConfig);
            }
        }
        clientConfig.setConnectionStrategyConfig(strategyConfig);
    }

    private void handleConnectionRetry(Node node, ClientConnectionStrategyConfig strategyConfig) {
        ConnectionRetryConfig connectionRetryConfig = new ConnectionRetryConfig();

        String initialBackoffMillis = "initial-backoff-millis";
        String maxBackoffMillis = "max-backoff-millis";
        String multiplier = "multiplier";
        String jitter = "jitter";
        String timeoutMillis = "cluster-connect-timeout-millis";
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches(initialBackoffMillis, nodeName)) {
                connectionRetryConfig.setInitialBackoffMillis(getIntegerValue(initialBackoffMillis, getTextContent(child)));
            } else if (matches(maxBackoffMillis, nodeName)) {
                connectionRetryConfig.setMaxBackoffMillis(getIntegerValue(maxBackoffMillis, getTextContent(child)));
            } else if (matches(multiplier, nodeName)) {
                connectionRetryConfig.setMultiplier(getDoubleValue(multiplier, getTextContent(child)));
            } else if (matches(timeoutMillis, nodeName)) {
                connectionRetryConfig.setClusterConnectTimeoutMillis(getLongValue(timeoutMillis, getTextContent(child)));
            } else if (matches(jitter, nodeName)) {
                connectionRetryConfig.setJitter(getDoubleValue(jitter, getTextContent(child)));
            }
        }
        strategyConfig.setConnectionRetryConfig(connectionRetryConfig);
    }

    private void handleUserCodeDeployment(Node node) {
        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        ClientUserCodeDeploymentConfig userCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        userCodeDeploymentConfig.setEnabled(enabled);

        for (Node child : childElements(node)) {
            handleUserCodeDeploymentNode(userCodeDeploymentConfig, child);
        }
        clientConfig.setUserCodeDeploymentConfig(userCodeDeploymentConfig);
    }

    protected void handleUserCodeDeploymentNode(ClientUserCodeDeploymentConfig userCodeDeploymentConfig, Node child) {
        String childNodeName = cleanNodeName(child);
        if (matches("classnames", childNodeName)) {
            for (Node classNameNode : childElements(child)) {
                userCodeDeploymentConfig.addClass(getTextContent(classNameNode));
            }
        } else if (matches("jarpaths", childNodeName)) {
            for (Node jarPathNode : childElements(child)) {
                userCodeDeploymentConfig.addJar(getTextContent(jarPathNode));
            }
        } else {
            throw new InvalidConfigurationException("User code deployement can either be className or jarPath. "
                    + childNodeName + " is invalid");
        }
    }

    protected void handleNearCache(Node node) {
        handleNearCacheNode(node);
    }

    protected void handleNearCacheNode(Node node) {
        String name = getName(node);
        name = name == null ? NearCacheConfig.DEFAULT_NAME : name;
        NearCacheConfig nearCacheConfig = new NearCacheConfig(name);
        Boolean serializeKeys = null;
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("time-to-live-seconds", nodeName)) {
                nearCacheConfig.setTimeToLiveSeconds(Integer.parseInt(getTextContent(child)));
            } else if (matches("max-idle-seconds", nodeName)) {
                nearCacheConfig.setMaxIdleSeconds(Integer.parseInt(getTextContent(child)));
            } else if (matches("in-memory-format", nodeName)) {
                nearCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(upperCaseInternal(getTextContent(child))));
            } else if (matches("serialize-keys", nodeName)) {
                serializeKeys = Boolean.parseBoolean(getTextContent(child));
                nearCacheConfig.setSerializeKeys(serializeKeys);
            } else if (matches("invalidate-on-change", nodeName)) {
                nearCacheConfig.setInvalidateOnChange(Boolean.parseBoolean(getTextContent(child)));
            } else if (matches("local-update-policy", nodeName)) {
                nearCacheConfig.setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.valueOf(getTextContent(child)));
            } else if (matches("eviction", nodeName)) {
                nearCacheConfig.setEvictionConfig(getEvictionConfig(child));
            } else if (matches("preloader", nodeName)) {
                nearCacheConfig.setPreloaderConfig(getNearCachePreloaderConfig(child));
            }
        }
        if (serializeKeys != null && !serializeKeys && nearCacheConfig.getInMemoryFormat() == InMemoryFormat.NATIVE) {
            LOGGER.warning("The Near Cache doesn't support keys by-reference with NATIVE in-memory-format."
                    + " This setting will have no effect!");
        }
        clientConfig.addNearCacheConfig(nearCacheConfig);
    }

    protected String getName(Node node) {
        return getAttribute(node, "name");
    }

    protected void handleFlakeIdGenerator(Node node) {
        handleFlakeIdGeneratorNode(node);
    }

    protected void handleFlakeIdGeneratorNode(Node node) {
        String name = getName(node);
        ClientFlakeIdGeneratorConfig config = new ClientFlakeIdGeneratorConfig(name);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("prefetch-count", nodeName)) {
                config.setPrefetchCount(Integer.parseInt(getTextContent(child)));
            } else if (matches("prefetch-validity-millis", lowerCaseInternal(nodeName))) {
                config.setPrefetchValidityMillis(Long.parseLong(getTextContent(child)));
            }
        }
        clientConfig.addFlakeIdGeneratorConfig(config);
    }

    protected void handleReliableTopic(Node node) {
        handleReliableTopicNode(node);
    }

    protected void handleReliableTopicNode(Node node) {
        String name = getName(node);
        ClientReliableTopicConfig config = new ClientReliableTopicConfig(name);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("topic-overload-policy", nodeName)) {
                config.setTopicOverloadPolicy(TopicOverloadPolicy.valueOf(getTextContent(child)));
            } else if (matches("read-batch-size", lowerCaseInternal(nodeName))) {
                config.setReadBatchSize(Integer.parseInt(getTextContent(child)));
            }
        }
        clientConfig.addReliableTopicConfig(config);
    }

    private EvictionConfig getEvictionConfig(Node node) {
        EvictionConfig evictionConfig = new EvictionConfig();
        Node size = getNamedItemNode(node, "size");
        Node maxSizePolicy = getNamedItemNode(node, "max-size-policy");
        Node evictionPolicy = getNamedItemNode(node, "eviction-policy");
        Node comparatorClassName = getNamedItemNode(node, "comparator-class-name");
        if (size != null) {
            evictionConfig.setSize(Integer.parseInt(getTextContent(size)));
        }
        if (maxSizePolicy != null) {
            evictionConfig
                    .setMaxSizePolicy(MaxSizePolicy.valueOf(upperCaseInternal(getTextContent(maxSizePolicy)))
                    );
        }
        if (evictionPolicy != null) {
            evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(upperCaseInternal(getTextContent(evictionPolicy)))
            );
        }
        if (comparatorClassName != null) {
            evictionConfig.setComparatorClassName(getTextContent(comparatorClassName));
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
        if (matches("random", type)) {
            clientConfig.setLoadBalancer(new RandomLB());
        } else if (matches("round-robin", type)) {
            clientConfig.setLoadBalancer(new RoundRobinLB());
        } else if ("custom".equals(type)) {
            String loadBalancerClassName = parseCustomLoadBalancerClassName(node);
            clientConfig.setLoadBalancerClassName(loadBalancerClassName);
        }
    }

    protected String parseCustomLoadBalancerClassName(Node node) {
        return getTextContent(node);
    }

    private void handleNetwork(Node node) {
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("cluster-members", nodeName)) {
                handleClusterMembers(child, clientNetworkConfig);
            } else if (matches("smart-routing", nodeName)) {
                clientNetworkConfig.setSmartRouting(Boolean.parseBoolean(getTextContent(child)));
            } else if (matches("redo-operation", nodeName)) {
                clientNetworkConfig.setRedoOperation(Boolean.parseBoolean(getTextContent(child)));
            } else if (matches("connection-timeout", nodeName)) {
                clientNetworkConfig.setConnectionTimeout(Integer.parseInt(getTextContent(child)));
            } else if (matches("socket-options", nodeName)) {
                handleSocketOptions(child, clientNetworkConfig);
            } else if (matches("socket-interceptor", nodeName)) {
                handleSocketInterceptorConfig(child, clientNetworkConfig);
            } else if (matches("ssl", nodeName)) {
                handleSSLConfig(child, clientNetworkConfig);
            } else if (AliasedDiscoveryConfigUtils.supports(nodeName)) {
                handleAliasedDiscoveryStrategy(child, clientNetworkConfig, nodeName);
            } else if (matches("discovery-strategies", nodeName)) {
                handleDiscoveryStrategies(child, clientNetworkConfig);
            } else if (matches("auto-detection", nodeName)) {
                handleAutoDetection(child, clientNetworkConfig);
            } else if (matches("outbound-ports", nodeName)) {
                handleOutboundPorts(child, clientNetworkConfig);
            } else if (matches("icmp-ping", nodeName)) {
                handleIcmpPing(child, clientNetworkConfig);
            } else if (matches("hazelcast-cloud", nodeName)) {
                handleHazelcastCloud(child, clientNetworkConfig);
            }
        }
        clientConfig.setNetworkConfig(clientNetworkConfig);
    }

    private void handleHazelcastCloud(Node node, ClientNetworkConfig clientNetworkConfig) {
        ClientCloudConfig cloudConfig = clientNetworkConfig.getCloudConfig();

        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        cloudConfig.setEnabled(enabled);

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("discovery-token", nodeName)) {
                cloudConfig.setDiscoveryToken(getTextContent(child));
            }
        }
    }

    private void handleIcmpPing(Node node, ClientNetworkConfig clientNetworkConfig) {
        ClientIcmpPingConfig icmpPingConfig = clientNetworkConfig.getClientIcmpPingConfig();

        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        icmpPingConfig.setEnabled(enabled);

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("timeout-milliseconds", nodeName)) {
                icmpPingConfig.setTimeoutMilliseconds(Integer.parseInt(getTextContent(child)));
            } else if (matches("interval-milliseconds", nodeName)) {
                icmpPingConfig.setIntervalMilliseconds(Integer.parseInt(getTextContent(child)));
            } else if (matches("ttl", nodeName)) {
                icmpPingConfig.setTtl(Integer.parseInt(getTextContent(child)));
            } else if (matches("max-attempts", nodeName)) {
                icmpPingConfig.setMaxAttempts(Integer.parseInt(getTextContent(child)));
            } else if (matches("echo-fail-fast-on-startup", nodeName)) {
                icmpPingConfig.setEchoFailFastOnStartup(Boolean.parseBoolean(getTextContent(child)));
            }
        }
        clientNetworkConfig.setClientIcmpPingConfig(icmpPingConfig);
    }

    protected void handleDiscoveryStrategies(Node node, ClientNetworkConfig clientNetworkConfig) {
        DiscoveryConfig discoveryConfig = clientNetworkConfig.getDiscoveryConfig();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("discovery-strategy", name)) {
                handleDiscoveryStrategy(child, discoveryConfig);
            } else if (matches("node-filter", name)) {
                handleDiscoveryNodeFilter(child, discoveryConfig);
            }
        }
    }

    protected void handleAutoDetection(Node node, ClientNetworkConfig clientNetworkConfig) {
        AutoDetectionConfig discoveryConfig = clientNetworkConfig.getAutoDetectionConfig();
        NamedNodeMap atts = node.getAttributes();
        for (int i = 0; i < atts.getLength(); i++) {
            Node att = atts.item(i);
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                discoveryConfig.setEnabled(getBooleanValue(getTextContent(att)));
            }
        }
    }

    protected void handleDiscoveryNodeFilter(Node node, DiscoveryConfig discoveryConfig) {
        Node att = getNamedItemNode(node, "class");
        if (att != null) {
            discoveryConfig.setNodeFilterClass(getTextContent(att).trim());
        }
    }

    protected void handleDiscoveryStrategy(Node node, DiscoveryConfig discoveryConfig) {
        NamedNodeMap atts = node.getAttributes();

        boolean enabled = false;
        String clazz = null;

        for (int a = 0; a < atts.getLength(); a++) {
            Node att = atts.item(a);
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                enabled = getBooleanValue(getTextContent(att));
            } else if (matches("class", att.getNodeName())) {
                clazz = getTextContent(att);
            }
        }

        if (!enabled || clazz == null) {
            return;
        }

        Map<String, Comparable> properties = new HashMap<>();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("properties", name)) {
                fillProperties(child, properties);
            }
        }

        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(clazz, properties));
    }

    private void handleAliasedDiscoveryStrategy(Node node, ClientNetworkConfig clientNetworkConfig, String tag) {
        AliasedDiscoveryConfig config = ClientAliasedDiscoveryConfigUtils.getConfigByTag(clientNetworkConfig, tag);
        NamedNodeMap atts = node.getAttributes();
        for (int i = 0; i < atts.getLength(); i++) {
            Node att = atts.item(i);
            if (matches("enabled", lowerCaseInternal(att.getNodeName()))) {
                config.setEnabled(getBooleanValue(getTextContent(att)));
            } else if (matches(att.getNodeName(), "connection-timeout-seconds")) {
                config.setProperty("connection-timeout-seconds", getTextContent(att));
            }
        }
        for (Node n : childElements(node)) {
            String key = n.getLocalName();
            String value = getTextContent(n);
            config.setProperty(key, value);
        }
    }

    private void handleSSLConfig(Node node, ClientNetworkConfig clientNetworkConfig) {
        SSLConfig sslConfig = new SSLConfig();
        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        sslConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("factory-class-name", nodeName)) {
                sslConfig.setFactoryClassName(getTextContent(n).trim());
            } else if (matches("properties", nodeName)) {
                fillProperties(n, sslConfig.getProperties());
            }
        }
        clientNetworkConfig.setSSLConfig(sslConfig);
    }

    private void handleSocketOptions(Node node, ClientNetworkConfig clientNetworkConfig) {
        SocketOptions socketOptions = clientConfig.getNetworkConfig().getSocketOptions();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("tcp-no-delay", nodeName)) {
                socketOptions.setTcpNoDelay(Boolean.parseBoolean(getTextContent(child)));
            } else if (matches("keep-alive", nodeName)) {
                socketOptions.setKeepAlive(Boolean.parseBoolean(getTextContent(child)));
            } else if (matches("reuse-address", nodeName)) {
                socketOptions.setReuseAddress(Boolean.parseBoolean(getTextContent(child)));
            } else if (matches("linger-seconds", nodeName)) {
                socketOptions.setLingerSeconds(Integer.parseInt(getTextContent(child)));
            } else if (matches("buffer-size", nodeName)) {
                socketOptions.setBufferSize(Integer.parseInt(getTextContent(child)));
            }
        }
        clientNetworkConfig.setSocketOptions(socketOptions);
    }

    protected void handleClusterMembers(Node node, ClientNetworkConfig clientNetworkConfig) {
        for (Node child : childElements(node)) {
            if (matches("address", cleanNodeName(child))) {
                clientNetworkConfig.addAddress(getTextContent(child));
            }
        }
    }

    protected void handleListeners(Node node) {
        for (Node child : childElements(node)) {
            if (matches("listener", cleanNodeName(child))) {
                String className = getTextContent(child);
                clientConfig.addListenerConfig(new ListenerConfig(className));
            }
        }
    }

    private void handleSerialization(Node node) {
        SerializationConfig serializationConfig = parseSerialization(node);
        clientConfig.setSerializationConfig(serializationConfig);
    }

    private void handleProxyFactories(Node node) {
        for (Node child : childElements(node)) {
            handleProxyFactoryNode(child);
        }
    }

    protected void handleProxyFactoryNode(Node child) {
        String nodeName = cleanNodeName(child);
        if (matches("proxy-factory", nodeName)) {
            handleProxyFactory(child);
        }
    }

    protected void handleProxyFactory(Node node) {
        String service = getAttribute(node, "service");
        String className = getAttribute(node, "class-name");

        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig(className, service);
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);
    }

    private void handleSocketInterceptorConfig(Node node, ClientNetworkConfig clientNetworkConfig) {
        SocketInterceptorConfig socketInterceptorConfig = parseSocketInterceptorConfig(node);
        clientNetworkConfig.setSocketInterceptorConfig(socketInterceptorConfig);
    }

    private void handleSecurity(Node node) {
        ClientSecurityConfig clientSecurityConfig = new ClientSecurityConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("username-password", nodeName)) {
                clientSecurityConfig.setUsernamePasswordIdentityConfig(
                        getAttribute(child, "username"),
                        getAttribute(child, "password"));
            } else if (matches("token", nodeName)) {
                handleTokenIdentity(clientSecurityConfig, child);
            } else if (matches("credentials-factory", nodeName)) {
                handleCredentialsFactory(child, clientSecurityConfig);
            } else if (matches("kerberos", nodeName)) {
                handleKerberosIdentity(child, clientSecurityConfig);
            } else if (matches("realms", nodeName)) {
                handleRealms(child, clientSecurityConfig);
            }
        }
        clientConfig.setSecurityConfig(clientSecurityConfig);
    }

    protected void handleTokenIdentity(ClientSecurityConfig clientSecurityConfig, Node node) {
        clientSecurityConfig.setTokenIdentityConfig(new TokenIdentityConfig(
                getTokenEncoding(getAttribute(node, "encoding")), getTextContent(node)));
    }

    private void handleKerberosIdentity(Node node, ClientSecurityConfig clientSecurityConfig) {
        KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("realm", nodeName)) {
                kerbIdentity.setRealm(getTextContent(child));
            } else if (matches("security-realm", nodeName)) {
                kerbIdentity.setSecurityRealm(getTextContent(child));
            } else if (matches("principal", nodeName)) {
                kerbIdentity.setPrincipal(getTextContent(child));
            } else if (matches("keytab-file", nodeName)) {
                kerbIdentity.setKeytabFile(getTextContent(child));
            } else if (matches("service-name-prefix", nodeName)) {
                kerbIdentity.setServiceNamePrefix(getTextContent(child));
            } else if (matches("spn", nodeName)) {
                kerbIdentity.setSpn(getTextContent(child));
            } else if (matches("use-canonical-hostname", nodeName)) {
                kerbIdentity.setUseCanonicalHostname(getBooleanValue(getTextContent(child)));
            }
        }
        clientSecurityConfig.setKerberosIdentityConfig(kerbIdentity);
    }

    protected void handleRealms(Node node, ClientSecurityConfig clientSecurityConfig) {
        for (Node child : childElements(node)) {
            if (matches("realm", cleanNodeName(child))) {
                handleRealm(child, clientSecurityConfig);
            }
        }
    }

    protected void handleRealm(Node node, ClientSecurityConfig clientSecurityConfig) {
        String realmName = getAttribute(node, "name");
        RealmConfig realmConfig = new RealmConfig();
        clientSecurityConfig.addRealmConfig(realmName, realmConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("authentication", nodeName)) {
                handleAuthentication(realmConfig, child);
            }
        }
    }

    private void handleAuthentication(RealmConfig realmConfig, Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("jaas", nodeName)) {
                handleJaasAuthentication(realmConfig, child);
            }
        }
    }

    private void handleCredentialsFactory(Node node, ClientSecurityConfig clientSecurityConfig) {
        Node classNameNode = getNamedItemNode(node, "class-name");
        String className = getTextContent(classNameNode);
        CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig(className);
        clientSecurityConfig.setCredentialsFactoryConfig(credentialsFactoryConfig);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("properties", nodeName)) {
                fillProperties(child, credentialsFactoryConfig.getProperties());
                break;
            }
        }
    }

    protected void handleOutboundPorts(Node child, ClientNetworkConfig clientNetworkConfig) {
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if (matches("ports", nodeName)) {
                clientNetworkConfig.addOutboundPortDefinition(getTextContent(n));
            }
        }
    }

    private void handleMetrics(Node node) {
        ClientMetricsConfig metricsConfig = clientConfig.getMetricsConfig();

        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
                metricsConfig.setEnabled(enabled);
            }
        }

        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("jmx", nodeName)) {
                handleMetricsJmx(child);
            } else if (matches("collection-frequency-seconds", nodeName)) {
                metricsConfig.setCollectionFrequencySeconds(Integer.parseInt(getTextContent(child)));
            }
        }
    }

    private void handleMetricsJmx(Node node) {
        MetricsJmxConfig jmxConfig = clientConfig.getMetricsConfig().getJmxConfig();

        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            if (matches("enabled", att.getNodeName())) {
                boolean enabled = getBooleanValue(getAttribute(node, "enabled"));
                jmxConfig.setEnabled(enabled);
            }
        }
    }

}
