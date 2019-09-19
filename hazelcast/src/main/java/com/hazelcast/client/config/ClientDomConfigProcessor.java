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

package com.hazelcast.client.config;

import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.AbstractDomConfigProcessor;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EvictionConfig;
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
import com.hazelcast.topic.TopicOverloadPolicy;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.client.config.ClientConfigSections.CONNECTION_STRATEGY;
import static com.hazelcast.client.config.ClientConfigSections.EXECUTOR_POOL_SIZE;
import static com.hazelcast.client.config.ClientConfigSections.FLAKE_ID_GENERATOR;
import static com.hazelcast.client.config.ClientConfigSections.GROUP;
import static com.hazelcast.client.config.ClientConfigSections.INSTANCE_NAME;
import static com.hazelcast.client.config.ClientConfigSections.LABELS;
import static com.hazelcast.client.config.ClientConfigSections.LISTENERS;
import static com.hazelcast.client.config.ClientConfigSections.LOAD_BALANCER;
import static com.hazelcast.client.config.ClientConfigSections.NATIVE_MEMORY;
import static com.hazelcast.client.config.ClientConfigSections.NEAR_CACHE;
import static com.hazelcast.client.config.ClientConfigSections.NETWORK;
import static com.hazelcast.client.config.ClientConfigSections.PROPERTIES;
import static com.hazelcast.client.config.ClientConfigSections.PROXY_FACTORIES;
import static com.hazelcast.client.config.ClientConfigSections.QUERY_CACHES;
import static com.hazelcast.client.config.ClientConfigSections.RELIABLE_TOPIC;
import static com.hazelcast.client.config.ClientConfigSections.SECURITY;
import static com.hazelcast.client.config.ClientConfigSections.SERIALIZATION;
import static com.hazelcast.client.config.ClientConfigSections.USER_CODE_DEPLOYMENT;
import static com.hazelcast.client.config.ClientConfigSections.canOccurMultipleTimes;
import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.config.DomConfigHelper.getDoubleValue;
import static com.hazelcast.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

class ClientDomConfigProcessor extends AbstractDomConfigProcessor {

    private static final ILogger LOGGER = Logger.getLogger(ClientDomConfigProcessor.class);

    protected final ClientConfig clientConfig;

    protected final QueryCacheConfigBuilderHelper queryCacheConfigBuilderHelper;

    ClientDomConfigProcessor(boolean domLevel3, ClientConfig clientConfig) {
        this(domLevel3, clientConfig, new QueryCacheXmlConfigBuilderHelper(domLevel3));
    }

    ClientDomConfigProcessor(boolean domLevel3, ClientConfig clientConfig, QueryCacheConfigBuilderHelper
            queryCacheConfigBuilderHelper) {
        super(domLevel3);
        this.clientConfig = clientConfig;
        this.queryCacheConfigBuilderHelper = queryCacheConfigBuilderHelper;
    }

    @Override
    public void buildConfig(Node rootNode) throws Exception {
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
        } else if (LABELS.isEqual(nodeName)) {
            handleLabels(node);
        }
    }

    private void handleLabels(Node node) {
        for (Node n : childElements(node)) {
            String value = getTextContent(n);
            clientConfig.addLabel(value);
        }
    }

    private void handleConnectionStrategy(Node node) {
        ClientConnectionStrategyConfig strategyConfig = new ClientConnectionStrategyConfig();
        String attrValue = getAttribute(node, "async-start");
        strategyConfig.setAsyncStart(attrValue != null && getBooleanValue(attrValue.trim()));
        attrValue = getAttribute(node, "reconnect-mode");
        if (attrValue != null) {
            strategyConfig
                    .setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.valueOf(upperCaseInternal(attrValue.trim())));
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
            handleUserCodeDeploymentNode(userCodeDeploymentConfig, child);
        }
        clientConfig.setUserCodeDeploymentConfig(userCodeDeploymentConfig);
    }

    protected void handleUserCodeDeploymentNode(ClientUserCodeDeploymentConfig userCodeDeploymentConfig, Node child) {
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

    private void handleExecutorPoolSize(Node node) {
        int poolSize = Integer.parseInt(getTextContent(node));
        clientConfig.setExecutorPoolSize(poolSize);
    }

    protected void handleNearCache(Node node) {
        handleNearCacheNode(node);
    }

    protected void handleNearCacheNode(Node node) {
        String name = getName(node);
        NearCacheConfig nearCacheConfig = new NearCacheConfig(name);
        Boolean serializeKeys = null;
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            String value = getTextContent(child).trim();
            if ("time-to-live-seconds".equals(nodeName)) {
                nearCacheConfig.setTimeToLiveSeconds(Integer.parseInt(value));
            } else if ("max-idle-seconds".equals(nodeName)) {
                nearCacheConfig.setMaxIdleSeconds(Integer.parseInt(value));
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
            String value = getTextContent(child).trim();
            if ("prefetch-count".equals(nodeName)) {
                config.setPrefetchCount(Integer.parseInt(value));
            } else if ("prefetch-validity-millis".equalsIgnoreCase(nodeName)) {
                config.setPrefetchValidityMillis(Long.parseLong(value));
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
        Node comparatorClassName = node.getAttributes().getNamedItem("comparator-class-name");
        if (size != null) {
            evictionConfig.setSize(Integer.parseInt(getTextContent(size)));
        }
        if (maxSizePolicy != null) {
            evictionConfig
                    .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.valueOf(upperCaseInternal(getTextContent(maxSizePolicy)))
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
            } else if (AliasedDiscoveryConfigUtils.supports(nodeName)) {
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

    protected void handleDiscoveryStrategies(Node node, ClientNetworkConfig clientNetworkConfig) {
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

    protected void handleDiscoveryNodeFilter(Node node, DiscoveryConfig discoveryConfig) {
        NamedNodeMap atts = node.getAttributes();
        Node att = atts.getNamedItem("class");
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
        AliasedDiscoveryConfig config = ClientAliasedDiscoveryConfigUtils.getConfigByTag(clientNetworkConfig, tag);
        NamedNodeMap atts = node.getAttributes();
        for (int i = 0; i < atts.getLength(); i++) {
            Node att = atts.item(i);
            String value = getTextContent(att).trim();
            if ("enabled".equalsIgnoreCase(att.getNodeName())) {
                config.setEnabled(getBooleanValue(value));
            } else if (att.getNodeName().equals("connection-timeout-seconds")) {
                config.setProperty("connection-timeout-seconds", value);
            }
        }
        for (Node n : childElements(node)) {
            String key = n.getLocalName();
            String value = getTextContent(n).trim();
            config.setProperty(key, value);
        }
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
        SocketOptions socketOptions = clientConfig.getNetworkConfig().getSocketOptions();
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

    protected void handleClusterMembers(Node node, ClientNetworkConfig clientNetworkConfig) {
        for (Node child : childElements(node)) {
            if ("address".equals(cleanNodeName(child))) {
                clientNetworkConfig.addAddress(getTextContent(child));
            }
        }
    }

    protected void handleListeners(Node node) {
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

    private void handleProxyFactories(Node node) {
        for (Node child : childElements(node)) {
            handleProxyFactoryNode(child);
        }
    }

    protected void handleProxyFactoryNode(Node child) {
        String nodeName = cleanNodeName(child);
        if ("proxy-factory".equals(nodeName)) {
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

    protected void handleOutboundPorts(Node child, ClientNetworkConfig clientNetworkConfig) {
        for (Node n : childElements(child)) {
            String nodeName = cleanNodeName(n);
            if ("ports".equals(nodeName)) {
                String value = getTextContent(n);
                clientNetworkConfig.addOutboundPortDefinition(value);
            }
        }
    }
}
