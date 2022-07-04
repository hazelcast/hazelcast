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

package com.hazelcast.internal.config;

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.LocalDeviceConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.OnJoinPermissionOperationName;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.TrustedInterfacesConfigurable;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.SimpleAuthenticationConfig;
import com.hazelcast.config.security.TokenEncoding;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlSequence;
import com.hazelcast.query.impl.IndexUtils;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.yaml.W3cDomUtil.getWrappedYamlSequence;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;
import static com.hazelcast.internal.yaml.YamlUtil.asScalar;
import static java.lang.Integer.parseInt;

@SuppressWarnings({
        "checkstyle:methodcount",
        "checkstyle:cyclomaticcomplexity",
        "checkstyle:classfanoutcomplexity",
        "checkstyle:classdataabstractioncoupling"
})
public class YamlMemberDomConfigProcessor extends MemberDomConfigProcessor {

    public YamlMemberDomConfigProcessor(boolean domLevel3, Config config) {
        super(domLevel3, config);
    }

    public YamlMemberDomConfigProcessor(boolean domLevel3, Config config, boolean strict) {
        super(domLevel3, config, strict);
    }

    @Override
    protected void handleSecurityInterceptorsChild(SecurityConfig cfg, Node child) {
        String className = child.getTextContent();
        cfg.addSecurityInterceptorConfig(new SecurityInterceptorConfig(className));
    }

    @SuppressWarnings({ "checkstyle:npathcomplexity", "checkstyle:methodlength" })
    protected void handleSecurityPermissions(Node node) {
        String onJoinOp = getAttribute(node, "on-join-operation");
        if (onJoinOp != null) {
            OnJoinPermissionOperationName onJoinPermissionOperation = OnJoinPermissionOperationName
                    .valueOf(upperCaseInternal(onJoinOp));
            config.getSecurityConfig().setOnJoinPermissionOperation(onJoinPermissionOperation);
        }
        Iterable<Node> nodes = childElements(node);
        for (Node child : nodes) {
            String nodeName = cleanNodeName(child);
            if (matches("on-join-operation", nodeName)) {
                continue;
            }
            nodeName = matches("all", nodeName) ? nodeName + "-permissions" : nodeName + "-permission";
            PermissionType type = PermissionConfig.PermissionType.getType(nodeName);
            if (type == null) {
                throw new InvalidConfigurationException("Security permission type is not valid " + nodeName);
            }

            if (PermissionConfig.PermissionType.CONFIG == type || PermissionConfig.PermissionType.ALL == type
                    || PermissionConfig.PermissionType.TRANSACTION == type) {
                handleSecurityPermission(child, type);
            } else {
                handleSecurityPermissionGroup(child, type);
            }
        }
    }

    private void handleSecurityPermissionGroup(Node node, PermissionConfig.PermissionType type) {
        for (Node permissionNode : childElements(node)) {
            handleSecurityPermission(permissionNode, type);
        }
    }

    @Override
    void handleSecurityPermissionActions(Node node, PermissionConfig permConfig) {
        for (Node child : childElements(node)) {
            permConfig.addAction(getTextContent(child));
        }
    }

    @Override
    void handleSecurityPermissionEndpoints(Node node, PermissionConfig permConfig) {
        for (Node child : childElements(node)) {
            permConfig.addEndpoint(getTextContent(child).trim());
        }
    }

    @Override
    protected void handleTrustedInterfaces(TrustedInterfacesConfigurable<?> tiConfig, Node n) {
        YamlSequence yamlNode = getWrappedYamlSequence(n);
        for (YamlNode interfaceNode : yamlNode.children()) {
            String trustedInterface = asScalar(interfaceNode).nodeValue();
            tiConfig.addTrustedInterface(trustedInterface);
        }
        super.handleTrustedInterfaces(tiConfig, n);
    }

    @Override
    protected void handleWanReplication(Node node) {
        for (Node wanReplicationNode : childElements(node)) {
            WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
            wanReplicationConfig.setName(wanReplicationNode.getNodeName());
            handleWanReplicationNode(wanReplicationNode, wanReplicationConfig);
        }
    }

    @Override
    protected void handleWanReplicationChild(WanReplicationConfig wanReplicationConfig, Node nodeTarget, String nodeName) {
        if (matches("batch-publisher", nodeName)) {
            for (Node publisherNode : childElements(nodeTarget)) {
                WanBatchPublisherConfig publisherConfig = new WanBatchPublisherConfig();
                String clusterNameOrPublisherId = publisherNode.getNodeName();
                Node clusterNameAttr = getNamedItemNode(publisherNode, "cluster-name");

                // the publisher's key may mean either the publisher-id or the
                // cluster-name depending on whether the cluster-name is explicitly defined
                String clusterName = clusterNameAttr != null ? clusterNameAttr.getTextContent() : clusterNameOrPublisherId;
                String publisherId = clusterNameAttr != null ? clusterNameOrPublisherId : null;
                publisherConfig.setPublisherId(publisherId);
                publisherConfig.setClusterName(clusterName);

                handleBatchWanPublisherNode(wanReplicationConfig, publisherNode, publisherConfig);
            }
        } else if (matches("custom-publisher", nodeName)) {
            for (Node publisherNode : childElements(nodeTarget)) {
                WanCustomPublisherConfig publisherConfig = new WanCustomPublisherConfig();
                publisherConfig.setPublisherId(publisherNode.getNodeName());
                handleCustomWanPublisherNode(wanReplicationConfig, publisherNode, publisherConfig);
            }
        } else if (matches("consumer", nodeName)) {
            handleWanConsumerNode(wanReplicationConfig, nodeTarget);
        }
    }

    @Override
    protected void handlePort(Node node, Config config) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);

            if (matches("port", att.getNodeName())) {
                int portCount = parseInt(getTextContent(att));
                networkConfig.setPort(portCount);
            } else if (matches("auto-increment", att.getNodeName())) {
                networkConfig.setPortAutoIncrement(getBooleanValue(getTextContent(att)));
            } else if (matches("port-count", att.getNodeName())) {
                int portCount = parseInt(getTextContent(att));
                networkConfig.setPortCount(portCount);
            }
        }
    }

    @Override
    protected void handleQueue(Node node) {
        for (Node queueNode : childElements(node)) {
            QueueConfig queueConfig = ConfigUtils.getByNameOrNew(config.getQueueConfigs(), queueNode.getNodeName(),
                    QueueConfig.class);
            handleQueueNode(queueNode, queueConfig);
        }
    }

    @Override
    protected void handleList(Node node) {
        for (Node listNode : childElements(node)) {
            ListConfig listConfig = ConfigUtils.getByNameOrNew(config.getListConfigs(), listNode.getNodeName(),
                    ListConfig.class);
            handleListNode(listNode, listConfig);
        }
    }

    @Override
    protected void handleSet(Node node) {
        for (Node setNode : childElements(node)) {
            SetConfig setConfig = ConfigUtils.getByNameOrNew(config.getSetConfigs(), setNode.getNodeName(), SetConfig.class);
            handleSetNode(setNode, setConfig);
        }
    }

    @Override
    protected void handleReliableTopic(Node node) {
        for (Node topicNode : childElements(node)) {
            ReliableTopicConfig topicConfig = new ReliableTopicConfig();
            topicConfig.setName(topicNode.getNodeName());
            handleReliableTopicNode(topicNode, topicConfig);
        }
    }

    @Override
    protected void handleTopic(Node node) {
        for (Node topicNode : childElements(node)) {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setName(topicNode.getNodeName());
            handleTopicNode(topicNode, topicConfig);
        }
    }

    @Override
    protected void handleRingbuffer(Node node) {
        for (Node rbNode : childElements(node)) {
            handleRingBufferNode(rbNode,
                    ConfigUtils.getByNameOrNew(config.getRingbufferConfigs(), rbNode.getNodeName(), RingbufferConfig.class));
        }
    }

    @Override
    protected void handleMap(Node parentNode) throws Exception {
        for (Node mapNode : childElements(parentNode)) {
            String name = mapNode.getNodeName();
            MapConfig mapConfig = ConfigUtils.getByNameOrNew(config.getMapConfigs(), name, MapConfig.class);
            handleMapNode(mapNode, mapConfig);
        }
    }

    @Override
    protected void handleLocalDevice(Node parentNode) {
        for (Node deviceNode : childElements(parentNode)) {
            String name = deviceNode.getNodeName();
            LocalDeviceConfig localDeviceConfig =
                    (LocalDeviceConfig) ConfigUtils.getByNameOrNew(config.getDeviceConfigs(), name, LocalDeviceConfig.class);
            handleLocalDeviceNode(deviceNode, localDeviceConfig);
        }
    }

    @Override
    protected void handleCache(Node parentNode) throws Exception {
        for (Node cacheNode : childElements(parentNode)) {
            handleCacheNode(cacheNode,
                    ConfigUtils.getByNameOrNew(config.getCacheConfigs(), cacheNode.getNodeName(), CacheSimpleConfig.class));
        }
    }

    @Override
    protected void handleSplitBrainProtection(Node node) {
        for (Node splitBrainProtectionNode : childElements(node)) {
            String name = splitBrainProtectionNode.getNodeName();
            SplitBrainProtectionConfig splitBrainProtectionConfig = ConfigUtils
                    .getByNameOrNew(config.getSplitBrainProtectionConfigs(), name, SplitBrainProtectionConfig.class);
            handleSplitBrainProtectionNode(splitBrainProtectionNode, splitBrainProtectionConfig, name);
        }
    }

    @Override
    protected void handleFlakeIdGenerator(Node node) {
        for (Node genNode : childElements(node)) {
            FlakeIdGeneratorConfig genConfig = ConfigUtils.getByNameOrNew(config.getFlakeIdGeneratorConfigs(),
                    genNode.getNodeName(), FlakeIdGeneratorConfig.class);

            handleFlakeIdGeneratorNode(genNode, genConfig);
        }
    }

    @Override
    protected void handleExecutor(Node node) throws Exception {
        for (Node executorNode : childElements(node)) {
            ExecutorConfig executorConfig = ConfigUtils.getByNameOrNew(config.getExecutorConfigs(), executorNode.getNodeName(),
                    ExecutorConfig.class);
            handleViaReflection(executorNode, config, executorConfig);
        }
    }

    @Override
    protected void handleDurableExecutor(Node node) throws Exception {
        for (Node executorNode : childElements(node)) {
            DurableExecutorConfig executorConfig = ConfigUtils.getByNameOrNew(config.getDurableExecutorConfigs(),
                    executorNode.getNodeName(), DurableExecutorConfig.class);
            handleViaReflection(executorNode, config, executorConfig);
        }
    }

    @Override
    protected void handleScheduledExecutor(Node node) {
        for (Node executorNode : childElements(node)) {
            ScheduledExecutorConfig executorConfig = ConfigUtils.getByNameOrNew(config.getScheduledExecutorConfigs(),
                    executorNode.getNodeName(), ScheduledExecutorConfig.class);
            handleScheduledExecutorNode(executorNode, executorConfig);
        }
    }

    @Override
    protected void handleCardinalityEstimator(Node node) {
        for (Node estimatorNode : childElements(node)) {
            CardinalityEstimatorConfig estimatorConfig = ConfigUtils.getByNameOrNew(config.getCardinalityEstimatorConfigs(),
                    estimatorNode.getNodeName(), CardinalityEstimatorConfig.class);

            handleCardinalityEstimatorNode(estimatorNode, estimatorConfig);
        }
    }

    @Override
    protected void handlePNCounter(Node node) throws Exception {
        for (Node counterNode : childElements(node)) {
            PNCounterConfig counterConfig = ConfigUtils.getByNameOrNew(config.getPNCounterConfigs(), counterNode.getNodeName(),
                    PNCounterConfig.class);

            handleViaReflection(counterNode, config, counterConfig);
        }
    }

    @Override
    protected void handleMultiMap(Node node) {
        for (Node multiMapNode : childElements(node)) {
            MultiMapConfig multiMapConfig = ConfigUtils.getByNameOrNew(config.getMultiMapConfigs(), multiMapNode.getNodeName(),
                    MultiMapConfig.class);
            handleMultiMapNode(multiMapNode, multiMapConfig);
        }
    }

    @Override
    protected void handleReplicatedMap(Node node) {
        for (Node replicatedMapNode : childElements(node)) {
            ReplicatedMapConfig replicatedMapConfig = ConfigUtils.getByNameOrNew(config.getReplicatedMapConfigs(),
                    replicatedMapNode.getNodeName(), ReplicatedMapConfig.class);
            handleReplicatedMapNode(replicatedMapNode, replicatedMapConfig);
        }
    }

    @Override
    protected void mapWanReplicationRefHandle(Node n, MapConfig mapConfig) {
        for (Node mapNode : childElements(n)) {
            WanReplicationRef wanReplicationRef = new WanReplicationRef();
            wanReplicationRef.setName(mapNode.getNodeName());
            handleMapWanReplicationRefNode(mapNode, mapConfig, wanReplicationRef);
        }
    }

    @Override
    protected void handleWanFilters(Node wanChild, WanReplicationRef wanReplicationRef) {
        for (Node filter : childElements(wanChild)) {
            wanReplicationRef.addFilter(getTextContent(filter));
        }
    }

    @Override
    protected void mapIndexesHandle(Node n, MapConfig mapConfig) {
        for (Node indexNode : childElements(n)) {
            IndexConfig indexConfig = IndexUtils.getIndexConfigFromYaml(indexNode, domLevel3, strict);

            mapConfig.addIndexConfig(indexConfig);
        }
    }

    @Override
    protected void attributesHandle(Node n, MapConfig mapConfig) {
        for (Node extractorNode : childElements(n)) {
            String extractor = getTextContent(getNamedItemNode(extractorNode, "extractor-class-name"));
            String name = extractorNode.getNodeName();
            mapConfig.addAttributeConfig(new AttributeConfig(name, extractor));
        }
    }

    @Override
    protected void mapQueryCacheHandler(Node n, MapConfig mapConfig) {
        for (Node queryCacheNode : childElements(n)) {
            String cacheName = queryCacheNode.getNodeName();
            QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
            handleMapQueryCacheNode(mapConfig, queryCacheNode, queryCacheConfig);
        }
    }

    @Override
    protected void queryCachePredicateHandler(Node childNode, QueryCacheConfig queryCacheConfig) {
        Node classNameNode = getNamedItemNode(childNode, "class-name");
        Node sqlNode = getNamedItemNode(childNode, "sql");

        if (classNameNode != null && sqlNode != null) {
            throw new InvalidConfigurationException("Both class-name and sql is defined for the predicate of map "
                    + childNode.getParentNode().getParentNode().getNodeName());
        }

        if (classNameNode == null && sqlNode == null) {
            throw new InvalidConfigurationException("Either class-name and sql must be defined for the predicate of map "
                    + childNode.getParentNode().getParentNode().getNodeName());
        }

        PredicateConfig predicateConfig = new PredicateConfig();
        if (classNameNode != null) {
            predicateConfig.setClassName(getTextContent(classNameNode));
        } else if (sqlNode != null) {
            predicateConfig.setSql(getTextContent(sqlNode));
        }
        queryCacheConfig.setPredicateConfig(predicateConfig);
    }

    @Override
    protected void queryCacheIndexesHandle(Node n, QueryCacheConfig queryCacheConfig) {
        for (Node indexNode : childElements(n)) {
            IndexConfig indexConfig = IndexUtils.getIndexConfigFromYaml(indexNode, domLevel3, strict);

            queryCacheConfig.addIndexConfig(indexConfig);
        }
    }

    @Override
    protected void handleMemberGroup(Node node, Config config) {
        for (Node memberGroupNode : childElements(node)) {
            MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
            for (Node interfacesNode : childElements(memberGroupNode)) {
                memberGroupConfig.addInterface(interfacesNode.getNodeValue().trim());
            }
            config.getPartitionGroupConfig().addMemberGroupConfig(memberGroupConfig);
        }
    }

    @Override
    protected MergePolicyConfig createMergePolicyConfig(Node node, MergePolicyConfig baseMergePolicyConfig) {
        String policyString = getTextContent(getNamedItemNode(node, "class-name"));
        if (policyString != null) {
            baseMergePolicyConfig.setPolicy(policyString);
        }
        final String att = getAttribute(node, "batch-size");
        if (att != null) {
            baseMergePolicyConfig.setBatchSize(getIntegerValue("batch-size", att));
        }
        return baseMergePolicyConfig;
    }

    @Override
    protected void mapPartitionLostListenerHandle(Node n, MapConfig mapConfig) {
        for (Node listenerNode : childElements(n)) {
            String listenerClass = listenerNode.getNodeValue();
            mapConfig.addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig(listenerClass));
        }
    }

    @Override
    protected void cachePartitionLostListenerHandle(Node n, CacheSimpleConfig cacheConfig) {
        for (Node listenerNode : childElements(n)) {
            String listenerClass = listenerNode.getNodeValue();
            cacheConfig.addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig(listenerClass));
        }
    }

    @Override
    protected void cacheListenerHandle(Node n, CacheSimpleConfig cacheSimpleConfig) {
        for (Node listenerNode : childElements(n)) {
            handleCacheEntryListenerNode(cacheSimpleConfig, listenerNode);
        }
    }

    @Override
    protected void handleItemListeners(Node n, Consumer<ItemListenerConfig> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            boolean incValue = getBooleanValue(getTextContent(getNamedItemNode(listenerNode, "include-value")));
            String listenerClass = getTextContent(getNamedItemNode(listenerNode, "class-name"));
            configAddFunction.accept(new ItemListenerConfig(listenerClass, incValue));
        }
    }

    @Override
    protected void handleEntryListeners(Node n, Consumer<EntryListenerConfig> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            boolean incValue = getBooleanValue(getTextContent(getNamedItemNode(listenerNode, "include-value")));
            boolean local = getBooleanValue(getTextContent(getNamedItemNode(listenerNode, "local")));
            String listenerClass = getTextContent(getNamedItemNode(listenerNode, "class-name"));
            configAddFunction.accept(new EntryListenerConfig(listenerClass, local, incValue));
        }
    }

    @Override
    void handleMessageListeners(Node n, Function<ListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            String listenerClass = listenerNode.getNodeValue().trim();
            configAddFunction.apply(new ListenerConfig(listenerClass));
        }
    }

    @Override
    protected void handleSplitBrainProtectionListeners(SplitBrainProtectionConfig splitBrainProtectionConfig, Node n) {
        for (Node listenerNode : childElements(n)) {
            String listenerClass = listenerNode.getNodeValue().trim();
            splitBrainProtectionConfig.addListenerConfig(new SplitBrainProtectionListenerConfig(listenerClass));
        }
    }

    @Override
    protected void fillProperties(Node node, Map<String, Comparable> properties) {
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node childNode = childNodes.item(i);
            properties.put(childNode.getNodeName(), childNode.getNodeValue());
        }
    }

    @Override
    protected void fillProperties(Node node, Properties properties) {
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node childNode = childNodes.item(i);
            properties.put(childNode.getNodeName(), childNode.getNodeValue());
        }
    }

    @Override
    protected void handleDiscoveryStrategiesChild(DiscoveryConfig discoveryConfig, Node child) {
        String name = cleanNodeName(child);
        if (matches("discovery-strategies", name)) {
            NodeList strategies = child.getChildNodes();
            for (int i = 0; i < strategies.getLength(); i++) {
                Node strategy = strategies.item(i);
                handleDiscoveryStrategy(strategy, discoveryConfig);
            }
        } else if (matches("node-filter", name)) {
            handleDiscoveryNodeFilter(child, discoveryConfig);
        }
    }

    @Override
    protected SerializationConfig parseSerialization(final Node node) {
        SerializationConfig serializationConfig = new SerializationConfig();
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if (matches("portable-version", name)) {
                serializationConfig.setPortableVersion(getIntegerValue(name, getTextContent(child)));
            } else if (matches("check-class-def-errors", name)) {
                serializationConfig.setCheckClassDefErrors(getBooleanValue(getTextContent(child)));
            } else if (matches("use-native-byte-order", name)) {
                serializationConfig.setUseNativeByteOrder(getBooleanValue(getTextContent(child)));
            } else if (matches("byte-order", name)) {
                ByteOrder byteOrder = null;
                if (ByteOrder.BIG_ENDIAN.toString().equals(getTextContent(child))) {
                    byteOrder = ByteOrder.BIG_ENDIAN;
                } else if (ByteOrder.LITTLE_ENDIAN.toString().equals(getTextContent(child))) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }
                serializationConfig.setByteOrder(byteOrder != null ? byteOrder : ByteOrder.BIG_ENDIAN);
            } else if (matches("enable-compression", name)) {
                serializationConfig.setEnableCompression(getBooleanValue(getTextContent(child)));
            } else if (matches("enable-shared-object", name)) {
                serializationConfig.setEnableSharedObject(getBooleanValue(getTextContent(child)));
            } else if (matches("allow-unsafe", name)) {
                serializationConfig.setAllowUnsafe(getBooleanValue(getTextContent(child)));
            } else if (matches("allow-override-default-serializers", name)) {
                serializationConfig.setAllowOverrideDefaultSerializers(getBooleanValue(getTextContent(child)));
            } else if (matches("data-serializable-factories", name)) {
                fillDataSerializableFactories(child, serializationConfig);
            } else if (matches("portable-factories", name)) {
                fillPortableFactories(child, serializationConfig);
            } else if (matches("serializers", name)) {
                fillSerializers(child, serializationConfig);
            } else if (matches("global-serializer", name)) {
                fillGlobalSerializer(child, serializationConfig);
            } else if (matches("java-serialization-filter", name)) {
                fillJavaSerializationFilter(child, serializationConfig);
            } else if (matches("compact-serialization", name)) {
                handleCompactSerialization(child, serializationConfig);
            }
        }
        return serializationConfig;
    }

    @Override
    protected void handleCompactSerialization(Node node, SerializationConfig serializationConfig) {
        CompactSerializationConfig compactSerializationConfig = serializationConfig.getCompactSerializationConfig();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("enabled", name)) {
                boolean enabled = getBooleanValue(getTextContent(child));
                compactSerializationConfig.setEnabled(enabled);
            } else if (matches("registered-classes", name)) {
                fillCompactSerializableClasses(child, compactSerializationConfig);
            }
        }
    }

    @Override
    protected void fillCompactSerializableClasses(Node node, CompactSerializationConfig compactSerializationConfig) {
        for (Node child : childElements(node)) {
            String className = getAttribute(child, "class");
            String typeName = getAttribute(child, "type-name");
            String serializerClassName = getAttribute(child, "serializer");
            registerCompactSerializableClass(compactSerializationConfig, className, typeName, serializerClassName);
        }
    }

    private void fillGlobalSerializer(Node child, SerializationConfig serializationConfig) {
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        String attrClassName = getAttribute(child, "class-name");
        String attrOverrideJavaSerialization = getAttribute(child, "override-java-serialization");
        boolean overrideJavaSerialization = attrOverrideJavaSerialization != null
                && getBooleanValue(attrOverrideJavaSerialization.trim());
        globalSerializerConfig.setClassName(attrClassName);
        globalSerializerConfig.setOverrideJavaSerialization(overrideJavaSerialization);
        serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);
    }

    @Override
    protected void fillSerializers(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
            SerializerConfig serializerConfig = new SerializerConfig();
            final String typeClassName = getAttribute(child, "type-class");
            final String className = getAttribute(child, "class-name");
            serializerConfig.setTypeClassName(typeClassName);
            serializerConfig.setClassName(className);
            serializationConfig.addSerializerConfig(serializerConfig);
        }
    }

    @Override
    protected void fillDataSerializableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
            final Node factoryIdNode = getNamedItemNode(child, "factory-id");
            final Node classNameNode = getNamedItemNode(child, "class-name");
            if (factoryIdNode == null) {
                throw new IllegalArgumentException("'factory-id' attribute of 'data-serializable-factory' is required!");
            }
            if (classNameNode == null) {
                throw new IllegalArgumentException("'class-name' attribute of 'data-serializable-factory' is required!");
            }
            int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
            String className = getTextContent(classNameNode);
            serializationConfig.addDataSerializableFactoryClass(factoryId, className);
        }
    }

    @Override
    protected void fillPortableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
            final Node factoryIdNode = getNamedItemNode(child, "factory-id");
            final Node classNameNode = getNamedItemNode(child, "class-name");
            if (factoryIdNode == null) {
                throw new IllegalArgumentException("'factory-id' attribute of 'portable-factory' is required!");
            }
            if (classNameNode == null) {
                throw new IllegalArgumentException("'class-name' attribute of 'portable-factory' is required!");
            }
            int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
            String className = getTextContent(classNameNode);
            serializationConfig.addPortableFactoryClass(factoryId, className);
        }
    }

    @Override
    protected ClassFilter parseClassFilterList(Node node) {
        ClassFilter list = new ClassFilter();
        for (Node typeNode : childElements(node)) {
            final String name = cleanNodeName(typeNode);
            if (matches("class", name)) {
                for (Node classNode : childElements(typeNode)) {
                    list.addClasses(getTextContent(classNode));
                }
            } else if (matches("package", name)) {
                for (Node packageNode : childElements(typeNode)) {
                    list.addPackages(getTextContent(packageNode));
                }
            } else if (matches("prefix", name)) {
                for (Node prefixNode : childElements(typeNode)) {
                    list.addPrefixes(getTextContent(prefixNode));
                }
            }
        }
        return list;
    }

    @Override
    protected void handleMemberAttributes(Node node) {
        for (Node n : childElements(node)) {
            String attributeValue = getTextContent(getNamedItemNode(n, "value"));
            String attributeName = n.getNodeName();
            handleMemberAttributesNode(attributeName, attributeValue);
        }
    }

    @Override
    protected void handleOutboundPorts(Node child) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        for (Node n : childElements(child)) {
            networkConfig.addOutboundPortDefinition(getTextContent(n));
        }
    }

    @Override
    protected void handleOutboundPorts(Node child, EndpointConfig endpointConfig) {
        for (Node n : childElements(child)) {
            endpointConfig.addOutboundPortDefinition(getTextContent(n));
        }
    }

    @Override
    protected void handleInterfacesList(Node node, InterfacesConfig interfaces) {
        for (Node interfacesNode : childElements(node)) {
            if (matches("interfaces", lowerCaseInternal(cleanNodeName(interfacesNode)))) {
                for (Node interfaceNode : childElements(interfacesNode)) {
                    interfaces.addInterface(getTextContent(interfaceNode));
                }
            }
        }
    }

    @Override
    protected void handleListeners(Node node) {
        for (Node child : childElements(node)) {
            String listenerClass = getTextContent(child);
            config.addListenerConfig(new ListenerConfig(listenerClass));
        }
    }

    @Override
    protected void handleMemberList(Node node, boolean advancedNetworkConfig) {
        JoinConfig join = joinConfig(advancedNetworkConfig);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        for (Node n : childElements(node)) {
            tcpIpConfig.addMember(getTextContent(n));
        }
    }

    @Override
    protected void handleRestApiEndpointGroups(Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("endpoint-groups", nodeName)) {
                for (Node groupNode : childElements(child)) {
                    String groupName = groupNode.getNodeName();
                    handleEndpointGroup(groupNode, groupName);
                }
            }
        }
    }

    @Override
    protected String extractName(Node node) {
        return node.getNodeName();
    }

    @Override
    protected void handlePort(Node node, ServerSocketEndpointConfig endpointConfig) {
        Node portNode = getNamedItemNode(node, "port");
        if (portNode != null) {
            String portStr = portNode.getNodeValue().trim();
            if (portStr.length() > 0) {
                endpointConfig.setPort(parseInt(portStr));
            }
        }
        handlePortAttributes(node, endpointConfig);
    }

    @Override
    protected void handleWanServerSocketEndpointConfig(Node node) throws Exception {
        for (Node wanEndpointNode : childElements(node)) {
            ServerSocketEndpointConfig config = new ServerSocketEndpointConfig();
            config.setProtocolType(ProtocolType.WAN);
            String name = wanEndpointNode.getNodeName();
            handleServerSocketEndpointConfig(config, wanEndpointNode, name);
        }
    }

    @Override
    protected void handleWanEndpointConfig(Node node) throws Exception {
        for (Node wanEndpointNode : childElements(node)) {
            EndpointConfig config = new EndpointConfig();
            config.setProtocolType(ProtocolType.WAN);
            String endpointName = wanEndpointNode.getNodeName().trim();
            handleEndpointConfig(config, wanEndpointNode, endpointName);
        }
    }

    @Override
    void handleSemaphores(CPSubsystemConfig cpSubsystemConfig, Node node) {
        for (Node child : childElements(node)) {
            SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
            semaphoreConfig.setName(child.getNodeName());
            for (Node subChild : childElements(child)) {
                String nodeName = cleanNodeName(subChild);
                if (matches("jdk-compatible", nodeName)) {
                    semaphoreConfig.setJDKCompatible(Boolean.parseBoolean(getTextContent(subChild)));
                } else if (matches("initial-permits", nodeName)) {
                    semaphoreConfig.setInitialPermits(Integer.parseInt(getTextContent(subChild)));
                }
            }
            cpSubsystemConfig.addSemaphoreConfig(semaphoreConfig);
        }
    }

    @Override
    void handleFencedLocks(CPSubsystemConfig cpSubsystemConfig, Node node) {
        for (Node child : childElements(node)) {
            FencedLockConfig lockConfig = new FencedLockConfig();
            lockConfig.setName(child.getNodeName());
            for (Node subChild : childElements(child)) {
                String nodeName = cleanNodeName(subChild);
                if (matches("lock-acquire-limit", nodeName)) {
                    lockConfig.setLockAcquireLimit(Integer.parseInt(getTextContent(subChild)));
                }
            }
            cpSubsystemConfig.addLockConfig(lockConfig);
        }
    }

    @Override
    protected void handleRealms(Node node) {
        for (Node child : childElements(node)) {
            handleRealm(child);
        }
    }

    @Override
    protected void handleJaasAuthentication(RealmConfig realmConfig, Node node) {
        JaasAuthenticationConfig jaasAuthenticationConfig = new JaasAuthenticationConfig();
        for (Node child : childElements(node)) {
            jaasAuthenticationConfig.addLoginModuleConfig(handleLoginModule(child));
        }
        realmConfig.setJaasAuthenticationConfig(jaasAuthenticationConfig);
    }

    @Override
    protected void handleToken(RealmConfig realmConfig, Node node) {
        TokenEncoding encoding = TokenEncoding.getTokenEncoding(getAttribute(node, "encoding"));
        TokenIdentityConfig tic = new TokenIdentityConfig(encoding, getAttribute(node, "value"));
        realmConfig.setTokenIdentityConfig(tic);
    }

    @Override
    protected void handleSimpleAuthentication(RealmConfig realmConfig, Node node) {
        SimpleAuthenticationConfig simpleCfg = new SimpleAuthenticationConfig();
        fillClusterLoginConfig(simpleCfg, node);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("users", nodeName)) {
                addSimpleUsers(simpleCfg, child);
            } else if (matches("role-separator", nodeName)) {
                simpleCfg.setRoleSeparator(getTextContent(child));
            }
        }
        realmConfig.setSimpleAuthenticationConfig(simpleCfg);
    }

    protected void addSimpleUsers(SimpleAuthenticationConfig simpleCfg, Node node) {
        for (Node child : childElements(node)) {
            simpleCfg.addUser(
                    getAttribute(child, "username"),
                    getAttribute(child, "password"),
                    getSimpleUserRoles(child));
        }
    }

    private String[] getSimpleUserRoles(Node node) {
        List<String> roles = new ArrayList<>();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("roles", nodeName)) {
                for (Node roleNode : childElements(child)) {
                    roles.add(getTextContent(roleNode));
                }
            }
        }
        return roles.toArray(new String[roles.size()]);
    }

    @Override
    protected void handlePersistentMemoryDirectory(PersistentMemoryConfig persistentMemoryConfig, Node dirNode) {
        String directory = getTextContent(getNamedItemNode(dirNode, "directory"));
        String numaNodeIdStr = getTextContent(getNamedItemNode(dirNode, "numa-node"));
        if (!StringUtil.isNullOrEmptyAfterTrim(numaNodeIdStr)) {
            int numaNodeId = getIntegerValue("numa-node", numaNodeIdStr);
            persistentMemoryConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig(directory, numaNodeId));
        } else {
            persistentMemoryConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig(directory));
        }
    }
}
