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

package com.hazelcast.config;

import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlScalar;
import com.hazelcast.internal.yaml.YamlSequence;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.nio.ByteOrder;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.config.yaml.W3cDomUtil.getWrappedYamlMapping;
import static com.hazelcast.config.yaml.W3cDomUtil.getWrappedYamlSequence;
import static com.hazelcast.internal.yaml.YamlUtil.asScalar;
import static com.hazelcast.util.StringUtil.lowerCaseInternal;
import static com.hazelcast.util.StringUtil.upperCaseInternal;
import static java.lang.Integer.parseInt;

class YamlMemberDomConfigProcessor extends MemberDomConfigProcessor {
    YamlMemberDomConfigProcessor(boolean domLevel3, Config config) {
        super(domLevel3, config);
    }

    @Override
    protected void handleSecurityInterceptorsChild(SecurityConfig cfg, Node child) {
        String className = child.getTextContent();
        cfg.addSecurityInterceptorConfig(new SecurityInterceptorConfig(className));
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:methodlength"})
    protected void handleSecurityPermissions(Node node) {
        String onJoinOp = getAttribute(node, "on-join-operation");
        if (onJoinOp != null) {
            OnJoinPermissionOperationName onJoinPermissionOperation = OnJoinPermissionOperationName
                    .valueOf(upperCaseInternal(onJoinOp));
            config.getSecurityConfig().setOnJoinPermissionOperation(onJoinPermissionOperation);
        }
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            PermissionConfig.PermissionType type;
            if ("map".equals(nodeName)) {
                type = PermissionConfig.PermissionType.MAP;
            } else if ("queue".equals(nodeName)) {
                type = PermissionConfig.PermissionType.QUEUE;
            } else if ("multimap".equals(nodeName)) {
                type = PermissionConfig.PermissionType.MULTIMAP;
            } else if ("topic".equals(nodeName)) {
                type = PermissionConfig.PermissionType.TOPIC;
            } else if ("list".equals(nodeName)) {
                type = PermissionConfig.PermissionType.LIST;
            } else if ("set".equals(nodeName)) {
                type = PermissionConfig.PermissionType.SET;
            } else if ("lock".equals(nodeName)) {
                type = PermissionConfig.PermissionType.LOCK;
            } else if ("atomic-long".equals(nodeName)) {
                type = PermissionConfig.PermissionType.ATOMIC_LONG;
            } else if ("atomic-reference".equals(nodeName)) {
                type = PermissionConfig.PermissionType.ATOMIC_REFERENCE;
            } else if ("countdown-latch".equals(nodeName)) {
                type = PermissionConfig.PermissionType.COUNTDOWN_LATCH;
            } else if ("semaphore".equals(nodeName)) {
                type = PermissionConfig.PermissionType.SEMAPHORE;
            } else if ("id-generator".equals(nodeName)) {
                type = PermissionConfig.PermissionType.ID_GENERATOR;
            } else if ("flake-id-generator".equals(nodeName)) {
                type = PermissionConfig.PermissionType.FLAKE_ID_GENERATOR;
            } else if ("executor-service".equals(nodeName)) {
                type = PermissionConfig.PermissionType.EXECUTOR_SERVICE;
            } else if ("transaction".equals(nodeName)) {
                type = PermissionConfig.PermissionType.TRANSACTION;
            } else if ("all".equals(nodeName)) {
                type = PermissionConfig.PermissionType.ALL;
            } else if ("durable-executor-service".equals(nodeName)) {
                type = PermissionConfig.PermissionType.DURABLE_EXECUTOR_SERVICE;
            } else if ("cardinality-estimator".equals(nodeName)) {
                type = PermissionConfig.PermissionType.CARDINALITY_ESTIMATOR;
            } else if ("scheduled-executor".equals(nodeName)) {
                type = PermissionConfig.PermissionType.SCHEDULED_EXECUTOR;
            } else if ("pn-counter".equals(nodeName)) {
                type = PermissionConfig.PermissionType.PN_COUNTER;
            } else if ("cache".equals(nodeName)) {
                type = PermissionConfig.PermissionType.CACHE;
            } else if ("user-code-deployment".equals(nodeName)) {
                type = PermissionConfig.PermissionType.USER_CODE_DEPLOYMENT;
            } else if ("config".equals(nodeName)) {
                type = PermissionConfig.PermissionType.CONFIG;
            } else {
                continue;
            }

            if (PermissionConfig.PermissionType.CONFIG == type
                    || PermissionConfig.PermissionType.ALL == type
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
            permConfig.addAction(getTextContent(child).trim());
        }
    }

    @Override
    void handleSecurityPermissionEndpoints(Node node, PermissionConfig permConfig) {
        for (Node child : childElements(node)) {
            permConfig.addEndpoint(getTextContent(child).trim());
        }
    }

    protected void handleLoginModules(Node node, boolean member, Config config) {
        SecurityConfig cfg = config.getSecurityConfig();
        for (Node child : childElements(node)) {
            LoginModuleConfig lm = handleLoginModule(child);
            if (member) {
                cfg.addMemberLoginModuleConfig(lm);
            } else {
                cfg.addClientLoginModuleConfig(lm);
            }
        }
    }

    @Override
    protected void handleTrustedInterfaces(MulticastConfig multicastConfig, Node n) {
        YamlSequence yamlNode = getWrappedYamlSequence(n);
        for (YamlNode interfaceNode : yamlNode.children()) {
            String trustedInterface = asScalar(interfaceNode).nodeValue();
            multicastConfig.addTrustedInterface(trustedInterface);
        }
        super.handleTrustedInterfaces(multicastConfig, n);
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
        if ("wan-publisher".equals(nodeName)) {
            for (Node publisherNode : childElements(nodeTarget)) {
                WanPublisherConfig publisherConfig = new WanPublisherConfig();
                String groupNameOrPublisherId = publisherNode.getNodeName();
                Node groupNameAttr = publisherNode.getAttributes().getNamedItem("group-name");

                // the publisher's key may mean either the publisher-id or the
                // group-name depending on whether the group-name is explicitly defined
                String groupName = groupNameAttr != null ? groupNameAttr.getTextContent() : groupNameOrPublisherId;
                String publisherId = groupNameAttr != null ? groupNameOrPublisherId : null;
                publisherConfig.setPublisherId(publisherId);
                publisherConfig.setGroupName(groupName);

                handleWanPublisherNode(wanReplicationConfig, publisherNode, publisherConfig);
            }
        } else if ("wan-consumer".equals(nodeName)) {
            handleWanConsumerNode(wanReplicationConfig, nodeTarget);
        }
    }

    @Override
    protected void handlePort(Node node, Config config) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        NamedNodeMap attributes = node.getAttributes();
        for (int a = 0; a < attributes.getLength(); a++) {
            Node att = attributes.item(a);
            String value = getTextContent(att).trim();

            if ("port".equals(att.getNodeName())) {
                int portCount = parseInt(value);
                networkConfig.setPort(portCount);
            } else if ("auto-increment".equals(att.getNodeName())) {
                networkConfig.setPortAutoIncrement(getBooleanValue(value));
            } else if ("port-count".equals(att.getNodeName())) {
                int portCount = parseInt(value);
                networkConfig.setPortCount(portCount);
            }
        }
    }

    @Override
    protected void handleSemaphore(Node node) {
        for (Node semaphoreNode : childElements(node)) {
            SemaphoreConfig sConfig = new SemaphoreConfig();
            sConfig.setName(semaphoreNode.getNodeName());
            handleSemaphoreNode(semaphoreNode, sConfig);
        }
    }

    @Override
    protected void handleQueue(Node node) {
        for (Node queueNode : childElements(node)) {
            QueueConfig queueConfig = new QueueConfig();
            queueConfig.setName(queueNode.getNodeName());
            handleQueueNode(queueNode, queueConfig);
        }
    }

    @Override
    protected void handleList(Node node) {
        for (Node listNode : childElements(node)) {
            ListConfig listConfig = new ListConfig();
            listConfig.setName(listNode.getNodeName());
            handleListNode(listNode, listConfig);
        }
    }

    @Override
    protected void handleSet(Node node) {
        for (Node setNode : childElements(node)) {
            SetConfig setConfig = new SetConfig();
            setConfig.setName(setNode.getNodeName());
            handleSetNode(setNode, setConfig);
        }
    }

    @Override
    protected void handleLock(Node node) {
        for (Node lockNode : childElements(node)) {
            LockConfig lockConfig = new LockConfig();
            lockConfig.setName(lockNode.getNodeName());
            handleLockNode(lockNode, lockConfig);
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
            RingbufferConfig ringBufferConfig = new RingbufferConfig();
            ringBufferConfig.setName(rbNode.getNodeName());
            handleRingBufferNode(rbNode, ringBufferConfig);
        }
    }

    @Override
    protected void handleAtomicLong(Node node) {
        for (Node atomicLongNode : childElements(node)) {
            AtomicLongConfig atomicLongConfig = new AtomicLongConfig();
            atomicLongConfig.setName(atomicLongNode.getNodeName());
            handleAtomicLongNode(atomicLongNode, atomicLongConfig);
        }
    }

    @Override
    protected void handleAtomicReference(Node node) {
        for (Node atomicReferenceNode : childElements(node)) {
            AtomicReferenceConfig atomicReferenceConfig = new AtomicReferenceConfig();
            atomicReferenceConfig.setName(atomicReferenceNode.getNodeName());
            handleAtomicReferenceNode(atomicReferenceNode, atomicReferenceConfig);
        }
    }

    @Override
    protected void handleCountDownLatchConfig(Node node) {
        for (Node countDownLatchNode : childElements(node)) {
            CountDownLatchConfig countDownLatchConfig = new CountDownLatchConfig();
            countDownLatchConfig.setName(countDownLatchNode.getNodeName());
            handleCountDownLatchNode(countDownLatchNode, countDownLatchConfig);
        }
    }

    @Override
    protected void handleMap(Node parentNode) throws Exception {
        for (Node mapNode : childElements(parentNode)) {
            MapConfig mapConfig = new MapConfig();
            mapConfig.setName(mapNode.getNodeName());
            handleMapNode(mapNode, mapConfig);
        }
    }

    @Override
    protected void handleCache(Node parentNode) throws Exception {
        for (Node cacheNode : childElements(parentNode)) {
            CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
            cacheConfig.setName(cacheNode.getNodeName());
            handleCacheNode(cacheNode, cacheConfig);
        }
    }

    @Override
    protected void handleQuorum(Node node) {
        for (Node quorumNode : childElements(node)) {
            QuorumConfig quorumConfig = new QuorumConfig();
            String quorumName = quorumNode.getNodeName();
            quorumConfig.setName(quorumName);
            handleQuorumNode(quorumNode, quorumConfig, quorumName);
        }
    }

    @Override
    protected void handleFlakeIdGenerator(Node node) {
        for (Node genNode : childElements(node)) {
            FlakeIdGeneratorConfig genConfig = new FlakeIdGeneratorConfig();
            genConfig.setName(genNode.getNodeName());
            handleFlakeIdGeneratorNode(genNode, genConfig);
        }
    }

    @Override
    protected void handleExecutor(Node node) throws Exception {
        for (Node executorNode : childElements(node)) {
            ExecutorConfig executorConfig = new ExecutorConfig();
            executorConfig.setName(executorNode.getNodeName());
            handleViaReflection(executorNode, config, executorConfig);
        }
    }

    @Override
    protected void handleDurableExecutor(Node node) throws Exception {
        for (Node executorNode : childElements(node)) {
            DurableExecutorConfig executorConfig = new DurableExecutorConfig();
            executorConfig.setName(executorNode.getNodeName());
            handleViaReflection(executorNode, config, executorConfig);
        }
    }

    @Override
    protected void handleScheduledExecutor(Node node) {
        for (Node executorNode : childElements(node)) {
            ScheduledExecutorConfig executorConfig = new ScheduledExecutorConfig();
            executorConfig.setName(executorNode.getNodeName());
            handleScheduledExecutorNode(executorNode, executorConfig);
        }
    }

    @Override
    protected void handleCardinalityEstimator(Node node) {
        for (Node estimatorNode : childElements(node)) {
            CardinalityEstimatorConfig estimatorConfig = new CardinalityEstimatorConfig();
            estimatorConfig.setName(estimatorNode.getNodeName());
            handleCardinalityEstimatorNode(estimatorNode, estimatorConfig);
        }
    }

    @Override
    protected void handlePNCounter(Node node) throws Exception {
        for (Node counterNode : childElements(node)) {
            PNCounterConfig counterConfig = new PNCounterConfig();
            counterConfig.setName(counterNode.getNodeName());
            handleViaReflection(counterNode, config, counterConfig);
        }
    }

    @Override
    protected void handleMultiMap(Node node) {
        for (Node multiMapNode : childElements(node)) {
            MultiMapConfig multiMapConfig = new MultiMapConfig();
            multiMapConfig.setName(multiMapNode.getNodeName());
            handleMultiMapNode(multiMapNode, multiMapConfig);
        }
    }

    @Override
    protected void handleReplicatedMap(Node node) {
        for (Node replicatedMapNode : childElements(node)) {
            ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
            replicatedMapConfig.setName(replicatedMapNode.getNodeName());
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
    protected void handleMaxSizeConfig(MapConfig mapConfig, Node node, String value) {
        MaxSizeConfig msc = mapConfig.getMaxSizeConfig();
        NamedNodeMap attributes = node.getAttributes();
        Node maxSizePolicy = attributes.getNamedItem("policy");
        if (maxSizePolicy != null) {
            msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.valueOf(
                    upperCaseInternal(getTextContent(maxSizePolicy))));
        }
        msc.setSize(getIntegerValue("max-size", getTextContent(attributes.getNamedItem("max-size"))));
    }

    @Override
    protected void mapIndexesHandle(Node n, MapConfig mapConfig) {
        for (Node indexNode : childElements(n)) {
            NamedNodeMap attrs = indexNode.getAttributes();
            boolean ordered = getBooleanValue(getTextContent(attrs.getNamedItem("ordered")));
            String attribute = indexNode.getNodeName();
            mapConfig.addMapIndexConfig(new MapIndexConfig(attribute, ordered));
        }
    }

    @Override
    protected void mapAttributesHandle(Node n, MapConfig mapConfig) {
        for (Node extractorNode : childElements(n)) {
            NamedNodeMap attrs = extractorNode.getAttributes();
            String extractor = getTextContent(attrs.getNamedItem("extractor"));
            String name = extractorNode.getNodeName();
            mapConfig.addMapAttributeConfig(new MapAttributeConfig(name, extractor));
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
        NamedNodeMap predicateAttributes = childNode.getAttributes();
        Node classNameNode = predicateAttributes.getNamedItem("class-name");
        Node sqlNode = predicateAttributes.getNamedItem("sql");

        if (classNameNode != null && sqlNode != null) {
            throw new InvalidConfigurationException("Both class-name and sql is defined for the predicate of map "
                    + childNode.getParentNode().getParentNode().getNodeName());
        }

        if (classNameNode == null && sqlNode == null) {
            throw new InvalidConfigurationException("Either class-name and sql should be defined for the predicate of map "
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
            NamedNodeMap attrs = indexNode.getAttributes();
            boolean ordered = getBooleanValue(getTextContent(attrs.getNamedItem("ordered")));
            String attribute = indexNode.getNodeName();
            queryCacheConfig.addIndexConfig(new MapIndexConfig(attribute, ordered));
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
    protected MergePolicyConfig createMergePolicyConfig(Node node) {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
        String policyString = getTextContent(node.getAttributes().getNamedItem("class-name"));
        mergePolicyConfig.setPolicy(policyString);
        final String att = getAttribute(node, "batch-size");
        if (att != null) {
            mergePolicyConfig.setBatchSize(getIntegerValue("batch-size", att));
        }
        return mergePolicyConfig;
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
    protected void handleItemListeners(Node n, Function<ItemListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            NamedNodeMap attrs = listenerNode.getAttributes();
            boolean incValue = getBooleanValue(getTextContent(attrs.getNamedItem("include-value")));
            String listenerClass = getTextContent(attrs.getNamedItem("class-name"));
            configAddFunction.apply(new ItemListenerConfig(listenerClass, incValue));
        }
    }

    @Override
    protected void handleEntryListeners(Node n, Function<EntryListenerConfig, Void> configAddFunction) {
        for (Node listenerNode : childElements(n)) {
            NamedNodeMap attrs = listenerNode.getAttributes();
            boolean incValue = getBooleanValue(getTextContent(attrs.getNamedItem("include-value")));
            boolean local = getBooleanValue(getTextContent(attrs.getNamedItem("local")));
            String listenerClass = getTextContent(attrs.getNamedItem("class-name"));
            configAddFunction.apply(new EntryListenerConfig(listenerClass, local, incValue));
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
    protected void handleQuorumListeners(QuorumConfig quorumConfig, Node n) {
        for (Node listenerNode : childElements(n)) {
            String listenerClass = listenerNode.getNodeValue().trim();
            quorumConfig.addListenerConfig(new QuorumListenerConfig(listenerClass));
        }
    }

    @Override
    protected void handleServiceNodes(Node node, ServicesConfig servicesConfig) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (!"enable-defaults".equals(nodeName)) {
                ServiceConfig serviceConfig = new ServiceConfig();
                serviceConfig.setName(nodeName);
                String enabledValue = getAttribute(child, "enabled");
                boolean enabled = getBooleanValue(enabledValue);
                serviceConfig.setEnabled(enabled);

                for (Node n : childElements(child)) {
                    handleServiceNode(n, serviceConfig);
                }
                servicesConfig.addServiceConfig(serviceConfig);
            }
        }
    }

    @Override
    protected void fillProperties(Node node, Map<String, Comparable> properties) {
        YamlMapping propertiesMapping = getWrappedYamlMapping(node);
        for (YamlNode propNode : propertiesMapping.children()) {
            YamlScalar propScalar = asScalar(propNode);
            String key = propScalar.nodeName();
            String value = propScalar.nodeValue().toString();
            properties.put(key, value);
        }
    }

    @Override
    protected void fillProperties(Node node, Properties properties) {
        YamlMapping propertiesMapping = getWrappedYamlMapping(node);
        for (YamlNode propNode : propertiesMapping.children()) {
            YamlScalar propScalar = asScalar(propNode);
            String key = propScalar.nodeName();
            String value = propScalar.nodeValue().toString();
            properties.put(key, value);
        }
    }

    @Override
    protected void handleDiscoveryStrategiesChild(DiscoveryConfig discoveryConfig, Node child) {
        String name = cleanNodeName(child);
        if ("discovery-strategies".equals(name)) {
            NodeList strategies = child.getChildNodes();
            for (int i = 0; i < strategies.getLength(); i++) {
                Node strategy = strategies.item(i);
                handleDiscoveryStrategy(strategy, discoveryConfig);
            }
        } else if ("node-filter".equals(name)) {
            handleDiscoveryNodeFilter(child, discoveryConfig);
        }
    }

    @Override
    protected SerializationConfig parseSerialization(final Node node) {
        SerializationConfig serializationConfig = new SerializationConfig();
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if ("portable-version".equals(name)) {
                String value = getTextContent(child);
                serializationConfig.setPortableVersion(getIntegerValue(name, value));
            } else if ("check-class-def-errors".equals(name)) {
                String value = getTextContent(child);
                serializationConfig.setCheckClassDefErrors(getBooleanValue(value));
            } else if ("use-native-byte-order".equals(name)) {
                serializationConfig.setUseNativeByteOrder(getBooleanValue(getTextContent(child)));
            } else if ("byte-order".equals(name)) {
                String value = getTextContent(child);
                ByteOrder byteOrder = null;
                if (ByteOrder.BIG_ENDIAN.toString().equals(value)) {
                    byteOrder = ByteOrder.BIG_ENDIAN;
                } else if (ByteOrder.LITTLE_ENDIAN.toString().equals(value)) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }
                serializationConfig.setByteOrder(byteOrder != null ? byteOrder : ByteOrder.BIG_ENDIAN);
            } else if ("enable-compression".equals(name)) {
                serializationConfig.setEnableCompression(getBooleanValue(getTextContent(child)));
            } else if ("enable-shared-object".equals(name)) {
                serializationConfig.setEnableSharedObject(getBooleanValue(getTextContent(child)));
            } else if ("allow-unsafe".equals(name)) {
                serializationConfig.setAllowUnsafe(getBooleanValue(getTextContent(child)));
            } else if ("data-serializable-factories".equals(name)) {
                fillDataSerializableFactories(child, serializationConfig);
            } else if ("portable-factories".equals(name)) {
                fillPortableFactories(child, serializationConfig);
            } else if ("serializers".equals(name)) {
                fillSerializers(child, serializationConfig);
            } else if ("global-serializer".equals(name)) {
                fillGlobalSerializer(child, serializationConfig);
            } else if ("java-serialization-filter".equals(name)) {
                fillJavaSerializationFilter(child, serializationConfig);
            }
        }
        return serializationConfig;
    }

    private void fillGlobalSerializer(Node child, SerializationConfig serializationConfig) {
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        String attrClassName = getAttribute(child, "class-name");
        String attrOverrideJavaSerialization = getAttribute(child, "override-java-serialization");
        boolean overrideJavaSerialization =
                attrOverrideJavaSerialization != null && getBooleanValue(attrOverrideJavaSerialization.trim());
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
            NamedNodeMap attributes = child.getAttributes();
            final Node factoryIdNode = attributes.getNamedItem("factory-id");
            final Node classNameNode = attributes.getNamedItem("class-name");
            if (factoryIdNode == null) {
                throw new IllegalArgumentException(
                        "'factory-id' attribute of 'data-serializable-factory' is required!");
            }
            if (classNameNode == null) {
                throw new IllegalArgumentException(
                        "'class-name' attribute of 'data-serializable-factory' is required!");
            }
            int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
            String className = getTextContent(classNameNode);
            serializationConfig.addDataSerializableFactoryClass(factoryId, className);
        }
    }

    @Override
    protected void fillPortableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
            NamedNodeMap attributes = child.getAttributes();
            final Node factoryIdNode = attributes.getNamedItem("factory-id");
            final Node classNameNode = attributes.getNamedItem("class-name");
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
            if ("class".equals(name)) {
                for (Node classNode : childElements(typeNode)) {
                    list.addClasses(getTextContent(classNode));
                }
            } else if ("package".equals(name)) {
                for (Node packageNode : childElements(typeNode)) {
                    list.addPackages(getTextContent(packageNode));
                }
            } else if ("prefix".equals(name)) {
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
            String attributeValue = getTextContent(n.getAttributes().getNamedItem("value"));
            String attributeName = n.getNodeName();
            handleMemberAttributesNode(n, attributeName, attributeValue);
        }
    }

    @Override
    protected void handleOutboundPorts(Node child) {
        NetworkConfig networkConfig = config.getNetworkConfig();
        for (Node n : childElements(child)) {
            String value = getTextContent(n);
            networkConfig.addOutboundPortDefinition(value);
        }
    }

    @Override
    protected void handleOutboundPorts(Node child, EndpointConfig endpointConfig) {
        for (Node n : childElements(child)) {
            String value = getTextContent(n);
            endpointConfig.addOutboundPortDefinition(value);
        }
    }

    @Override
    protected void handleInterfacesList(Node node, InterfacesConfig interfaces) {
        for (Node interfacesNode : childElements(node)) {
            if ("interfaces".equals(lowerCaseInternal(cleanNodeName(interfacesNode)))) {
                for (Node interfaceNode : childElements(interfacesNode)) {
                    String value = getTextContent(interfaceNode).trim();
                    interfaces.addInterface(value);
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
            String value = getTextContent(n).trim();
            tcpIpConfig.addMember(value);
        }
    }

    @Override
    protected void handleRestApiEndpointGroups(Node node) {
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if ("endpoint-groups".equals(nodeName)) {
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
        Node portNode = node.getAttributes().getNamedItem("port");
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
    void handleCPSemaphores(CPSubsystemConfig cpSubsystemConfig, Node node) {
        for (Node child : childElements(node)) {
            CPSemaphoreConfig cpSemaphoreConfig = new CPSemaphoreConfig();
            cpSemaphoreConfig.setName(child.getNodeName());
            for (Node subChild : childElements(child)) {
                String nodeName = cleanNodeName(subChild);
                String value = getTextContent(subChild).trim();
                if ("jdk-compatible".equals(nodeName)) {
                    cpSemaphoreConfig.setJDKCompatible(Boolean.parseBoolean(value));
                }
            }
            cpSubsystemConfig.addSemaphoreConfig(cpSemaphoreConfig);
        }
    }

    @Override
    void handleFencedLocks(CPSubsystemConfig cpSubsystemConfig, Node node) {
        for (Node child : childElements(node)) {
            FencedLockConfig lockConfig = new FencedLockConfig();
            lockConfig.setName(child.getNodeName());
            for (Node subChild : childElements(child)) {
                String nodeName = cleanNodeName(subChild);
                String value = getTextContent(subChild).trim();
                if ("lock-acquire-limit".equals(nodeName)) {
                    lockConfig.setLockAcquireLimit(Integer.parseInt(value));
                }
            }
            cpSubsystemConfig.addLockConfig(lockConfig);
        }
    }
}
