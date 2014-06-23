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

package com.hazelcast.spring;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.PermissionPolicyConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.spring.context.SpringManagedContext;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.Assert;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.hazelcast.util.StringUtil.upperCaseInternal;

public class HazelcastConfigBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlConfigBuilder springXmlConfigBuilder = new SpringXmlConfigBuilder(parserContext);
        springXmlConfigBuilder.handleConfig(element);
        return springXmlConfigBuilder.getBeanDefinition();
    }

    private class SpringXmlConfigBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;

        private ManagedMap mapConfigManagedMap;
        private ManagedMap queueManagedMap;
        private ManagedMap listManagedMap;
        private ManagedMap setManagedMap;
        private ManagedMap topicManagedMap;
        private ManagedMap multiMapManagedMap;
        private ManagedMap executorManagedMap;
        private ManagedMap wanReplicationManagedMap;
        private ManagedMap jobTrackerManagedMap;

        public SpringXmlConfigBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.configBuilder = BeanDefinitionBuilder.rootBeanDefinition(Config.class);
            this.mapConfigManagedMap = createManagedMap("mapConfigs");
            this.queueManagedMap = createManagedMap("queueConfigs");
            this.listManagedMap = createManagedMap("listConfigs");
            this.setManagedMap = createManagedMap("setConfigs");
            this.topicManagedMap = createManagedMap("topicConfigs");
            this.multiMapManagedMap = createManagedMap("multiMapConfigs");
            this.executorManagedMap = createManagedMap("executorConfigs");
            this.wanReplicationManagedMap = createManagedMap("wanReplicationConfigs");
            this.jobTrackerManagedMap = createManagedMap("jobTrackerConfigs");

            BeanDefinitionBuilder managedContextBeanBuilder = createBeanBuilder(SpringManagedContext.class);
            this.configBuilder.addPropertyValue("managedContext", managedContextBeanBuilder.getBeanDefinition());
        }

        private ManagedMap createManagedMap(String configName) {
            ManagedMap managedMap = new ManagedMap();
            this.configBuilder.addPropertyValue(configName, managedMap);
            return managedMap;
        }

        public AbstractBeanDefinition getBeanDefinition() {
            return configBuilder.getBeanDefinition();
        }

        public void handleConfig(final Element element) {
            handleCommonBeanAttributes(element, configBuilder, parserContext);
            for (org.w3c.dom.Node node : new IterableNodeList(element, Node.ELEMENT_NODE)) {
                // handleViaReflection(node);
                final String nodeName = cleanNodeName(node.getNodeName());
                if ("network".equals(nodeName)) {
                    handleNetwork(node);
                } else if ("group".equals(nodeName)) {
                    handleGroup(node);
                } else if ("properties".equals(nodeName)) {
                    handleProperties(node);
                } else if ("executor-service".equals(nodeName)) {
                    handleExecutor(node);
                } else if ("queue".equals(nodeName)) {
                    handleQueue(node);
                } else if ("map".equals(nodeName)) {
                    handleMap(node);
                } else if ("multimap".equals(nodeName)) {
                    handleMultiMap(node);
                } else if ("list".equals(nodeName)) {
                    handleList(node);
                } else if ("set".equals(nodeName)) {
                    handleSet(node);
                } else if ("topic".equals(nodeName)) {
                    handleTopic(node);
                } else if ("jobtracker".equals(nodeName)) {
                    handleJobTracker(node);
                } else if ("wan-replication".equals(nodeName)) {
                    handleWanReplication(node);
                } else if ("partition-group".equals(nodeName)) {
                    handlePartitionGroup(node);
                } else if ("serialization".equals(nodeName)) {
                    handleSerialization(node);
                } else if ("security".equals(nodeName)) {
                    handleSecurity(node);
                } else if ("instance-name".equals(nodeName)) {
                    configBuilder.addPropertyValue(xmlToJavaName(nodeName), getTextContent(node));
                } else if ("listeners".equals(nodeName)) {
                    final List listeners = parseListeners(node, ListenerConfig.class);
                    configBuilder.addPropertyValue("listenerConfigs", listeners);
                } else if ("lite-member".equals(nodeName)) {
                    configBuilder.addPropertyValue(xmlToJavaName(nodeName), getTextContent(node));
                } else if ("license-key".equals(nodeName)) {
                    configBuilder.addPropertyValue(xmlToJavaName(nodeName), getTextContent(node));
                } else if ("management-center".equals(nodeName)) {
                    handleManagementCenter(node);
                }
            }
        }

        public void handleNetwork(Node node) {
            BeanDefinitionBuilder networkConfigBuilder = createBeanBuilder(NetworkConfig.class);
            final AbstractBeanDefinition beanDefinition = networkConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, networkConfigBuilder);
            for (org.w3c.dom.Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(child);
                if ("join".equals(nodeName)) {
                    handleJoin(child, networkConfigBuilder);
                } else if ("interfaces".equals(nodeName)) {
                    handleInterfaces(child, networkConfigBuilder);
                } else if ("symmetric-encryption".equals(nodeName)) {
                    handleSymmetricEncryption(child, networkConfigBuilder);
                } else if ("ssl".equals(nodeName)) {
                    handleSSLConfig(child, networkConfigBuilder);
                } else if ("socket-interceptor".equals(nodeName)) {
                    handleSocketInterceptorConfig(child, networkConfigBuilder);
                } else if ("outbound-ports".equals(nodeName)) {
                    handleOutboundPorts(child, networkConfigBuilder);
                }
            }
            configBuilder.addPropertyValue("networkConfig", beanDefinition);
        }

/*
        protected void handleViaReflection(org.w3c.dom.Node child) {
            final String methodName = xmlToJavaName("handle-" + cleanNodeName(child));
            final Method method;
            try {
                method = getClass().getMethod(methodName, new Class[]{org.w3c.dom.Node.class});
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
                return;
            }
            try {
                method.invoke(this, new Object[]{child});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
*/

        public void handleGroup(Node node) {
            createAndFillBeanBuilder(node, GroupConfig.class, "groupConfig", configBuilder);
        }

        public void handleProperties(Node node) {
            handleProperties(node, configBuilder);
        }

        public void handleInterfaces(Node node, final BeanDefinitionBuilder networkConfigBuilder) {
            BeanDefinitionBuilder builder = createBeanBuilder(InterfacesConfig.class);
            final AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
            final NamedNodeMap atts = node.getAttributes();
            if (atts != null) {
                for (int a = 0; a < atts.getLength(); a++) {
                    final org.w3c.dom.Node att = atts.item(a);
                    final String name = xmlToJavaName(att.getNodeName());
                    final String value = att.getNodeValue();
                    builder.addPropertyValue(name, value);
                }
            }
            ManagedList interfacesSet = new ManagedList();
            for (org.w3c.dom.Node n : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                String name = xmlToJavaName(cleanNodeName(n));
                String value = getTextContent(n);
                if ("interface".equals(name)) {
                    interfacesSet.add(value);
                }
            }
            builder.addPropertyValue("interfaces", interfacesSet);
            networkConfigBuilder.addPropertyValue("interfaces", beanDefinition);
        }

        public void handleJoin(Node node, BeanDefinitionBuilder networkConfigBuilder) {
            BeanDefinitionBuilder joinConfigBuilder = createBeanBuilder(JoinConfig.class);
            final AbstractBeanDefinition beanDefinition = joinConfigBuilder.getBeanDefinition();
            for (org.w3c.dom.Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(child);
                if ("multicast".equals(name)) {
                    handleMulticast(child, joinConfigBuilder);
                } else if ("tcp-ip".equals(name)) {
                    handleTcpIp(child, joinConfigBuilder);
                } else if ("aws".equals(name)) {
                    handleAws(child, joinConfigBuilder);
                }
            }
            networkConfigBuilder.addPropertyValue("join", beanDefinition);
        }


        private void handleOutboundPorts(final Node node, final BeanDefinitionBuilder networkConfigBuilder) {
            ManagedList outboundPorts = new ManagedList();
            for (org.w3c.dom.Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(child);
                if ("ports".equals(name)) {
                    String value = getTextContent(child);
                    outboundPorts.add(value);
                }
            }
            networkConfigBuilder.addPropertyValue("outboundPortDefinitions", outboundPorts);
        }

        private void handleSSLConfig(final Node node, final BeanDefinitionBuilder networkConfigBuilder) {
            BeanDefinitionBuilder sslConfigBuilder = createBeanBuilder(SSLConfig.class);
            final String implAttribute = "factory-implementation";
            fillAttributeValues(node, sslConfigBuilder, implAttribute);
            Node implNode = node.getAttributes().getNamedItem(implAttribute);
            String implementation = implNode != null ? getTextContent(implNode) : null;
            if (implementation != null) {
                sslConfigBuilder.addPropertyReference(xmlToJavaName(implAttribute), implementation);
            }
            for (org.w3c.dom.Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    handleProperties(child, sslConfigBuilder);
                }
            }
            networkConfigBuilder.addPropertyValue("SSLConfig", sslConfigBuilder.getBeanDefinition());
        }

        public void handleSymmetricEncryption(Node node, BeanDefinitionBuilder networkConfigBuilder) {
            createAndFillBeanBuilder(node, SymmetricEncryptionConfig.class, "symmetricEncryptionConfig", networkConfigBuilder);
        }

        public void handleExecutor(Node node) {
            createAndFillListedBean(node, ExecutorConfig.class, "name", executorManagedMap);
        }

        public void handleMulticast(Node node, BeanDefinitionBuilder joinConfigBuilder) {
            createAndFillBeanBuilder(node, MulticastConfig.class, "multicastConfig", joinConfigBuilder);
        }

        public void handleTcpIp(Node node, BeanDefinitionBuilder joinConfigBuilder) {
            final BeanDefinitionBuilder builder =
                    createAndFillBeanBuilder(node, TcpIpConfig.class,
                            "tcpIpConfig",
                            joinConfigBuilder,
                            "interface", "member", "members");
            final ManagedList members = new ManagedList();
            for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                String name = xmlToJavaName(cleanNodeName(n.getNodeName()));
                if ("member".equals(name) || "members".equals(name) || "interface".equals(name)) {
                    String value = getTextContent(n);
                    members.add(value);
                }
            }
            builder.addPropertyValue("members", members);
        }

        public void handleAws(Node node, BeanDefinitionBuilder joinConfigBuilder) {
            createAndFillBeanBuilder(node, AwsConfig.class, "awsConfig", joinConfigBuilder);
        }

        public void handleQueue(Node node) {
            BeanDefinitionBuilder queueConfigBuilder = createBeanBuilder(QueueConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, queueConfigBuilder);
            for (org.w3c.dom.Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(childNode);
                if ("item-listeners".equals(nodeName)) {
                    ManagedList listeners = parseListeners(childNode, ItemListenerConfig.class);
                    queueConfigBuilder.addPropertyValue("itemListenerConfigs", listeners);
                } else if ("queue-store".equals(nodeName)) {
                    handleQueueStoreConfig(childNode, queueConfigBuilder);
                }
            }
            queueManagedMap.put(name, queueConfigBuilder.getBeanDefinition());
        }

        public void handleQueueStoreConfig(Node node, BeanDefinitionBuilder queueConfigBuilder) {
            BeanDefinitionBuilder queueStoreConfigBuilder = createBeanBuilder(QueueStoreConfig.class);
            final AbstractBeanDefinition beanDefinition = queueStoreConfigBuilder.getBeanDefinition();
            for (org.w3c.dom.Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                if ("properties".equals(cleanNodeName(child))) {
                    handleProperties(child, queueStoreConfigBuilder);
                    break;
                }
            }
            final String implAttrName = "store-implementation";
            final String factoryImplAttrName = "factory-implementation";
            fillAttributeValues(node, queueStoreConfigBuilder, implAttrName, factoryImplAttrName);
            final NamedNodeMap attributes = node.getAttributes();
            final Node implRef = attributes.getNamedItem(implAttrName);
            final Node factoryImplRef = attributes.getNamedItem(factoryImplAttrName);
            if (factoryImplRef != null) {
                queueStoreConfigBuilder.addPropertyReference(xmlToJavaName(factoryImplAttrName), getTextContent(factoryImplRef));
            }
            if (implRef != null) {
                queueStoreConfigBuilder.addPropertyReference(xmlToJavaName(implAttrName), getTextContent(implRef));
            }
            queueConfigBuilder.addPropertyValue("queueStoreConfig", beanDefinition);
        }

        public void handleList(Node node) {
            BeanDefinitionBuilder listConfigBuilder = createBeanBuilder(ListConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, listConfigBuilder);
            for (org.w3c.dom.Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                if ("item-listeners".equals(cleanNodeName(childNode))) {
                    ManagedList listeners = parseListeners(childNode, ItemListenerConfig.class);
                    listConfigBuilder.addPropertyValue("itemListenerConfigs", listeners);
                }
            }
            listManagedMap.put(name, listConfigBuilder.getBeanDefinition());
        }

        public void handleSet(Node node) {
            BeanDefinitionBuilder setConfigBuilder = createBeanBuilder(SetConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, setConfigBuilder);
            for (org.w3c.dom.Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                if ("item-listeners".equals(cleanNodeName(childNode))) {
                    ManagedList listeners = parseListeners(childNode, ItemListenerConfig.class);
                    setConfigBuilder.addPropertyValue("itemListenerConfigs", listeners);
                }
            }
            setManagedMap.put(name, setConfigBuilder.getBeanDefinition());
        }

        public void handleMap(Node node) {
            BeanDefinitionBuilder mapConfigBuilder = createBeanBuilder(MapConfig.class);
            final AbstractBeanDefinition beanDefinition = mapConfigBuilder.getBeanDefinition();
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            mapConfigBuilder.addPropertyValue("name", name);
            fillAttributeValues(node, mapConfigBuilder, "maxSize", "maxSizePolicy");
            final BeanDefinitionBuilder maxSizeConfigBuilder = createBeanBuilder(MaxSizeConfig.class);
            final AbstractBeanDefinition maxSizeConfigBeanDefinition = maxSizeConfigBuilder.getBeanDefinition();
            mapConfigBuilder.addPropertyValue("maxSizeConfig", maxSizeConfigBeanDefinition);
            final Node maxSizeNode = node.getAttributes().getNamedItem("max-size");
            if (maxSizeNode != null) {
                maxSizeConfigBuilder.addPropertyValue("size", getTextContent(maxSizeNode));
            }
            final Node maxSizePolicyNode = node.getAttributes().getNamedItem("max-size-policy");
            if (maxSizePolicyNode != null) {
                maxSizeConfigBuilder
                        .addPropertyValue(xmlToJavaName(cleanNodeName(maxSizePolicyNode))
                                , MaxSizeConfig.MaxSizePolicy.valueOf(getTextContent(maxSizePolicyNode)));
            }
            for (org.w3c.dom.Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(childNode.getNodeName());
                if ("map-store".equals(nodeName)) {
                    handleMapStoreConfig(childNode, mapConfigBuilder);
                } else if ("near-cache".equals(nodeName)) {
                    handleNearCacheConfig(childNode, mapConfigBuilder);
                } else if ("wan-replication-ref".equals(nodeName)) {
                    final BeanDefinitionBuilder wanReplicationRefBuilder = createBeanBuilder(WanReplicationRef.class);
                    final AbstractBeanDefinition wanReplicationRefBeanDefinition = wanReplicationRefBuilder
                            .getBeanDefinition();
                    fillValues(childNode, wanReplicationRefBuilder);
                    mapConfigBuilder.addPropertyValue("wanReplicationRef", wanReplicationRefBeanDefinition);
                } else if ("indexes".equals(nodeName)) {
                    ManagedList indexes = new ManagedList();
                    for (Node indexNode : new IterableNodeList(childNode.getChildNodes(), Node.ELEMENT_NODE)) {
                        final BeanDefinitionBuilder indexConfBuilder = createBeanBuilder(MapIndexConfig.class);
                        fillAttributeValues(indexNode, indexConfBuilder);
                        indexes.add(indexConfBuilder.getBeanDefinition());
                    }
                    mapConfigBuilder.addPropertyValue("mapIndexConfigs", indexes);
                } else if ("entry-listeners".equals(nodeName)) {
                    ManagedList listeners = parseListeners(childNode, EntryListenerConfig.class);
                    mapConfigBuilder.addPropertyValue("entryListenerConfigs", listeners);
                }
            }
            mapConfigManagedMap.put(name, beanDefinition);
        }

        public void handleWanReplication(Node node) {
            final BeanDefinitionBuilder wanRepConfigBuilder = createBeanBuilder(WanReplicationConfig.class);
            final AbstractBeanDefinition beanDefinition = wanRepConfigBuilder.getBeanDefinition();
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            wanRepConfigBuilder.addPropertyValue("name", name);
            final ManagedList targetClusters = new ManagedList();
            for (Node n : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                final String nName = cleanNodeName(n);
                if ("target-cluster".equals(nName)) {
                    final BeanDefinitionBuilder targetClusterConfigBuilder = createBeanBuilder(
                            WanTargetClusterConfig.class);
                    final AbstractBeanDefinition childBeanDefinition = targetClusterConfigBuilder.getBeanDefinition();
                    fillAttributeValues(n, targetClusterConfigBuilder, Collections.<String>emptyList());
                    for (Node childNode : new IterableNodeList(n.getChildNodes(), Node.ELEMENT_NODE)) {
                        final String childNodeName = cleanNodeName(childNode);
                        if ("replication-impl".equals(childNodeName)) {
                            targetClusterConfigBuilder
                                    .addPropertyValue(xmlToJavaName(childNodeName), getTextContent(childNode));
                        } else if ("replication-impl-object".equals(childNodeName)) {
                            Node refName = childNode.getAttributes().getNamedItem("ref");
                            targetClusterConfigBuilder
                                    .addPropertyReference(xmlToJavaName(childNodeName), getTextContent(refName));
                        } else if ("end-points".equals(childNodeName)) {
                            final ManagedList addresses = new ManagedList();
                            for (Node addressNode : new IterableNodeList(childNode.getChildNodes(),
                                    Node.ELEMENT_NODE)) {
                                if ("address".equals(cleanNodeName(addressNode))) {
                                    addresses.add(getTextContent(addressNode));
                                }
                            }
                            targetClusterConfigBuilder.addPropertyValue("endpoints", addresses);
                        }
                    }
                    targetClusters.add(childBeanDefinition);
                }
            }
            wanRepConfigBuilder.addPropertyValue("targetClusterConfigs", targetClusters);
            wanReplicationManagedMap.put(name, beanDefinition);
        }

        private void handlePartitionGroup(final Node node) {
            final BeanDefinitionBuilder partitionConfigBuilder = createBeanBuilder(PartitionGroupConfig.class);
            fillAttributeValues(node, partitionConfigBuilder);

            ManagedList memberGroups = new ManagedList();
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(child.getNodeName());
                if ("member-group".equals(name)) {
                    BeanDefinitionBuilder memberGroupBuilder = createBeanBuilder(MemberGroupConfig.class);
                    ManagedList interfaces = new ManagedList();
                    for (org.w3c.dom.Node n : new IterableNodeList(child.getChildNodes(), Node.ELEMENT_NODE)) {
                        if ("interface".equals(cleanNodeName(n.getNodeName()))) {
                            interfaces.add(getTextContent(n));
                        }
                    }
                    memberGroupBuilder.addPropertyValue("interfaces", interfaces);
                    memberGroups.add(memberGroupBuilder.getBeanDefinition());
                }
            }
            partitionConfigBuilder.addPropertyValue("memberGroupConfigs", memberGroups);
            configBuilder.addPropertyValue("partitionGroupConfig", partitionConfigBuilder.getBeanDefinition());
        }

        private void handleManagementCenter(final Node node) {
            createAndFillBeanBuilder(node, ManagementCenterConfig.class, "managementCenterConfig", configBuilder);
        }

        public void handleNearCacheConfig(Node node, BeanDefinitionBuilder mapConfigBuilder) {
            createAndFillBeanBuilder(node, NearCacheConfig.class, "nearCacheConfig", mapConfigBuilder);
        }

        public void handleMapStoreConfig(Node node, BeanDefinitionBuilder mapConfigBuilder) {
            BeanDefinitionBuilder mapStoreConfigBuilder = createBeanBuilder(MapStoreConfig.class);
            final AbstractBeanDefinition beanDefinition = mapStoreConfigBuilder.getBeanDefinition();
            for (org.w3c.dom.Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                if ("properties".equals(cleanNodeName(child))) {
                    handleProperties(child, mapStoreConfigBuilder);
                    break;
                }
            }
            final String implAttrName = "implementation";
            final String factoryImplAttrName = "factory-implementation";
            final String initialModeAttrName = "initial-mode";
            fillAttributeValues(node, mapStoreConfigBuilder, implAttrName, factoryImplAttrName, "initialMode");
            final NamedNodeMap attrs = node.getAttributes();
            final Node implRef = attrs.getNamedItem(implAttrName);
            final Node factoryImplRef = attrs.getNamedItem(factoryImplAttrName);
            final Node initialMode = attrs.getNamedItem(initialModeAttrName);
            if (factoryImplRef != null) {
                mapStoreConfigBuilder
                        .addPropertyReference(xmlToJavaName(factoryImplAttrName), getTextContent(factoryImplRef));
            }
            if (implRef != null) {
                mapStoreConfigBuilder.addPropertyReference(xmlToJavaName(implAttrName), getTextContent(implRef));
            }
            if (initialMode != null) {
                final MapStoreConfig.InitialLoadMode mode
                        = MapStoreConfig.InitialLoadMode.valueOf(upperCaseInternal(getTextContent(initialMode)));
                mapStoreConfigBuilder.addPropertyValue("initialLoadMode", mode);
            }
            mapConfigBuilder.addPropertyValue("mapStoreConfig", beanDefinition);
            mapStoreConfigBuilder = null;
        }

        public void handleMultiMap(Node node) {
            BeanDefinitionBuilder multiMapConfigBuilder = createBeanBuilder(MultiMapConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, multiMapConfigBuilder);
            for (org.w3c.dom.Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                if ("entry-listeners".equals(cleanNodeName(childNode))) {
                    ManagedList listeners = parseListeners(childNode, EntryListenerConfig.class);
                    multiMapConfigBuilder.addPropertyValue("entryListenerConfigs", listeners);
                }
            }
            multiMapManagedMap.put(name, multiMapConfigBuilder.getBeanDefinition());
        }

        public void handleTopic(Node node) {
            BeanDefinitionBuilder topicConfigBuilder = createBeanBuilder(TopicConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, topicConfigBuilder);
            for (org.w3c.dom.Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                if ("message-listeners".equals(cleanNodeName(childNode))) {
                    ManagedList listeners = parseListeners(childNode, ListenerConfig.class);
                    topicConfigBuilder.addPropertyValue("messageListenerConfigs", listeners);
                } else if ("statistics-enabled".equals(cleanNodeName(childNode))) {
                    final String statisticsEnabled = getTextContent(childNode);
                    topicConfigBuilder.addPropertyValue("statisticsEnabled", statisticsEnabled);
                } else if ("global-ordering-enabled".equals(cleanNodeName(childNode))) {
                    final String globalOrderingEnabled = getTextContent(childNode);
                    topicConfigBuilder.addPropertyValue("globalOrderingEnabled", globalOrderingEnabled);
                }
            }
            topicManagedMap.put(name, topicConfigBuilder.getBeanDefinition());
        }

        public void handleJobTracker(Node node) {
            BeanDefinitionBuilder jobTrackerConfigBuilder = createBeanBuilder(JobTrackerConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, jobTrackerConfigBuilder);
            jobTrackerManagedMap.put(name, jobTrackerConfigBuilder.getBeanDefinition());
        }

        private void handleSecurity(final Node node) {
            final BeanDefinitionBuilder securityConfigBuilder = createBeanBuilder(SecurityConfig.class);
            final AbstractBeanDefinition beanDefinition = securityConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, securityConfigBuilder);
            for (Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("member-credentials-factory".equals(nodeName)) {
                    handleCredentialsFactory(child, securityConfigBuilder);
                } else if ("member-login-modules".equals(nodeName)) {
                    handleLoginModules(child, securityConfigBuilder, true);
                } else if ("client-login-modules".equals(nodeName)) {
                    handleLoginModules(child, securityConfigBuilder, false);
                } else if ("client-permission-policy".equals(nodeName)) {
                    handlePermissionPolicy(child, securityConfigBuilder);
                } else if ("client-permissions".equals(nodeName)) {
                    handleSecurityPermissions(child, securityConfigBuilder);
                } else if ("security-interceptors".equals(nodeName)) {
                    handleSecurityInterceptors(child, securityConfigBuilder);
                }
            }
            configBuilder.addPropertyValue("securityConfig", beanDefinition);
        }

        private void handleSecurityInterceptors(final Node node, final BeanDefinitionBuilder securityConfigBuilder) {
            final List lms = new ManagedList();
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("interceptor".equals(nodeName)) {
                    final BeanDefinitionBuilder lmConfigBuilder = createBeanBuilder(SecurityInterceptorConfig.class);
                    final AbstractBeanDefinition beanDefinition = lmConfigBuilder.getBeanDefinition();
                    fillAttributeValues(child, lmConfigBuilder);
                    lms.add(beanDefinition);
                }
            }
            securityConfigBuilder.addPropertyValue("securityInterceptorConfigs", lms);
        }

        private void handleCredentialsFactory(final Node node, final BeanDefinitionBuilder securityConfigBuilder) {
            final BeanDefinitionBuilder credentialsConfigBuilder = createBeanBuilder(CredentialsFactoryConfig.class);
            final AbstractBeanDefinition beanDefinition = credentialsConfigBuilder.getBeanDefinition();
            final NamedNodeMap attrs = node.getAttributes();
            Node classNameNode = attrs.getNamedItem("class-name");
            String className = classNameNode != null ? getTextContent(classNameNode) : null;
            Node implNode = attrs.getNamedItem("implementation");
            String implementation = implNode != null ? getTextContent(implNode) : null;
            credentialsConfigBuilder.addPropertyValue("className", className);
            if (implementation != null) {
                credentialsConfigBuilder.addPropertyReference("implementation", implementation);
            }
            Assert.isTrue(className != null || implementation != null, "One of 'class-name' or 'implementation' "
                    + "attributes is required to create CredentialsFactory!");
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("properties".equals(nodeName)) {
                    handleProperties(child, credentialsConfigBuilder);
                    break;
                }
            }
            securityConfigBuilder.addPropertyValue("memberCredentialsConfig", beanDefinition);
        }

        private void handleLoginModules(final Node node, final BeanDefinitionBuilder securityConfigBuilder, boolean member) {
            final List lms = new ManagedList();
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("login-module".equals(nodeName)) {
                    handleLoginModule(child, lms);
                }
            }
            if (member) {
                securityConfigBuilder.addPropertyValue("memberLoginModuleConfigs", lms);
            } else {
                securityConfigBuilder.addPropertyValue("clientLoginModuleConfigs", lms);
            }
        }

        private void handleLoginModule(final org.w3c.dom.Node node, List list) {
            final BeanDefinitionBuilder lmConfigBuilder = createBeanBuilder(LoginModuleConfig.class);
            final AbstractBeanDefinition beanDefinition = lmConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, lmConfigBuilder);
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("properties".equals(nodeName)) {
                    handleProperties(child, lmConfigBuilder);
                    break;
                }
            }
            list.add(beanDefinition);
        }

        private void handlePermissionPolicy(final Node node, final BeanDefinitionBuilder securityConfigBuilder) {
            final BeanDefinitionBuilder permPolicyConfigBuilder = createBeanBuilder(PermissionPolicyConfig.class);
            final AbstractBeanDefinition beanDefinition = permPolicyConfigBuilder.getBeanDefinition();
            final NamedNodeMap attrs = node.getAttributes();
            Node classNameNode = attrs.getNamedItem("class-name");
            String className = classNameNode != null ? getTextContent(classNameNode) : null;
            Node implNode = attrs.getNamedItem("implementation");
            String implementation = implNode != null ? getTextContent(implNode) : null;
            permPolicyConfigBuilder.addPropertyValue("className", className);
            if (implementation != null) {
                permPolicyConfigBuilder.addPropertyReference("implementation", implementation);
            }
            Assert.isTrue(className != null || implementation != null, "One of 'class-name' or 'implementation' "
                    + "attributes is required to create PermissionPolicy!");
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("properties".equals(nodeName)) {
                    handleProperties(child, permPolicyConfigBuilder);
                    break;
                }
            }
            securityConfigBuilder.addPropertyValue("clientPolicyConfig", beanDefinition);
        }

        private void handleSecurityPermissions(final Node node, final BeanDefinitionBuilder securityConfigBuilder) {
            final Set permissions = new ManagedSet();
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                PermissionType type = PermissionType.getType(nodeName);

                if (type == null) {
                    continue;
                }

                handleSecurityPermission(child, permissions, type);
            }
            securityConfigBuilder.addPropertyValue("clientPermissionConfigs", permissions);
        }

        private void handleSecurityPermission(final Node node, final Set permissions, PermissionType type) {
            final BeanDefinitionBuilder permissionConfigBuilder = createBeanBuilder(PermissionConfig.class);
            final AbstractBeanDefinition beanDefinition = permissionConfigBuilder.getBeanDefinition();
            permissionConfigBuilder.addPropertyValue("type", type);
            final NamedNodeMap attrs = node.getAttributes();
            Node nameNode = attrs.getNamedItem("name");
            String name = nameNode != null ? getTextContent(nameNode) : "*";
            permissionConfigBuilder.addPropertyValue("name", name);
            Node principalNode = attrs.getNamedItem("principal");
            String principal = principalNode != null ? getTextContent(principalNode) : "*";
            permissionConfigBuilder.addPropertyValue("principal", principal);
            final List endpoints = new ManagedList();
            final List actions = new ManagedList();
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("endpoints".equals(nodeName)) {
                    handleSecurityPermissionEndpoints(child, endpoints);
                } else if ("actions".equals(nodeName)) {
                    handleSecurityPermissionActions(child, actions);
                }
            }
            permissionConfigBuilder.addPropertyValue("endpoints", endpoints);
            permissionConfigBuilder.addPropertyValue("actions", actions);
            permissions.add(beanDefinition);
        }

        private void handleSecurityPermissionEndpoints(final Node node, final List endpoints) {
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("endpoint".equals(nodeName)) {
                    endpoints.add(getTextContent(child));
                }
            }
        }

        private void handleSecurityPermissionActions(final Node node, final List actions) {
            for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("action".equals(nodeName)) {
                    actions.add(getTextContent(child));
                }
            }
        }

    }
}
