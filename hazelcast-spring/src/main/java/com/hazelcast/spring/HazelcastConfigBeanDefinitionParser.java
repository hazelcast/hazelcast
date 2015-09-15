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

package com.hazelcast.spring;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.InvalidConfigurationException;
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
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.PermissionPolicyConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.QuorumListenerConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.spi.ServiceConfigurationParser;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.StringUtil;
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
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.StringUtil.upperCaseInternal;

/**
 * BeanDefinitionParser for Hazelcast Config Configuration
 * <p/>
 * <p/>
 * <b>Sample Spring XML for Hazelcast Config:</b>
 * <pre>
 * &lt;hz:config&gt;
 *  &lt;hz:map name="map1"&gt;
 *      &lt;hz:near-cache time-to-live-seconds="0" max-idle-seconds="60"
 *          eviction-policy="LRU" max-size="5000"  invalidate-on-change="true"/&gt;
 *
 *  &lt;hz:map-store enabled="true" class-name="com.foo.DummyStore"
 *          write-delay-seconds="0"/&gt;
 *  &lt;/hz:map&gt;
 *  &lt;hz:map name="map2"&gt;
 *      &lt;hz:map-store enabled="true" implementation="dummyMapStore"
 *          write-delay-seconds="0"/&gt;
 *  &lt;/hz:map&gt;
 *
 *  &lt;bean id="dummyMapStore" class="com.foo.DummyStore" /&gt;
 * &lt;/hz:config&gt;
 * </pre>
 */
public class HazelcastConfigBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlConfigBuilder springXmlConfigBuilder = new SpringXmlConfigBuilder(parserContext);
        springXmlConfigBuilder.handleConfig(element);
        return springXmlConfigBuilder.getBeanDefinition();
    }

    private class SpringXmlConfigBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;

        private ManagedMap mapConfigManagedMap;
        private ManagedMap cacheConfigManagedMap;
        private ManagedMap queueManagedMap;
        private ManagedMap listManagedMap;
        private ManagedMap setManagedMap;
        private ManagedMap topicManagedMap;
        private ManagedMap multiMapManagedMap;
        private ManagedMap executorManagedMap;
        private ManagedMap wanReplicationManagedMap;
        private ManagedMap jobTrackerManagedMap;
        private ManagedMap replicatedMapManagedMap;
        private ManagedMap quorumManagedMap;

        public SpringXmlConfigBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.configBuilder = BeanDefinitionBuilder.rootBeanDefinition(Config.class);
            this.mapConfigManagedMap = createManagedMap("mapConfigs");
            this.cacheConfigManagedMap = createManagedMap("cacheConfigs");
            this.queueManagedMap = createManagedMap("queueConfigs");
            this.listManagedMap = createManagedMap("listConfigs");
            this.setManagedMap = createManagedMap("setConfigs");
            this.topicManagedMap = createManagedMap("topicConfigs");
            this.multiMapManagedMap = createManagedMap("multiMapConfigs");
            this.executorManagedMap = createManagedMap("executorConfigs");
            this.wanReplicationManagedMap = createManagedMap("wanReplicationConfigs");
            this.jobTrackerManagedMap = createManagedMap("jobTrackerConfigs");
            this.replicatedMapManagedMap = createManagedMap("replicatedMapConfigs");
            this.quorumManagedMap = createManagedMap("quorumConfigs");
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
            if (element != null) {
                handleCommonBeanAttributes(element, configBuilder, parserContext);
                for (Node node : new IterableNodeList(element, Node.ELEMENT_NODE)) {
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
                    } else if ("cache".equals(nodeName)) {
                        handleCache(node);
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
                    } else if ("replicatedmap".equals(nodeName)) {
                        handleReplicatedMap(node);
                    } else if ("wan-replication".equals(nodeName)) {
                        handleWanReplication(node);
                    } else if ("partition-group".equals(nodeName)) {
                        handlePartitionGroup(node);
                    } else if ("serialization".equals(nodeName)) {
                        handleSerialization(node);
                    } else if ("native-memory".equals(nodeName)) {
                        handleNativeMemory(node);
                    } else if ("security".equals(nodeName)) {
                        handleSecurity(node);
                    } else if ("member-attributes".equals(nodeName)) {
                        handleMemberAttributes(node);
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
                    } else if ("services".equals(nodeName)) {
                        handleServices(node);
                    } else if ("spring-aware".equals(nodeName)) {
                        handleSpringAware();
                    } else if ("quorum".equals(nodeName)) {
                        handleQuorum(node);
                    }
                }
            }
        }

        private void handleQuorum(final org.w3c.dom.Node node) {
            BeanDefinitionBuilder quorumConfigBuilder = createBeanBuilder(QuorumConfig.class);
            final AbstractBeanDefinition beanDefinition = quorumConfigBuilder.getBeanDefinition();
            final String name = getAttribute(node, "name");
            quorumConfigBuilder.addPropertyValue("name", name);
            Node attrEnabled = node.getAttributes().getNamedItem("enabled");
            final boolean enabled = attrEnabled != null ? checkTrue(getTextContent(attrEnabled)) : false;
            quorumConfigBuilder.addPropertyValue("enabled", enabled);
            for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
                final String value = getTextContent(n).trim();
                final String nodeName = cleanNodeName(n.getNodeName());
                if ("quorum-size".equals(nodeName)) {
                    quorumConfigBuilder.addPropertyValue("size", getIntegerValue("quorum-size", value, 0));
                } else if ("quorum-listeners".equals(nodeName)) {
                    ManagedList listeners = parseListeners(n, QuorumListenerConfig.class);
                    quorumConfigBuilder.addPropertyValue("listenerConfigs", listeners);
                } else if ("quorum-type".equals(nodeName)) {
                    quorumConfigBuilder.addPropertyValue("type", QuorumType.valueOf(value));
                } else if ("quorum-function-class-name".equals(nodeName)) {
                    quorumConfigBuilder.addPropertyValue(xmlToJavaName(nodeName), value);
                }
            }
            quorumManagedMap.put(name, beanDefinition);
        }

        public void handleServices(Node node) {
            BeanDefinitionBuilder servicesConfigBuilder = createBeanBuilder(ServicesConfig.class);
            final AbstractBeanDefinition beanDefinition = servicesConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, servicesConfigBuilder);
            ManagedList<AbstractBeanDefinition> serviceConfigManagedList = new ManagedList<AbstractBeanDefinition>();
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(child);
                if ("service".equals(nodeName)) {
                    serviceConfigManagedList.add(handleService(child));
                }

            }
            servicesConfigBuilder.addPropertyValue("serviceConfigs", serviceConfigManagedList);
            configBuilder.addPropertyValue("servicesConfig", beanDefinition);
        }

        private AbstractBeanDefinition handleService(Node node) {
            BeanDefinitionBuilder serviceConfigBuilder = createBeanBuilder(ServiceConfig.class);
            final AbstractBeanDefinition beanDefinition = serviceConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, serviceConfigBuilder);
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(child);
                if ("name".equals(nodeName)) {
                    serviceConfigBuilder.addPropertyValue(xmlToJavaName(nodeName), getTextContent(child));
                } else if ("class-name".equals(nodeName)) {
                    serviceConfigBuilder.addPropertyValue(xmlToJavaName(nodeName), getTextContent(child));
                } else if ("properties".equals(nodeName)) {
                    handleProperties(child, serviceConfigBuilder);
                } else if ("configuration".equals(nodeName)) {
                    final Node parser = child.getAttributes().getNamedItem("parser");
                    final String name = getTextContent(parser);
                    try {
                        ServiceConfigurationParser serviceConfigurationParser =
                                ClassLoaderUtil.newInstance(getClass().getClassLoader(), name);
                        Object obj = serviceConfigurationParser.parse((Element) child);
                        serviceConfigBuilder.addPropertyValue(xmlToJavaName("config-object"), obj);
                    } catch (Exception e) {
                        ExceptionUtil.sneakyThrow(e);
                    }
                }
            }
            return beanDefinition;
        }

        public void handleReplicatedMap(Node node) {
            BeanDefinitionBuilder replicatedMapConfigBuilder = createBeanBuilder(ReplicatedMapConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, replicatedMapConfigBuilder);
            for (Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                if ("entry-listeners".equals(cleanNodeName(childNode))) {
                    ManagedList listeners = parseListeners(childNode, EntryListenerConfig.class);
                    replicatedMapConfigBuilder.addPropertyValue("listenerConfigs", listeners);
                }
            }
            replicatedMapManagedMap.put(name, replicatedMapConfigBuilder.getBeanDefinition());
        }

        public void handleNetwork(Node node) {
            BeanDefinitionBuilder networkConfigBuilder = createBeanBuilder(NetworkConfig.class);
            final AbstractBeanDefinition beanDefinition = networkConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, networkConfigBuilder);
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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
                } else if ("reuse-address".equals(nodeName)) {
                    handleReuseAddress(child, networkConfigBuilder);
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
                    final Node att = atts.item(a);
                    final String name = xmlToJavaName(att.getNodeName());
                    final String value = att.getNodeValue();
                    builder.addPropertyValue(name, value);
                }
            }
            ManagedList interfacesSet = new ManagedList();
            for (Node n : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(child);
                if ("ports".equals(name)) {
                    String value = getTextContent(child);
                    outboundPorts.add(value);
                }
            }
            networkConfigBuilder.addPropertyValue("outboundPortDefinitions", outboundPorts);
        }

        private void handleReuseAddress(final Node node, final BeanDefinitionBuilder networkConfigBuilder) {
            String value = node.getTextContent();
            networkConfigBuilder.addPropertyValue("reuseAddress", value);
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
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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
            for (Node n : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
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
            for (Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
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
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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
            for (Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
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
            for (Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
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
            for (Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
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
                } else if ("quorum-ref".equals(nodeName)) {
                    mapConfigBuilder.addPropertyValue("quorumName", getTextContent(node));
                } else if ("query-caches".equals(nodeName)) {
                    ManagedList queryCaches = getQueryCaches(childNode);
                    mapConfigBuilder.addPropertyValue("queryCacheConfigs", queryCaches);
                }
            }
            mapConfigManagedMap.put(name, beanDefinition);
        }

        private ManagedList getQueryCaches(Node childNode) {
            ManagedList queryCaches = new ManagedList();
            for (Node queryCacheNode : new IterableNodeList(childNode.getChildNodes(), Node.ELEMENT_NODE)) {
                BeanDefinitionBuilder beanDefinitionBuilder = parseQueryCaches(queryCacheNode);
                queryCaches.add(beanDefinitionBuilder.getBeanDefinition());
            }
            return queryCaches;
        }

        private BeanDefinitionBuilder parseQueryCaches(Node queryCacheNode) {
            final BeanDefinitionBuilder builder = createBeanBuilder(QueryCacheConfig.class);

            for (Node node : new IterableNodeList(queryCacheNode.getChildNodes(), Node.ELEMENT_NODE)) {
                String nodeName = cleanNodeName(node.getNodeName());
                String textContent = getTextContent(node);
                NamedNodeMap attrs = queryCacheNode.getAttributes();
                String cacheName = getTextContent(attrs.getNamedItem("name"));
                builder.addPropertyValue("name", cacheName);

                if ("predicate".equals(nodeName)) {
                    BeanDefinitionBuilder predicateBuilder = createBeanBuilder(PredicateConfig.class);
                    String predicateType = getTextContent(node.getAttributes().getNamedItem("type"));
                    if ("sql".equals(predicateType)) {
                        predicateBuilder.addPropertyValue("sql", textContent);
                    } else if ("class-name".equals(predicateType)) {
                        predicateBuilder.addPropertyValue("className", textContent);
                    }
                    builder.addPropertyValue("predicateConfig", predicateBuilder.getBeanDefinition());
                } else if ("entry-listeners".equals(nodeName)) {
                    ManagedList listeners = new ManagedList();
                    final String implementationAttr = "implementation";
                    for (Node listenerNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                        BeanDefinitionBuilder listenerConfBuilder = createBeanBuilder(EntryListenerConfig.class);
                        fillAttributeValues(listenerNode, listenerConfBuilder, implementationAttr);
                        Node implementationNode = listenerNode.getAttributes().getNamedItem(implementationAttr);
                        if (implementationNode != null) {
                            listenerConfBuilder.addPropertyReference(implementationAttr, getTextContent(implementationNode));
                        }
                        listeners.add(listenerConfBuilder.getBeanDefinition());
                    }
                    builder.addPropertyValue("entryListenerConfigs", listeners);
                } else if ("include-value".equals(nodeName)) {
                    boolean includeValue = checkTrue(textContent);
                    builder.addPropertyValue("includeValue", includeValue);
                } else if ("batch-size".equals(nodeName)) {
                    int batchSize = getIntegerValue("batch-size", textContent.trim(),
                            QueryCacheConfig.DEFAULT_BATCH_SIZE);
                    builder.addPropertyValue("batchSize", batchSize);
                } else if ("buffer-size".equals(nodeName)) {
                    int bufferSize = getIntegerValue("buffer-size", textContent.trim(),
                            QueryCacheConfig.DEFAULT_BUFFER_SIZE);
                    builder.addPropertyValue("bufferSize", bufferSize);
                } else if ("delay-seconds".equals(nodeName)) {
                    int delaySeconds = getIntegerValue("delay-seconds", textContent.trim(),
                            QueryCacheConfig.DEFAULT_DELAY_SECONDS);
                    builder.addPropertyValue("delaySeconds", delaySeconds);
                } else if ("in-memory-format".equals(nodeName)) {
                    String value = textContent.trim();
                    builder.addPropertyValue("inMemoryFormat", InMemoryFormat.valueOf(upperCaseInternal(value)));
                } else if ("coalesce".equals(nodeName)) {
                    boolean coalesce = checkTrue(textContent);
                    builder.addPropertyValue("coalesce", coalesce);
                } else if ("populate".equals(nodeName)) {
                    boolean populate = checkTrue(textContent);
                    builder.addPropertyValue("populate", populate);
                } else if ("indexes".equals(nodeName)) {
                    ManagedList indexes = new ManagedList();
                    for (Node indexNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                        final BeanDefinitionBuilder indexConfBuilder = createBeanBuilder(MapIndexConfig.class);
                        fillAttributeValues(indexNode, indexConfBuilder);
                        indexes.add(indexConfBuilder.getBeanDefinition());
                    }
                    builder.addPropertyValue("indexConfigs", indexes);
                } else if ("eviction".equals(nodeName)) {
                    builder.addPropertyValue("evictionConfig", getEvictionConfig(node));
                }
            }
            return builder;
        }

        public void handleCache(Node node) {
            BeanDefinitionBuilder cacheConfigBuilder = createBeanBuilder(CacheSimpleConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, cacheConfigBuilder);
            for (Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                if ("eviction".equals(cleanNodeName(childNode))) {
                    EvictionConfig evictionConfig = getEvictionConfig(childNode);
                    EvictionPolicy evictionPolicy = evictionConfig.getEvictionPolicy();
                    if (evictionPolicy == null || evictionPolicy == EvictionPolicy.NONE) {
                        throw new InvalidConfigurationException("Eviction policy of cache cannot be null or \"NONE\"");
                    }
                    cacheConfigBuilder.addPropertyValue("evictionConfig", evictionConfig);
                } else if ("expiry-policy-factory".equals(cleanNodeName(childNode))) {
                    cacheConfigBuilder.addPropertyValue("expiryPolicyFactoryConfig", getExpiryPolicyFactoryConfig(childNode));
                } else if ("cache-entry-listeners".equals(cleanNodeName(childNode))) {
                    ManagedList listeners = new ManagedList();
                    for (Node listenerNode : new IterableNodeList(childNode.getChildNodes(), Node.ELEMENT_NODE)) {
                        final BeanDefinitionBuilder listenerConfBuilder = createBeanBuilder(CacheSimpleEntryListenerConfig.class);
                        fillAttributeValues(listenerNode, listenerConfBuilder);
                        listeners.add(listenerConfBuilder.getBeanDefinition());
                    }
                    cacheConfigBuilder.addPropertyValue("cacheEntryListeners", listeners);
                } else if ("wan-replication-ref".equals(cleanNodeName(childNode))) {
                    final BeanDefinitionBuilder wanReplicationRefBuilder = createBeanBuilder(WanReplicationRef.class);
                    final AbstractBeanDefinition wanReplicationRefBeanDefinition = wanReplicationRefBuilder
                            .getBeanDefinition();
                    fillValues(childNode, wanReplicationRefBuilder);
                    cacheConfigBuilder.addPropertyValue("wanReplicationRef", wanReplicationRefBeanDefinition);
                } else if ("partition-lost-listeners".equals(cleanNodeName(childNode))) {
                    ManagedList listeners = parseListeners(childNode, CachePartitionLostListenerConfig.class);
                    cacheConfigBuilder.addPropertyValue("partitionLostListenerConfigs", listeners);
                } else if ("quorum-ref".equals(cleanNodeName(childNode))) {
                    cacheConfigBuilder.addPropertyValue("quorumName", getTextContent(childNode));
                } else if ("partition-lost-listeners".equals(cleanNodeName(childNode))) {
                    ManagedList listeners = parseListeners(childNode, CachePartitionLostListenerConfig.class);
                    cacheConfigBuilder.addPropertyValue("partitionLostListenerConfigs", listeners);
                } else if ("merge-policy".equals(cleanNodeName(childNode))) {
                    cacheConfigBuilder.addPropertyValue("mergePolicy", getTextContent(childNode));
                }
            }
            cacheConfigManagedMap.put(name, cacheConfigBuilder.getBeanDefinition());
        }

        public void handleWanReplication(Node node) {
            final BeanDefinitionBuilder wanRepConfigBuilder = createBeanBuilder(WanReplicationConfig.class);
            final AbstractBeanDefinition beanDefinition = wanRepConfigBuilder.getBeanDefinition();
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            wanRepConfigBuilder.addPropertyValue("name", name);
            final Node attSnapshotEnabled = node.getAttributes().getNamedItem("snapshot-enabled");
            final boolean snapshotEnabled = checkTrue(getTextContent(attSnapshotEnabled));
            wanRepConfigBuilder.addPropertyValue("snapshotEnabled", snapshotEnabled);

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
            for (Node child : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(child.getNodeName());
                if ("member-group".equals(name)) {
                    BeanDefinitionBuilder memberGroupBuilder = createBeanBuilder(MemberGroupConfig.class);
                    ManagedList interfaces = new ManagedList();
                    for (Node n : new IterableNodeList(child.getChildNodes(), Node.ELEMENT_NODE)) {
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

        public void handleNearCacheConfig(Node node, BeanDefinitionBuilder configBuilder) {
            BeanDefinitionBuilder nearCacheConfigBuilder = createBeanBuilder(NearCacheConfig.class);
            fillAttributeValues(node, nearCacheConfigBuilder);
            for (Node childNode : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(childNode.getNodeName());
                if ("eviction".equals(nodeName)) {
                    handleEvictionConfig(childNode, nearCacheConfigBuilder);
                }
            }
            configBuilder.addPropertyValue("nearCacheConfig", nearCacheConfigBuilder.getBeanDefinition());
        }

        private void handleEvictionConfig(Node node, BeanDefinitionBuilder configBuilder) {
            configBuilder.addPropertyValue("evictionConfig", getEvictionConfig(node));
        }

        private ExpiryPolicyFactoryConfig getExpiryPolicyFactoryConfig(Node node) {
            final String className = getAttribute(node, "class-name");
            if (!StringUtil.isNullOrEmpty(className)) {
                return new ExpiryPolicyFactoryConfig(className);
            } else {
                TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig = null;
                for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
                    final String nodeName = cleanNodeName(n.getNodeName());
                    if ("timed-expiry-policy-factory".equals(nodeName)) {
                        final String expiryPolicyTypeStr = getAttribute(n, "expiry-policy-type");
                        final String durationAmountStr = getAttribute(n, "duration-amount");
                        final String timeUnitStr = getAttribute(n, "time-unit");
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
                            final long durationAmount =
                                    Long.parseLong(durationAmountStr);
                            final TimeUnit timeUnit =
                                    TimeUnit.valueOf(upperCaseInternal(timeUnitStr));
                            durationConfig = new DurationConfig(durationAmount, timeUnit);
                        }
                        timedExpiryPolicyFactoryConfig =
                                new TimedExpiryPolicyFactoryConfig(expiryPolicyType, durationConfig);
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

        public void handleMapStoreConfig(Node node, BeanDefinitionBuilder mapConfigBuilder) {
            BeanDefinitionBuilder mapStoreConfigBuilder = createBeanBuilder(MapStoreConfig.class);
            final AbstractBeanDefinition beanDefinition = mapStoreConfigBuilder.getBeanDefinition();
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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
        }

        public void handleMultiMap(Node node) {
            BeanDefinitionBuilder multiMapConfigBuilder = createBeanBuilder(MultiMapConfig.class);
            final Node attName = node.getAttributes().getNamedItem("name");
            final String name = getTextContent(attName);
            fillAttributeValues(node, multiMapConfigBuilder);
            for (Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
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
            for (Node childNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
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

        private void handleMemberAttributes(final Node node) {
            final BeanDefinitionBuilder memberAttributeConfigBuilder = createBeanBuilder(MemberAttributeConfig.class);
            final AbstractBeanDefinition beanDefinition = memberAttributeConfigBuilder.getBeanDefinition();
            ManagedMap<String, Object> attributes = new ManagedMap<String, Object>();
            for (Node n : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(n.getNodeName());
                if (!"attribute".equals(name)) {
                    continue;
                }
                final String attributeName = getTextContent(n.getAttributes().getNamedItem("name")).trim();
                final String attributeType = getTextContent(n.getAttributes().getNamedItem("type")).trim();
                final String value = getTextContent(n);
                final Object oValue;
                if ("string".equals(attributeType)) {
                    oValue = value;
                } else if ("boolean".equals(attributeType)) {
                    oValue = Boolean.parseBoolean(value);
                } else if ("byte".equals(attributeType)) {
                    oValue = Byte.parseByte(value);
                } else if ("double".equals(attributeType)) {
                    oValue = Double.parseDouble(value);
                } else if ("float".equals(attributeType)) {
                    oValue = Float.parseFloat(value);
                } else if ("int".equals(attributeType)) {
                    oValue = Integer.parseInt(value);
                } else if ("long".equals(attributeType)) {
                    oValue = Long.parseLong(value);
                } else if ("short".equals(attributeType)) {
                    oValue = Short.parseShort(value);
                } else {
                    oValue = value;
                }
                attributes.put(attributeName, oValue);
            }
            memberAttributeConfigBuilder.addPropertyValue("attributes", attributes);
            configBuilder.addPropertyValue("memberAttributeConfig", beanDefinition);
        }

        private void handleNativeMemory(final Node node) {
            final BeanDefinitionBuilder nativeMemoryConfigBuilder = createBeanBuilder(NativeMemoryConfig.class);
            final AbstractBeanDefinition beanDefinition = nativeMemoryConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, nativeMemoryConfigBuilder);
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(child);
                if ("size".equals(nodeName)) {
                    handleMemorySizeConfig(child, nativeMemoryConfigBuilder);
                }
            }
            configBuilder.addPropertyValue("nativeMemoryConfig", beanDefinition);
        }

        private void handleMemorySizeConfig(Node node, BeanDefinitionBuilder nativeMemoryConfigBuilder) {
            BeanDefinitionBuilder memorySizeConfigBuilder = createBeanBuilder(MemorySize.class);
            final NamedNodeMap attrs = node.getAttributes();
            final Node value = attrs.getNamedItem("value");
            final Node unit = attrs.getNamedItem("unit");
            memorySizeConfigBuilder.addConstructorArgValue(getTextContent(value));
            memorySizeConfigBuilder.addConstructorArgValue(MemoryUnit.valueOf(getTextContent(unit)));
            nativeMemoryConfigBuilder.addPropertyValue("size", memorySizeConfigBuilder.getBeanDefinition());
        }

        private void handleSecurityInterceptors(final Node node, final BeanDefinitionBuilder securityConfigBuilder) {
            final List lms = new ManagedList();
            for (Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("interceptor".equals(nodeName)) {
                    final BeanDefinitionBuilder siConfigBuilder = createBeanBuilder(SecurityInterceptorConfig.class);
                    final AbstractBeanDefinition beanDefinition = siConfigBuilder.getBeanDefinition();
                    final NamedNodeMap attrs = child.getAttributes();
                    Node classNameNode = attrs.getNamedItem("class-name");
                    String className = classNameNode != null ? getTextContent(classNameNode) : null;
                    Node implNode = attrs.getNamedItem("implementation");
                    String implementation = implNode != null ? getTextContent(implNode) : null;
                    Assert.isTrue(className != null || implementation != null,
                            "One of 'class-name' or 'implementation' attributes is required "
                                    + "to create SecurityInterceptorConfig!");
                    siConfigBuilder.addPropertyValue("className", className);
                    if (implementation != null) {
                        siConfigBuilder.addPropertyReference("implementation", implementation);
                    }
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
            for (Node child : new IterableNodeList(node.getChildNodes())) {
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
            for (Node child : new IterableNodeList(node.getChildNodes())) {
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

        private void handleLoginModule(final Node node, List list) {
            final BeanDefinitionBuilder lmConfigBuilder = createBeanBuilder(LoginModuleConfig.class);
            final AbstractBeanDefinition beanDefinition = lmConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, lmConfigBuilder);
            for (Node child : new IterableNodeList(node.getChildNodes())) {
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
            for (Node child : new IterableNodeList(node.getChildNodes())) {
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
            for (Node child : new IterableNodeList(node.getChildNodes())) {
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
            for (Node child : new IterableNodeList(node.getChildNodes())) {
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
            for (Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("endpoint".equals(nodeName)) {
                    endpoints.add(getTextContent(child));
                }
            }
        }

        private void handleSecurityPermissionActions(final Node node, final List actions) {
            for (Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
                if ("action".equals(nodeName)) {
                    actions.add(getTextContent(child));
                }
            }
        }

    }
}
