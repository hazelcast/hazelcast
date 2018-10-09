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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientAliasedDiscoveryConfigUtils;
import com.hazelcast.client.config.ClientCloudConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.config.ClientIcmpPingConfig;
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
import com.hazelcast.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SSLConfig;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.StringUtil.upperCaseInternal;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;
import static org.springframework.util.Assert.isTrue;

/**
 * BeanDefinitionParser for Hazelcast Client Configuration.
 * <p>
 * <b>Sample Spring XML for Hazelcast Client:</b>
 * <pre>{@code
 *   <hz:client id="client">
 *      <hz:group name="${cluster.group.name}" password="${cluster.group.password}" />
 *      <hz:network connection-attempt-limit="3"
 *          connection-attempt-period="3000"
 *          connection-timeout="1000"
 *          redo-operation="true"
 *          smart-routing="true">
 *              <hz:member>10.10.1.2:5701</hz:member>
 *              <hz:member>10.10.1.3:5701</hz:member>
 *      </hz:network>
 *   </hz:client>
 * }</pre>
 */
public class HazelcastClientBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        return springXmlBuilder.handleClient(element);
    }

    /**
     * Client bean definition builder
     */
    public class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private static final int INITIAL_CAPACITY = 10;

        private final ParserContext parserContext;
        private final BeanDefinitionBuilder builder;
        private final ManagedMap<String, BeanDefinition> nearCacheConfigMap = new ManagedMap<String, BeanDefinition>();
        private final ManagedMap<String, BeanDefinition> flakeIdGeneratorConfigMap = new ManagedMap<String, BeanDefinition>();
        private final ManagedMap<String, BeanDefinition> reliableTopicConfigMap = new ManagedMap<String, BeanDefinition>();

        SpringXmlBuilder(ParserContext parserContext) {
            this(parserContext, rootBeanDefinition(HazelcastClient.class)
                    .setFactoryMethod("newHazelcastClient")
                    .setDestroyMethodName("shutdown"));
        }

        public SpringXmlBuilder(ParserContext parserContext, BeanDefinitionBuilder builder) {
            this.parserContext = parserContext;
            this.builder = builder;

            this.configBuilder = rootBeanDefinition(ClientConfig.class);
            configBuilder.addPropertyValue("nearCacheConfigMap", nearCacheConfigMap);
            configBuilder.addPropertyValue("flakeIdGeneratorConfigMap", flakeIdGeneratorConfigMap);
            configBuilder.addPropertyValue("reliableTopicConfigMap", reliableTopicConfigMap);
        }

        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        public AbstractBeanDefinition handleClient(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            handleClientAttributes(element);
            for (Node node : childElements(element)) {
                String nodeName = cleanNodeName(node);
                if ("group".equals(nodeName)) {
                    createAndFillBeanBuilder(node, GroupConfig.class, "groupConfig", configBuilder);
                } else if ("properties".equals(nodeName)) {
                    handleProperties(node, configBuilder);
                } else if ("network".equals(nodeName)) {
                    handleNetwork(node);
                } else if ("listeners".equals(nodeName)) {
                    List listeners = parseListeners(node, ListenerConfig.class);
                    configBuilder.addPropertyValue("listenerConfigs", listeners);
                } else if ("serialization".equals(nodeName)) {
                    handleSerialization(node);
                } else if ("proxy-factories".equals(nodeName)) {
                    List list = parseProxyFactories(node, ProxyFactoryConfig.class);
                    configBuilder.addPropertyValue("proxyFactoryConfigs", list);
                } else if ("load-balancer".equals(nodeName)) {
                    handleLoadBalancer(node);
                } else if ("near-cache".equals(nodeName)) {
                    handleNearCache(node);
                } else if ("spring-aware".equals(nodeName)) {
                    handleSpringAware();
                } else if ("query-caches".equals(nodeName)) {
                    ManagedMap queryCaches = getQueryCaches(node);
                    configBuilder.addPropertyValue("queryCacheConfigs", queryCaches);
                } else if ("connection-strategy".equals(nodeName)) {
                    handleConnectionStrategy(node);
                } else if ("user-code-deployment".equals(nodeName)) {
                    handleUserCodeDeployment(node);
                } else if ("flake-id-generator".equals(nodeName)) {
                    handleFlakeIdGenerator(node);
                } else if ("reliable-topic".equals(nodeName)) {
                    handleReliableTopic(node);
                } else if ("security".equals(nodeName)) {
                    handleSecurity(node);
                }
            }
            builder.addConstructorArgValue(configBuilder.getBeanDefinition());
            return builder.getBeanDefinition();
        }

        private void handleSecurity(Node node) {
            BeanDefinitionBuilder securityConfigBuilder = createBeanBuilder(ClientSecurityConfig.class);
            AbstractBeanDefinition beanDefinition = securityConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, securityConfigBuilder);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("credentials-factory".equals(nodeName)) {
                    handleCredentialsFactory(child, securityConfigBuilder);
                } else if ("credentials".equals(nodeName)) {
                    securityConfigBuilder.addPropertyValue("credentialsClassname", getTextContent(child));
                }
            }
            configBuilder.addPropertyValue("securityConfig", beanDefinition);
        }

        void handleCredentialsFactory(Node node, BeanDefinitionBuilder securityConfigBuilder) {
            BeanDefinitionBuilder credentialsConfigBuilder = createBeanBuilder(CredentialsFactoryConfig.class);
            AbstractBeanDefinition beanDefinition = credentialsConfigBuilder.getBeanDefinition();
            NamedNodeMap attributes = node.getAttributes();
            Node classNameNode = attributes.getNamedItem("class-name");
            String className = classNameNode != null ? getTextContent(classNameNode) : null;
            Node implNode = attributes.getNamedItem("implementation");
            String implementation = implNode != null ? getTextContent(implNode) : null;
            credentialsConfigBuilder.addPropertyValue("className", className);
            if (implementation != null) {
                credentialsConfigBuilder.addPropertyReference("implementation", implementation);
            }
            isTrue(className != null || implementation != null, "One of 'class-name' or 'implementation'"
                    + " attributes is required to create CredentialsFactory!");
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("properties".equals(nodeName)) {
                    handleProperties(child, credentialsConfigBuilder);
                    break;
                }
            }
            securityConfigBuilder.addPropertyValue("credentialsFactoryConfig", beanDefinition);
        }

        private void handleConnectionStrategy(Node node) {
            BeanDefinitionBuilder clientConnectionStrategyConfig = createBeanBuilder(ClientConnectionStrategyConfig.class);
            fillAttributeValues(node, clientConnectionStrategyConfig);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("connection-retry".equals(nodeName)) {
                    createAndFillBeanBuilder(child, ConnectionRetryConfig.class,
                            "ConnectionRetryConfig", clientConnectionStrategyConfig);
                }
            }
            configBuilder.addPropertyValue("connectionStrategyConfig", clientConnectionStrategyConfig.getBeanDefinition());
        }

        private void handleUserCodeDeployment(Node node) {
            BeanDefinitionBuilder userCodeDeploymentConfig = createBeanBuilder(ClientUserCodeDeploymentConfig.class);
            List<String> jarPaths = new ArrayList<String>(INITIAL_CAPACITY);
            List<String> classNames = new ArrayList<String>(INITIAL_CAPACITY);
            fillAttributeValues(node, userCodeDeploymentConfig);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("jarpaths".equals(nodeName)) {
                    for (Node child1 : childElements(child)) {
                        String nodeName1 = cleanNodeName(child1);
                        if ("jarpath".equals(nodeName1)) {
                            jarPaths.add(getTextContent(child1));
                        }
                    }
                } else if ("classnames".equals(nodeName)) {
                    for (Node child1 : childElements(child)) {
                        String nodeName1 = cleanNodeName(child1);
                        if ("classname".equals(nodeName1)) {
                            classNames.add(getTextContent(child1));
                        }
                    }
                }
            }
            userCodeDeploymentConfig.addPropertyValue("jarPaths", jarPaths);
            userCodeDeploymentConfig.addPropertyValue("classNames", classNames);

            configBuilder.addPropertyValue("userCodeDeploymentConfig", userCodeDeploymentConfig.getBeanDefinition());
        }

        private void handleClientAttributes(Element element) {
            NamedNodeMap attributes = element.getAttributes();
            if (attributes != null) {
                for (int a = 0; a < attributes.getLength(); a++) {
                    Node att = attributes.item(a);
                    String name = att.getNodeName();
                    String value = att.getNodeValue();
                    if ("executor-pool-size".equals(name)) {
                        configBuilder.addPropertyValue("executorPoolSize", value);
                    } else if ("credentials-ref".equals(name)) {
                        configBuilder.addPropertyReference("credentials", value);
                    }
                }
            }
        }

        private void handleNetwork(Node node) {
            BeanDefinitionBuilder clientNetworkConfig = createBeanBuilder(ClientNetworkConfig.class);
            List<String> members = new ArrayList<String>(INITIAL_CAPACITY);
            fillAttributeValues(node, clientNetworkConfig);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("member".equals(nodeName)) {
                    members.add(getTextContent(child));
                } else if ("socket-options".equals(nodeName)) {
                    createAndFillBeanBuilder(child, SocketOptions.class, "socketOptions", clientNetworkConfig);
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
                    createAndFillBeanBuilder(child, ClientIcmpPingConfig.class,
                            "clientIcmpPingConfig", clientNetworkConfig);
                } else if ("hazelcast-cloud".equals(nodeName)) {
                    createAndFillBeanBuilder(child, ClientCloudConfig.class,
                            "cloudConfig", clientNetworkConfig);
                }

            }
            clientNetworkConfig.addPropertyValue("addresses", members);

            configBuilder.addPropertyValue("networkConfig", clientNetworkConfig.getBeanDefinition());
        }

        private void handleAliasedDiscoveryStrategy(Node node, BeanDefinitionBuilder builder, String name) {
            AliasedDiscoveryConfig config = ClientAliasedDiscoveryConfigUtils.newAliasedDiscoveryConfig(name);
            fillAttributesForAliasedDiscoveryStrategy(config, node, builder, name);
        }

        private void handleSSLConfig(Node node, BeanDefinitionBuilder networkConfigBuilder) {
            BeanDefinitionBuilder sslConfigBuilder = createBeanBuilder(SSLConfig.class);
            String implAttribute = "factory-implementation";
            fillAttributeValues(node, sslConfigBuilder, implAttribute);
            Node implNode = node.getAttributes().getNamedItem(implAttribute);
            String implementation = implNode != null ? getTextContent(implNode) : null;
            if (implementation != null) {
                sslConfigBuilder.addPropertyReference(xmlToJavaName(implAttribute), implementation);
            }
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    handleProperties(child, sslConfigBuilder);
                }
            }
            networkConfigBuilder.addPropertyValue("SSLConfig", sslConfigBuilder.getBeanDefinition());
        }

        private void handleLoadBalancer(Node node) {
            String type = getAttribute(node, "type");
            if ("random".equals(type)) {
                configBuilder.addPropertyValue("loadBalancer", new RandomLB());
            } else if ("round-robin".equals(type)) {
                configBuilder.addPropertyValue("loadBalancer", new RoundRobinLB());
            }
        }

        private void handleNearCache(Node node) {
            BeanDefinitionBuilder nearCacheConfigBuilder = createBeanBuilder(NearCacheConfig.class);
            fillAttributeValues(node, nearCacheConfigBuilder);
            for (Node childNode : childElements(node)) {
                String nodeName = cleanNodeName(childNode);
                if ("eviction".equals(nodeName)) {
                    handleEvictionConfig(childNode, nearCacheConfigBuilder);
                } else if ("preloader".equals(nodeName)) {
                    handlePreloaderConfig(childNode, nearCacheConfigBuilder);
                }
            }
            String name = getAttribute(node, "name");
            nearCacheConfigMap.put(name, nearCacheConfigBuilder.getBeanDefinition());
        }

        private void handleFlakeIdGenerator(Node node) {
            BeanDefinitionBuilder configBuilder = createBeanBuilder(ClientFlakeIdGeneratorConfig.class);
            fillAttributeValues(node, configBuilder);
            String name = getAttribute(node, "name");
            flakeIdGeneratorConfigMap.put(name, configBuilder.getBeanDefinition());
        }

        private void handleReliableTopic(Node node) {
            BeanDefinitionBuilder configBuilder = createBeanBuilder(ClientReliableTopicConfig.class);
            String name = getAttribute(node, "name");
            fillValues(node, configBuilder);
            reliableTopicConfigMap.put(name, configBuilder.getBeanDefinition());
        }

        private void handleEvictionConfig(Node node, BeanDefinitionBuilder configBuilder) {
            configBuilder.addPropertyValue("evictionConfig", getEvictionConfig(node));
        }

        private void handlePreloaderConfig(Node node, BeanDefinitionBuilder configBuilder) {
            configBuilder.addPropertyValue("preloaderConfig", getPreloaderConfig(node));
        }

        private ManagedMap getQueryCaches(Node childNode) {
            ManagedMap<String, ManagedMap<String, BeanDefinition>> queryCaches
                    = new ManagedMap<String, ManagedMap<String, BeanDefinition>>();
            for (Node queryCacheNode : childElements(childNode)) {
                parseQueryCache(queryCaches, queryCacheNode);
            }
            return queryCaches;
        }

        private void parseQueryCache(ManagedMap<String, ManagedMap<String, BeanDefinition>> queryCaches, Node queryCacheNode) {
            BeanDefinitionBuilder builder = createBeanBuilder(QueryCacheConfig.class);

            NamedNodeMap attributes = queryCacheNode.getAttributes();
            String mapName = getTextContent(attributes.getNamedItem("map-name"));
            String cacheName = getTextContent(attributes.getNamedItem("name"));

            for (Node node : childElements(queryCacheNode)) {
                String nodeName = cleanNodeName(node);
                String textContent = getTextContent(node);

                parseQueryCacheInternal(builder, node, nodeName, textContent);
            }

            builder.addPropertyValue("name", cacheName);

            ManagedMap<String, BeanDefinition> configMap = queryCaches.get(mapName);
            if (configMap == null) {
                configMap = new ManagedMap<String, BeanDefinition>();
                queryCaches.put(mapName, configMap);
            }
            configMap.put(cacheName, builder.getBeanDefinition());
        }

        private void parseQueryCacheInternal(BeanDefinitionBuilder builder, Node node, String nodeName, String textContent) {
            if ("predicate".equals(nodeName)) {
                BeanDefinitionBuilder predicateBuilder = getPredicate(node, textContent);
                builder.addPropertyValue("predicateConfig", predicateBuilder.getBeanDefinition());
            } else if ("entry-listeners".equals(nodeName)) {
                ManagedList listeners = getEntryListeners(node);
                builder.addPropertyValue("entryListenerConfigs", listeners);
            } else if ("include-value".equals(nodeName)) {
                boolean includeValue = getBooleanValue(textContent);
                builder.addPropertyValue("includeValue", includeValue);
            } else if ("batch-size".equals(nodeName)) {
                int batchSize = getIntegerValue("batch-size", textContent.trim());
                builder.addPropertyValue("batchSize", batchSize);
            } else if ("buffer-size".equals(nodeName)) {
                int bufferSize = getIntegerValue("buffer-size", textContent.trim());
                builder.addPropertyValue("bufferSize", bufferSize);
            } else if ("delay-seconds".equals(nodeName)) {
                int delaySeconds = getIntegerValue("delay-seconds", textContent.trim()
                );
                builder.addPropertyValue("delaySeconds", delaySeconds);
            } else if ("in-memory-format".equals(nodeName)) {
                String value = textContent.trim();
                builder.addPropertyValue("inMemoryFormat", InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("coalesce".equals(nodeName)) {
                boolean coalesce = getBooleanValue(textContent);
                builder.addPropertyValue("coalesce", coalesce);
            } else if ("populate".equals(nodeName)) {
                boolean populate = getBooleanValue(textContent);
                builder.addPropertyValue("populate", populate);
            } else if ("indexes".equals(nodeName)) {
                ManagedList indexes = getIndexes(node);
                builder.addPropertyValue("indexConfigs", indexes);
            } else if ("eviction".equals(nodeName)) {
                builder.addPropertyValue("evictionConfig", getEvictionConfig(node));
            }
        }

        private BeanDefinitionBuilder getPredicate(Node node, String textContent) {
            BeanDefinitionBuilder predicateBuilder = createBeanBuilder(PredicateConfig.class);
            String predicateType = getTextContent(node.getAttributes().getNamedItem("type"));
            if ("sql".equals(predicateType)) {
                predicateBuilder.addPropertyValue("sql", textContent);
            } else if ("class-name".equals(predicateType)) {
                predicateBuilder.addPropertyValue("className", textContent);
            }
            return predicateBuilder;
        }

        private ManagedList<BeanDefinition> getIndexes(Node node) {
            ManagedList<BeanDefinition> indexes = new ManagedList<BeanDefinition>();
            for (Node indexNode : childElements(node)) {
                BeanDefinitionBuilder indexConfBuilder = createBeanBuilder(MapIndexConfig.class);
                fillAttributeValues(indexNode, indexConfBuilder);
                indexes.add(indexConfBuilder.getBeanDefinition());
            }
            return indexes;
        }

        private ManagedList<BeanDefinition> getEntryListeners(Node node) {
            ManagedList<BeanDefinition> listeners = new ManagedList<BeanDefinition>();
            String implementationAttr = "implementation";
            for (Node listenerNode : childElements(node)) {
                BeanDefinitionBuilder listenerConfBuilder = createBeanBuilder(EntryListenerConfig.class);
                fillAttributeValues(listenerNode, listenerConfBuilder, implementationAttr);
                Node implementationNode = listenerNode.getAttributes().getNamedItem(implementationAttr);
                if (implementationNode != null) {
                    listenerConfBuilder.addPropertyReference(implementationAttr, getTextContent(implementationNode));
                }
                listeners.add(listenerConfBuilder.getBeanDefinition());
            }
            return listeners;
        }

        private void handleOutboundPorts(Node node, BeanDefinitionBuilder networkConfigBuilder) {
            ManagedList<String> outboundPorts = new ManagedList<String>();
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("ports".equals(name)) {
                    String value = getTextContent(child);
                    outboundPorts.add(value);
                }
            }
            networkConfigBuilder.addPropertyValue("outboundPortDefinitions", outboundPorts);
        }
    }


}
